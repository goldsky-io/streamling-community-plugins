//! Deferred-ack bookkeeping tying etl `AsyncResult` acks to streamling
//! checkpoint epochs.
//!
//! Every envelope row delivered downstream advances `emitted`. A unit
//! (one etl `write_events`/`write_table_rows` call) is *armed* when its last
//! row has been delivered. `marker(epoch)` snapshots `emitted`;
//! `finalize(epoch)` releases every unit armed at-or-before that snapshot.
//! Units never released before a covering checkpoint finalizes ⇒ etl never
//! advances `confirmed_flush_lsn` past streamling's durable checkpoint.

use etl::destination::async_result::AsyncResult;
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};

#[derive(Debug)]
pub struct AckLedger<A> {
    /// Rows delivered downstream so far.
    emitted: u64,
    /// Units whose rows are fully delivered, in arming order: (armed_at, ack).
    pending: VecDeque<(u64, A)>,
    /// epoch → `emitted` at marker time.
    markers: BTreeMap<u64, u64>,
}

impl<A> AckLedger<A> {
    pub fn new() -> Self {
        Self {
            emitted: 0,
            pending: VecDeque::new(),
            markers: BTreeMap::new(),
        }
    }

    /// Records `n` rows delivered downstream via generate_batch.
    pub fn rows_delivered(&mut self, n: u64) {
        self.emitted += n;
    }

    /// Arms a unit whose final row was just delivered.
    pub fn unit_completed(&mut self, ack: A) {
        self.pending.push_back((self.emitted, ack));
    }

    /// Snapshots the delivery position for a checkpoint epoch.
    pub fn marker(&mut self, epoch: u64) {
        self.markers.insert(epoch, self.emitted);
    }

    /// Releases all acks armed at-or-before `epoch`'s marker. Unknown epoch
    /// releases nothing. Markers at-or-before `epoch` are discarded.
    pub fn finalize(&mut self, epoch: u64) -> Vec<A> {
        let Some(boundary) = self.markers.get(&epoch).copied() else {
            return Vec::new();
        };
        // Keep markers strictly after `epoch`; earlier ones are subsumed
        // (their boundaries are <= this one).
        self.markers = self.markers.split_off(&(epoch + 1));

        let mut released = Vec::new();
        while let Some((armed_at, _)) = self.pending.front() {
            if *armed_at > boundary {
                break;
            }
            released.push(self.pending.pop_front().expect("front checked").1);
        }
        released
    }
}

/// A monotonic identifier for a registered source within a shared group.
pub type SourceId = u64;

/// The terminal etl ack, abstracted so [`SharedAck`] is unit-testable without
/// an `AsyncResult` (which has no public constructor).
pub trait FinalAck: Send {
    fn send_ok(self);
}

impl FinalAck for AsyncResult<()> {
    fn send_ok(self) {
        self.send(Ok(()));
    }
}

/// One etl write's ack, shared across the subscribers it fed. The real etl
/// ack fires only after every contributing subscriber has released its slice
/// (i.e. durably checkpointed past it), so a shared slot's confirmed_flush_lsn
/// never advances past the slowest subscriber. Constructed with an empty
/// subscriber set, it fires immediately.
pub struct SharedAck<A: FinalAck = AsyncResult<()>> {
    inner: Mutex<SharedAckInner<A>>,
}

struct SharedAckInner<A: FinalAck> {
    etl_ack: Option<A>,
    pending: HashSet<SourceId>,
}

impl<A: FinalAck> SharedAck<A> {
    pub fn new(etl_ack: A, subscribers: HashSet<SourceId>) -> Arc<Self> {
        let shared = Arc::new(Self {
            inner: Mutex::new(SharedAckInner {
                etl_ack: Some(etl_ack),
                pending: subscribers,
            }),
        });
        Self::fire_if_done(&mut shared.inner.lock().expect("SharedAck poisoned"));
        shared
    }

    /// Marks `source`'s slice durable; fires the etl ack when none remain.
    pub fn release(&self, source: SourceId) {
        let mut inner = self.inner.lock().expect("SharedAck poisoned");
        inner.pending.remove(&source);
        Self::fire_if_done(&mut inner);
    }

    fn fire_if_done(inner: &mut SharedAckInner<A>) {
        if inner.pending.is_empty()
            && let Some(ack) = inner.etl_ack.take()
        {
            ack.send_ok();
        }
    }
}

/// What a source's `AckLedger` stores per unit: a shared ack and which
/// subscriber slice this unit belongs to.
pub type SourceAckHandle = (Arc<SharedAck>, SourceId);

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn finalize_releases_only_units_armed_before_marker() {
        let mut l = AckLedger::new();
        l.rows_delivered(3);
        l.unit_completed(1u32); // armed at 3
        l.marker(10);
        l.rows_delivered(2);
        l.unit_completed(2u32); // armed at 5, after marker 10

        assert_eq!(l.finalize(10), vec![1]);
        // Unit 2 still pending until a later epoch covers it.
        l.marker(11);
        assert_eq!(l.finalize(11), vec![2]);
    }

    #[test]
    fn unit_split_across_batches_is_armed_at_last_row() {
        let mut l = AckLedger::new();
        // Unit of 4 rows: 2 delivered before marker, 2 after.
        l.rows_delivered(2);
        l.marker(1);
        l.rows_delivered(2);
        l.unit_completed(7u32); // armed at 4 > marker(1) boundary 2

        assert_eq!(l.finalize(1), Vec::<u32>::new());
        l.marker(2);
        assert_eq!(l.finalize(2), vec![7]);
    }

    #[test]
    fn finalize_skipped_epoch_subsumes_earlier_markers() {
        let mut l = AckLedger::new();
        l.rows_delivered(1);
        l.unit_completed(1u32);
        l.marker(1);
        l.rows_delivered(1);
        l.unit_completed(2u32);
        l.marker(2);

        // Finalizing epoch 2 directly releases both units and drops marker 1.
        assert_eq!(l.finalize(2), vec![1, 2]);
        assert_eq!(l.finalize(1), Vec::<u32>::new());
    }

    #[test]
    fn finalize_unknown_epoch_releases_nothing() {
        let mut l = AckLedger::new();
        l.rows_delivered(1);
        l.unit_completed(1u32);
        assert_eq!(l.finalize(99), Vec::<u32>::new());
    }

    #[test]
    fn zero_row_units_release_on_next_finalize() {
        // write_table_rows is called even for empty tables; the unit arms
        // immediately at the current position.
        let mut l = AckLedger::new();
        l.unit_completed(5u32); // armed at 0
        l.marker(1);
        assert_eq!(l.finalize(1), vec![5]);
    }

    struct TestAck(std::sync::Arc<AtomicUsize>);
    impl FinalAck for TestAck {
        fn send_ok(self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn shared_ack_fires_only_after_all_subscribers_release() {
        let fired = std::sync::Arc::new(AtomicUsize::new(0));
        let ack = SharedAck::new(TestAck(fired.clone()), HashSet::from([1u64, 2u64]));
        ack.release(1);
        assert_eq!(fired.load(Ordering::SeqCst), 0, "not all released yet");
        ack.release(2);
        assert_eq!(fired.load(Ordering::SeqCst), 1, "fires once all released");
    }

    #[test]
    fn shared_ack_with_no_subscribers_fires_immediately() {
        let fired = std::sync::Arc::new(AtomicUsize::new(0));
        let _ack = SharedAck::new(TestAck(fired.clone()), HashSet::new());
        assert_eq!(fired.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn shared_ack_releasing_unknown_or_repeat_source_is_safe() {
        let fired = std::sync::Arc::new(AtomicUsize::new(0));
        let ack = SharedAck::new(TestAck(fired.clone()), HashSet::from([1u64]));
        ack.release(99); // unknown
        assert_eq!(fired.load(Ordering::SeqCst), 0);
        ack.release(1);
        ack.release(1); // repeat after fire
        assert_eq!(fired.load(Ordering::SeqCst), 1, "fires exactly once");
    }
}

//! Background LookupEvents poll task and its pure windowing/dedupe logic.
//!
//! The poller tails CloudTrail with sliding time windows: each cycle scans
//! `[watermark − lookback, min(start + max_window, now))`, filters events it
//! has already emitted (LookupEvents has no cursor, so the lookback overlap
//! re-fetches recent history to catch late-delivered events), sorts the new
//! ones ascending by event time, and queues them for `generate_batch`. A
//! fully-scanned capped window advances the watermark to its end even when
//! empty, so backfills progress through quiet periods; capped cycles loop
//! immediately (catch-up) while uncapped ones sleep `poll_interval`.

use crate::sources::cloudtrail::config::{CloudTrailSourceConfig, OnMalformed, StartTime};
use crate::sources::cloudtrail::convert::CloudTrailRecord;
use aws_sdk_cloudtrail::Client;
use aws_sdk_cloudtrail::error::{DisplayErrorContext, ProvideErrorMetadata};
use aws_sdk_cloudtrail::primitives::DateTime;
use aws_sdk_cloudtrail::types::LookupAttribute;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use streamling_plugin::ffi::PluginMetricsRecorder;
use tokio::sync::mpsc;
use tracing::warn;

/// LookupEvents' maximum page size.
const LOOKUP_PAGE_SIZE: i32 = 50;
/// Self-throttle after every request to half of the 2 TPS LookupEvents
/// budget, which is shared with the console's Event history page.
const INTER_REQUEST_DELAY: Duration = Duration::from_secs(1);
/// Backoff bounds for a failed poll cycle.
const ERROR_BACKOFF_MIN: Duration = Duration::from_secs(1);
const ERROR_BACKOFF_MAX: Duration = Duration::from_secs(30);
/// LookupEvents' retention: management events from the last 90 days.
/// `earliest` starts here, and a checkpoint older than this resumes past a
/// gap that can no longer be read.
pub(crate) const LOOKUP_RETENTION_MS: i64 = 90 * 24 * 3600 * 1000;

fn secs_to_ms(secs: u64) -> i64 {
    i64::try_from(secs).unwrap_or(i64::MAX).saturating_mul(1000)
}

pub(crate) fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before 1970")
        .as_millis() as i64
}

/// Initial `(watermark, floor)` from the persisted checkpoint and
/// `start_time`. The floor keeps the lookback from reaching before the
/// configured start; a checkpoint always wins over `start_time` for the
/// watermark itself.
pub(crate) fn resolve_start(
    checkpoint_ms: Option<i64>,
    start_time: StartTime,
    now_ms: i64,
) -> (i64, i64) {
    let floor = match start_time {
        StartTime::At(t) => t,
        StartTime::Now | StartTime::Earliest => 0,
    };
    match checkpoint_ms {
        Some(watermark) => (watermark, floor),
        None => match start_time {
            StartTime::Now => (now_ms, now_ms),
            // Starting at epoch 0 would walk decades of empty capped windows
            // before reaching retained data; start at the retention horizon.
            StartTime::Earliest => {
                let horizon = (now_ms - LOOKUP_RETENTION_MS).max(0);
                (horizon, horizon)
            }
            StartTime::At(t) => (t, t),
        },
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Window {
    pub start_ms: i64,
    /// None → uncapped: the window reaches "now", so EndTime is omitted from
    /// the request and clock skew cannot cut off fresh events.
    pub end_ms: Option<i64>,
}

pub(crate) fn plan_window(
    watermark_ms: i64,
    floor_ms: i64,
    lookback_ms: i64,
    max_window_ms: i64,
    now_ms: i64,
) -> Window {
    let start_ms = watermark_ms
        .saturating_sub(lookback_ms)
        .max(floor_ms)
        .max(0);
    let end_ms = start_ms.saturating_add(max_window_ms);
    Window {
        start_ms,
        end_ms: (end_ms < now_ms).then_some(end_ms),
    }
}

/// The next watermark after a fully-paginated window: the max of the current
/// value, the newest fetched event, and — for capped windows — the window
/// end (an empty backfill slice must still advance, or it loops forever).
/// Greedy: this feeds window planning only, never the checkpoint.
pub(crate) fn advance_watermark(
    current_ms: i64,
    max_fetched_ms: Option<i64>,
    capped_end_ms: Option<i64>,
) -> i64 {
    [max_fetched_ms, capped_end_ms]
        .into_iter()
        .flatten()
        .fold(current_ms, i64::max)
}

/// Event ids already emitted, each with its event time so ids sliding out of
/// the poll window can be evicted. Bounded by API physics: LookupEvents
/// serves ≤ ~100 events/s, so the set holds at most
/// ~(lookback + delivery lag) × 100 entries. Not persisted — a restart
/// re-emits the lookback window (documented at-least-once behavior).
#[derive(Debug, Default)]
pub(crate) struct Dedupe {
    seen: HashMap<String, i64>,
}

impl Dedupe {
    pub fn contains(&self, event_id: &str) -> bool {
        self.seen.contains_key(event_id)
    }

    pub fn insert(&mut self, event_id: String, event_time_ms: i64) {
        self.seen.insert(event_id, event_time_ms);
    }

    /// Safe because window starts are monotonic: an evicted id can only
    /// reappear if the window start regressed, which it never does.
    pub fn evict_before(&mut self, cutoff_ms: i64) {
        self.seen.retain(|_, time| *time >= cutoff_ms);
    }
}

/// Sorts ascending by event time and splits into `batch_size` chunks.
/// Load-bearing: LookupEvents returns newest-first, and the FIFO
/// channel + carry means "max delivered event time" is only a true
/// low-watermark for in-flight events if delivery order is ascending —
/// checkpoint restart-safety depends on it.
pub(crate) fn sort_into_batches(
    mut records: Vec<CloudTrailRecord>,
    batch_size: usize,
) -> Vec<Vec<CloudTrailRecord>> {
    records.sort_by_key(|record| record.event_time_ms);
    let mut batches = Vec::with_capacity(records.len().div_ceil(batch_size.max(1)));
    while records.len() > batch_size {
        let rest = records.split_off(batch_size);
        batches.push(std::mem::replace(&mut records, rest));
    }
    if !records.is_empty() {
        batches.push(records);
    }
    batches
}

pub(crate) struct Poller {
    pub client: Client,
    pub config: Arc<CloudTrailSourceConfig>,
    pub lookup_attribute: Option<LookupAttribute>,
    pub tx: mpsc::Sender<Vec<CloudTrailRecord>>,
    pub metrics: PluginMetricsRecorder,
    pub watermark_ms: i64,
    pub floor_ms: i64,
}

/// New records and (for `on_malformed=skip`) the skipped ids from one fully
/// paginated window. Nothing is committed to the dedupe set until the whole
/// window succeeds: a partial fetch is dropped and re-planned, and marking
/// its events as seen would turn that retry into data loss.
struct FetchedWindow {
    records: Vec<CloudTrailRecord>,
    skipped: Vec<(String, i64)>,
}

impl Poller {
    pub async fn run(self) {
        let lookback_ms = secs_to_ms(self.config.lookback_secs);
        let max_window_ms = secs_to_ms(self.config.max_window_secs);
        let poll_interval = Duration::from_secs(self.config.poll_interval_secs);
        let mut watermark_ms = self.watermark_ms;
        let mut dedupe = Dedupe::default();
        let mut error_backoff = ERROR_BACKOFF_MIN;

        loop {
            let window = plan_window(
                watermark_ms,
                self.floor_ms,
                lookback_ms,
                max_window_ms,
                now_ms(),
            );
            dedupe.evict_before(window.start_ms);

            match self.fetch_window(&window, &dedupe).await {
                Ok(fetched) => {
                    error_backoff = ERROR_BACKOFF_MIN;
                    for record in &fetched.records {
                        dedupe.insert(record.event_id.clone(), record.event_time_ms);
                    }
                    for (event_id, event_time_ms) in fetched.skipped {
                        dedupe.insert(event_id, event_time_ms);
                    }
                    let max_fetched_ms = fetched.records.iter().map(|r| r.event_time_ms).max();
                    for chunk in sort_into_batches(fetched.records, self.config.batch_size) {
                        if self.tx.send(chunk).await.is_err() {
                            return; // source terminated
                        }
                    }
                    watermark_ms = advance_watermark(watermark_ms, max_fetched_ms, window.end_ms);
                    self.metrics.record_gauge(
                        "cloudtrail_source.watermark_lag_ms",
                        (now_ms() - watermark_ms).max(0) as u64,
                    );
                    if window.end_ms.is_none() {
                        tokio::time::sleep(poll_interval).await;
                    }
                    // Capped → behind: loop immediately to catch up.
                }
                Err(e) => {
                    warn!(
                        window_start_ms = window.start_ms,
                        error = %e,
                        "cloudtrail_source: poll cycle failed; backing off and re-planning"
                    );
                    tokio::time::sleep(error_backoff).await;
                    error_backoff = (error_backoff * 2).min(ERROR_BACKOFF_MAX);
                }
            }
        }
    }

    async fn fetch_window(
        &self,
        window: &Window,
        dedupe: &Dedupe,
    ) -> Result<FetchedWindow, String> {
        let mut fetched = FetchedWindow {
            records: Vec::new(),
            skipped: Vec::new(),
        };
        // Intra-window duplicates (an event straddling a page boundary
        // re-fetch) are possible on top of cross-cycle ones.
        let mut window_seen: HashSet<String> = HashSet::new();
        let mut next_token: Option<String> = None;

        loop {
            let mut request = self
                .client
                .lookup_events()
                .start_time(DateTime::from_millis(window.start_ms))
                .max_results(LOOKUP_PAGE_SIZE)
                .set_next_token(next_token);
            if let Some(end_ms) = window.end_ms {
                request = request.end_time(DateTime::from_millis(end_ms));
            }
            if let Some(attribute) = &self.lookup_attribute {
                request = request.lookup_attributes(attribute.clone());
            }

            self.metrics
                .record_count("cloudtrail_source.api_requests", 1);
            let page = request.send().await.map_err(|e| {
                if e.code() == Some("ThrottlingException") {
                    self.metrics
                        .record_count("cloudtrail_source.throttled_requests", 1);
                }
                format!("LookupEvents request failed: {}", DisplayErrorContext(&e))
            })?;

            self.metrics
                .record_count("cloudtrail_source.events_read", page.events().len() as u64);
            for event in page.events() {
                match CloudTrailRecord::from_sdk_event(event) {
                    Ok(record) => {
                        if dedupe.contains(&record.event_id)
                            || !window_seen.insert(record.event_id.clone())
                        {
                            self.metrics
                                .record_count("cloudtrail_source.duplicates_filtered", 1);
                        } else {
                            fetched.records.push(record);
                        }
                    }
                    Err(e) => match self.config.on_malformed {
                        OnMalformed::Error => {
                            return Err(format!(
                                "malformed event (id {:?}): {e}; the source stalls rather \
                                 than losing data — set on_malformed=skip to drop such events",
                                event.event_id()
                            ));
                        }
                        OnMalformed::Skip => {
                            let already_skipped =
                                event.event_id().is_some_and(|id| dedupe.contains(id));
                            if !already_skipped {
                                warn!(
                                    event_id = ?event.event_id(),
                                    error = %e,
                                    "cloudtrail_source: skipping malformed event"
                                );
                                self.metrics
                                    .record_count("cloudtrail_source.malformed_skipped", 1);
                            }
                            // Remember the id (when present) so the lookback
                            // overlap does not re-warn every cycle.
                            if let Some(id) = event.event_id() {
                                let time_ms = event
                                    .event_time()
                                    .and_then(|t| t.to_millis().ok())
                                    .unwrap_or(window.start_ms);
                                fetched.skipped.push((id.to_string(), time_ms));
                            }
                        }
                    },
                }
            }

            next_token = page.next_token().map(str::to_string);
            tokio::time::sleep(INTER_REQUEST_DELAY).await;
            if next_token.is_none() {
                return Ok(fetched);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECS: i64 = 1000;
    const NOW: i64 = 1_700_000_000_000;

    #[test]
    fn resolve_start_follows_the_start_table() {
        // Virgin: start_time picks both watermark and floor.
        assert_eq!(resolve_start(None, StartTime::Now, NOW), (NOW, NOW));
        assert_eq!(
            resolve_start(None, StartTime::Earliest, NOW),
            (NOW - LOOKUP_RETENTION_MS, NOW - LOOKUP_RETENTION_MS)
        );
        assert_eq!(resolve_start(None, StartTime::At(42), NOW), (42, 42));
        // Restored: the checkpoint wins; only an explicit timestamp floors.
        assert_eq!(resolve_start(Some(500), StartTime::Now, NOW), (500, 0));
        assert_eq!(resolve_start(Some(500), StartTime::Earliest, NOW), (500, 0));
        assert_eq!(resolve_start(Some(500), StartTime::At(42), NOW), (500, 42));
    }

    #[test]
    fn plan_window_is_uncapped_near_now() {
        let window = plan_window(NOW - 10 * SECS, 0, 900 * SECS, 3600 * SECS, NOW);
        assert_eq!(window.start_ms, NOW - 910 * SECS);
        assert_eq!(window.end_ms, None);
    }

    #[test]
    fn plan_window_caps_backlog() {
        let watermark = NOW - 86_400 * SECS;
        let window = plan_window(watermark, 0, 900 * SECS, 3600 * SECS, NOW);
        assert_eq!(window.start_ms, watermark - 900 * SECS);
        assert_eq!(window.end_ms, Some(watermark - 900 * SECS + 3600 * SECS));
    }

    #[test]
    fn plan_window_clamps_to_floor_on_virgin_start() {
        // A virgin `start_time=now` start has floor == watermark: the
        // lookback must not reach into pre-start history.
        let window = plan_window(NOW, NOW, 900 * SECS, 3600 * SECS, NOW);
        assert_eq!(window.start_ms, NOW);
        assert_eq!(window.end_ms, None);

        // Earliest backfill: watermark and floor both start at the retention
        // horizon, so the first window begins there, capped.
        let horizon = NOW - LOOKUP_RETENTION_MS;
        let window = plan_window(horizon, horizon, 900 * SECS, 3600 * SECS, NOW);
        assert_eq!(window.start_ms, horizon);
        assert_eq!(window.end_ms, Some(horizon + 3600 * SECS));
    }

    #[test]
    fn plan_window_applies_lookback_after_progress() {
        // Once the watermark has moved past floor + lookback, the floor is
        // inert and each window starts lookback behind the watermark.
        let window = plan_window(
            NOW - 5 * SECS,
            NOW - 7200 * SECS,
            900 * SECS,
            3600 * SECS,
            NOW,
        );
        assert_eq!(window.start_ms, NOW - 905 * SECS);
        assert_eq!(window.end_ms, None);
    }

    #[test]
    fn advance_watermark_uses_capped_end_for_empty_windows() {
        // Empty capped window: advance to the window end.
        assert_eq!(advance_watermark(100, None, Some(500)), 500);
        // Empty uncapped window: hold position.
        assert_eq!(advance_watermark(100, None, None), 100);
        // Events past the cap cannot exist (server-side EndTime), but the
        // max keeps the invariant obvious.
        assert_eq!(advance_watermark(100, Some(700), Some(500)), 700);
        // Never regress.
        assert_eq!(advance_watermark(1000, Some(700), Some(500)), 1000);
    }

    #[test]
    fn dedupe_filters_repeats_and_evicts_before_cutoff() {
        let mut dedupe = Dedupe::default();
        assert!(!dedupe.contains("a"));
        dedupe.insert("a".to_string(), 100);
        dedupe.insert("b".to_string(), 200);
        assert!(dedupe.contains("a"));
        assert!(dedupe.contains("b"));

        dedupe.evict_before(150);
        assert!(!dedupe.contains("a"));
        assert!(dedupe.contains("b"), "entries at/after the cutoff survive");
    }

    #[test]
    fn evicted_ids_count_as_new_again() {
        let mut dedupe = Dedupe::default();
        dedupe.insert("a".to_string(), 100);
        dedupe.evict_before(200);
        assert!(!dedupe.contains("a"));
        dedupe.insert("a".to_string(), 300);
        assert!(dedupe.contains("a"));
    }

    fn record(event_id: &str, event_time_ms: i64) -> CloudTrailRecord {
        CloudTrailRecord {
            event_id: event_id.to_string(),
            event_time_ms,
            ..CloudTrailRecord::default()
        }
    }

    #[test]
    fn collected_events_are_sorted_and_chunked_ascending() {
        // LookupEvents returns newest-first.
        let records = vec![
            record("e", 50),
            record("d", 40),
            record("c", 30),
            record("b", 20),
            record("a", 10),
        ];
        let batches = sort_into_batches(records, 2);
        let times: Vec<Vec<i64>> = batches
            .iter()
            .map(|batch| batch.iter().map(|r| r.event_time_ms).collect())
            .collect();
        assert_eq!(times, vec![vec![10, 20], vec![30, 40], vec![50]]);

        assert!(sort_into_batches(Vec::new(), 2).is_empty());
    }
}

//! Per-stream background readers over long-lived S2 read sessions.
//!
//! Each active stream gets one task that tails the stream via
//! `S2Stream::read_session` (the SDK resumes the session transparently on
//! retryable errors; the task reopens it with backoff otherwise) and pushes
//! record batches into a bounded channel — when the channel is full the task
//! stops pulling from S2, giving natural backpressure. With `stream_prefix`,
//! a refresh task periodically re-lists streams and starts/stops readers to
//! match.

use crate::sources::s2::config::{S2SourceConfig, StartPosition};
use crate::sources::s2::convert::SourceRecord;
use futures::{StreamExt, TryStreamExt};
use s2_sdk::types::{ListAllStreamsInput, ReadFrom, ReadInput, ReadStart, StreamName};
use s2_sdk::{S2Basin, S2Stream};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;
use streamling_plugin::ffi::PluginMetricsRecorder;
use streamling_plugin::{PluginError, PluginStateBackend};
use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinHandle;
use tracing::{info, warn};

/// Reopen backoff bounds for a read session the SDK gave up on.
const REOPEN_DELAY_MIN: Duration = Duration::from_secs(1);
const REOPEN_DELAY_MAX: Duration = Duration::from_secs(30);

pub(crate) struct StreamReaders {
    basin: S2Basin,
    config: Arc<S2SourceConfig>,
    state: Arc<PluginStateBackend<u64>>,
    tx: mpsc::Sender<Vec<SourceRecord>>,
    metrics: PluginMetricsRecorder,
    /// The configured exact streams; a prefix refresh never stops these.
    exact_streams: BTreeSet<String>,
    tasks: Mutex<HashMap<String, JoinHandle<()>>>,
    /// Next sequence number to be *emitted* per stream — advanced by the
    /// source as batches are delivered; snapshotted for checkpoints. Contains
    /// exactly the streams with a running reader task.
    positions: Mutex<BTreeMap<String, u64>>,
}

impl StreamReaders {
    pub fn new(
        basin: S2Basin,
        config: Arc<S2SourceConfig>,
        state: Arc<PluginStateBackend<u64>>,
        tx: mpsc::Sender<Vec<SourceRecord>>,
        metrics: PluginMetricsRecorder,
    ) -> Self {
        Self {
            basin,
            exact_streams: config.streams.iter().map(ToString::to_string).collect(),
            config,
            state,
            tx,
            metrics,
            tasks: Mutex::new(HashMap::new()),
            positions: Mutex::new(BTreeMap::new()),
        }
    }

    /// Starts readers for the configured streams; with a prefix, also runs an
    /// initial listing (so errors fail initialization) and returns the
    /// periodic refresh task.
    pub async fn start(self: &Arc<Self>) -> Result<Option<JoinHandle<()>>, PluginError> {
        for name in self.config.streams.clone() {
            self.ensure_stream(&name, self.config.start_position)
                .await?;
        }
        if self.config.stream_prefix.is_none() {
            return Ok(None);
        }
        self.refresh_streams(self.config.start_position).await?;

        let readers = self.clone();
        let refresh_interval = Duration::from_secs(self.config.update_streams_interval_secs);
        Ok(Some(tokio::spawn(async move {
            let mut interval = tokio::time::interval(refresh_interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            interval.tick().await; // immediate first tick; start() already refreshed
            loop {
                interval.tick().await;
                // A stream discovered after startup is entirely new, so it is
                // read from the beginning regardless of `start_position` —
                // starting at its tail would skip records appended between
                // its creation and this refresh tick.
                if let Err(e) = readers.refresh_streams(StartPosition::Earliest).await {
                    warn!(error = %e, "s2_source: failed to refresh stream list");
                }
            }
        })))
    }

    /// Lists streams matching the prefix, starts readers for new ones and
    /// stops readers for streams that no longer exist.
    async fn refresh_streams(&self, fallback: StartPosition) -> Result<(), PluginError> {
        let prefix = self
            .config
            .stream_prefix
            .clone()
            .expect("refresh_streams requires a stream_prefix");
        let names: Vec<StreamName> = self
            .basin
            .list_all_streams(ListAllStreamsInput::new().with_prefix(prefix))
            .map(|info| info.map(|info| info.name))
            .try_collect()
            .await
            .map_err(|e| {
                PluginError::Internal(format!("s2_source: failed to list streams: {e}"))
            })?;

        for name in &names {
            self.ensure_stream(name, fallback).await?;
        }

        let listed: BTreeSet<String> = names.iter().map(ToString::to_string).collect();
        let removed: Vec<String> = {
            let mut tasks = self.tasks.lock().await;
            let removed: Vec<String> = tasks
                .keys()
                .filter(|name| !listed.contains(*name) && !self.exact_streams.contains(*name))
                .cloned()
                .collect();
            for name in &removed {
                if let Some(task) = tasks.remove(name) {
                    task.abort();
                }
            }
            if !removed.is_empty() {
                self.metrics
                    .record_gauge("s2_source.open_readers", tasks.len() as u64);
            }
            removed
        };
        if !removed.is_empty() {
            let mut positions = self.positions.lock().await;
            for name in &removed {
                positions.remove(name);
            }
            drop(positions);
            for name in &removed {
                // The stream is gone from S2; drop its checkpoint so a
                // recreated stream (sequence numbers restart at 0) is not
                // resumed from the old incarnation's position.
                if let Err(e) = self.state.remove_kv(name).await {
                    warn!(stream = %name, error = %e, "s2_source: failed to remove checkpoint for deleted stream");
                }
                info!(stream = %name, "s2_source: stopped reading deleted stream");
            }
        }
        Ok(())
    }

    /// Starts a reader for the stream unless one is already running, resuming
    /// from the persisted checkpoint, then `fallback`.
    async fn ensure_stream(
        &self,
        name: &StreamName,
        fallback: StartPosition,
    ) -> Result<(), PluginError> {
        let key = name.to_string();
        if self.tasks.lock().await.contains_key(&key) {
            return Ok(());
        }

        let stream = self.basin.stream(name.clone());
        let next_seq_num = match self.state.get_kv(&key).await.map_err(PluginError::State)? {
            Some(seq_num) => seq_num,
            None => match fallback {
                StartPosition::Earliest => 0,
                StartPosition::Latest => {
                    stream
                        .check_tail()
                        .await
                        .map_err(|e| {
                            PluginError::Internal(format!(
                                "s2_source: failed to check tail of stream '{key}': {e}"
                            ))
                        })?
                        .seq_num
                }
            },
        };

        let mut tasks = self.tasks.lock().await;
        self.positions
            .lock()
            .await
            .insert(key.clone(), next_seq_num);
        let task = tokio::spawn(read_stream(
            stream,
            Arc::from(key.as_str()),
            next_seq_num,
            self.tx.clone(),
            self.metrics.clone(),
        ));
        tasks.insert(key.clone(), task);
        self.metrics
            .record_gauge("s2_source.open_readers", tasks.len() as u64);
        info!(stream = %key, next_seq_num, "s2_source: started stream reader");
        Ok(())
    }

    /// Advances per-stream positions after a batch has been delivered.
    /// Positions only move forward, and only for streams that still have a
    /// reader: records buffered from a since-pruned (or pruned-and-re-added)
    /// stream must not resurrect or regress its position.
    pub async fn record_emitted(&self, updates: &BTreeMap<String, u64>) {
        let mut positions = self.positions.lock().await;
        for (stream, next_seq_num) in updates {
            if let Some(position) = positions.get_mut(stream) {
                *position = (*position).max(*next_seq_num);
            }
        }
    }

    pub async fn snapshot_positions(&self) -> BTreeMap<String, u64> {
        self.positions.lock().await.clone()
    }

    pub async fn shutdown(&self) {
        for (_, task) in self.tasks.lock().await.drain() {
            task.abort();
        }
    }
}

/// Tails one stream forever, pushing record batches into the channel.
/// Returns only when the channel is closed (source terminated) or the task
/// is aborted.
async fn read_stream(
    stream: S2Stream,
    stream_name: Arc<str>,
    mut next_seq_num: u64,
    tx: mpsc::Sender<Vec<SourceRecord>>,
    metrics: PluginMetricsRecorder,
) {
    let mut reopen_delay = REOPEN_DELAY_MIN;
    loop {
        let input = ReadInput::new()
            .with_start(
                ReadStart::new()
                    .with_from(ReadFrom::SeqNum(next_seq_num))
                    .with_clamp_to_tail(true),
            )
            // Command records (fence/trim) are S2-internal control records.
            .with_ignore_command_records(true);

        match stream.read_session(input).await {
            Ok(mut session) => {
                while let Some(result) = session.next().await {
                    match result {
                        Ok(batch) => {
                            let Some(last) = batch.records.last() else {
                                continue;
                            };
                            next_seq_num = last.seq_num.saturating_add(1);
                            reopen_delay = REOPEN_DELAY_MIN;
                            metrics
                                .record_count("s2_source.records_read", batch.records.len() as u64);
                            let records = batch
                                .records
                                .into_iter()
                                .map(|record| SourceRecord {
                                    stream: stream_name.clone(),
                                    seq_num: record.seq_num,
                                    timestamp: record.timestamp,
                                    headers: record
                                        .headers
                                        .into_iter()
                                        .map(|header| (header.name, header.value))
                                        .collect(),
                                    body: record.body,
                                })
                                .collect();
                            if tx.send(records).await.is_err() {
                                return;
                            }
                        }
                        Err(e) => {
                            warn!(
                                stream = %stream_name,
                                error = %e,
                                "s2_source: read session failed; reopening"
                            );
                            break;
                        }
                    }
                }
            }
            Err(e) => {
                warn!(
                    stream = %stream_name,
                    error = %e,
                    "s2_source: failed to open read session; retrying"
                );
            }
        }
        metrics.record_count("s2_source.read_session_reopens", 1);
        tokio::time::sleep(reopen_delay).await;
        reopen_delay = (reopen_delay * 2).min(REOPEN_DELAY_MAX);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::plugin_options::PluginOptions;
    use crate::utils::test_support;
    use s2_sdk::S2;
    use s2_sdk::types::S2Config;

    fn test_readers() -> Arc<StreamReaders> {
        let opts = PluginOptions::for_test(
            "s2_source",
            "STREAMLING__PLUGIN__S2_SOURCE_READER_TEST",
            &[("basin", "test-basin"), ("streams", "events")],
        );
        let config = Arc::new(crate::sources::s2::config::parse_config(&opts).unwrap());
        let s2 = S2::new(S2Config::new("token")).unwrap();
        let (tx, _rx) = mpsc::channel(1);
        Arc::new(StreamReaders::new(
            s2.basin(config.basin.clone()),
            config,
            test_support::state_backend_factory("test_s2_source").create(),
            tx,
            test_support::metrics_recorder(),
        ))
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn record_emitted_is_monotonic_and_ignores_untracked_streams() {
        let readers = test_readers();
        readers.positions.lock().await.insert("a".to_string(), 10);

        // Advances forward.
        readers
            .record_emitted(&BTreeMap::from([("a".to_string(), 20)]))
            .await;
        assert_eq!(readers.snapshot_positions().await["a"], 20);

        // A stale lower update (buffered rows from a replaced reader) must
        // not regress the position.
        readers
            .record_emitted(&BTreeMap::from([("a".to_string(), 15)]))
            .await;
        assert_eq!(readers.snapshot_positions().await["a"], 20);

        // A pruned (untracked) stream must not be resurrected.
        readers
            .record_emitted(&BTreeMap::from([("gone".to_string(), 5)]))
            .await;
        assert!(!readers.snapshot_positions().await.contains_key("gone"));
    }
}

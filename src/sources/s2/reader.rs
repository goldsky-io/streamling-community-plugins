//! Per-stream background readers over long-lived S2 read sessions.
//!
//! Each active stream gets one task that tails the stream via
//! `S2Stream::read_session` (the SDK resumes the session transparently on
//! retryable errors) and pushes record batches into a bounded channel — when
//! the channel is full the task stops pulling from S2, giving natural
//! backpressure. With `stream_prefix`, a refresh task periodically re-lists
//! streams and starts/stops readers to match.

use crate::sources::s2::config::{S2SourceConfig, StartPosition};
use crate::sources::s2::convert::SourceRecord;
use futures::{StreamExt, TryStreamExt};
use s2_sdk::types::{ListAllStreamsInput, ReadFrom, ReadInput, ReadStart, StreamName};
use s2_sdk::{S2Basin, S2Stream};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;
use streamling_plugin::{PluginError, PluginStateBackend};
use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinHandle;
use tracing::{info, warn};

/// Delay before reopening a read session the SDK gave up on.
const REOPEN_DELAY: Duration = Duration::from_secs(5);

pub(crate) struct StreamReaders {
    basin: S2Basin,
    config: Arc<S2SourceConfig>,
    state: Arc<PluginStateBackend<u64>>,
    tx: mpsc::Sender<Vec<SourceRecord>>,
    /// Streams configured explicitly; never pruned by prefix refresh.
    exact_streams: BTreeSet<String>,
    tasks: Mutex<HashMap<String, JoinHandle<()>>>,
    /// Next sequence number to be *emitted* per stream — advanced by the
    /// source as batches are generated; snapshotted for checkpoints.
    positions: Mutex<BTreeMap<String, u64>>,
}

impl StreamReaders {
    pub fn new(
        basin: S2Basin,
        config: Arc<S2SourceConfig>,
        state: Arc<PluginStateBackend<u64>>,
        tx: mpsc::Sender<Vec<SourceRecord>>,
    ) -> Self {
        let exact_streams = config.streams.iter().map(ToString::to_string).collect();
        Self {
            basin,
            config,
            state,
            tx,
            exact_streams,
            tasks: Mutex::new(HashMap::new()),
            positions: Mutex::new(BTreeMap::new()),
        }
    }

    /// Starts readers for the configured streams; with a prefix, also runs an
    /// initial listing (so errors fail initialization) and returns the
    /// periodic refresh task.
    pub async fn start(self: &Arc<Self>) -> Result<Option<JoinHandle<()>>, PluginError> {
        for name in self.config.streams.clone() {
            self.ensure_stream(&name).await?;
        }
        if self.config.stream_prefix.is_none() {
            return Ok(None);
        }
        self.refresh_streams().await?;

        let readers = self.clone();
        let refresh_interval = Duration::from_secs(self.config.update_streams_interval_secs.max(1));
        Ok(Some(tokio::spawn(async move {
            let mut interval = tokio::time::interval(refresh_interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            interval.tick().await; // immediate first tick; start() already refreshed
            loop {
                interval.tick().await;
                if let Err(e) = readers.refresh_streams().await {
                    warn!(error = %e, "s2_source: failed to refresh stream list");
                }
            }
        })))
    }

    /// Lists streams matching the prefix, starts readers for new ones and
    /// stops readers for streams that no longer exist.
    async fn refresh_streams(&self) -> Result<(), PluginError> {
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
            self.ensure_stream(name).await?;
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
            removed
        };
        if !removed.is_empty() {
            let mut positions = self.positions.lock().await;
            for name in &removed {
                positions.remove(name);
                info!(stream = %name, "s2_source: stopped reading deleted stream");
            }
        }
        Ok(())
    }

    /// Starts a reader for the stream unless one is already running, resuming
    /// from the in-memory position, then the persisted checkpoint, then
    /// `start_position`.
    async fn ensure_stream(&self, name: &StreamName) -> Result<(), PluginError> {
        let key = name.to_string();
        if self.tasks.lock().await.contains_key(&key) {
            return Ok(());
        }

        let stream = self.basin.stream(name.clone());
        let next_seq_num = match self.positions.lock().await.get(&key).copied() {
            Some(seq_num) => seq_num,
            None => match self.state.get_kv(&key).await.map_err(PluginError::State)? {
                Some(seq_num) => seq_num,
                None => match self.config.start_position {
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
            },
        };

        let mut tasks = self.tasks.lock().await;
        if tasks.contains_key(&key) {
            return Ok(());
        }
        self.positions
            .lock()
            .await
            .insert(key.clone(), next_seq_num);
        let task = tokio::spawn(read_stream(
            self.config.clone(),
            stream,
            Arc::from(key.as_str()),
            next_seq_num,
            self.tx.clone(),
        ));
        tasks.insert(key.clone(), task);
        info!(stream = %key, next_seq_num, "s2_source: started stream reader");
        Ok(())
    }

    /// Advances per-stream positions after a batch has been emitted.
    pub async fn record_emitted(&self, updates: &BTreeMap<String, u64>) {
        let mut positions = self.positions.lock().await;
        for (stream, next_seq_num) in updates {
            positions.insert(stream.clone(), *next_seq_num);
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
    config: Arc<S2SourceConfig>,
    stream: S2Stream,
    stream_name: Arc<str>,
    mut next_seq_num: u64,
    tx: mpsc::Sender<Vec<SourceRecord>>,
) {
    loop {
        let input = ReadInput::new()
            .with_start(
                ReadStart::new()
                    .with_from(ReadFrom::SeqNum(next_seq_num))
                    .with_clamp_to_tail(true),
            )
            .with_ignore_command_records(config.ignore_command_records);

        match stream.read_session(input).await {
            Ok(mut session) => {
                while let Some(result) = session.next().await {
                    match result {
                        Ok(batch) => {
                            let Some(last) = batch.records.last() else {
                                continue;
                            };
                            next_seq_num = last.seq_num.saturating_add(1);
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
        tokio::time::sleep(REOPEN_DELAY).await;
    }
}

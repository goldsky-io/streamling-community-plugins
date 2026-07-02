//! Process-global registry that lets `postgres_cdc_source` instances sharing a
//! `slot_name` share one etl `Pipeline` + replication slot. Keyed by
//! `slot_name`; each group is one `SharedPipeline` owning the etl pipeline and
//! fanning decoded rows out to per-table subscriber channels.
//!
//! Registration happens in each source's `new()`. Because streamling
//! constructs every source before running any `initialize()`, the group's
//! membership is complete before the first `ensure_started()` builds the
//! fan-out destination and starts the one etl pipeline.

use crate::postgres_cdc::bridge::{ChannelDestination, Subscriber, Unit};
use crate::postgres_cdc::config::GroupIdentity;
use crate::postgres_cdc::ledger::SourceId;
use etl::config::PipelineConfig;
use etl::pipeline::Pipeline;
use etl::store::PostgresStore;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use tokio::sync::mpsc;
use tracing::{error, info};

static REGISTRY: OnceLock<Mutex<HashMap<String, Arc<SharedPipeline>>>> = OnceLock::new();

fn registry() -> &'static Mutex<HashMap<String, Arc<SharedPipeline>>> {
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

/// A source's handle into its group: its id, its row channel, and the shared
/// pipeline it belongs to.
pub struct Subscription {
    pub source_id: SourceId,
    pub rx: mpsc::Receiver<Unit>,
    pub shared: Arc<SharedPipeline>,
}

impl std::fmt::Debug for Subscription {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Subscription")
            .field("source_id", &self.source_id)
            .finish_non_exhaustive()
    }
}

/// A table registered into a group before the pipeline starts.
struct PendingSub {
    source_id: SourceId,
    table_label: String,
    data_columns: Vec<String>,
    tx: mpsc::Sender<Unit>,
}

pub struct SharedPipeline {
    slot_name: String,
    identity: GroupIdentity,
    /// Config from the first registrant; used to start the one etl pipeline.
    pipeline_config: Mutex<Option<PipelineConfig>>,
    inner: Mutex<Inner>,
    running: Arc<AtomicBool>,
}

struct Inner {
    next_source_id: SourceId,
    refcount: usize,
    subs: Vec<PendingSub>,
    started: bool,
    shutdown: Option<etl::concurrency::ShutdownTx>,
}

/// Registers `table_label` into the group named `slot_name`, creating the
/// group on first call. Validates that an existing group has matching
/// `identity`; rejects a duplicate table. Returns the source's subscription.
pub fn register(
    slot_name: &str,
    pipeline: PipelineConfig,
    identity: GroupIdentity,
    table_label: String,
    data_columns: Vec<String>,
    capacity: usize,
) -> Result<Subscription, String> {
    let shared = {
        let mut map = registry().lock().expect("registry poisoned");
        match map.get(slot_name) {
            Some(existing) => {
                if existing.identity != identity {
                    return Err(format!(
                        "postgres_cdc_source: slot_name '{slot_name}' is shared by sources with \
                         different connection/publication; all must match"
                    ));
                }
                existing.clone()
            }
            None => {
                let sp = Arc::new(SharedPipeline {
                    slot_name: slot_name.to_string(),
                    identity,
                    pipeline_config: Mutex::new(Some(pipeline)),
                    inner: Mutex::new(Inner {
                        next_source_id: 0,
                        refcount: 0,
                        subs: Vec::new(),
                        started: false,
                        shutdown: None,
                    }),
                    running: Arc::new(AtomicBool::new(true)),
                });
                map.insert(slot_name.to_string(), sp.clone());
                sp
            }
        }
    };

    let mut inner = shared.inner.lock().expect("shared inner poisoned");
    if inner.subs.iter().any(|s| s.table_label == table_label) {
        return Err(format!(
            "postgres_cdc_source: table '{table_label}' is already registered in slot_name \
             '{slot_name}'"
        ));
    }
    let source_id = inner.next_source_id;
    inner.next_source_id += 1;
    inner.refcount += 1;
    let (tx, rx) = mpsc::channel(capacity);
    inner.subs.push(PendingSub {
        source_id,
        table_label,
        data_columns,
        tx,
    });
    drop(inner);

    Ok(Subscription {
        source_id,
        rx,
        shared,
    })
}

impl SharedPipeline {
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Acquire)
    }

    /// Starts the one etl pipeline for this group (idempotent; first caller
    /// wins). Builds the fan-out destination from all registered subscribers.
    pub async fn ensure_started(&self) -> Result<(), String> {
        let subscribers = {
            let mut inner = self.inner.lock().expect("shared inner poisoned");
            if inner.started {
                return Ok(());
            }
            inner.started = true;
            inner
                .subs
                .iter()
                .map(|s| Subscriber {
                    source_id: s.source_id,
                    table_label: s.table_label.clone(),
                    converter: crate::postgres_cdc::bridge::RowConverter::new(
                        s.table_label.clone(),
                        &s.data_columns,
                    ),
                    tx: s.tx.clone(),
                })
                .collect::<Vec<_>>()
        };

        let pipeline_config = self
            .pipeline_config
            .lock()
            .expect("pipeline_config poisoned")
            .take()
            .ok_or_else(|| "shared pipeline already consumed its config".to_string())?;

        let store_conn = pipeline_config
            .store_pg_connection
            .clone()
            .unwrap_or_else(|| pipeline_config.pg_connection.clone());
        let store = PostgresStore::new(pipeline_config.id, store_conn)
            .await
            .map_err(|e| format!("etl store init: {e}"))?;
        let destination = ChannelDestination::new(subscribers);

        info!(
            slot_name = %self.slot_name,
            pipeline_id = pipeline_config.id,
            publication = %pipeline_config.publication_name,
            "postgres_cdc_source: starting shared etl pipeline"
        );

        let mut pipeline = Pipeline::new(pipeline_config, store, destination);
        pipeline
            .start()
            .await
            .map_err(|e| format!("etl pipeline start: {e}"))?;
        {
            let mut inner = self.inner.lock().expect("shared inner poisoned");
            inner.shutdown = Some(pipeline.shutdown_tx());
        }

        let running = self.running.clone();
        tokio::spawn(async move {
            match pipeline.wait().await {
                Ok(()) => info!("postgres_cdc_source: shared etl pipeline completed"),
                Err(e) => error!(error = %e, "postgres_cdc_source: shared etl pipeline failed"),
            }
            running.store(false, Ordering::Release);
        });
        Ok(())
    }

    /// Decrements the group refcount; the last source out shuts the etl
    /// pipeline down and removes the registry entry.
    pub fn deregister(&self, _source_id: SourceId) {
        let last = {
            let mut inner = self.inner.lock().expect("shared inner poisoned");
            inner.refcount = inner.refcount.saturating_sub(1);
            inner.refcount == 0
        };
        if last {
            let mut inner = self.inner.lock().expect("shared inner poisoned");
            if let Some(shutdown) = inner.shutdown.take() {
                let _ = shutdown.shutdown();
            }
            registry()
                .lock()
                .expect("registry poisoned")
                .remove(&self.slot_name);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use etl::config::{
        BatchConfig, InvalidatedSlotBehavior, MemoryBackpressureConfig, PgConnectionConfig,
        TableSyncCopyConfig, TcpKeepaliveConfig, TlsConfig,
    };

    fn pipeline_config(publication: &str) -> PipelineConfig {
        PipelineConfig {
            id: 1,
            publication_name: publication.to_string(),
            pg_connection: PgConnectionConfig {
                host: "h".into(),
                hostaddr: None,
                port: 5432,
                name: "db".into(),
                username: "u".into(),
                password: None,
                tls: TlsConfig::disabled(),
                keepalive: TcpKeepaliveConfig::default(),
            },
            store_pg_connection: None,
            batch: BatchConfig {
                max_fill_ms: 1000,
                memory_budget_ratio: 0.2,
                max_bytes: 1024,
            },
            table_error_retry_delay_ms: 1,
            table_error_retry_max_attempts: 1,
            max_table_sync_workers: 1,
            memory_refresh_interval_ms: 100,
            memory_backpressure: Some(MemoryBackpressureConfig::default()),
            table_sync_copy: TableSyncCopyConfig::default(),
            invalidated_slot_behavior: InvalidatedSlotBehavior::default(),
            max_copy_connections_per_table: PipelineConfig::DEFAULT_MAX_COPY_CONNECTIONS_PER_TABLE,
        }
    }

    fn identity(publication: &str) -> GroupIdentity {
        GroupIdentity {
            host: "h".into(),
            port: 5432,
            database: "db".into(),
            username: "u".into(),
            publication_name: publication.to_string(),
        }
    }

    #[test]
    fn second_table_shares_the_same_pipeline_and_gets_distinct_id() {
        let g = "grp_share";
        let s1 = register(
            g,
            pipeline_config("p"),
            identity("p"),
            "public.a".into(),
            vec!["id".into()],
            4,
        )
        .unwrap();
        let s2 = register(
            g,
            pipeline_config("p"),
            identity("p"),
            "public.b".into(),
            vec!["id".into()],
            4,
        )
        .unwrap();
        assert_ne!(s1.source_id, s2.source_id);
        assert!(Arc::ptr_eq(&s1.shared, &s2.shared));
        s1.shared.deregister(s1.source_id);
        s2.shared.deregister(s2.source_id);
    }

    #[test]
    fn duplicate_table_in_a_group_is_rejected() {
        let g = "grp_dup";
        let _s1 = register(
            g,
            pipeline_config("p"),
            identity("p"),
            "public.a".into(),
            vec!["id".into()],
            4,
        )
        .unwrap();
        let err = register(
            g,
            pipeline_config("p"),
            identity("p"),
            "public.a".into(),
            vec!["id".into()],
            4,
        )
        .unwrap_err();
        assert!(err.contains("already registered"));
    }

    #[test]
    fn identity_mismatch_under_same_slot_name_is_rejected() {
        let g = "grp_mismatch";
        let _s1 = register(
            g,
            pipeline_config("p"),
            identity("p"),
            "public.a".into(),
            vec!["id".into()],
            4,
        )
        .unwrap();
        let err = register(
            g,
            pipeline_config("q"),
            identity("q"),
            "public.b".into(),
            vec!["id".into()],
            4,
        )
        .unwrap_err();
        assert!(err.contains("must match"));
    }

    #[test]
    fn last_deregister_removes_the_group() {
        let g = "grp_refcount";
        let s1 = register(
            g,
            pipeline_config("p"),
            identity("p"),
            "public.a".into(),
            vec!["id".into()],
            4,
        )
        .unwrap();
        assert!(registry().lock().unwrap().contains_key(g));
        s1.shared.deregister(s1.source_id);
        assert!(!registry().lock().unwrap().contains_key(g));
    }
}

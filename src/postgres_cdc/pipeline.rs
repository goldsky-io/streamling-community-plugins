//! Process-global registry that lets `postgres_cdc_source` instances sharing a
//! `slot_name` share one etl `Pipeline` + replication slot.
//!
//! Keyed by `slot_name`; each group is one [`SharedPipeline`] owning the etl
//! pipeline and fanning decoded rows out to per-table subscriber channels.
//! Registration happens in each source's `new()`. Because streamling
//! constructs every source before running any `initialize()`, the group's
//! membership is complete before the first [`SharedPipeline::ensure_started`]
//! builds the fan-out destination and starts the one etl pipeline.
//!
//! ## Metadata store
//! etl persists its replication state (table schemas, per-table sync progress,
//! slot state) via its built-in [`PostgresStore`]. By default that store is the
//! same database as the source (`pg_connection`); override with the `store_*`
//! options to keep this bookkeeping out of the source database. etl also
//! installs an `etl` schema (helper functions + a DDL trigger) in the source
//! database on pipeline start, regardless of the store setting.

use crate::postgres_cdc::bridge::{ChannelDestination, Subscriber, WriteUnit};
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
    pub rx: mpsc::Receiver<WriteUnit>,
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
    emit_update_before_row: bool,
    tx: mpsc::Sender<WriteUnit>,
}

pub struct SharedPipeline {
    slot_name: String,
    identity: GroupIdentity,
    /// Config from the first registrant; used to start the one etl pipeline.
    pipeline_config: Mutex<Option<PipelineConfig>>,
    /// Whether to auto-create the publication / add missing tables before
    /// starting. Taken from the first registrant (the publication is shared by
    /// the whole group, so all sources implicitly agree on it).
    auto_create_publication: bool,
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

/// The table a source contributes to its group, and how its rows are shaped.
pub struct TableSpec {
    /// "schema.table" this source replicates.
    pub label: String,
    /// Output data-column names in schema order (without `_gs_op`).
    pub data_columns: Vec<String>,
    /// Precede each update's new image with a `d` row carrying the old image.
    pub emit_update_before_row: bool,
}

/// Registers `table.label` into the group named `slot_name`, creating the
/// group on first call. Validates that an existing group has matching
/// `identity`; rejects a duplicate table. Returns the source's subscription.
pub fn register(
    slot_name: &str,
    pipeline: PipelineConfig,
    identity: GroupIdentity,
    table: TableSpec,
    capacity: usize,
    auto_create_publication: bool,
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
                    auto_create_publication,
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
    if inner.subs.iter().any(|s| s.table_label == table.label) {
        return Err(format!(
            "postgres_cdc_source: table '{}' is already registered in slot_name '{slot_name}'",
            table.label
        ));
    }
    let source_id = inner.next_source_id;
    inner.next_source_id += 1;
    inner.refcount += 1;
    let (tx, rx) = mpsc::channel(capacity);
    inner.subs.push(PendingSub {
        source_id,
        table_label: table.label,
        data_columns: table.data_columns,
        emit_update_before_row: table.emit_update_before_row,
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
                        s.emit_update_before_row,
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

        // The publication must exist and include every replicated table before
        // etl validates it in `Pipeline::start`; create it (or add missing
        // tables) when auto_create_publication is set. This touches the SOURCE
        // database — publications live there regardless of the store setting.
        if self.auto_create_publication {
            let tables: Vec<String> = subscribers.iter().map(|s| s.table_label.clone()).collect();
            ensure_publication(
                &pipeline_config.pg_connection,
                &pipeline_config.publication_name,
                &tables,
            )
            .await?;
        }
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
/// Ensures the Postgres publication `publication_name` exists and contains
/// every table in `tables` (each a "schema.name" label). Creates the
/// publication if missing, or `ALTER ... ADD TABLE`s any registered tables not
/// yet in it. A no-op for a `FOR ALL TABLES` publication.
///
/// Runs against the SOURCE database (where publications live), using the
/// source connection. Requires a role with `CREATE` on the database (to create
/// a publication) and/or ownership of the tables (to add them); a plain
/// replication role usually lacks these and should manage the publication
/// manually instead.
async fn ensure_publication(
    conn: &etl::config::PgConnectionConfig,
    publication_name: &str,
    tables: &[String],
) -> Result<(), String> {
    use sqlx::postgres::PgPoolOptions;
    use std::time::Duration;

    let opts = crate::postgres_cdc::discovery::pg_connect_options(conn);
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_secs(10))
        .connect_with(opts)
        .await
        .map_err(|e| format!("postgres_cdc_source: auto-publication connect: {e}"))?;

    // `puballtables` is NULL when the publication does not exist.
    let puballtables: Option<bool> =
        sqlx::query_scalar("SELECT puballtables FROM pg_publication WHERE pubname = $1")
            .bind(publication_name)
            .fetch_optional(&pool)
            .await
            .map_err(|e| format!("postgres_cdc_source: auto-publication read: {e}"))?;

    match puballtables {
        None => {
            let mut sql = format!(
                "CREATE PUBLICATION {} FOR TABLE",
                quote_ident(publication_name)
            );
            for (i, label) in tables.iter().enumerate() {
                if i > 0 {
                    sql.push(',');
                }
                sql.push(' ');
                sql.push_str(&quote_table_label(label));
            }
            sqlx::query(&sql).execute(&pool).await.map_err(|e| {
                format!(
                    "postgres_cdc_source: auto-publication create failed (the role needs \
                         CREATE privilege on the database, or create the publication manually): \
                         {e}"
                )
            })?;
            info!(
                publication = publication_name,
                "postgres_cdc_source: created publication"
            );
        }
        Some(true) => {
            // `FOR ALL TABLES` already covers every table; adding is an error.
        }
        Some(false) => {
            let have: Vec<String> = sqlx::query_scalar(
                "SELECT schemaname::text || '.' || tablename::text \
                 FROM pg_publication_tables WHERE pubname = $1",
            )
            .bind(publication_name)
            .fetch_all(&pool)
            .await
            .map_err(|e| format!("postgres_cdc_source: auto-publication read tables: {e}"))?;
            let missing: Vec<&String> = tables
                .iter()
                .filter(|t| !have.iter().any(|h| h == *t))
                .collect();
            if missing.is_empty() {
                pool.close().await;
                return Ok(());
            }
            let mut sql = format!(
                "ALTER PUBLICATION {} ADD TABLE",
                quote_ident(publication_name)
            );
            for (i, label) in missing.iter().enumerate() {
                if i > 0 {
                    sql.push(',');
                }
                sql.push(' ');
                sql.push_str(&quote_table_label(label));
            }
            sqlx::query(&sql).execute(&pool).await.map_err(|e| {
                format!(
                    "postgres_cdc_source: auto-publication add-table failed (the role needs \
                         ownership of the tables, or add them manually): {e}"
                )
            })?;
            info!(
                publication = publication_name,
                added = missing.len(),
                "postgres_cdc_source: added tables to publication"
            );
        }
    }
    pool.close().await;
    Ok(())
}

/// Quotes a SQL identifier (publication or table name), doubling embedded
/// double quotes per the PostgreSQL identifier rules.
fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Quotes a "schema.name" label as two separate identifiers.
fn quote_table_label(label: &str) -> String {
    let (schema, name) = label.split_once('.').unwrap_or((label, ""));
    format!("{}.{}", quote_ident(schema), quote_ident(name))
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

    fn table_spec(label: &str) -> TableSpec {
        TableSpec {
            label: label.to_string(),
            data_columns: vec!["id".into()],
            emit_update_before_row: false,
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
            table_spec("public.a"),
            4,
            false,
        )
        .unwrap();
        let s2 = register(
            g,
            pipeline_config("p"),
            identity("p"),
            table_spec("public.b"),
            4,
            false,
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
            table_spec("public.a"),
            4,
            false,
        )
        .unwrap();
        let err = register(
            g,
            pipeline_config("p"),
            identity("p"),
            table_spec("public.a"),
            4,
            false,
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
            table_spec("public.a"),
            4,
            false,
        )
        .unwrap();
        let err = register(
            g,
            pipeline_config("q"),
            identity("q"),
            table_spec("public.b"),
            4,
            false,
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
            table_spec("public.a"),
            4,
            false,
        )
        .unwrap();
        assert!(registry().lock().unwrap().contains_key(g));
        s1.shared.deregister(s1.source_id);
        assert!(!registry().lock().unwrap().contains_key(g));
    }

    #[test]
    fn quote_helpers_quote_identifiers_safely() {
        // Plain identifier wrapped in double quotes.
        assert_eq!(quote_ident("my_pub"), r#""my_pub""#);
        // Embedded double quote is doubled (SQL identifier escaping).
        assert_eq!(quote_ident(r#"a"b"#), r#""a""b""#);
        // schema.name is split into two separately-quoted identifiers.
        assert_eq!(quote_table_label("public.users"), r#""public"."users""#);
        // A quoted table name containing a dot survives correctly.
        assert_eq!(
            quote_table_label(r#"public.odd"name"#),
            r#""public"."odd""name""#
        );
    }
}

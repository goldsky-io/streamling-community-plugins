//! E2e test for postgres_cdc_source.
//!
//! Gated on `E2E_POSTGRES_CDC_URL` pointing at a Postgres with
//! `wal_level=logical` and a superuser/replication role. Self-skips when
//! unset. Each run uses a unique table/publication/slot_name so reruns
//! don't collide on replication slots.

use abi_stable::external_types::crossbeam_channel;
use arrow::array::{Array, Int64Array, StringArray};
use community_plugins::postgres_cdc::PostgresCdcSource;
use sqlx::postgres::PgPoolOptions;
use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use streamling_plugin::PluginStateBackendConfig;
use streamling_plugin::api::PluginStateBackendFactory;
use streamling_plugin::api::{CheckpointEpoch, SourcePlugin, SupportsGracefulShutdown};
use streamling_plugin::r#async::DirectTokioProxy;
use streamling_plugin::ffi::PluginMetricsRecorder;

fn test_state_backend() -> PluginStateBackendFactory {
    PluginStateBackendFactory::new(PluginStateBackendConfig::new(
        "test_app".to_string(),
        "test_postgres_cdc_source".to_string(),
        r#"{"backend_type": "InMemory"}"#.to_string(),
    ))
}

fn test_metrics() -> PluginMetricsRecorder {
    let (sender, _receiver) = crossbeam_channel::bounded(1);
    PluginMetricsRecorder::new(sender)
}

/// One observed row: (_gs_op, id, name). The output is the table's own
/// columns plus `_gs_op` — no CDC envelope.
type ObservedRow = (String, i64, Option<String>);

fn collect_rows(batch: &arrow::record_batch::RecordBatch) -> Vec<ObservedRow> {
    let gs_op = batch
        .column_by_name("_gs_op")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .clone();
    let id = batch
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .clone();
    let name = batch
        .column_by_name("name")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .clone();
    (0..batch.num_rows())
        .map(|i| {
            (
                gs_op.value(i).to_string(),
                id.value(i),
                (!name.is_null(i)).then(|| name.value(i).to_string()),
            )
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn copy_then_stream_insert_update_delete() {
    let Ok(url) = std::env::var("E2E_POSTGRES_CDC_URL") else {
        eprintln!("skipping: E2E_POSTGRES_CDC_URL not set");
        return;
    };

    let parsed = url::Url::parse(&url).expect("valid E2E_POSTGRES_CDC_URL");
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let table = format!("cdc_e2e_{nonce}");
    let publication = format!("cdc_e2e_pub_{nonce}");
    let pipeline_id = nonce % 1_000_000;

    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&url)
        .await
        .expect("connect to e2e postgres");

    sqlx::query(&format!(
        "CREATE TABLE {table} (id BIGINT PRIMARY KEY, name TEXT)"
    ))
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(&format!("ALTER TABLE {table} REPLICA IDENTITY FULL"))
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(&format!(
        "CREATE PUBLICATION {publication} FOR TABLE {table}"
    ))
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(&format!(
        "INSERT INTO {table} VALUES (1, 'seed-a'), (2, 'seed-b')"
    ))
    .execute(&pool)
    .await
    .unwrap();

    let mut options: HashMap<String, String> = HashMap::new();
    options.insert("host".into(), parsed.host_str().unwrap().to_string());
    options.insert("port".into(), parsed.port().unwrap_or(5432).to_string());
    options.insert(
        "database".into(),
        parsed.path().trim_start_matches('/').to_string(),
    );
    options.insert("username".into(), parsed.username().to_string());
    if let Some(p) = parsed.password() {
        options.insert("password".into(), p.to_string());
    }
    options.insert("publication_name".into(), publication.clone());
    options.insert("table".into(), format!("public.{table}"));
    options.insert("slot_name".into(), format!("e2e_{pipeline_id}"));
    options.insert("batch_interval_ms".into(), "200".into());

    let source = PostgresCdcSource::new(
        DirectTokioProxy::new().into_async_runtime_obj(),
        test_state_backend(),
        test_metrics(),
        options,
    )
    .expect("source construction");

    source.initialize().await.expect("initialize");

    let mut seen: Vec<ObservedRow> = Vec::new();
    let mut epoch = 1u64;

    // Phase 1: initial copy of the 2 seed rows (copy rows arrive as "i").
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    while seen
        .iter()
        .filter(|(gs_op, id, _)| gs_op == "i" && (*id == 1 || *id == 2))
        .count()
        < 2
    {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for copy rows; saw {seen:?}"
        );
        let batch = source.generate_batch().await.expect("generate_batch");
        if batch.num_rows() > 0 {
            seen.extend(collect_rows(&batch));
        }
        // Checkpoint regularly so etl acks flow and copy can progress.
        source
            .process_checkpoint_marker(CheckpointEpoch(epoch))
            .await
            .unwrap();
        source
            .process_checkpoint_finalizer(CheckpointEpoch(epoch))
            .await
            .unwrap();
        epoch += 1;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Phase 2: live insert / update / delete.
    sqlx::query(&format!("INSERT INTO {table} VALUES (3, 'live')"))
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(&format!("UPDATE {table} SET name = 'live2' WHERE id = 3"))
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(&format!("DELETE FROM {table} WHERE id = 3"))
        .execute(&pool)
        .await
        .unwrap();

    // Live events on id=3: insert "i", update "u", delete "d".
    let want = ["i", "u", "d"];
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    while !want
        .iter()
        .all(|w| seen.iter().any(|(gs_op, id, _)| gs_op == w && *id == 3))
    {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for stream events; saw {seen:?}"
        );
        let batch = source.generate_batch().await.expect("generate_batch");
        if batch.num_rows() > 0 {
            seen.extend(collect_rows(&batch));
        }
        source
            .process_checkpoint_marker(CheckpointEpoch(epoch))
            .await
            .unwrap();
        source
            .process_checkpoint_finalizer(CheckpointEpoch(epoch))
            .await
            .unwrap();
        epoch += 1;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Copy rows carry the seed data as typed columns.
    assert!(
        seen.contains(&("i".into(), 1, Some("seed-a".into()))),
        "missing copy row for id=1; saw {seen:?}"
    );
    assert!(
        seen.contains(&("i".into(), 2, Some("seed-b".into()))),
        "missing copy row for id=2; saw {seen:?}"
    );

    // The live insert and update carry the new row image.
    assert!(seen.contains(&("i".into(), 3, Some("live".into()))));
    assert!(seen.contains(&("u".into(), 3, Some("live2".into()))));

    // The delete carries the deleted row's image (REPLICA IDENTITY FULL →
    // full old row) so sinks can key the delete.
    assert!(seen.contains(&("d".into(), 3, Some("live2".into()))));

    source.terminate().await.expect("terminate");

    // Cleanup: etl at the pinned rev does NOT drop the apply replication
    // slot on shutdown, and inactive slots pin WAL on the shared e2e
    // database. Best-effort drop all inactive supabase_etl_% slots,
    // retrying once if a slot is still releasing.
    let drop_slots = "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots \
                      WHERE slot_name LIKE 'supabase_etl_%' AND active = false";
    let count_slots = "SELECT count(*) FROM pg_replication_slots \
                       WHERE slot_name LIKE 'supabase_etl_%'";
    let _ = sqlx::query(drop_slots).execute(&pool).await;
    let remaining: i64 = sqlx::query_scalar(count_slots)
        .fetch_one(&pool)
        .await
        .unwrap_or(0);
    if remaining > 0 {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let _ = sqlx::query(drop_slots).execute(&pool).await;
    }

    let _ = sqlx::query(&format!("DROP PUBLICATION IF EXISTS {publication}"))
        .execute(&pool)
        .await;
    let _ = sqlx::query(&format!("DROP TABLE IF EXISTS {table}"))
        .execute(&pool)
        .await;
}

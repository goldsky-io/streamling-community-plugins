//! E2e: two tables share one etl pipeline + slot via a common `slot_name`.
//! Gated on `E2E_POSTGRES_CDC_URL` (Postgres with `wal_level=logical`).

use abi_stable::external_types::crossbeam_channel;
use arrow::array::{Array, Int64Array};
use community_plugins::postgres_cdc::PostgresCdcSource;
use sqlx::postgres::PgPoolOptions;
use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use streamling_plugin::PluginStateBackendConfig;
use streamling_plugin::api::PluginStateBackendFactory;
use streamling_plugin::api::{CheckpointEpoch, SourcePlugin, SupportsGracefulShutdown};
use streamling_plugin::r#async::DirectTokioProxy;
use streamling_plugin::ffi::PluginMetricsRecorder;

fn backend() -> PluginStateBackendFactory {
    PluginStateBackendFactory::new(PluginStateBackendConfig::new(
        "test_app".into(),
        "test_pg_cdc_shared".into(),
        r#"{"backend_type": "InMemory"}"#.into(),
    ))
}
fn metrics() -> PluginMetricsRecorder {
    let (tx, _rx) = crossbeam_channel::bounded(1);
    PluginMetricsRecorder::new(tx)
}

fn opts(url: &url::Url, table: &str, slot: &str, publication: &str) -> HashMap<String, String> {
    let mut o = HashMap::new();
    o.insert("host".into(), url.host_str().unwrap().into());
    o.insert("port".into(), url.port().unwrap_or(5432).to_string());
    o.insert("database".into(), url.path().trim_start_matches('/').into());
    o.insert("username".into(), url.username().into());
    if let Some(p) = url.password() {
        o.insert("password".into(), p.into());
    }
    o.insert("publication_name".into(), publication.into());
    o.insert("table".into(), table.into());
    o.insert("slot_name".into(), slot.into());
    o.insert("batch_interval_ms".into(), "200".into());
    o
}

/// Drains one source, returning collected (id, name?) pairs from `name` table.
fn ids(batch: &arrow::record_batch::RecordBatch) -> Vec<i64> {
    let id = batch
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .clone();
    (0..batch.num_rows()).map(|i| id.value(i)).collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn two_tables_share_one_slot() {
    let Ok(url_s) = std::env::var("E2E_POSTGRES_CDC_URL") else {
        eprintln!("skipping: E2E_POSTGRES_CDC_URL not set");
        return;
    };
    let url = url::Url::parse(&url_s).unwrap();
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let (ta, tb) = (format!("sa_{nonce}"), format!("sb_{nonce}"));
    let pubname = format!("shared_pub_{nonce}");
    let slot = format!("shared_{nonce}");

    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&url_s)
        .await
        .unwrap();
    for t in [&ta, &tb] {
        sqlx::query(&format!(
            "CREATE TABLE public.{t} (id BIGINT PRIMARY KEY, name TEXT)"
        ))
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(&format!("ALTER TABLE public.{t} REPLICA IDENTITY FULL"))
            .execute(&pool)
            .await
            .unwrap();
    }
    sqlx::query(&format!(
        "CREATE PUBLICATION {pubname} FOR TABLE public.{ta}, public.{tb}"
    ))
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(&format!("INSERT INTO public.{ta} VALUES (1,'a-seed')"))
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(&format!("INSERT INTO public.{tb} VALUES (10,'b-seed')"))
        .execute(&pool)
        .await
        .unwrap();

    let src_a = PostgresCdcSource::new(
        DirectTokioProxy::new().into_async_runtime_obj(),
        backend(),
        metrics(),
        opts(&url, &format!("public.{ta}"), &slot, &pubname),
    )
    .unwrap();
    let src_b = PostgresCdcSource::new(
        DirectTokioProxy::new().into_async_runtime_obj(),
        backend(),
        metrics(),
        opts(&url, &format!("public.{tb}"), &slot, &pubname),
    )
    .unwrap();
    src_a.initialize().await.unwrap();
    src_b.initialize().await.unwrap();

    let mut seen_a = Vec::new();
    let mut seen_b = Vec::new();
    let mut epoch = 1u64;
    sqlx::query(&format!("INSERT INTO public.{ta} VALUES (2,'a-live')"))
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(&format!("INSERT INTO public.{tb} VALUES (20,'b-live')"))
        .execute(&pool)
        .await
        .unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    while !(seen_a.contains(&1)
        && seen_a.contains(&2)
        && seen_b.contains(&10)
        && seen_b.contains(&20))
    {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timeout; a={seen_a:?} b={seen_b:?}"
        );
        let ba = src_a.generate_batch().await.unwrap();
        if ba.num_rows() > 0 {
            seen_a.extend(ids(&ba));
        }
        let bb = src_b.generate_batch().await.unwrap();
        if bb.num_rows() > 0 {
            seen_b.extend(ids(&bb));
        }
        for e in [epoch] {
            src_a
                .process_checkpoint_marker(CheckpointEpoch(e))
                .await
                .unwrap();
            src_b
                .process_checkpoint_marker(CheckpointEpoch(e))
                .await
                .unwrap();
            src_a
                .process_checkpoint_finalizer(CheckpointEpoch(e))
                .await
                .unwrap();
            src_b
                .process_checkpoint_finalizer(CheckpointEpoch(e))
                .await
                .unwrap();
        }
        epoch += 1;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Exactly one apply slot for the shared group.
    let slots: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE 'supabase_etl_apply_%'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        slots, 1,
        "expected exactly one shared apply slot, found {slots}"
    );

    src_a.terminate().await.unwrap();
    src_b.terminate().await.unwrap();
    let _ = sqlx::query(&format!("DROP PUBLICATION IF EXISTS {pubname}"))
        .execute(&pool)
        .await;
    for t in [&ta, &tb] {
        let _ = sqlx::query(&format!("DROP TABLE IF EXISTS public.{t}"))
            .execute(&pool)
            .await;
    }
    let _ = sqlx::query("SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name LIKE 'supabase_etl_apply_%' AND active = false").execute(&pool).await;
}

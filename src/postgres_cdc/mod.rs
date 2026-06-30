//! postgres_cdc_source plugin.
//!
//! Streams Postgres logical-replication changes (initial table copy +
//! continuous CDC) by embedding the supabase/etl pipeline. See
//! docs/plans/2026-06-11-001-feat-postgres-cdc-source-design.md.

mod arrow;
mod bridge;
mod config;
mod discovery;
mod json;
mod ledger;
mod shared;
mod source;

pub use source::PostgresCdcSource;

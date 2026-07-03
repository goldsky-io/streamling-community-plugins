//! postgres_cdc_source plugin.
//!
//! Streams Postgres logical-replication changes (initial table copy +
//! continuous CDC) by embedding the supabase/etl pipeline.

mod arrow;
mod bridge;
mod config;
mod discovery;
mod json;
mod ledger;
mod pipeline;
mod source;

pub use source::PostgresCdcSource;

//! Shared s2-lite fixture for the S2 plugin e2e tests.
#![allow(dead_code)] // each test binary uses its own subset

use s2_sdk::S2;
use s2_sdk::types::{
    AppendInput, AppendRecord, AppendRecordBatch, BasinName, EnsureBasinInput, EnsureStreamInput,
    Header, ReadFrom, ReadInput, ReadLimits, ReadStart, ReadStop, SequencedRecord, StreamName,
};
use s2_testcontainers::S2Lite;
use streamling_e2e::{TestContext, TestContextOptions, init_tracing};

pub struct S2Fixture {
    pub ctx: TestContext,
    pub s2: S2,
    pub basin: BasinName,
    pub endpoint: String,
    _s2_lite: S2Lite,
}

impl S2Fixture {
    pub async fn setup(options: TestContextOptions) -> S2Fixture {
        init_tracing();
        let s2_lite = S2Lite::start().await.expect("failed to start s2-lite");
        let ctx = TestContext::with_options(options)
            .await
            .expect("failed to create test context");
        let s2 = s2_lite.client().expect("failed to construct s2 client");
        let basin = format!("basin-{}", &ctx.test_id[..8])
            .parse::<BasinName>()
            .expect("valid basin name");
        s2.ensure_basin(EnsureBasinInput::new(basin.clone()))
            .await
            .expect("failed to ensure s2-lite basin");
        let endpoint = s2_lite.endpoint().to_string();
        S2Fixture {
            ctx,
            s2,
            basin,
            endpoint,
            _s2_lite: s2_lite,
        }
    }

    pub async fn create_stream(&self, name: &str) -> StreamName {
        let stream = name.parse::<StreamName>().expect("valid stream name");
        self.s2
            .basin(self.basin.clone())
            .ensure_stream(EnsureStreamInput::new(stream.clone()))
            .await
            .expect("failed to ensure s2-lite stream");
        stream
    }

    pub async fn append(&self, stream: &StreamName, bodies: impl IntoIterator<Item = String>) {
        self.append_with_ops(stream, bodies.into_iter().map(|body| (body, None)))
            .await;
    }

    /// Appends records, optionally tagged with a Debezium-style `dbz.op`
    /// header (as the s2_sink writes them).
    pub async fn append_with_ops(
        &self,
        stream: &StreamName,
        records: impl IntoIterator<Item = (String, Option<&str>)>,
    ) {
        let records = records
            .into_iter()
            .map(|(body, op)| {
                let record = AppendRecord::new(body).expect("valid S2 record");
                match op {
                    Some(op) => record
                        .with_headers([Header::new("dbz.op", op.to_string())])
                        .expect("valid S2 record headers"),
                    None => record,
                }
            })
            .collect::<Vec<_>>();
        self.s2
            .basin(self.basin.clone())
            .stream(stream.clone())
            .append(AppendInput::new(
                AppendRecordBatch::try_from_iter(records).expect("valid S2 batch"),
            ))
            .await
            .expect("failed to append S2 records");
    }

    /// Reads up to `count` records from the start of the stream.
    pub async fn read_from_start(&self, stream: &StreamName, count: usize) -> Vec<SequencedRecord> {
        self.s2
            .basin(self.basin.clone())
            .stream(stream.clone())
            .read(
                ReadInput::new()
                    .with_start(ReadStart::new().with_from(ReadFrom::SeqNum(0)))
                    .with_stop(ReadStop::new().with_limits(ReadLimits::new().with_count(count))),
            )
            .await
            .expect("failed to read s2-lite records")
            .records
    }
}

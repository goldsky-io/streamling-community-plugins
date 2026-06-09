pub mod sinks;
pub mod utils;

use crate::sinks::mysql::MySqlSink;
use crate::sinks::s2::sink::S2Sink;
use crate::sinks::s3::S3Sink;
use crate::sinks::sqs::sink::SqsSink;

use streamling_plugin::{init_plugin_with_async_runtime, register_plugin_sink};

register_plugin_sink!("s3_sink", S3Sink);
register_plugin_sink!("mysql_sink", MySqlSink);
register_plugin_sink!("sqs", SqsSink);
register_plugin_sink!("s2_sink", S2Sink);

init_plugin_with_async_runtime!();

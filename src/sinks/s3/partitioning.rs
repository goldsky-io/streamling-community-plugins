use arrow::array::{Array, RecordBatch};
use arrow::compute::take;
use arrow_schema::SchemaRef;
use std::collections::HashMap;
use streamling_plugin::PluginError;

/// Configuration for Hive-style partitioning of S3 output.
#[derive(Debug, Clone)]
pub struct PartitionConfig {
    /// Ordered list of column names to partition by.
    pub columns: Vec<String>,
    /// Column indices in the schema (parallel to `columns`).
    column_indices: Vec<usize>,
}

impl PartitionConfig {
    /// Parse `partition_columns` from plugin options and validate against the schema.
    /// Returns `None` if `partition_columns` is not set.
    pub fn from_options(
        options: &HashMap<String, String>,
        schema: &SchemaRef,
    ) -> Result<Option<Self>, PluginError> {
        let raw = match options.get("partition_columns") {
            Some(v) if !v.trim().is_empty() => v,
            _ => return Ok(None),
        };

        let columns: Vec<String> = raw
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        if columns.is_empty() {
            return Ok(None);
        }

        // Reject duplicate column names
        let mut seen = std::collections::HashSet::new();
        for col in &columns {
            if !seen.insert(col.as_str()) {
                return Err(PluginError::Internal(format!(
                    "duplicate partition column: '{}'",
                    col
                )));
            }
        }

        // Validate column names are Hive-compatible (alphanumeric + underscore)
        for col in &columns {
            if !col.chars().all(|c| c.is_alphanumeric() || c == '_') {
                return Err(PluginError::Internal(format!(
                    "partition column name '{}' contains invalid characters. \
                     Only alphanumeric characters and underscores are allowed.",
                    col
                )));
            }
        }

        let available: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        let mut column_indices = Vec::with_capacity(columns.len());

        for col in &columns {
            match schema.index_of(col) {
                Ok(idx) => column_indices.push(idx),
                Err(_) => {
                    return Err(PluginError::Internal(format!(
                        "partition column '{}' not found in schema. Available columns: {:?}",
                        col, available
                    )));
                }
            }
        }

        Ok(Some(PartitionConfig {
            columns,
            column_indices,
        }))
    }
}

/// A single partition: its Hive-style path segment and the corresponding sub-batch.
pub struct PartitionedOutput {
    /// Hive-style path segment, e.g. `dt=2025-03-24/chain_id=1`.
    pub path_segment: String,
    /// The rows belonging to this partition.
    pub batch: RecordBatch,
}

const HIVE_DEFAULT_PARTITION: &str = "__HIVE_DEFAULT_PARTITION__";

/// Split a RecordBatch into sub-batches grouped by partition column values.
///
/// Returns one `PartitionedOutput` per unique combination of partition column values.
/// The partition columns are kept in the output batches (not stripped).
pub fn partition_batch(
    batch: &RecordBatch,
    config: &PartitionConfig,
) -> Result<Vec<PartitionedOutput>, PluginError> {
    let num_rows = batch.num_rows();
    if num_rows == 0 {
        return Ok(vec![]);
    }

    // Row-by-row grouping: build a string key per row and collect indices.
    // A columnar approach (hash columns independently, group by composite hash, build
    // string keys once per partition) would reduce allocations from O(rows * cols) to
    // O(partitions * cols), but for typical batch sizes (8K-64K rows) this takes <5ms
    // while S3 upload latency dominates at 50-100ms. Optimize if profiling shows need.
    let mut groups: HashMap<String, Vec<u32>> = HashMap::new();

    for row_idx in 0..num_rows {
        let key = build_partition_key(batch, config, row_idx)?;
        groups.entry(key).or_default().push(row_idx as u32);
    }

    // Fast path: if all rows land in a single partition, return the batch as-is
    // (clone is cheap — just Arc refcount increments, avoids the take() copy).
    if groups.len() == 1 {
        let (path_segment, _) = groups.into_iter().next().ok_or_else(|| {
            PluginError::Internal("unexpected empty partition groups".to_string())
        })?;
        return Ok(vec![PartitionedOutput {
            path_segment,
            batch: batch.clone(),
        }]);
    }

    let mut result = Vec::with_capacity(groups.len());
    for (path_segment, indices) in groups {
        let indices_array = arrow::array::UInt32Array::from(indices);
        let columns: Vec<_> = batch
            .columns()
            .iter()
            .map(|col| take(col.as_ref(), &indices_array, None))
            .collect::<Result<_, _>>()
            .map_err(|e| PluginError::Internal(format!("failed to partition batch: {}", e)))?;

        let sub_batch = RecordBatch::try_new(batch.schema(), columns).map_err(|e| {
            PluginError::Internal(format!("failed to build partitioned RecordBatch: {}", e))
        })?;

        result.push(PartitionedOutput {
            path_segment,
            batch: sub_batch,
        });
    }

    Ok(result)
}

/// Build the Hive-style path segment for a single row, e.g. `dt=2025-03-24/chain_id=1`.
fn build_partition_key(
    batch: &RecordBatch,
    config: &PartitionConfig,
    row_idx: usize,
) -> Result<String, PluginError> {
    let mut parts = Vec::with_capacity(config.columns.len());

    for (col_name, &col_idx) in config.columns.iter().zip(&config.column_indices) {
        let array = batch.column(col_idx);
        let part = if array.is_null(row_idx) {
            format!("{}={}", col_name, HIVE_DEFAULT_PARTITION)
        } else {
            let value_str = array_value_to_string(array.as_ref(), row_idx)?;
            let encoded = urlencoding::encode(&value_str);
            format!("{}={}", col_name, encoded)
        };
        parts.push(part);
    }

    Ok(parts.join("/"))
}

/// Convert an Arrow array value at the given row index to a string representation.
/// Delegates to Arrow's built-in formatter which handles all types including
/// Dictionary, Utf8View, Decimal, Timestamp, Date, and more.
fn array_value_to_string(array: &dyn Array, row_idx: usize) -> Result<String, PluginError> {
    arrow::util::display::array_value_to_string(array, row_idx)
        .map_err(|e| PluginError::Internal(format!("failed to format partition value: {}", e)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn make_schema(fields: Vec<(&str, DataType)>) -> SchemaRef {
        Arc::new(Schema::new(
            fields
                .into_iter()
                .map(|(name, dt)| Field::new(name, dt, true))
                .collect::<Vec<_>>(),
        ))
    }

    fn make_options(partition_columns: &str) -> HashMap<String, String> {
        let mut opts = HashMap::new();
        opts.insert(
            "partition_columns".to_string(),
            partition_columns.to_string(),
        );
        opts
    }

    // ── PartitionConfig tests ──

    #[test]
    fn test_from_options_none_when_missing() {
        let schema = make_schema(vec![("id", DataType::Int64)]);
        let opts = HashMap::new();
        let result = PartitionConfig::from_options(&opts, &schema).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_from_options_single_column() {
        let schema = make_schema(vec![("id", DataType::Int64), ("dt", DataType::Utf8)]);
        let opts = make_options("dt");
        let config = PartitionConfig::from_options(&opts, &schema)
            .unwrap()
            .unwrap();
        assert_eq!(config.columns, vec!["dt"]);
        assert_eq!(config.column_indices, vec![1]);
    }

    #[test]
    fn test_from_options_multiple_columns() {
        let schema = make_schema(vec![
            ("id", DataType::Int64),
            ("chain_id", DataType::Int32),
            ("dt", DataType::Utf8),
        ]);
        let opts = make_options("dt, chain_id");
        let config = PartitionConfig::from_options(&opts, &schema)
            .unwrap()
            .unwrap();
        assert_eq!(config.columns, vec!["dt", "chain_id"]);
        assert_eq!(config.column_indices, vec![2, 1]);
    }

    #[test]
    fn test_from_options_invalid_column() {
        let schema = make_schema(vec![("id", DataType::Int64), ("dt", DataType::Utf8)]);
        let opts = make_options("nonexistent");
        let err = PartitionConfig::from_options(&opts, &schema).unwrap_err();
        let msg = format!("{}", err);
        assert!(msg.contains("nonexistent"));
        assert!(msg.contains("not found in schema"));
        assert!(msg.contains("id"));
        assert!(msg.contains("dt"));
    }

    // ── partition_batch tests ──

    fn make_test_batch() -> RecordBatch {
        let schema = make_schema(vec![
            ("id", DataType::Int64),
            ("dt", DataType::Utf8),
            ("value", DataType::Float64),
        ]);
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
                Arc::new(StringArray::from(vec![
                    "2025-03-24",
                    "2025-03-25",
                    "2025-03-24",
                    "2025-03-25",
                ])),
                Arc::new(Float64Array::from(vec![10.0, 20.0, 30.0, 40.0])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_partition_single_column() {
        let batch = make_test_batch();
        let config = PartitionConfig {
            columns: vec!["dt".to_string()],
            column_indices: vec![1],
        };

        let mut partitions = partition_batch(&batch, &config).unwrap();
        partitions.sort_by(|a, b| a.path_segment.cmp(&b.path_segment));

        assert_eq!(partitions.len(), 2);
        assert_eq!(partitions[0].path_segment, "dt=2025-03-24");
        assert_eq!(partitions[0].batch.num_rows(), 2);
        assert_eq!(partitions[1].path_segment, "dt=2025-03-25");
        assert_eq!(partitions[1].batch.num_rows(), 2);

        // Verify correct rows in each partition
        let ids_0 = partitions[0]
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert!(ids_0.values().contains(&1));
        assert!(ids_0.values().contains(&3));

        let ids_1 = partitions[1]
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert!(ids_1.values().contains(&2));
        assert!(ids_1.values().contains(&4));
    }

    #[test]
    fn test_partition_multiple_columns() {
        let schema = make_schema(vec![
            ("id", DataType::Int64),
            ("chain", DataType::Int32),
            ("dt", DataType::Utf8),
        ]);
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![1, 42161, 1])),
                Arc::new(StringArray::from(vec![
                    "2025-03-24",
                    "2025-03-24",
                    "2025-03-25",
                ])),
            ],
        )
        .unwrap();

        let config = PartitionConfig {
            columns: vec!["chain".to_string(), "dt".to_string()],
            column_indices: vec![1, 2],
        };

        let mut partitions = partition_batch(&batch, &config).unwrap();
        partitions.sort_by(|a, b| a.path_segment.cmp(&b.path_segment));

        assert_eq!(partitions.len(), 3);
        assert_eq!(partitions[0].path_segment, "chain=1/dt=2025-03-24");
        assert_eq!(partitions[0].batch.num_rows(), 1);
        assert_eq!(partitions[1].path_segment, "chain=1/dt=2025-03-25");
        assert_eq!(partitions[1].batch.num_rows(), 1);
        assert_eq!(partitions[2].path_segment, "chain=42161/dt=2025-03-24");
        assert_eq!(partitions[2].batch.num_rows(), 1);
    }

    #[test]
    fn test_partition_with_nulls() {
        let schema = make_schema(vec![("id", DataType::Int64), ("dt", DataType::Utf8)]);
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![
                    Some("2025-03-24"),
                    None,
                    Some("2025-03-24"),
                ])),
            ],
        )
        .unwrap();

        let config = PartitionConfig {
            columns: vec!["dt".to_string()],
            column_indices: vec![1],
        };

        let mut partitions = partition_batch(&batch, &config).unwrap();
        partitions.sort_by(|a, b| a.path_segment.cmp(&b.path_segment));

        assert_eq!(partitions.len(), 2);
        assert_eq!(partitions[0].path_segment, "dt=2025-03-24");
        assert_eq!(partitions[0].batch.num_rows(), 2);
        assert_eq!(partitions[1].path_segment, "dt=__HIVE_DEFAULT_PARTITION__");
        assert_eq!(partitions[1].batch.num_rows(), 1);
    }

    #[test]
    fn test_partition_empty_batch() {
        let schema = make_schema(vec![("id", DataType::Int64), ("dt", DataType::Utf8)]);
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(Vec::<i64>::new())),
                Arc::new(StringArray::from(Vec::<&str>::new())),
            ],
        )
        .unwrap();

        let config = PartitionConfig {
            columns: vec!["dt".to_string()],
            column_indices: vec![1],
        };

        let partitions = partition_batch(&batch, &config).unwrap();
        assert!(partitions.is_empty());
    }

    #[test]
    fn test_partition_all_same_value() {
        let schema = make_schema(vec![("id", DataType::Int64), ("dt", DataType::Utf8)]);
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![
                    "2025-03-24",
                    "2025-03-24",
                    "2025-03-24",
                ])),
            ],
        )
        .unwrap();

        let config = PartitionConfig {
            columns: vec!["dt".to_string()],
            column_indices: vec![1],
        };

        let partitions = partition_batch(&batch, &config).unwrap();
        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0].path_segment, "dt=2025-03-24");
        assert_eq!(partitions[0].batch.num_rows(), 3);
    }

    #[test]
    fn test_partition_special_characters_encoded() {
        let schema = make_schema(vec![("id", DataType::Int64), ("category", DataType::Utf8)]);
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["foo/bar=baz"])),
            ],
        )
        .unwrap();

        let config = PartitionConfig {
            columns: vec!["category".to_string()],
            column_indices: vec![1],
        };

        let partitions = partition_batch(&batch, &config).unwrap();
        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0].path_segment, "category=foo%2Fbar%3Dbaz");
    }

    #[test]
    fn test_from_options_duplicate_columns() {
        let schema = make_schema(vec![("id", DataType::Int64), ("dt", DataType::Utf8)]);
        let opts = make_options("dt, dt");
        let err = PartitionConfig::from_options(&opts, &schema).unwrap_err();
        let msg = format!("{}", err);
        assert!(msg.contains("duplicate partition column"));
        assert!(msg.contains("dt"));
    }

    #[test]
    fn test_from_options_trailing_comma_ignored() {
        let schema = make_schema(vec![("id", DataType::Int64), ("dt", DataType::Utf8)]);
        let opts = make_options("dt,");
        let config = PartitionConfig::from_options(&opts, &schema)
            .unwrap()
            .unwrap();
        assert_eq!(config.columns, vec!["dt"]);
    }

    #[test]
    fn test_from_options_rejects_special_char_column_name() {
        let schema = make_schema(vec![("id", DataType::Int64), ("my col", DataType::Utf8)]);
        let opts = make_options("my col");
        let err = PartitionConfig::from_options(&opts, &schema).unwrap_err();
        let msg = format!("{}", err);
        assert!(msg.contains("invalid characters"));
    }

    #[test]
    fn test_partition_dictionary_encoded_column() {
        use arrow::datatypes::Int8Type;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "status",
                DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                false,
            ),
        ]));

        let dict_array: DictionaryArray<Int8Type> =
            vec!["active", "inactive", "active"].into_iter().collect();

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(dict_array),
            ],
        )
        .unwrap();

        let config = PartitionConfig {
            columns: vec!["status".to_string()],
            column_indices: vec![1],
        };

        let mut partitions = partition_batch(&batch, &config).unwrap();
        partitions.sort_by(|a, b| a.path_segment.cmp(&b.path_segment));

        assert_eq!(partitions.len(), 2);
        assert_eq!(partitions[0].path_segment, "status=active");
        assert_eq!(partitions[0].batch.num_rows(), 2);
        assert_eq!(partitions[1].path_segment, "status=inactive");
        assert_eq!(partitions[1].batch.num_rows(), 1);
    }

    #[test]
    fn test_partition_date32_column() {
        let schema = make_schema(vec![("id", DataType::Int64), ("dt", DataType::Date32)]);
        // Date32 stores days since 1970-01-01. 2025-03-24 = 20171 days
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(Date32Array::from(vec![20171, 20172])),
            ],
        )
        .unwrap();

        let config = PartitionConfig {
            columns: vec!["dt".to_string()],
            column_indices: vec![1],
        };

        let mut partitions = partition_batch(&batch, &config).unwrap();
        partitions.sort_by(|a, b| a.path_segment.cmp(&b.path_segment));

        assert_eq!(partitions.len(), 2);
        assert_eq!(partitions[0].path_segment, "dt=2025-03-24");
        assert_eq!(partitions[1].path_segment, "dt=2025-03-25");
    }
}

//! Snapshot ancestry: which snapshots the source still has to process, and
//! what to do with each one.

use crate::sources::iceberg::config::OnNonAppendSnapshot;
use iceberg::spec::{Operation, SnapshotRef, TableMetadata};
use streamling_plugin::PluginError;

/// Whether a snapshot's added rows are emitted or the snapshot is passed over.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum SnapshotAction {
    Read,
    Skip,
}

/// Snapshots committed after `last` (exclusive) up to the table's current
/// snapshot, oldest first; every snapshot back to the root when `last` is
/// `None`.
///
/// `last` counts as reached when the walk meets a snapshot whose parent is
/// `last`, so it may itself have expired as long as its child survives. The
/// walk fails when the chain breaks first: `last` expired together with its
/// descendants (retention shorter than the source's downtime), or it is not
/// an ancestor of the current snapshot (the table was rolled back or the
/// branch replaced).
pub fn snapshots_since(
    metadata: &TableMetadata,
    last: Option<i64>,
) -> Result<Vec<SnapshotRef>, PluginError> {
    let Some(current) = metadata.current_snapshot() else {
        return match last {
            None => Ok(Vec::new()),
            Some(last) => Err(not_an_ancestor(metadata, last, None)),
        };
    };
    let mut chain = Vec::new();
    let mut snapshot = current;
    loop {
        if Some(snapshot.snapshot_id()) == last {
            break;
        }
        chain.push(snapshot.clone());
        match snapshot.parent_snapshot_id() {
            Some(parent) if Some(parent) == last => break,
            Some(parent) => match metadata.snapshot_by_id(parent) {
                Some(next) => snapshot = next,
                None => return Err(broken_chain(metadata, last, current.snapshot_id(), parent)),
            },
            None if last.is_none() => break,
            None => {
                return Err(not_an_ancestor(
                    metadata,
                    last.unwrap_or_default(),
                    Some(current.snapshot_id()),
                ));
            }
        }
    }
    chain.reverse();
    Ok(chain)
}

fn broken_chain(
    metadata: &TableMetadata,
    last: Option<i64>,
    current: i64,
    missing: i64,
) -> PluginError {
    if let Some(last) = last
        && metadata.snapshot_by_id(last).is_none()
    {
        return expired(last);
    }
    PluginError::Execution(format!(
        "iceberg_source: snapshot {missing}, an ancestor of current snapshot {current}, \
         expired before the source read it; reset the source state or raise the table's \
         snapshot retention (history.expire.*)"
    ))
}

fn not_an_ancestor(metadata: &TableMetadata, last: i64, current: Option<i64>) -> PluginError {
    if metadata.snapshot_by_id(last).is_none() {
        return expired(last);
    }
    let current = current.map_or_else(|| "none".to_string(), |c| c.to_string());
    PluginError::Execution(format!(
        "iceberg_source: last processed snapshot {last} is not an ancestor of the current \
         snapshot {current} (was the table rolled back or its branch replaced?); reset the \
         source state to re-read the table"
    ))
}

/// The error for a snapshot the source needs that is gone from table metadata.
pub fn expired(snapshot_id: i64) -> PluginError {
    PluginError::Execution(format!(
        "iceberg_source: snapshot {snapshot_id} recorded in the source state no longer exists \
         in table metadata (expired); reset the source state to re-read the table, or raise \
         the table's snapshot retention (history.expire.max-snapshot-age-ms, \
         history.expire.min-snapshots-to-keep) above the source's maximum downtime"
    ))
}

/// `append` snapshots are read; `replace` (compaction, no logical change) is
/// always skipped; `overwrite` and `delete` follow `on_non_append_snapshot`.
pub fn classify(
    snapshot: &SnapshotRef,
    policy: OnNonAppendSnapshot,
) -> Result<SnapshotAction, PluginError> {
    let operation = &snapshot.summary().operation;
    match (operation, policy) {
        (Operation::Append, _) => Ok(SnapshotAction::Read),
        (Operation::Replace, _) => Ok(SnapshotAction::Skip),
        (Operation::Overwrite | Operation::Delete, OnNonAppendSnapshot::Skip) => {
            Ok(SnapshotAction::Skip)
        }
        (Operation::Overwrite | Operation::Delete, OnNonAppendSnapshot::Error) => {
            Err(PluginError::Execution(format!(
                "iceberg_source: snapshot {} is a '{}' snapshot; incremental reads support \
                 append snapshots only. Set on_non_append_snapshot=skip to pass over it (its \
                 added rows and deletes are not emitted)",
                snapshot.snapshot_id(),
                operation.as_str()
            )))
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use iceberg::spec::{
        FormatVersion, NestedField, PrimitiveType, Schema, Snapshot, SortOrder, Summary,
        TableMetadataBuilder, Type, UnboundPartitionSpec,
    };
    use std::collections::HashMap;

    pub(crate) fn metadata(snapshots: &[(i64, Option<i64>, Operation)]) -> TableMetadata {
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .unwrap();
        let mut builder = TableMetadataBuilder::new(
            schema,
            UnboundPartitionSpec::builder().build(),
            SortOrder::unsorted_order(),
            "file:///tmp/iceberg_source_test".to_string(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .unwrap();
        for (seq, (id, parent, operation)) in snapshots.iter().enumerate() {
            let snapshot = Snapshot::builder()
                .with_snapshot_id(*id)
                .with_parent_snapshot_id(*parent)
                .with_sequence_number(seq as i64 + 1)
                .with_timestamp_ms(1_700_000_000_000 + seq as i64)
                .with_manifest_list(format!("file:///tmp/snap-{id}.avro"))
                .with_summary(Summary {
                    operation: operation.clone(),
                    additional_properties: HashMap::new(),
                })
                .build();
            builder = builder.set_branch_snapshot(snapshot, "main").unwrap();
        }
        builder.build().unwrap().metadata
    }

    fn ids(snapshots: &[SnapshotRef]) -> Vec<i64> {
        snapshots.iter().map(|s| s.snapshot_id()).collect()
    }

    fn chain() -> TableMetadata {
        metadata(&[
            (1, None, Operation::Append),
            (2, Some(1), Operation::Replace),
            (3, Some(2), Operation::Overwrite),
            (4, Some(3), Operation::Append),
        ])
    }

    #[test]
    fn walks_from_last_exclusive_oldest_first() {
        let md = chain();
        assert_eq!(ids(&snapshots_since(&md, Some(2)).unwrap()), vec![3, 4]);
        assert_eq!(ids(&snapshots_since(&md, None).unwrap()), vec![1, 2, 3, 4]);
        assert!(snapshots_since(&md, Some(4)).unwrap().is_empty());
        assert!(snapshots_since(&metadata(&[]), None).unwrap().is_empty());
    }

    #[test]
    fn classifies_operations() {
        let md = chain();
        let actions = |policy| {
            snapshots_since(&md, None)
                .unwrap()
                .iter()
                .map(|s| classify(s, policy).map_err(|e| e.to_string()))
                .collect::<Vec<_>>()
        };
        let skip = actions(OnNonAppendSnapshot::Skip);
        assert_eq!(
            skip,
            vec![
                Ok(SnapshotAction::Read),
                Ok(SnapshotAction::Skip),
                Ok(SnapshotAction::Skip),
                Ok(SnapshotAction::Read),
            ]
        );
        let error = actions(OnNonAppendSnapshot::Error);
        assert_eq!(
            error[1],
            Ok(SnapshotAction::Skip),
            "replace is always skipped"
        );
        let err = error[2].clone().unwrap_err();
        assert!(
            err.contains("'overwrite'") && err.contains("on_non_append_snapshot"),
            "{err}"
        );

        let delete = metadata(&[(1, None, Operation::Delete)]);
        let snapshot = delete.current_snapshot().unwrap();
        assert!(classify(snapshot, OnNonAppendSnapshot::Error).is_err());
    }

    #[test]
    fn expired_last_snapshot_is_tolerated_while_its_child_survives() {
        let md = TableMetadataBuilder::new_from_metadata(chain(), None)
            .remove_snapshots(&[1])
            .build()
            .unwrap()
            .metadata;
        assert_eq!(ids(&snapshots_since(&md, Some(1)).unwrap()), vec![2, 3, 4]);
    }

    #[test]
    fn expired_chain_is_an_error() {
        let md = TableMetadataBuilder::new_from_metadata(chain(), None)
            .remove_snapshots(&[1, 2])
            .build()
            .unwrap()
            .metadata;
        let err = snapshots_since(&md, Some(1)).unwrap_err().to_string();
        assert!(
            err.contains("no longer exists") && err.contains("reset"),
            "{err}"
        );
        let err = snapshots_since(&md, None).unwrap_err().to_string();
        assert!(err.contains("expired"), "{err}");
    }

    #[test]
    fn rollback_is_not_an_ancestor_error() {
        // Branch rolled back to 2; 3 and 4 still exist but are no ancestors.
        let md = chain();
        let rolled_back = TableMetadataBuilder::new_from_metadata(md.clone(), None)
            .set_branch_snapshot(
                Snapshot::builder()
                    .with_snapshot_id(5)
                    .with_parent_snapshot_id(Some(2))
                    .with_sequence_number(5)
                    .with_timestamp_ms(1_700_000_000_010)
                    .with_manifest_list("file:///tmp/snap-5.avro")
                    .with_summary(Summary {
                        operation: Operation::Append,
                        additional_properties: HashMap::new(),
                    })
                    .build(),
                "main",
            )
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let err = snapshots_since(&rolled_back, Some(4))
            .unwrap_err()
            .to_string();
        assert!(err.contains("not an ancestor"), "{err}");
        assert_eq!(
            ids(&snapshots_since(&rolled_back, Some(2)).unwrap()),
            vec![5]
        );
    }
}

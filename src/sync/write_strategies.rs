//! Write-strategy implementations for each [`WriteMode`].
//!
//! Each strategy receives a committed Iceberg [`Table`] reference, the
//! incoming [`RecordBatch`], and job-level config, and returns the
//! completed [`Transaction`] ready to commit.
//!
//! ## Strategy summary
//!
//! | Mode        | Reads existing files?                              | Produces delete files? | Cost   |
//! |-------------|---------------------------------------------------|------------------------|--------|
//! | Append      | No                                                | No                     | Low    |
//! | Overwrite   | Partition-scoped (real spec) / full scan (legacy) | No                     | Medium |
//! | Upsert      | Yes (COW full scan)                               | No                     | High   |
//! | MergeInto   | Yes (COW full scan)                               | No                     | High   |
//!
//! ## iceberg-rust 0.9 Transaction API constraints
//!
//! `Transaction` in 0.9.0 exposes only `fast_append()`, which validates that
//! every `DataFile` has `DataContentType::Data`.  There is no
//! `replace_data_files()`, `row_delta()`, or `add_delete_files()` action.
//!
//! Because equality-delete files have `DataContentType::EqualityDeletes`,
//! they **cannot be committed** through `fast_append` — the validation check
//! inside `FastAppendAction` rejects them with:
//!   `DataInvalid => Only data content type is allowed for fast append`
//!
//! ## Copy-on-write (COW) approach
//!
//! To work within these API constraints we implement all write modes using
//! **copy-on-write**:
//!
//! 1. Scan all existing rows from the current Iceberg snapshot.
//! 2. Filter out the rows that the incoming batch logically replaces
//!    (by partition column for Overwrite; by key columns for Upsert/MergeInto).
//! 3. Concatenate the surviving existing rows with the new/updated rows.
//! 4. Write the merged result as fresh Parquet data files.
//! 5. Commit only `DataContentType::Data` files via `fast_append`.
//!
//! This avoids delete files entirely and works within the iceberg-rust 0.9 API.
//! The trade-off is a full table scan per batch for non-append modes, which is
//! acceptable for the typical table sizes targeted by this tool.
use anyhow::{
    Context,
    Result, // , bail
};
use arrow_array::Array;
use arrow_array::{RecordBatch, StringArray};
use arrow_schema::DataType;
use futures::TryStreamExt;
use iceberg::{
    expr::Reference,
    spec::{DataFile, DataFileFormat, Datum, SchemaRef},
    table::Table,
    transaction::{ApplyTransactionAction, Transaction},
    writer::{
        IcebergWriter, IcebergWriterBuilder,
        base_writer::{
            data_file_writer::DataFileWriterBuilder,
            equality_delete_writer::{EqualityDeleteFileWriterBuilder, EqualityDeleteWriterConfig},
        },
        file_writer::{
            ParquetWriterBuilder, location_generator::DefaultLocationGenerator,
            rolling_writer::RollingFileWriterBuilder,
        },
    },
};
use parquet::file::properties::WriterProperties;
use tracing::{debug, info, warn};

use crate::config::{MergeConfig, SyncJob};
use crate::sync::file_name::ProductionFileNameGenerator;

// ── Public entry: dispatch by write mode ──────────────────────────────────────

/// Dispatch to the appropriate write strategy and return the committed data-
/// file list and (optionally) delete-file list to be composed into a
/// transaction by the caller.
///
/// Returns `(data_files, delete_files)`.  Both vecs may be empty for
/// pure-delete operations.
#[allow(dead_code)]
pub struct WritePlan {
    pub data_files: Vec<DataFile>,
    pub delete_files: Vec<DataFile>,
    /// Rows that were actually appended (may differ from batch len for D-only ops).
    pub rows_appended: usize,
    /// Rows that were logically deleted (via position deletes).
    pub rows_deleted: usize,
}

// ── Append ────────────────────────────────────────────────────────────────────

/// Pure append: write a new Parquet data file, no existing-file reads.
pub async fn plan_append(job: &SyncJob, table: &Table, batch: RecordBatch) -> Result<WritePlan> {
    let n = batch.num_rows();
    let data_files = write_parquet(job, table, batch)
        .await
        .context("Append strategy: failed to write Parquet data")?;

    Ok(WritePlan {
        data_files,
        delete_files: vec![],
        rows_appended: n,
        rows_deleted: 0,
    })
}

// ── Overwrite ─────────────────────────────────────────────────────────────────

/// Partition overwrite: replace all existing rows for the partition value(s)
/// present in the incoming batch with the new batch (copy-on-write).
///
/// ## Partition column resolution
///
/// The column used to identify which rows belong to the overwritten partition
/// is resolved in priority order:
///
/// 1. `job.iceberg_partition.column` — preferred; the table has a real Iceberg
///    partition spec using a day/month/year/hour transform.
/// 2. `job.partition_column` — legacy fallback; used as a plain string equality
///    filter with no native Iceberg partition spec on the table.
///
/// ## Implementation (COW)
///
/// 1. Scan all existing rows from the current Iceberg snapshot.
/// 2. Remove rows whose `partition_column` value is present in the incoming
///    batch (i.e. suppress old rows for the overwritten partition).
/// 3. Concatenate surviving rows with the incoming batch.
/// 4. Write the merged result as a fresh Parquet data file.
/// 5. Commit only `DataContentType::Data` files via `fast_append`.
///
/// On first run (no existing snapshot) the behaviour is identical to a plain
/// append, since there is nothing to scan.
pub async fn plan_overwrite(job: &SyncJob, table: &Table, batch: RecordBatch) -> Result<WritePlan> {
    // Resolve partition column: prefer iceberg_partition.column, fall back to
    // the legacy partition_column field.  Config validation guarantees at least
    // one is set for write_mode=overwrite.
    let part_col: &str = job
        .iceberg_partition
        .as_ref()
        .map(|ip| ip.column.as_str())
        .or(job.partition_column.as_deref())
        .expect(
            "overwrite requires iceberg_partition or partition_column — validated at config load",
        );

    let n = batch.num_rows();

    // Collect the distinct partition values present in the incoming batch.
    let incoming_parts = extract_string_column_values(&batch, part_col)
        .context("Overwrite: failed to extract incoming partition values")?;

    // Does the table have a real Iceberg partition spec (non-empty fields)?
    // When true, we can use a predicate-pushdown scan scoped to only the
    // relevant day/month/year bucket — far cheaper than a full table scan.
    let has_real_spec = !table
        .metadata()
        .default_partition_spec()
        .fields()
        .is_empty();

    // If the table has no snapshot yet this is the first run — plain append.
    let merged = if table.metadata().current_snapshot().is_some() {
        if has_real_spec {
            // Fast path: scan only files in the target partition bucket.
            // The Iceberg manifest evaluator prunes all other data files at
            // the manifest level — no row-level filter needed.
            //
            // Partition field name convention: "<column>_<transform>"
            // e.g. "sale_date_day" for transform=day on column sale_date.
            let partition_field_name = job
                .iceberg_partition
                .as_ref()
                .map(|ip| format!("{}_{}", ip.column, ip.transform))
                .unwrap_or_else(|| format!("{part_col}_day"));

            let existing =
                scan_table_for_partition_values(table, &partition_field_name, &incoming_parts)
                    .await
                    .context("Overwrite: partition-scoped scan failed")?;

            let rows_scanned: usize = existing
                .iter()
                .map(|b| b.num_rows())
                .collect::<Vec<_>>()
                .iter()
                .sum();
            info!(
                job = %job.name,
                partition_field = %partition_field_name,
                rows_scanned,
                rows_incoming = n,
                "overwrite: partition-scoped scan (real spec) — replacing partition data"
            );

            // All rows returned by the scoped scan belong to the target
            // partition and are fully replaced by the incoming batch.
            // No row-level filter needed — just prepend existing (empty on
            // first overwrite of this partition) then push new batch.
            let mut all = existing;
            all.push(batch);
            if all.len() == 1 {
                all.remove(0)
            } else {
                concat_batches_list(&all)
                    .context("Overwrite: failed to concatenate partition-scoped batches")?
            }
        } else {
            // Legacy COW path: full table scan + row-level partition filter.
            // Used when the table was created without a real partition spec
            // (i.e. using the legacy partition_column field).
            let existing = scan_table_to_batches(table)
                .await
                .context("Overwrite: failed to scan existing table data")?;

            let mut survivors: Vec<RecordBatch> = Vec::new();
            let mut rows_removed = 0usize;
            for existing_batch in existing {
                let (kept, dropped) = partition_filter(&existing_batch, part_col, &incoming_parts)
                    .context("Overwrite: failed to filter existing rows by partition")?;
                rows_removed += dropped;
                if let Some(kept_batch) = kept {
                    survivors.push(kept_batch);
                }
            }
            info!(
                job = %job.name,
                partition_col = part_col,
                rows_removed,
                rows_incoming = n,
                "overwrite: legacy COW — removed old partition rows, merging with incoming"
            );
            survivors.push(batch);
            concat_batches_list(&survivors).context("Overwrite: failed to concatenate batches")?
        }
    } else {
        debug!(job = %job.name, "overwrite: no existing snapshot — first run, plain append");
        batch
    };

    let rows_appended = merged.num_rows();
    let data_files = write_parquet(job, table, merged)
        .await
        .context("Overwrite strategy: failed to write Parquet data")?;

    Ok(WritePlan {
        data_files,
        delete_files: vec![],
        rows_appended,
        rows_deleted: n, // incoming rows replace old partition
    })
}

// ── Upsert ────────────────────────────────────────────────────────────────────

/// Upsert: for each incoming row, remove the existing row with the same key
/// (if any), then append the new row (copy-on-write).
///
/// ## Implementation (COW)
///
/// 1. Build a set of incoming key-tuples from `merge.key_columns`.
/// 2. Scan all existing rows from the current Iceberg snapshot.
/// 3. Remove existing rows whose key-tuple is in the incoming set.
/// 4. Concatenate surviving rows with the incoming batch.
/// 5. Write the merged result as a fresh Parquet data file and commit via
///    `fast_append` (only `DataContentType::Data` files).
pub async fn plan_upsert(job: &SyncJob, table: &Table, batch: RecordBatch) -> Result<WritePlan> {
    let merge_cfg = job
        .merge
        .as_ref()
        .expect("merge config required for upsert — validated at config load");

    let n = batch.num_rows();

    // If the table has no snapshot yet this is the first run — plain append.
    let merged = if table.metadata().current_snapshot().is_some() {
        // Build the set of key-tuples in the incoming batch.
        let incoming_keys = build_key_set(&batch, &merge_cfg.key_columns)
            .context("Upsert: failed to build incoming key set")?;

        let existing = scan_table_to_batches(table)
            .await
            .context("Upsert: failed to scan existing table data")?;

        let mut survivors: Vec<RecordBatch> = Vec::new();
        let mut rows_removed = 0usize;
        for existing_batch in existing {
            let (kept, dropped) =
                key_filter(&existing_batch, &merge_cfg.key_columns, &incoming_keys)
                    .context("Upsert: failed to filter existing rows by key")?;
            rows_removed += dropped;
            if let Some(kept_batch) = kept {
                survivors.push(kept_batch);
            }
        }

        info!(
            job = %job.name,
            key_columns = ?merge_cfg.key_columns,
            rows_removed,
            rows_incoming = n,
            "upsert: COW — removed old keyed rows, appending new"
        );

        survivors.push(batch);
        concat_batches_list(&survivors).context("Upsert: failed to concatenate batches")?
    } else {
        debug!(job = %job.name, "upsert: no existing snapshot — first run, plain append");
        batch
    };

    let rows_appended = merged.num_rows();
    let data_files = write_parquet(job, table, merged)
        .await
        .context("Upsert strategy: failed to write Parquet data")?;

    info!(
        job = %job.name,
        rows_incoming = n,
        rows_appended,
        "upsert: complete"
    );

    Ok(WritePlan {
        data_files,
        delete_files: vec![],
        rows_appended,
        rows_deleted: n,
    })
}

// ── MergeInto ─────────────────────────────────────────────────────────────────

/// Full MERGE INTO semantics driven by an `_op` column in the batch (COW).
///
/// Expected `_op` values (case-insensitive):
///   - `I` or `INSERT`  → append this row, existing row with same key is kept
///   - `U` or `UPDATE`  → remove existing row by key, append new row
///   - `D` or `DELETE`  → remove existing row by key, do NOT append
///
/// Unknown `_op` values are treated as inserts and a warning is emitted.
/// The `_op` column is stripped from the data before writing.
///
/// ## Implementation (COW)
///
/// 1. Split the batch by `_op` into inserts, updates, and deletes.
/// 2. Build the set of keys that must be removed (U + D rows).
/// 3. Scan existing rows and keep only those NOT in the remove-set.
/// 4. Append I + U rows to the surviving rows.
/// 5. Write the merged result as a single fresh data file via `fast_append`.
pub async fn plan_merge_into(
    job: &SyncJob,
    table: &Table,
    batch: RecordBatch,
) -> Result<WritePlan> {
    let merge_cfg = job
        .merge
        .as_ref()
        .expect("merge config required for merge_into — validated at config load");

    // ── 1. Split batch by _op ────────────────────────────────────────────────
    let OpSplit {
        inserts,
        updates,
        deletes,
    } = split_by_op(&batch)?;

    let rows_deleted = deletes.as_ref().map(|b| b.num_rows()).unwrap_or(0)
        + updates.as_ref().map(|b| b.num_rows()).unwrap_or(0);

    let _rows_appended_incoming = inserts.as_ref().map(|b| b.num_rows()).unwrap_or(0)
        + updates.as_ref().map(|b| b.num_rows()).unwrap_or(0);

    info!(
        job = %job.name,
        inserts = inserts.as_ref().map(|b| b.num_rows()).unwrap_or(0),
        updates = updates.as_ref().map(|b| b.num_rows()).unwrap_or(0),
        deletes = deletes.as_ref().map(|b| b.num_rows()).unwrap_or(0),
        "merge_into: split by _op"
    );

    // ── 2. Build the key-set of rows to remove (U + D) ────────────────────────
    // rows_for_delete is the batch of U+D rows; we need their keys to filter
    // existing rows out.
    let rows_for_delete = concat_batches_opt(&[updates.as_ref(), deletes.as_ref()])?;
    let rows_to_append = concat_batches_opt(&[inserts.as_ref(), updates.as_ref()])?;

    // ── 3. Scan + filter existing rows if there are any keys to remove ────────
    let merged = if let Some(delete_batch) = &rows_for_delete {
        if table.metadata().current_snapshot().is_some() {
            let delete_keys = build_key_set(delete_batch, &merge_cfg.key_columns)
                .context("MergeInto: failed to build delete key set")?;

            let existing = scan_table_to_batches(table)
                .await
                .context("MergeInto: failed to scan existing table data")?;

            let mut survivors: Vec<RecordBatch> = Vec::new();
            let mut rows_removed = 0usize;
            for existing_batch in existing {
                let (kept, dropped) =
                    key_filter(&existing_batch, &merge_cfg.key_columns, &delete_keys)
                        .context("MergeInto: failed to filter existing rows by key")?;
                rows_removed += dropped;
                if let Some(kept_batch) = kept {
                    survivors.push(kept_batch);
                }
            }
            info!(
                job = %job.name,
                rows_removed,
                "merge_into: COW — removed old rows matching U/D keys"
            );

            // Append I+U rows at the end.
            if let Some(append_batch) = rows_to_append {
                survivors.push(append_batch);
            }
            // All existing rows were deleted and there are no rows to insert
            // (e.g. an all-D batch where every keyed row was found and removed).
            // Return an empty plan so the caller can truncate the table.
            if survivors.is_empty() {
                return Ok(WritePlan {
                    data_files: vec![],
                    delete_files: vec![],
                    rows_appended: 0,
                    rows_deleted,
                });
            }
            concat_batches_list(&survivors).context("MergeInto: failed to concatenate batches")?
        } else {
            // No existing snapshot — first run: only append I+U rows.
            match rows_to_append {
                Some(b) => b,
                None => {
                    // All ops are D on an empty table — nothing to write.
                    return Ok(WritePlan {
                        data_files: vec![],
                        delete_files: vec![],
                        rows_appended: 0,
                        rows_deleted: 0,
                    });
                }
            }
        }
    } else {
        // No U or D rows at all — pure insert batch.
        match rows_to_append {
            Some(b) => b,
            None => {
                return Ok(WritePlan {
                    data_files: vec![],
                    delete_files: vec![],
                    rows_appended: 0,
                    rows_deleted: 0,
                });
            }
        }
    };

    let rows_appended = merged.num_rows();
    let data_files = write_parquet(job, table, merged)
        .await
        .context("MergeInto strategy: failed to write Parquet data")?;

    info!(
        job = %job.name,
        rows_appended,
        rows_deleted,
        "merge_into: plan complete"
    );

    Ok(WritePlan {
        data_files,
        delete_files: vec![],
        rows_appended,
        rows_deleted,
    })
}

// ── Apply plan to a Transaction ───────────────────────────────────────────────

/// Given a [`WritePlan`] and an open [`Transaction`], apply the appropriate
/// Iceberg transaction actions and return the updated transaction ready to
/// commit.
///
/// ## iceberg-rust 0.9 constraints
///
/// `Transaction` only exposes `fast_append()` which only accepts data files
/// with `DataContentType::Data`.  Because all write strategies now use
/// copy-on-write (no delete files are produced), all plans consist solely of
/// data files and are committed via a single `fast_append` action.
///
/// If the plan has no data files (e.g. a merge_into batch with only Delete ops
/// against an empty table), the transaction is returned unchanged.
pub fn apply_plan_to_transaction(
    _write_mode: &crate::config::WriteMode,
    plan: WritePlan,
    tx: Transaction,
) -> Result<Transaction> {
    if plan.data_files.is_empty() {
        // Nothing to commit (e.g. all-D batch on empty table).
        return Ok(tx);
    }

    let tx = tx
        .fast_append()
        .add_data_files(plan.data_files)
        .apply(tx)
        .context("Failed to apply fast_append action to transaction")?;

    Ok(tx)
}

// ── COW scan helpers ──────────────────────────────────────────────────────────

/// Scan all rows from the current Iceberg snapshot and collect them as Arrow
/// [`RecordBatch`]es.
///
/// Returns an empty `Vec` if the table has no snapshot (i.e. is empty).
async fn scan_table_to_batches(table: &Table) -> Result<Vec<RecordBatch>> {
    let scan = table
        .scan()
        .build()
        .context("COW scan: failed to build table scan")?;

    let stream = scan
        .to_arrow()
        .await
        .context("COW scan: failed to open Arrow stream")?;

    let batches: Vec<RecordBatch> = stream
        .try_collect()
        .await
        .context("COW scan: failed to collect Arrow batches")?;

    Ok(batches)
}

/// Scan only the Parquet files that belong to the given partition values.
///
/// Uses an Iceberg `is_in` predicate on the **partition field name** (e.g.
/// `sale_date_day`) so the manifest evaluator prunes unrelated data files at
/// the manifest level — no Arrow row-filter needed afterwards.
///
/// The `partition_field_name` must be the Iceberg partition field name
/// (i.e. `<column>_<transform>`, e.g. `sale_date_day`), not the raw source
/// column name.
///
/// Returns an empty `Vec` if the table has no snapshot (first run).
async fn scan_table_for_partition_values(
    table: &Table,
    partition_field_name: &str,
    string_values: &std::collections::HashSet<String>,
) -> Result<Vec<RecordBatch>> {
    if string_values.is_empty() {
        return Ok(vec![]);
    }

    // Build Datum::date values from the string set.  The day() transform
    // produces i32 days-since-epoch values; the manifest evaluator compares
    // against them when pruning.  We parse each string as a date and convert.
    let datums: Vec<Datum> = string_values
        .iter()
        .filter_map(|s| Datum::date_from_str(s).ok())
        .collect();

    // If none of the values parse as dates (e.g. we're on the legacy
    // string-equality partition_column path), fall back to string datums.
    let datums = if datums.is_empty() {
        string_values
            .iter()
            .map(|s| Datum::string(s.clone()))
            .collect()
    } else {
        datums
    };

    let predicate = Reference::new(partition_field_name).is_in(datums);

    let scan = table
        .scan()
        .with_filter(predicate)
        .build()
        .context("partition-scoped scan: failed to build")?;

    let stream = scan
        .to_arrow()
        .await
        .context("partition-scoped scan: failed to open Arrow stream")?;

    let batches: Vec<RecordBatch> = stream
        .try_collect()
        .await
        .context("partition-scoped scan: failed to collect batches")?;

    Ok(batches)
}

/// Filter a batch, keeping rows whose `partition_col` value is NOT in
/// `remove_values`.
///
/// Returns `(Some(kept_batch), dropped_count)` or `(None, dropped_count)` if
/// all rows were removed.
fn partition_filter(
    batch: &RecordBatch,
    partition_col: &str,
    remove_values: &std::collections::HashSet<String>,
) -> Result<(Option<RecordBatch>, usize)> {
    let idx = match batch.schema().index_of(partition_col) {
        Ok(i) => i,
        Err(_) => {
            // Column absent in this batch — keep all rows.
            return Ok((Some(batch.clone()), 0));
        }
    };

    let mut keep_rows: Vec<usize> = Vec::new();
    let mut dropped = 0usize;

    let col = batch.column(idx);
    for row in 0..batch.num_rows() {
        let val = scalar_to_string(col.as_ref(), row);
        if remove_values.contains(&val) {
            dropped += 1;
        } else {
            keep_rows.push(row);
        }
    }

    let kept = filter_rows(batch, &keep_rows)
        .context("partition_filter: failed to build kept sub-batch")?;
    Ok((kept, dropped))
}

/// Filter a batch, keeping rows whose key-tuple (from `key_columns`) is NOT in
/// `remove_keys`.
///
/// Returns `(Some(kept_batch), dropped_count)` or `(None, dropped_count)` if
/// all rows were removed.
fn key_filter(
    batch: &RecordBatch,
    key_columns: &[String],
    remove_keys: &std::collections::HashSet<String>,
) -> Result<(Option<RecordBatch>, usize)> {
    let mut keep_rows: Vec<usize> = Vec::new();
    let mut dropped = 0usize;

    for row in 0..batch.num_rows() {
        // Build the key tuple for this row, falling back gracefully when a key
        // column is absent (e.g. the existing batch has a different schema).
        let key = row_key_tuple(batch, key_columns, row).unwrap_or_else(|_| {
            // If key columns are absent, treat this row as non-matching → keep it.
            format!("__missing_key_row_{row}")
        });
        if remove_keys.contains(&key) {
            dropped += 1;
        } else {
            keep_rows.push(row);
        }
    }

    let kept =
        filter_rows(batch, &keep_rows).context("key_filter: failed to build kept sub-batch")?;
    Ok((kept, dropped))
}

/// Concatenate a list of batches into one.  Returns the single batch if the
/// list contains exactly one element; handles the empty-list edge case by
/// returning an error (callers must guard against empty lists).
fn concat_batches_list(batches: &[RecordBatch]) -> Result<RecordBatch> {
    anyhow::ensure!(!batches.is_empty(), "concat_batches_list: empty batch list");
    if batches.len() == 1 {
        return Ok(batches[0].clone());
    }
    let schema = batches[0].schema();
    let columns: Vec<_> = (0..schema.fields().len())
        .map(|col_idx| {
            let arrays: Vec<_> = batches.iter().map(|b| b.column(col_idx).clone()).collect();
            let refs: Vec<&dyn arrow_array::Array> = arrays.iter().map(|a| a.as_ref()).collect();
            arrow::compute::concat(&refs).with_context(|| {
                format!(
                    "concat_batches_list: failed to concat column '{}'",
                    schema.field(col_idx).name()
                )
            })
        })
        .collect::<Result<_>>()?;

    RecordBatch::try_new(schema, columns)
        .context("concat_batches_list: failed to build concatenated RecordBatch")
}

/// Write a [`RecordBatch`] as Parquet files using the Iceberg writer stack.
/// Returns the list of [`DataFile`]s produced.
pub(crate) async fn write_parquet(
    job: &SyncJob,
    table: &Table,
    batch: RecordBatch,
) -> Result<Vec<DataFile>> {
    use std::sync::Arc;

    let meta = table.metadata();
    let full_schema = meta.current_schema();

    // Project the Iceberg schema to only the columns present in the batch.
    // This is necessary for write modes that drop columns before writing
    // (e.g. merge_into strips `_op`; overwrite/upsert project to key-only
    // for equality deletes).  If we hand the full schema to ParquetWriterBuilder
    // it will iterate all field IDs and error when it finds one that is absent
    // from the Arrow batch's struct array ("Field id N not found").
    let schema = batch.schema();
    let batch_col_names: std::collections::HashSet<&str> =
        schema.fields().iter().map(|f| f.name().as_str()).collect();

    let projected_fields: Vec<_> = full_schema
        .as_struct()
        .fields()
        .iter()
        .filter(|f| batch_col_names.contains(f.name.as_str()))
        .cloned()
        .collect();

    let schema: iceberg::spec::SchemaRef = Arc::new(
        iceberg::spec::Schema::builder()
            .with_fields(projected_fields)
            .with_schema_id(full_schema.schema_id())
            .build()
            .context("build projected Iceberg schema for write_parquet")?,
    );

    let loc_gen = DefaultLocationGenerator::new(meta.clone())
        .context("Failed to create location generator")?;

    let name_gen =
        ProductionFileNameGenerator::new("data", Some(job.name.as_str()), DataFileFormat::Parquet);

    let parquet_builder =
        ParquetWriterBuilder::new(WriterProperties::builder().build(), schema.clone());

    let rolling = RollingFileWriterBuilder::new(
        parquet_builder,
        512 * 1024 * 1024,
        table.file_io().clone(),
        loc_gen,
        name_gen,
    );

    let mut writer = DataFileWriterBuilder::new(rolling)
        .build(None)
        .await
        .context("Failed to build Parquet data writer")?;

    writer
        .write(batch)
        .await
        .context("Failed to stream RecordBatch to Parquet writer")?;

    writer
        .close()
        .await
        .context("Failed to finalize and close Parquet data file")
}

// ── Equality-delete helpers ──────────────────────────────────────────────────

/// Emit Iceberg equality-delete files for the given batch.
///
/// Uses `EqualityDeleteFileWriter` keyed on `merge.key_columns`.
/// Readers will suppress any existing rows whose key-column values match
/// a row in the delete file — effectively superseding the old version.
///
/// ## Why equality deletes?
///
/// iceberg-rust 0.9.0 ships only `EqualityDeleteFileWriter` in `base_writer`.
/// Position-delete writers are absent.  Equality deletes are semantically
/// correct for key-based upsert/merge: no data-file scanning is needed.
///
/// ## Schema requirement
///
/// `EqualityDeleteWriterConfig::new` takes the **full table Iceberg schema**
/// and `equality_ids` (the field IDs of the key columns).  It projects the
/// batch internally — the caller does not need to pre-project.
async fn _emit_equality_deletes(
    job: &SyncJob,
    table: &Table,
    batch: &RecordBatch,
    merge_cfg: &MergeConfig,
) -> Result<Vec<DataFile>> {
    if batch.num_rows() == 0 {
        return Ok(vec![]);
    }

    let meta = table.metadata();
    let iceberg_schema: SchemaRef = meta.current_schema().clone();

    // Resolve each key-column name to its Iceberg field ID.
    let equality_ids: Vec<i32> = merge_cfg
        .key_columns
        .iter()
        .map(|col_name| {
            iceberg_schema
                .field_by_name(col_name)
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "key column '{col_name}' not found in Iceberg schema for table '{}'",
                        job.table
                    )
                })
                .map(|field| field.id)
        })
        .collect::<Result<Vec<_>>>()?;

    // Build config — projects full schema to key columns only.
    let eq_config = EqualityDeleteWriterConfig::new(equality_ids.clone(), iceberg_schema.clone())
        .context("build EqualityDeleteWriterConfig")?;

    // Build a projected Iceberg schema containing only the key columns.
    // ParquetWriterBuilder requires an Iceberg SchemaRef; we project from the
    // full schema to the equality-id fields so the writer sees only key cols.
    let projected_iceberg_schema: SchemaRef = {
        use std::sync::Arc;
        let key_fields: Vec<_> = iceberg_schema
            .as_struct()
            .fields()
            .iter()
            .filter(|f| equality_ids.contains(&f.id))
            .cloned()
            .collect();

        Arc::new(
            iceberg::spec::Schema::builder()
                .with_fields(key_fields)
                .with_schema_id(iceberg_schema.schema_id())
                .build()
                .context("build projected Iceberg schema for equality-delete writer")?,
        )
    };

    let loc_gen = DefaultLocationGenerator::new(meta.clone())
        .context("Failed to create location generator for delete files")?;

    let name_gen = ProductionFileNameGenerator::new(
        "delete",
        Some(job.name.as_str()),
        DataFileFormat::Parquet,
    );

    let parquet_builder = ParquetWriterBuilder::new(
        WriterProperties::builder().build(),
        projected_iceberg_schema,
    );

    let rolling = RollingFileWriterBuilder::new(
        parquet_builder,
        64 * 1024 * 1024,
        table.file_io().clone(),
        loc_gen,
        name_gen,
    );

    let mut writer = EqualityDeleteFileWriterBuilder::new(rolling, eq_config)
        .build(None)
        .await
        .context("build equality-delete writer")?;

    // Build a key-only Arrow batch so that `RecordBatchProjector` inside the
    // EqualityDeleteFileWriter only sees the columns it expects.
    //
    // `EqualityDeleteWriterConfig` stores the projected Iceberg schema
    // (key-columns only).  Internally `RecordBatchProjector::project_batch`
    // iterates the projected schema fields in order (field 0, 1, …) and calls
    // `get_column_by_field_index(i)` where `i` is the position in that
    // projected schema — NOT the Iceberg field_id value.  If the Arrow batch
    // contains more columns than the projected schema expects, the projector
    // iterates past the end of the batch and panics with "index out of bounds".
    //
    // Fix: build a batch that contains *only* the key columns, in the same
    // order they appear in `projected_iceberg_schema`, with the correct
    // `PARQUET:field_id` metadata already set (inherited from the incoming
    // batch which has been through `inject_field_ids_lenient`).
    let batch_schema = batch.schema();
    let mut key_fields = Vec::with_capacity(merge_cfg.key_columns.len());
    let mut key_columns_arr = Vec::with_capacity(merge_cfg.key_columns.len());

    for col_name in &merge_cfg.key_columns {
        let idx = batch_schema.index_of(col_name.as_str()).with_context(|| {
            format!("key column '{col_name}' not found in batch for equality-delete projection")
        })?;
        key_fields.push(batch_schema.field(idx).clone());
        key_columns_arr.push(batch.column(idx).clone());
    }

    let key_arrow_schema = std::sync::Arc::new(
        arrow_schema::Schema::new(key_fields).with_metadata(batch_schema.metadata().clone()),
    );

    let key_batch = RecordBatch::try_new(key_arrow_schema, key_columns_arr)
        .context("project batch to key columns for equality-delete writer")?;

    writer
        .write(key_batch)
        .await
        .context("Failed to write to equality-delete writer")?;

    let delete_files = writer
        .close()
        .await
        .context("Failed to close equality-delete writer")?;

    info!(
        job = %job.name,
        delete_files = delete_files.len(),
        rows = batch.num_rows(),
        "emitted equality-delete files"
    );

    Ok(delete_files)
}
// ── Partition-file collection ─────────────────────────────────────────────────

/// Collect data files from the current snapshot whose partition value for
/// `partition_col` matches any of the values in `part_values`.
///
/// When the Iceberg table has no partition spec (unpartitioned), all data
/// files are returned — effectively a full table overwrite.
#[allow(dead_code)]
async fn collect_partition_files(
    table: &Table,
    partition_col: &str,
    part_values: &std::collections::HashSet<String>,
) -> Result<Vec<DataFile>> {
    let snapshot = match table.metadata().current_snapshot() {
        Some(s) => s,
        None => return Ok(vec![]),
    };

    let manifest_list = snapshot
        .load_manifest_list(table.file_io(), &table.metadata_ref())
        .await
        .context("Failed to load manifest list for partition evaluation")?; //

    let mut files: Vec<DataFile> = Vec::new();

    for manifest_entry in manifest_list.entries() {
        let manifest = manifest_entry
            .load_manifest(table.file_io())
            .await
            .with_context(|| "Failed to load manifest file for overwrite strategy".to_string())?; // Dynamic context

        for entry in manifest.entries() {
            let df = entry.data_file();
            if df.content_type() != iceberg::spec::DataContentType::Data {
                continue;
            }

            // Check if the partition record contains our target column value.
            // The partition record is a Struct; we compare its fields by name.
            let partition = df.partition();
            let matches = partition_matches(partition, partition_col, part_values);
            if matches {
                files.push(df.clone());
            }
        }
    }

    Ok(files)
}

/// Returns true if the partition record contains a value for `col` that is in
/// `part_values`.  Falls back to `true` (match all) for unpartitioned tables
/// (empty partition record).
#[allow(dead_code)]
fn partition_matches(
    partition: &iceberg::spec::Struct,
    _col: &str,
    values: &std::collections::HashSet<String>,
) -> bool {
    // An unpartitioned table has an empty partition struct — match all files.
    if partition.fields().is_empty() {
        return true;
    }

    // We can't easily look up by name in iceberg::spec::Struct; we convert the
    // debug representation to a string as a best-effort fallback.
    // For production use, the partition spec schema would be consulted here.
    let repr = format!("{partition:?}");
    values.iter().any(|v| repr.contains(v.as_str()))
}

// ── _op split ────────────────────────────────────────────────────────────────

struct OpSplit {
    inserts: Option<RecordBatch>,
    updates: Option<RecordBatch>,
    deletes: Option<RecordBatch>,
}

/// Split `batch` into insert / update / delete sub-batches by the `_op` column.
///
/// The `_op` column is removed from each sub-batch before it is returned.
fn split_by_op(batch: &RecordBatch) -> Result<OpSplit> {
    let op_idx = batch
        .schema()
        .index_of("_op")
        .context("merge_into: batch must contain an '_op' column (I/U/D)")?;

    let op_col = batch.column(op_idx);
    let op_str = op_col
        .as_any()
        .downcast_ref::<StringArray>()
        .context("merge_into: '_op' column must be of type Utf8/String")?;

    let mut insert_rows: Vec<usize> = Vec::new();
    let mut update_rows: Vec<usize> = Vec::new();
    let mut delete_rows: Vec<usize> = Vec::new();

    for i in 0..op_str.len() {
        if op_str.is_null(i) {
            warn!("merge_into: null _op at row {i} — treating as insert");
            insert_rows.push(i);
            continue;
        }
        match op_str.value(i).to_uppercase().as_str() {
            "I" | "INSERT" => insert_rows.push(i),
            "U" | "UPDATE" => update_rows.push(i),
            "D" | "DELETE" => delete_rows.push(i),
            other => {
                warn!("merge_into: unknown _op '{other}' at row {i} — treating as insert");
                insert_rows.push(i)
            }
        }
    }

    // Remove _op column from sub-batches.
    let without_op = drop_column(batch, op_idx)
        .context("Failed to drop '_op' column from batch during CDC split")?; //

    Ok(OpSplit {
        inserts: filter_rows(&without_op, &insert_rows)
            .context("Failed to extract INSERT sub-batch")?, //
        updates: filter_rows(&without_op, &update_rows)
            .context("Failed to extract UPDATE sub-batch")?, //
        deletes: filter_rows(&without_op, &delete_rows)
            .context("Failed to extract DELETE sub-batch")?, //
    })
}

/// Drop the column at `col_idx` from `batch`.
fn drop_column(batch: &RecordBatch, col_idx: usize) -> Result<RecordBatch> {
    use std::sync::Arc;
    let schema = batch.schema();
    let new_fields: Vec<_> = schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != col_idx)
        .map(|(_, f)| f.clone())
        .collect();
    let new_columns: Vec<_> = batch
        .columns()
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != col_idx)
        .map(|(_, c)| c.clone())
        .collect();
    let new_schema = Arc::new(arrow_schema::Schema::new(new_fields));

    RecordBatch::try_new(new_schema, new_columns).with_context(|| {
        format!(
            "Failed to rebuild RecordBatch after dropping column at index {}",
            col_idx
        )
    }) //
}

/// Build a sub-batch containing only the rows at `row_indices`.
fn filter_rows(batch: &RecordBatch, row_indices: &[usize]) -> Result<Option<RecordBatch>> {
    if row_indices.is_empty() {
        return Ok(None);
    }
    use arrow_array::UInt64Array;
    let indices = UInt64Array::from_iter_values(row_indices.iter().map(|&i| i as u64));
    let columns: Vec<_> = batch
        .columns()
        .iter()
        .enumerate() // Added enumerate to get col index for error tracking
        .map(|(idx, col)| {
            arrow_cast::cast(
                arrow::compute::take(col.as_ref(), &indices, None)
                    .with_context(|| format!("Failed to take rows for column index {}", idx))? //
                    .as_ref(),
                col.data_type(),
            )
            .with_context(|| {
                format!(
                    "Failed to cast taken rows back to original type for column index {}",
                    idx
                )
            }) //
        })
        .collect::<Result<_>>()?;

    Ok(Some(
        RecordBatch::try_new(batch.schema(), columns)
            .context("filter_rows: Failed to instantiate filtered RecordBatch")?, //
    ))
}

/// Concatenate two optional batches into one, or return None if both are None.
fn concat_batches_opt(batches: &[Option<&RecordBatch>]) -> Result<Option<RecordBatch>> {
    let present: Vec<&RecordBatch> = batches.iter().filter_map(|b| *b).collect();
    if present.is_empty() {
        return Ok(None);
    }
    if present.len() == 1 {
        return Ok(Some(present[0].clone()));
    }
    // All must share the same schema.
    let schema = present[0].schema();
    let columns: Vec<_> = (0..schema.fields().len())
        .map(|col_idx| {
            let arrays: Vec<_> = present.iter().map(|b| b.column(col_idx).clone()).collect();
            let refs: Vec<&dyn arrow_array::Array> = arrays.iter().map(|a| a.as_ref()).collect();

            arrow::compute::concat(&refs).with_context(|| {
                format!(
                    "Failed to concatenate arrays for column '{}'",
                    schema.field(col_idx).name()
                )
            }) //
        })
        .collect::<Result<_>>()?;

    Ok(Some(
        RecordBatch::try_new(schema, columns)
            .context("concat_batches_opt: Failed to build final concatenated RecordBatch")?, //
    ))
}

// ── Key-set helpers ───────────────────────────────────────────────────────────

/// Render each row's key-column values as a single string tuple for hashing.
/// e.g. key_columns=["tenant_id","user_id"], row=(7, 42) → "7\x00\x0042"
fn row_key_tuple(batch: &RecordBatch, key_columns: &[String], row: usize) -> Result<String> {
    let mut parts = Vec::with_capacity(key_columns.len());
    for col_name in key_columns {
        let idx = batch
            .schema()
            .index_of(col_name)
            .with_context(|| format!("Key column '{}' not found in batch", col_name))?; //
        let col = batch.column(idx);
        let val = scalar_to_string(col.as_ref(), row);
        parts.push(val);
    }
    Ok(parts.join("\x00\x00"))
}

fn scalar_to_string(array: &dyn arrow_array::Array, row: usize) -> String {
    // use arrow_array::Array;
    if array.is_null(row) {
        return "NULL".to_string();
    }
    if let Some(a) = array.as_any().downcast_ref::<arrow_array::Int64Array>() {
        return a.value(row).to_string();
    }
    if let Some(a) = array.as_any().downcast_ref::<arrow_array::Int32Array>() {
        return a.value(row).to_string();
    }
    if let Some(a) = array.as_any().downcast_ref::<arrow_array::Float64Array>() {
        return a.value(row).to_string();
    }
    if let Some(a) = array.as_any().downcast_ref::<arrow_array::BooleanArray>() {
        return a.value(row).to_string();
    }
    if let Some(a) = array.as_any().downcast_ref::<StringArray>() {
        return a.value(row).to_string();
    }
    if let Some(a) = array
        .as_any()
        .downcast_ref::<arrow_array::TimestampMicrosecondArray>()
    {
        return a.value(row).to_string();
    }
    "UNKNOWN".to_string()
}

/// Build a `HashSet<String>` of key-tuples from a batch.
fn build_key_set(
    batch: &RecordBatch,
    key_columns: &[String],
) -> Result<std::collections::HashSet<String>> {
    let mut set = std::collections::HashSet::new();
    for row in 0..batch.num_rows() {
        let key = row_key_tuple(batch, key_columns, row)
            .with_context(|| format!("Failed to build row key tuple for row index {}", row))?; //
        set.insert(key);
    }
    Ok(set)
}

// ── String column extraction ──────────────────────────────────────────────────

/// Extract unique string values from `col_name` in `batch`.
#[allow(dead_code)]
fn extract_string_column_values(
    batch: &RecordBatch,
    col_name: &str,
) -> Result<std::collections::HashSet<String>> {
    let idx = batch.schema().index_of(col_name).with_context(|| {
        format!(
            "Partition extraction failed: column '{}' not found in batch",
            col_name
        )
    })?; //
    let col = batch.column(idx);
    let mut values = std::collections::HashSet::new();

    match col.data_type() {
        DataType::Utf8 => {
            let arr = col
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("utf8 downcast");
            for i in 0..arr.len() {
                if !arr.is_null(i) {
                    values.insert(arr.value(i).to_string());
                }
            }
        }
        other => {
            // Fallback: stringify via scalar_to_string.
            for i in 0..col.len() {
                values.insert(scalar_to_string(col.as_ref(), i));
            }
            // `other` prints as Debug since DataType implements it natively
            tracing::debug!(
                "partition column '{}' has type {:?} — stringified for matching",
                col_name,
                other
            );
        }
    }

    Ok(values)
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn make_batch(cols: &[(&str, Vec<Option<&str>>)]) -> RecordBatch {
        let fields: Vec<Field> = cols
            .iter()
            .map(|(name, _)| Field::new(*name, DataType::Utf8, true))
            .collect();
        let schema = Arc::new(Schema::new(fields));
        let arrays: Vec<_> = cols
            .iter()
            .map(|(_, vals)| {
                Arc::new(StringArray::from(vals.clone())) as Arc<dyn arrow_array::Array>
            })
            .collect();
        RecordBatch::try_new(schema, arrays).unwrap()
    }

    fn make_int_batch(id_vals: Vec<Option<i64>>, name_vals: Vec<Option<&str>>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(id_vals)) as Arc<dyn arrow_array::Array>,
                Arc::new(StringArray::from(name_vals)) as Arc<dyn arrow_array::Array>,
            ],
        )
        .unwrap()
    }

    // ── split_by_op ──────────────────────────────────────────────────────────

    #[test]
    fn split_by_op_routes_correctly() {
        let batch = make_batch(&[
            ("id", vec![Some("1"), Some("2"), Some("3"), Some("4")]),
            ("_op", vec![Some("I"), Some("U"), Some("D"), Some("I")]),
        ]);

        let split = split_by_op(&batch).unwrap();

        let inserts = split.inserts.unwrap();
        let updates = split.updates.unwrap();
        let deletes = split.deletes.unwrap();

        assert_eq!(inserts.num_rows(), 2, "expected 2 inserts");
        assert_eq!(updates.num_rows(), 1, "expected 1 update");
        assert_eq!(deletes.num_rows(), 1, "expected 1 delete");

        // _op column should be stripped.
        assert!(inserts.schema().index_of("_op").is_err());
        assert!(updates.schema().index_of("_op").is_err());
        assert!(deletes.schema().index_of("_op").is_err());
    }

    #[test]
    fn split_by_op_missing_column_errors() {
        let batch = make_batch(&[("id", vec![Some("1")])]);
        assert!(split_by_op(&batch).is_err());
    }

    #[test]
    fn split_by_op_all_inserts() {
        let batch = make_batch(&[
            ("id", vec![Some("1"), Some("2")]),
            ("_op", vec![Some("I"), Some("I")]),
        ]);
        let split = split_by_op(&batch).unwrap();
        assert_eq!(split.inserts.unwrap().num_rows(), 2);
        assert!(split.updates.is_none());
        assert!(split.deletes.is_none());
    }

    #[test]
    fn split_by_op_all_deletes() {
        let batch = make_batch(&[
            ("id", vec![Some("1"), Some("2")]),
            ("_op", vec![Some("D"), Some("DELETE")]),
        ]);
        let split = split_by_op(&batch).unwrap();
        assert!(split.inserts.is_none());
        assert!(split.updates.is_none());
        assert_eq!(split.deletes.unwrap().num_rows(), 2);
    }

    #[test]
    fn split_by_op_case_insensitive() {
        let batch = make_batch(&[
            ("id", vec![Some("1"), Some("2"), Some("3")]),
            ("_op", vec![Some("insert"), Some("UPDATE"), Some("delete")]),
        ]);
        let split = split_by_op(&batch).unwrap();
        assert_eq!(split.inserts.unwrap().num_rows(), 1);
        assert_eq!(split.updates.unwrap().num_rows(), 1);
        assert_eq!(split.deletes.unwrap().num_rows(), 1);
    }

    // ── filter_rows ──────────────────────────────────────────────────────────

    #[test]
    fn filter_rows_empty_returns_none() {
        let batch = make_batch(&[("id", vec![Some("1"), Some("2")])]);
        assert!(filter_rows(&batch, &[]).unwrap().is_none());
    }

    #[test]
    fn filter_rows_selects_correct_rows() {
        let batch = make_batch(&[("id", vec![Some("a"), Some("b"), Some("c")])]);
        let result = filter_rows(&batch, &[0, 2]).unwrap().unwrap();
        assert_eq!(result.num_rows(), 2);
        let arr = result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(arr.value(0), "a");
        assert_eq!(arr.value(1), "c");
    }

    // ── drop_column ──────────────────────────────────────────────────────────

    #[test]
    fn drop_column_removes_correct_column() {
        let batch = make_batch(&[
            ("a", vec![Some("1")]),
            ("b", vec![Some("2")]),
            ("c", vec![Some("3")]),
        ]);
        let result = drop_column(&batch, 1).unwrap();
        assert_eq!(result.num_columns(), 2);
        assert!(result.schema().index_of("a").is_ok());
        assert!(result.schema().index_of("b").is_err());
        assert!(result.schema().index_of("c").is_ok());
    }

    // ── build_key_set ─────────────────────────────────────────────────────────

    #[test]
    fn build_key_set_single_column() {
        let batch = make_int_batch(
            vec![Some(1), Some(2), Some(3)],
            vec![Some("a"), Some("b"), Some("c")],
        );
        let keys = build_key_set(&batch, &["id".to_string()]).unwrap();
        assert_eq!(keys.len(), 3);
        assert!(keys.contains("1"));
        assert!(keys.contains("2"));
        assert!(keys.contains("3"));
    }

    #[test]
    fn build_key_set_composite_key() {
        let batch = make_batch(&[
            ("tenant_id", vec![Some("t1"), Some("t1"), Some("t2")]),
            ("user_id", vec![Some("u1"), Some("u2"), Some("u1")]),
        ]);
        let keys =
            build_key_set(&batch, &["tenant_id".to_string(), "user_id".to_string()]).unwrap();
        assert_eq!(keys.len(), 3);
        assert!(keys.contains("t1\x00\x00u1"));
        assert!(keys.contains("t1\x00\x00u2"));
        assert!(keys.contains("t2\x00\x00u1"));
    }

    #[test]
    fn build_key_set_missing_column_errors() {
        let batch = make_batch(&[("id", vec![Some("1")])]);
        assert!(build_key_set(&batch, &["nonexistent".to_string()]).is_err());
    }

    // ── concat_batches_opt ────────────────────────────────────────────────────

    #[test]
    fn concat_batches_opt_none_inputs() {
        assert!(concat_batches_opt(&[None, None]).unwrap().is_none());
    }

    #[test]
    fn concat_batches_opt_single() {
        let batch = make_batch(&[("id", vec![Some("1"), Some("2")])]);
        let result = concat_batches_opt(&[Some(&batch), None]).unwrap().unwrap();
        assert_eq!(result.num_rows(), 2);
    }

    #[test]
    fn concat_batches_opt_two_batches() {
        let a = make_batch(&[("id", vec![Some("1"), Some("2")])]);
        let b = make_batch(&[("id", vec![Some("3")])]);
        let result = concat_batches_opt(&[Some(&a), Some(&b)]).unwrap().unwrap();
        assert_eq!(result.num_rows(), 3);
    }

    // ── extract_string_column_values ─────────────────────────────────────────

    #[test]
    fn extract_string_column_values_deduplicates() {
        let batch = make_batch(&[(
            "date",
            vec![Some("2024-01-01"), Some("2024-01-01"), Some("2024-01-02")],
        )]);
        let vals = extract_string_column_values(&batch, "date").unwrap();
        assert_eq!(vals.len(), 2);
        assert!(vals.contains("2024-01-01"));
        assert!(vals.contains("2024-01-02"));
    }

    #[test]
    fn extract_string_column_values_unknown_col_errors() {
        let batch = make_batch(&[("id", vec![Some("1")])]);
        assert!(extract_string_column_values(&batch, "not_a_col").is_err());
    }
}

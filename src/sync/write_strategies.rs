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
//! ### Overwrite (native partition spec)
//!
//! 1. Use Iceberg predicate pushdown (`scan().with_filter`) to read **only**
//!    the target partition's rows — O(partition) instead of O(table).
//! 2. Collect DataFile pointers for all other partitions from the manifest
//!    without reading their rows (O(manifest) pointer copy).
//! 3. COW-rewrite the target partition: survivors + incoming batch.
//! 4. Commit (other-partition DataFiles) + (new target-partition DataFile)
//!    via `fast_append` after drop+recreate.
//!
//! ### Upsert / MergeInto
//!
//! 1. Scan all existing rows from the current Iceberg snapshot.
//! 2. Filter out the rows that the incoming batch logically replaces
//!    (by key columns).
//! 3. Concatenate the surviving existing rows with the new/updated rows.
//! 4. Write the merged result as fresh Parquet data files.
//! 5. Commit only `DataContentType::Data` files via `fast_append`.
//!
//! This avoids delete files entirely and works within the iceberg-rust 0.9 API.
//! Overwrite with native partitioning is O(partition); upsert/merge_into remain
//! O(table) since they require key-based deduplication across all partitions.
use anyhow::{
    Context,
    Result, // , bail
};
use arrow_array::Array;
use arrow_array::{RecordBatch, StringArray};
use arrow_schema::DataType;
use chrono;
use chrono::Datelike;
use futures::TryStreamExt;
use iceberg::{
    expr::{Predicate, Reference},
    spec::{
        DataContentType, DataFile, DataFileFormat, Datum, Literal, PartitionKey, PartitionSpec,
        PrimitiveType, SchemaRef, Struct, Transform,
    },
    table::Table,
    transaction::{ApplyTransactionAction, Transaction},
    writer::{
        IcebergWriter, IcebergWriterBuilder,
        base_writer::{
            data_file_writer::DataFileWriterBuilder,
            equality_delete_writer::{EqualityDeleteFileWriterBuilder, EqualityDeleteWriterConfig},
        },
        file_writer::{ParquetWriterBuilder, rolling_writer::RollingFileWriterBuilder},
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
///    partition spec using a day/month/year/hour transform.  When this is set
///    the scan is **scoped to the target partition** via Iceberg predicate
///    pushdown, avoiding a full-table row read.
/// 2. `job.partition_column` — legacy fallback; used as a plain string equality
///    filter with no native Iceberg partition spec on the table (full scan).
///
/// ## Implementation (partition-scoped COW)
///
/// When `iceberg_partition` is configured **and** the table already has a
/// non-trivial partition spec (i.e. this is not the first run):
///
/// 1. Build an Iceberg predicate that matches only the target partition's
///    column values (e.g. `sale_date IN ('2024-01-15', '2024-01-16')`).
/// 2. Use `table.scan().with_filter(predicate)` to read **only** the rows
///    belonging to the target partition — O(partition) instead of O(table).
/// 3. Collect the DataFiles for **all other partitions** from the manifest
///    without reading their rows (O(manifest) pointer copy).
/// 4. Rewrite only the target partition: COW-filter old rows + append new rows.
/// 5. Return (other-partition DataFiles) + (new target-partition DataFile) so
///    the engine's drop+recreate commit produces a complete snapshot.
///
/// When `iceberg_partition` is **not** configured (legacy `partition_column`
/// mode) the old full-table scan path is used unchanged.
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
    // `incoming_parts_raw` uses the raw Arrow value as a string (e.g. raw µs
    // for timestamp columns) and is used for:
    //   • `build_partition_predicate`  (parses µs as i64 → Datum::long)
    //   • `partition_filter`           (scalar_to_string also yields raw µs)
    //
    // `incoming_parts_canonical` uses the *post-transform* canonical format
    // (e.g. "2024-05-10" for Day, "2024-05" for Month) and is used for:
    //   • `collect_non_partition_files` → `partition_matches` →
    //     `literal_primitive_to_string`, which produces the same canonical form.
    let incoming_parts_raw = extract_string_column_values(&batch, part_col)
        .context("Overwrite: failed to extract incoming partition values")?;

    // ── Route: native Iceberg partition spec vs. legacy string-equality scan ──
    //
    // When `iceberg_partition` is configured and the table has a real partition
    // spec we use predicate pushdown to scope the COW scan to the target
    // partition only.  Other partitions' DataFiles are harvested from the
    // manifest and carried through unchanged — no row reads required for them.
    //
    // The legacy path (partition_column only, no iceberg_partition) falls back
    // to a full-table scan for backwards compatibility.
    let use_native_partition = job.iceberg_partition.is_some()
        && !table.metadata().default_partition_spec().is_unpartitioned();

    if table.metadata().current_snapshot().is_none() {
        // First run — no existing data, plain append.
        debug!(job = %job.name, "overwrite: no existing snapshot — first run, plain append");
        let rows_appended = batch.num_rows();
        let data_files = write_parquet(job, table, batch)
            .await
            .context("Overwrite strategy: failed to write Parquet data (first run)")?;
        return Ok(WritePlan {
            data_files,
            delete_files: vec![],
            rows_appended,
            rows_deleted: 0,
        });
    }

    let (new_partition_file, untouched_files, rows_removed) = if use_native_partition {
        // ── Native partition-scoped COW ───────────────────────────────────────
        //
        // Step 1: Build an Iceberg predicate for the incoming partition values.
        // For a single value this is `col = value`; for multiple values we OR
        // them together.  The scan executor uses the PartitionSpec to prune
        // manifest entries before reading any Parquet, so only files that
        // overlap the target partition are opened.
        let predicate = build_partition_predicate(
            part_col,
            &incoming_parts_raw,
            table.metadata().current_schema(),
        )
        .context("Overwrite: failed to build partition predicate for scan pushdown")?;

        // Step 2: Scan only the target partition's rows.
        let target_rows = scan_table_with_predicate(table, predicate)
            .await
            .context("Overwrite: failed to scan target partition rows")?;

        // Step 3: Collect DataFiles for all OTHER partitions from the manifest.
        // These are passed through unchanged — we never read their rows.
        //
        // IMPORTANT: `partition_matches` compares DataFile partition literals
        // (formatted by `literal_primitive_to_string`) against this set.  For
        // temporal transforms the literal is stored post-transform — e.g. a Day
        // transform yields days-since-epoch rendered as "2024-05-10", NOT the raw
        // microsecond integer.  We must therefore build a *canonical* value set
        // that matches that format, distinct from `incoming_parts_raw` which uses
        // raw Arrow scalar strings.
        let incoming_parts_canonical = extract_canonical_partition_values(
            &batch,
            part_col,
            table.metadata().default_partition_spec(),
            table.metadata().current_schema(),
        )
        .context("Overwrite: failed to extract canonical partition values for manifest scan")?;

        let untouched = collect_non_partition_files(table, part_col, &incoming_parts_canonical)
            .await
            .context("Overwrite: failed to collect non-target-partition DataFiles")?;

        // Step 4: COW-filter the target partition's rows (drop any that are
        // being replaced by the incoming batch), then concat with incoming.
        let mut survivors: Vec<RecordBatch> = Vec::new();
        let mut rows_removed = 0usize;
        for existing_batch in target_rows {
            let (kept, dropped) = partition_filter(&existing_batch, part_col, &incoming_parts_raw)
                .context("Overwrite: failed to filter target partition rows")?;
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
            untouched_files = untouched.len(),
            "overwrite: partition-scoped COW — rewrote target partition only"
        );

        // Step 5: Write only the merged target-partition data as a new Parquet
        // file.  Survivors may be empty if all old rows are being replaced.
        survivors.push(batch);
        let merged =
            concat_batches_list(&survivors).context("Overwrite: failed to concatenate batches")?;
        let new_files = write_parquet(job, table, merged)
            .await
            .context("Overwrite strategy: failed to write Parquet data (partition-scoped)")?;

        (new_files, untouched, rows_removed)
    } else {
        // ── Legacy full-table scan (no native partition spec) ─────────────────
        //
        // For tables created without `iceberg_partition` (legacy
        // `partition_column` only) we keep the original full-table COW scan.
        // The comment in the old implementation noted that the engine's
        // drop+recreate means the plan must carry ALL surviving rows.  That
        // remains true here — untouched_files is empty because we read and
        // rewrite everything inline.
        let existing = scan_table_to_batches(table)
            .await
            .context("Overwrite: failed to scan existing table data (legacy path)")?;

        let mut survivors: Vec<RecordBatch> = Vec::new();
        let mut rows_removed = 0usize;
        for existing_batch in existing {
            let (kept, dropped) = partition_filter(&existing_batch, part_col, &incoming_parts_raw)
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
            "overwrite: full-table COW (legacy partition_column path)"
        );
        survivors.push(batch);
        let merged =
            concat_batches_list(&survivors).context("Overwrite: failed to concatenate batches")?;
        let new_files = write_parquet(job, table, merged)
            .await
            .context("Overwrite strategy: failed to write Parquet data (legacy)")?;

        (new_files, vec![], rows_removed)
    };

    // Combine the untouched other-partition files with the freshly written
    // target-partition file(s).  The engine's drop+recreate then commits this
    // complete set via fast_append, producing a correct new snapshot.
    let mut all_data_files = untouched_files;
    all_data_files.extend(new_partition_file);
    let rows_appended = all_data_files
        .iter()
        .map(|f| f.record_count() as usize)
        .sum();

    Ok(WritePlan {
        data_files: all_data_files,
        delete_files: vec![],
        rows_appended,
        rows_deleted: rows_removed,
    })
}

// ── Partition predicate helpers ───────────────────────────────────────────────

/// Build an Iceberg [`Predicate`] that matches any row whose `col` value is in
/// `values`.
///
/// `values` is a `HashSet<String>` produced by [`extract_string_column_values`]
/// whose format depends on the Arrow column type:
///
/// | Arrow type | String format  | Datum constructor         |
/// |------------|---------------|--------------------------|
/// | Date32     | `"YYYY-MM-DD"` | `Datum::date_from_str`    |
/// | Utf8       | raw string     | `Datum::string`           |
/// | Int32      | decimal digits | `Datum::int`              |
/// | Int64      | decimal digits | `Datum::long`             |
///
/// We look up the column's Iceberg field type from `schema` and dispatch to the
/// correct constructor so the scan executor never has to coerce across types.
/// The error `Can't convert datum from string type to date type` was caused by
/// using `Datum::string` for a column that Iceberg knows as `date`.
fn build_partition_predicate(
    col: &str,
    values: &std::collections::HashSet<String>,
    schema: &iceberg::spec::Schema,
) -> Result<Predicate> {
    anyhow::ensure!(
        !values.is_empty(),
        "build_partition_predicate: incoming batch has no partition values for column '{col}'"
    );

    // Look up the Iceberg field type for this column so we emit the right Datum.
    let field_type = schema
        .field_by_name(col)
        .with_context(|| {
            format!("build_partition_predicate: column '{col}' not found in Iceberg schema")
        })?
        .field_type
        .as_primitive_type()
        .cloned();

    let mut predicates: Vec<Predicate> = Vec::with_capacity(values.len());
    for v in values {
        let datum = match &field_type {
            // Date columns: values are "YYYY-MM-DD" strings from scalar_to_string.
            Some(PrimitiveType::Date) => Datum::date_from_str(v).with_context(|| {
                format!(
                    "build_partition_predicate: failed to parse date value '{v}' for column '{col}'"
                )
            })?,

            // Integer columns: parse and emit the right width.
            Some(PrimitiveType::Int) => {
                let n: i32 = v.parse().with_context(|| {
                    format!(
                        "build_partition_predicate: failed to parse int value '{v}' for column '{col}'"
                    )
                })?;
                Datum::int(n)
            }

            // Long / Timestamp / TimestampTz: parse as i64.
            Some(PrimitiveType::Long)
            | Some(PrimitiveType::Timestamp)
            | Some(PrimitiveType::Timestamptz) => {
                let n: i64 = v.parse().with_context(|| {
                    format!(
                        "build_partition_predicate: failed to parse long value '{v}' for column '{col}'"
                    )
                })?;
                Datum::long(n)
            }

            // Float / Double.
            Some(PrimitiveType::Float) => {
                let f: f32 = v.parse().with_context(|| {
                    format!(
                        "build_partition_predicate: failed to parse float value '{v}' for column '{col}'"
                    )
                })?;
                Datum::float(f)
            }
            Some(PrimitiveType::Double) => {
                let d: f64 = v.parse().with_context(|| {
                    format!(
                        "build_partition_predicate: failed to parse double value '{v}' for column '{col}'"
                    )
                })?;
                Datum::double(d)
            }

            // Boolean.
            Some(PrimitiveType::Boolean) => {
                let b: bool = v.parse().with_context(|| {
                    format!(
                        "build_partition_predicate: failed to parse boolean value '{v}' for column '{col}'"
                    )
                })?;
                Datum::bool(b)
            }

            // String and everything else (UUID, Fixed, Binary, Decimal): treat as string.
            Some(PrimitiveType::String) => Datum::string(v.clone()),
            None => Datum::string(v.clone()),
            _ => Datum::string(v.clone()),
        };
        predicates.push(Reference::new(col).equal_to(datum));
    }

    // Fold into a single OR predicate.
    let combined = predicates
        .into_iter()
        .reduce(|acc, p| acc.or(p))
        .expect("non-empty predicates; reduce always succeeds");

    Ok(combined)
}

/// Scan only the rows that satisfy `predicate` using Iceberg's native predicate
/// pushdown.  The scan engine prunes manifests using the PartitionSpec before
/// opening any Parquet file, so only files that might contain matching rows are
/// read.
async fn scan_table_with_predicate(
    table: &Table,
    predicate: Predicate,
) -> Result<Vec<RecordBatch>> {
    let scan = table
        .scan()
        .with_filter(predicate)
        .build()
        .context("partition-scoped scan: failed to build filtered table scan")?;

    let stream = scan
        .to_arrow()
        .await
        .context("partition-scoped scan: failed to open Arrow stream")?;

    let batches: Vec<RecordBatch> = stream
        .try_collect()
        .await
        .context("partition-scoped scan: failed to collect Arrow batches")?;

    Ok(batches)
}

/// Walk the current snapshot's manifest list and collect [`DataFile`]s whose
/// partition value for `partition_col` is **not** in `target_values`.
///
/// These are the files that belong to partitions other than the one being
/// overwritten.  They are returned as opaque pointers and carried into the new
/// snapshot via `fast_append` after the drop+recreate, so their rows are never
/// read.
///
/// Files with `DataContentType` other than `Data` (e.g. equality-delete files)
/// are skipped because `fast_append` only accepts data files.
async fn collect_non_partition_files(
    table: &Table,
    partition_col: &str,
    target_values: &std::collections::HashSet<String>,
) -> Result<Vec<DataFile>> {
    let snapshot = match table.metadata().current_snapshot() {
        Some(s) => s,
        None => return Ok(vec![]),
    };

    let manifest_list = snapshot
        .load_manifest_list(table.file_io(), &table.metadata_ref())
        .await
        .context("collect_non_partition_files: failed to load manifest list")?;

    let mut files: Vec<DataFile> = Vec::new();

    for manifest_entry in manifest_list.entries() {
        let manifest = manifest_entry
            .load_manifest(table.file_io())
            .await
            .context("collect_non_partition_files: failed to load manifest")?;

        for entry in manifest.entries() {
            let df = entry.data_file();

            // Only carry forward data files — fast_append rejects delete files.
            if df.content_type() != DataContentType::Data {
                continue;
            }

            // Check whether this file belongs to the target partition.
            // We pass the bound PartitionSpec and table schema so that
            // partition_matches can resolve the transform (Day/Month/Year/Hour)
            // for each partition field and format literals correctly — no
            // range heuristics required.
            let spec = table.metadata().default_partition_spec();
            let schema = table.metadata().current_schema();
            if !partition_matches(df.partition(), partition_col, target_values, spec, schema) {
                files.push(df.clone());
            }
        }
    }

    Ok(files)
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

    // Build the partition key first so we have the raw partition value (µs)
    // available for SafeLocationGenerator before constructing the writer stack.
    //
    // iceberg-rust 0.9 bug: DefaultLocationGenerator calls
    // PartitionSpec::partition_to_path → Datum::to_human_string, which hits
    // unreachable!() for Timestamp/Timestamptz variants.  SafeLocationGenerator
    // computes the partition directory from raw µs without touching that path.
    let partition_key = build_partition_key_for_batch(table, &batch, &schema)
        .context("write_parquet: failed to build partition key")?;

    let partition_us = extract_partition_timestamp_us(table, &batch);

    let loc_gen = crate::sync::file_name::SafeLocationGenerator::new(meta, partition_us)
        .context("Failed to create SafeLocationGenerator")?;

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
        .build(partition_key)
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

/// Build a [`PartitionKey`] for the first row of `batch` when the table has a
/// non-empty partition spec, or return `None` for unpartitioned tables.
///
/// For the COW overwrite path all rows in `batch` belong to the same partition
/// (they were filtered / merged by partition value before this call), so
/// sampling the first row is correct.
///
/// Supported source column types: `Date` (Arrow `Date32`), `String`, `Long`,
/// `Int`, `Timestamp`, `Timestamptz`.  The value is taken from the *source*
/// column (before the transform is applied); Iceberg's `PartitionKey` applies
/// the transform internally.
fn build_partition_key_for_batch(
    table: &Table,
    batch: &RecordBatch,
    schema: &iceberg::spec::SchemaRef,
) -> Result<Option<PartitionKey>> {
    use arrow_array::{
        Date32Array, Int32Array, Int64Array, StringArray, TimestampMicrosecondArray,
    };

    let meta = table.metadata();
    let partition_spec = meta.default_partition_spec();

    if partition_spec.is_unpartitioned() {
        return Ok(None);
    }

    if batch.num_rows() == 0 {
        return Ok(None);
    }

    let full_schema = meta.current_schema();
    let mut literals: Vec<Option<Literal>> = Vec::new();

    for field in partition_spec.fields() {
        // Resolve the source column name from the schema.
        let source_field = full_schema.field_by_id(field.source_id).with_context(|| {
            format!(
                "build_partition_key: source field id {} not in schema",
                field.source_id
            )
        })?;
        let col_name = &source_field.name;

        // Find that column in the batch (may be projected — must exist for
        // partition columns since we always SELECT them).
        let col_idx = batch.schema().index_of(col_name).with_context(|| {
            format!("build_partition_key: partition column '{col_name}' missing from batch")
        })?;
        let col = batch.column(col_idx);

        // Extract the first row's value and convert to an Iceberg Literal.
        //
        // IMPORTANT: `PartitionKey.data` must contain the **post-transform
        // result** literal, not the raw source literal.  `transaction.rs` in
        // iceberg-rust 0.9 validates each partition value against the result
        // type of the transform (e.g. `Day(Timestamp)` -> `Date`, stored as
        // `Literal::date(days)`).  Passing `Literal::timestamp(us)` instead
        // fails with "DataInvalid => Partition value is not compatible partition
        // type".
        //
        // Temporal transforms applied here:
        //   Hour   -> Literal::int(hours_since_epoch)
        //   Day    -> Literal::date(days_since_epoch)
        //   Month  -> Literal::int(months_since_epoch)
        //   Year   -> Literal::int(years_since_epoch)
        //
        // For identity / non-temporal transforms the raw source literal is
        // correct because the result type equals the source type.
        let lit = match source_field.field_type.as_primitive_type() {
            Some(PrimitiveType::Timestamp) | Some(PrimitiveType::Timestamptz) => {
                let arr = col
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .with_context(|| {
                        format!(
                            "build_partition_key: column '{col_name}' is not TimestampMicrosecond"
                        )
                    })?;
                let us: i64 = arr.value(0);
                // Apply the transform to produce the result literal.
                match field.transform {
                    Transform::Hour => {
                        let hours = (us / 3_600_000_000) as i32;
                        Literal::int(hours)
                    }
                    Transform::Day => {
                        let days = (us / 86_400_000_000) as i32;
                        Literal::date(days)
                    }
                    Transform::Month => {
                        let days = us / 86_400_000_000;
                        let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                        let d = epoch
                            + chrono::Duration::try_days(days).unwrap_or(chrono::Duration::zero());
                        let months = (d.year() - 1970) * 12 + (d.month() as i32 - 1);
                        Literal::int(months)
                    }
                    Transform::Year => {
                        let days = us / 86_400_000_000;
                        let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                        let d = epoch
                            + chrono::Duration::try_days(days).unwrap_or(chrono::Duration::zero());
                        let years = d.year() - 1970;
                        Literal::int(years)
                    }
                    // Identity or other non-temporal transforms: raw source literal.
                    _ => {
                        if matches!(
                            source_field.field_type.as_primitive_type(),
                            Some(PrimitiveType::Timestamptz)
                        ) {
                            Literal::timestamptz(us)
                        } else {
                            Literal::timestamp(us)
                        }
                    }
                }
            }
            Some(PrimitiveType::Date) => {
                // Arrow Date32 = days since Unix epoch; Iceberg date literal = same.
                let arr = col
                    .as_any()
                    .downcast_ref::<Date32Array>()
                    .with_context(|| {
                        format!("build_partition_key: column '{col_name}' is not Date32")
                    })?;
                Literal::date(arr.value(0))
            }
            Some(PrimitiveType::String) => {
                let arr = col
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .with_context(|| {
                        format!("build_partition_key: column '{col_name}' is not Utf8")
                    })?;
                Literal::string(arr.value(0).to_string())
            }
            Some(PrimitiveType::Long) => {
                let arr = col.as_any().downcast_ref::<Int64Array>().with_context(|| {
                    format!("build_partition_key: column '{col_name}' is not Int64")
                })?;
                Literal::long(arr.value(0))
            }
            Some(PrimitiveType::Int) => {
                let arr = col.as_any().downcast_ref::<Int32Array>().with_context(|| {
                    format!("build_partition_key: column '{col_name}' is not Int32")
                })?;
                Literal::int(arr.value(0))
            }
            other => anyhow::bail!(
                "build_partition_key: unsupported partition source type {:?} for column '{col_name}'",
                other
            ),
        };
        literals.push(Some(lit));
    }

    let partition_data = Struct::from_iter(literals);
    Ok(Some(PartitionKey::new(
        partition_spec.as_ref().clone(),
        schema.clone(),
        partition_data,
    )))
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

    // Equality-delete files are always unpartitioned in our COW model;
    // SafeLocationGenerator with partition_us=0 falls through to DefaultLocationGenerator
    // for unpartitioned specs, so this is safe even if the data table is partitioned.
    let loc_gen = crate::sync::file_name::SafeLocationGenerator::new(meta, 0)
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
            if df.content_type() != DataContentType::Data {
                continue;
            }

            // Check if the partition record contains our target column value.
            // The partition record is a Struct; we compare its fields by name.
            let partition = df.partition();
            let spec = table.metadata().default_partition_spec();
            let schema = table.metadata().current_schema();
            let matches = partition_matches(partition, partition_col, part_values, spec, schema);
            if matches {
                files.push(df.clone());
            }
        }
    }

    Ok(files)
}

/// Returns true if the partition [`Struct`] of a DataFile contains a value for
/// any partition field that matches one of `values`.
///
/// This is used by [`collect_non_partition_files`] to identify files belonging
/// to the target partition that is being overwritten.  Files that match are
/// excluded from the untouched-files list (they belong to the target partition
/// and will be replaced by the COW rewrite).  Files that do not match are
/// returned as untouched.
///
/// For an unpartitioned table (empty partition struct) no files are excluded
/// because there is no partition boundary — in that case the caller should not
/// be using this function.
///
/// ## Matching strategy
///
/// The Iceberg [`Struct`] stores partition values as `Option<Literal>`.  The
/// bound [`PartitionSpec`] tells us the `Transform` for each field (Day, Month,
/// Year, Hour, Identity, …), which we use to format each literal into the same
/// canonical string that [`scalar_to_string`] produces for the source Arrow
/// column — no range heuristics required.
fn partition_matches(
    partition: &iceberg::spec::Struct,
    _col: &str,
    values: &std::collections::HashSet<String>,
    spec: &PartitionSpec,
    schema: &iceberg::spec::Schema,
) -> bool {
    // An unpartitioned table has an empty partition struct — conservatively
    // match (caller should not include these in untouched files).
    if partition.fields().is_empty() {
        return true;
    }

    // Zip the DataFile's partition struct fields with the PartitionSpec fields
    // so we know the Transform for each position.
    for (lit_opt, part_field) in partition.fields().iter().zip(spec.fields().iter()) {
        // Resolve the source column's Iceberg type so we can format correctly.
        let source_type = schema
            .field_by_id(part_field.source_id)
            .and_then(|f| f.field_type.as_primitive_type().cloned());

        let lit_str = match lit_opt {
            Some(iceberg::spec::Literal::Primitive(prim)) => {
                literal_primitive_to_string(prim, &part_field.transform, source_type.as_ref())
            }
            Some(_) => continue, // non-primitive partition literals — skip
            None => continue,    // null partition value — skip
        };
        if values.contains(&lit_str) {
            return true;
        }
    }

    false
}

/// Convert an Iceberg [`PrimitiveLiteral`] to the same canonical string format
/// used by [`scalar_to_string`] so that the two ends of the matching pipeline
/// produce identical strings.
///
/// The `transform` parameter — taken from the bound [`PartitionSpec`] — is the
/// authoritative source of truth for how the literal should be interpreted.
/// No range heuristics are used: `Transform::Day` always means days-since-epoch
/// → "YYYY-MM-DD", `Transform::Month` → "YYYY-MM", etc.  The `source_type` is
/// only needed for `Transform::Identity` to distinguish a plain `Int` column
/// from a `Date` column whose wire value is also stored as `Int(days)`.
fn literal_primitive_to_string(
    prim: &iceberg::spec::PrimitiveLiteral,
    transform: &Transform,
    source_type: Option<&PrimitiveType>,
) -> String {
    use iceberg::spec::PrimitiveLiteral;

    /// Format days-since-epoch as "YYYY-MM-DD", matching scalar_to_string's Date32 output.
    fn days_to_date_str(days: i64) -> String {
        let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        match epoch.checked_add_signed(chrono::Duration::days(days)) {
            Some(d) => d.format("%Y-%m-%d").to_string(),
            None => days.to_string(),
        }
    }

    /// Format months-since-epoch as "YYYY-MM".
    fn months_to_month_str(months: i64) -> String {
        let total_months = 1970 * 12 + months;
        let year = total_months.div_euclid(12);
        let month = total_months.rem_euclid(12) + 1;
        format!("{year:04}-{month:02}")
    }

    /// Format years-since-epoch as "YYYY".
    fn years_to_year_str(years: i64) -> String {
        format!("{:04}", 1970 + years)
    }

    match transform {
        // Day transform: partition value is Int (days since epoch).
        // scalar_to_string formats Date32 as "YYYY-MM-DD" — match that exactly.
        Transform::Day => match prim {
            PrimitiveLiteral::Int(v) => days_to_date_str(*v as i64),
            PrimitiveLiteral::Long(v) => days_to_date_str(*v),
            _ => format!("{prim:?}"),
        },

        Transform::Month => match prim {
            PrimitiveLiteral::Int(v) => months_to_month_str(*v as i64),
            PrimitiveLiteral::Long(v) => months_to_month_str(*v),
            _ => format!("{prim:?}"),
        },

        Transform::Year => match prim {
            PrimitiveLiteral::Int(v) => years_to_year_str(*v as i64),
            PrimitiveLiteral::Long(v) => years_to_year_str(*v),
            _ => format!("{prim:?}"),
        },

        // Hour: raw integer (hours since epoch).
        Transform::Hour => match prim {
            PrimitiveLiteral::Int(v) => v.to_string(),
            PrimitiveLiteral::Long(v) => v.to_string(),
            _ => format!("{prim:?}"),
        },

        // Identity: the partition value IS the source value — use the source
        // column type to decide how to format it.
        Transform::Identity => match (prim, source_type) {
            (PrimitiveLiteral::Int(v), Some(PrimitiveType::Date)) => days_to_date_str(*v as i64),
            (PrimitiveLiteral::Long(v), Some(PrimitiveType::Date)) => days_to_date_str(*v),
            (PrimitiveLiteral::Boolean(b), _) => b.to_string(),
            (PrimitiveLiteral::Int(v), _) => v.to_string(),
            (PrimitiveLiteral::Long(v), _) => v.to_string(),
            (PrimitiveLiteral::Float(f), _) => f.to_string(),
            (PrimitiveLiteral::Double(d), _) => d.to_string(),
            (PrimitiveLiteral::String(s), _) => s.clone(),
            (PrimitiveLiteral::Int128(v), _) => v.to_string(),
            (PrimitiveLiteral::UInt128(v), _) => v.to_string(),
            (PrimitiveLiteral::AboveMax, _) => "AboveMax".to_string(),
            (PrimitiveLiteral::BelowMin, _) => "BelowMin".to_string(),
            (PrimitiveLiteral::Binary(_), _) => String::new(),
        },

        // Bucket / Truncate / Void / unknown: opaque integer — emit raw value.
        _ => match prim {
            PrimitiveLiteral::Int(v) => v.to_string(),
            PrimitiveLiteral::Long(v) => v.to_string(),
            PrimitiveLiteral::String(s) => s.clone(),
            _ => format!("{prim:?}"),
        },
    }
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
    // Date32: days since Unix epoch — convert to "YYYY-MM-DD" for consistent
    // string comparison in partition_filter.
    if let Some(a) = array.as_any().downcast_ref::<arrow_array::Date32Array>() {
        let days = a.value(row);
        let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        let date = epoch + chrono::Duration::days(days as i64);
        return date.format("%Y-%m-%d").to_string();
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

/// Extract unique *canonical* partition value strings from `col_name` in
/// `batch`, applying the same post-transform formatting used by
/// [`literal_primitive_to_string`] so that the resulting set can be compared
/// directly against DataFile partition literals loaded from Iceberg manifests.
///
/// For temporal transforms the canonical format is:
/// - `Day`   → "YYYY-MM-DD" (days since epoch → date string)
/// - `Month` → "YYYY-MM"    (months since epoch → year-month string)
/// - `Year`  → "YYYY"       (years since epoch → year string)
/// - `Hour`  → raw integer string (hours since epoch)
///
/// For all other types/transforms the raw `scalar_to_string` value is used,
/// which matches the string format stored in partition literals for identity /
/// non-temporal transforms.
///
/// This is intentionally *different* from [`extract_string_column_values`],
/// which returns raw Arrow scalars (e.g. raw µs for timestamps).  The raw form
/// is correct for `partition_filter` and `build_partition_predicate`; the
/// canonical form is required for `collect_non_partition_files` /
/// `partition_matches`.
fn extract_canonical_partition_values(
    batch: &RecordBatch,
    col_name: &str,
    spec: &PartitionSpec,
    schema: &iceberg::spec::Schema,
) -> Result<std::collections::HashSet<String>> {
    use arrow_array::TimestampMicrosecondArray;

    let idx = batch.schema().index_of(col_name).with_context(|| {
        format!(
            "extract_canonical_partition_values: column '{}' not found in batch",
            col_name
        )
    })?;
    let col = batch.column(idx);

    // Find the transform for this column by matching the source field id.
    let source_field = schema.field_by_name(col_name);
    let transform = source_field.and_then(|sf| {
        spec.fields()
            .iter()
            .find(|pf| pf.source_id == sf.id)
            .map(|pf| pf.transform)
    });

    let source_type = source_field.and_then(|sf| sf.field_type.as_primitive_type().cloned());

    let mut values = std::collections::HashSet::new();

    // For timestamp columns with a temporal transform, produce the same string
    // that `literal_primitive_to_string` produces for the stored partition literal.
    if matches!(
        source_type,
        Some(PrimitiveType::Timestamp) | Some(PrimitiveType::Timestamptz)
    ) && let Some(arr) = col.as_any().downcast_ref::<TimestampMicrosecondArray>()
    {
        let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        for i in 0..arr.len() {
            if arr.is_null(i) {
                continue;
            }
            let us: i64 = arr.value(i);
            let s = match &transform {
                Some(Transform::Day) => {
                    let days = us / 86_400_000_000_i64;
                    match epoch.checked_add_signed(chrono::Duration::days(days)) {
                        Some(d) => d.format("%Y-%m-%d").to_string(),
                        None => days.to_string(),
                    }
                }
                Some(Transform::Month) => {
                    let days = us / 86_400_000_000_i64;
                    let d = epoch
                        + chrono::Duration::try_days(days).unwrap_or(chrono::Duration::zero());
                    let total_months = (d.year() as i64 - 1970) * 12 + (d.month() as i64 - 1);
                    let abs_months = 1970i64 * 12 + total_months;
                    let year = abs_months.div_euclid(12);
                    let month = abs_months.rem_euclid(12) + 1;
                    format!("{year:04}-{month:02}")
                }
                Some(Transform::Year) => {
                    let days = us / 86_400_000_000_i64;
                    let d = epoch
                        + chrono::Duration::try_days(days).unwrap_or(chrono::Duration::zero());
                    format!("{:04}", d.year())
                }
                Some(Transform::Hour) => {
                    // Hours since epoch — stored as raw int in the literal.
                    let hours = us / 3_600_000_000_i64;
                    hours.to_string()
                }
                // Identity or other: fall through to scalar_to_string below.
                _ => scalar_to_string(col.as_ref(), i),
            };
            values.insert(s);
        }
        return Ok(values);
    }

    // Non-timestamp columns (Date32, String, Int64, Int32, …): the stored
    // partition literal IS the source value, so scalar_to_string already
    // matches literal_primitive_to_string for Identity/Day-on-Date transforms.
    for i in 0..col.len() {
        values.insert(scalar_to_string(col.as_ref(), i));
    }

    Ok(values)
}

// ── Unit tests ────────────────────────────────────────────────────────────────

/// Extract the raw microsecond timestamp of the first row's partition column.
///
/// Returns `0` for unpartitioned tables or when the partition source column
/// is not a timestamp type.  Used by [`SafeLocationGenerator`] to compute the
/// partition directory path without calling `Datum::to_human_string`.
fn extract_partition_timestamp_us(table: &iceberg::table::Table, batch: &RecordBatch) -> i64 {
    use arrow_array::TimestampMicrosecondArray;
    use iceberg::spec::PrimitiveType;

    let meta = table.metadata();
    let spec = meta.default_partition_spec();
    if spec.is_unpartitioned() || batch.num_rows() == 0 {
        return 0;
    }

    let full_schema = meta.current_schema();
    for field in spec.fields() {
        let is_temporal = matches!(
            field.transform,
            Transform::Hour | Transform::Day | Transform::Month | Transform::Year
        );
        if !is_temporal {
            continue;
        }

        let source_field = match full_schema.field_by_id(field.source_id) {
            Some(f) => f,
            None => continue,
        };
        let is_ts = matches!(
            source_field.field_type.as_primitive_type(),
            Some(PrimitiveType::Timestamp) | Some(PrimitiveType::Timestamptz)
        );
        if !is_ts {
            continue;
        }

        // Found the first temporal timestamp partition field.
        let col_name = &source_field.name;
        if let Ok(idx) = batch.schema().index_of(col_name) {
            let col = batch.column(idx);
            if let Some(arr) = col.as_any().downcast_ref::<TimestampMicrosecondArray>()
                && !arr.is_null(0)
            {
                return arr.value(0);
            }
        }
    }
    0
}

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

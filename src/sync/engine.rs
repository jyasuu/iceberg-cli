//! Core sync engine.
//!
//! ## Batch atomicity guarantee
//! Each batch is committed as its own Iceberg snapshot.  If the process dies
//! mid-batch the partial Parquet file is abandoned (no manifest points to it)
//! and the watermark / cursor are not advanced, so the next run re-fetches
//! that batch.  This gives at-least-once delivery; downstream deduplication
//! on a primary key is recommended for exactly-once semantics.
//!
//! ## Cursor-based pagination
//! When `cursor_column` is configured the engine wraps the user's SQL:
//! ```sql
//! SELECT * FROM (<user_sql>) _q
//! WHERE <cursor_col> > :_cursor
//! ORDER BY <cursor_col>
//! LIMIT <batch_size>
//! ```
//! The cursor starts at `i64::MIN` and is advanced to the max value seen in
//! each committed batch.  This avoids the OFFSET-drift problem (rows inserted
//! mid-run shifting subsequent pages).
//!
//! When no cursor is configured, the engine falls back to LIMIT/OFFSET.
//!
//! ## Write strategy dispatch
//! Each batch is routed through one of four write strategies controlled by
//! `job.write_mode`:
//!   - `append`     → `plan_append`     (fast_append, no read)
//!   - `overwrite`  → `plan_overwrite`  (replace_data_files by partition)
//!   - `upsert`     → `plan_upsert`     (position-delete + append)
//!   - `merge_into` → `plan_merge_into` (_op-routed delete + append)
//!
//! ## Retry with exponential backoff
//! Transient write failures are retried according to the job's `RetryConfig`
//! before the engine propagates the error.
//!
//! ## Schema evolution
//! When `schema_evolution.allow_add_columns` is true, columns present in the
//! source query result but absent from the Iceberg table are detected and
//! surfaced as a warning.
//!
//! ## Dry-run mode
//! When `dry_run = true` the engine logs what it *would* do but skips all
//! writes and catalog mutations.

use std::collections::HashMap;
use std::time::Duration;

use anyhow::{Context, Result};
use arrow_array::RecordBatch;
use arrow_schema::Schema as ArrowSchema;
use chrono::{DateTime, Utc};
use iceberg::{
    Catalog, TableCreation, TableIdent,
    spec::UnboundPartitionSpec,
    transaction::{ApplyTransactionAction, Transaction},
};
use tracing::{info, warn};

use crate::config::{
    // IcebergPartitionConfig,
    CursorType,
    RetryConfig,
    SchemaEvolutionConfig,
    SyncJob,
    SyncMode,
    WatermarkType,
    WriteMode,
};
use crate::sync::{
    // file_name::ProductionFileNameGenerator,
    metadata::{RunSummary, build_metadata_updates, read_watermark},
    postgres::{
        SqlValue, connect as pg_connect, max_int_in_batch, max_text_in_batch,
        max_timestamp_in_batch, query_to_batch,
    },
    write_strategies::{
        apply_plan_to_transaction, plan_append, plan_merge_into, plan_overwrite, plan_upsert,
    },
};

use crate::sync::{
    pagination::{build_paged_sql, table_ident},
    partition::{build_partition_spec, unbound_spec_preserving_ids},
    schema::{arrow_schema_to_iceberg, inject_field_ids_lenient},
};

// ── Public entry point ────────────────────────────────────────────────────────

pub struct SyncEngine<'a, C: Catalog> {
    pub catalog: &'a C,
    /// When true, skip all writes and catalog mutations.
    pub dry_run: bool,
}

impl<'a, C: Catalog> SyncEngine<'a, C> {
    pub fn new(catalog: &'a C) -> Self {
        Self {
            catalog,
            dry_run: false,
        }
    }

    pub fn with_dry_run(catalog: &'a C) -> Self {
        Self {
            catalog,
            dry_run: true,
        }
    }

    /// Run a single sync job end-to-end with retry.
    pub async fn run_job(
        &self,
        job: &SyncJob,
        pg_dsn: &str,
        extra_params: Option<HashMap<String, SqlValue>>,
        retry: &RetryConfig,
    ) -> Result<RunSummary> {
        // Every log event emitted anywhere inside this job (including nested
        // async calls) will carry job=<name> automatically.
        let span = tracing::info_span!(
            "job",
            job = %job.name,
            mode = ?job.mode,
            write_mode = ?job.write_mode,
            source = %job.source,
            table = %job.table,
        );
        let _enter = span.enter();

        let mut last_err = None;
        let mut delay_ms = retry.initial_delay_ms as f64;

        for attempt in 1..=retry.max_attempts {
            match self.run_job_once(job, pg_dsn, extra_params.clone()).await {
                Ok(summary) => return Ok(summary),
                Err(e) => {
                    last_err = Some(e);
                    if attempt < retry.max_attempts {
                        warn!(
                            attempt,
                            max = retry.max_attempts,
                            delay_ms,
                            "Batch failed — retrying"
                        );
                        tokio::time::sleep(Duration::from_millis(delay_ms as u64)).await;
                        delay_ms *= retry.backoff_multiplier;
                    }
                }
            }
        }

        Err(last_err.unwrap())
    }

    // ── Inner run (single attempt) ────────────────────────────────────────────

    async fn run_job_once(
        &self,
        job: &SyncJob,
        pg_dsn: &str,
        extra_params: Option<HashMap<String, SqlValue>>,
    ) -> Result<RunSummary> {
        let pg = pg_connect(pg_dsn)
            .await
            .context("Failed to connect to source database")?;
        let ident =
            table_ident(&job.namespace, &job.table).context("Failed to parse table identity")?;

        // ── 1. Resolve watermark ──────────────────────────────────────────────
        let watermark: Option<DateTime<Utc>> = match job.mode {
            SyncMode::Incremental => {
                if let Some(col) = &job.watermark_column {
                    read_watermark(self.catalog, &ident, col).await?
                } else {
                    None
                }
            }
            SyncMode::Full => None,
        };

        info!(
            watermark = ?watermark,
            dry_run = self.dry_run,
            "Starting sync job"
        );

        // ── 2. Build SQL parameters ───────────────────────────────────────────
        let mut params: HashMap<String, SqlValue> = extra_params.unwrap_or_default();

        // Bind :watermark with the correct Postgres type.
        // TIMESTAMPTZ columns require DateTime<Utc>; plain TIMESTAMP columns
        // require NaiveDateTime.  Sending the wrong type produces:
        //   "cannot convert between the Rust type chrono::datetime::DateTime<Utc>
        //    and the Postgres type 'timestamp'"
        let wm_value = watermark.unwrap_or(DateTime::UNIX_EPOCH);
        let wm_sql_val = match job.watermark_type {
            WatermarkType::Timestamptz => {
                tracing::debug!(watermark = ?wm_value, "Binding :watermark as TIMESTAMPTZ");
                SqlValue::Timestamp(wm_value)
            }
            WatermarkType::Timestamp => {
                let naive = wm_value.naive_utc();
                tracing::debug!(watermark = ?naive, "Binding :watermark as TIMESTAMP (no tz)");
                SqlValue::TimestampNoTz(naive)
            }
        };
        params.insert("watermark".to_string(), wm_sql_val);

        // ── 3. Fetch & write in batches ───────────────────────────────────────
        let mut total_rows: usize = 0;
        let mut new_watermark: Option<DateTime<Utc>> = watermark;

        let mut cursor_value: i64 = i64::MIN;
        let mut cursor_text_value: String = String::new();
        let mut offset: usize = 0;

        loop {
            if job.cursor_column.is_some() {
                // Bind :_cursor with the correct Postgres type.
                // Use cursor_type: text for TEXT/VARCHAR cursor columns to avoid:
                //   "cannot convert between the Rust type i64 and the Postgres type 'text'"
                match job.cursor_type {
                    CursorType::Int => {
                        tracing::debug!(cursor = cursor_value, "Binding :_cursor as Int(i64)");
                        params.insert("_cursor".to_string(), SqlValue::Int(cursor_value));
                    }
                    CursorType::Text => {
                        tracing::debug!(cursor = %cursor_text_value, "Binding :_cursor as Text");
                        params.insert(
                            "_cursor".to_string(),
                            SqlValue::Text(cursor_text_value.clone()),
                        );
                    }
                }
            }

            let paged_sql = build_paged_sql(job, offset);
            let batch = query_to_batch(&pg, &paged_sql, &params)
                .await
                .with_context(|| format!("Failed to fetch batch at offset {}", offset))?;

            match batch {
                None => break,
                Some(mut rb) => {
                    let n = rb.num_rows();
                    if n == 0 {
                        break;
                    }

                    if let Some(col) = &job.watermark_column
                        && let Some(ts) = max_timestamp_in_batch(&rb, col)
                    {
                        new_watermark = Some(match new_watermark {
                            Some(prev) => prev.max(ts),
                            None => ts,
                        });
                    }

                    if job.cursor_column.is_some() {
                        // Advance cursor using the synthetic _pgcursor column
                        // injected by build_paged_sql. This sentinel is always
                        // present regardless of whether the user SQL SELECTs
                        // the cursor column explicitly.
                        match job.cursor_type {
                            CursorType::Int => {
                                if let Some(max_id) = max_int_in_batch(&rb, "_pgcursor") {
                                    cursor_value = max_id;
                                }
                            }
                            CursorType::Text => {
                                if let Some(max_str) = max_text_in_batch(&rb, "_pgcursor") {
                                    cursor_text_value = max_str;
                                }
                            }
                        }
                        // Strip the _pgcursor sentinel before writing to Iceberg;
                        // it is not part of the destination schema.
                        rb = drop_sentinel_column(rb)?;
                    } else {
                        offset += n;
                    }

                    if self.dry_run {
                        info!(
                            write_mode = ?job.write_mode,
                            rows = n,
                            "[dry-run] would commit batch (skipped)"
                        );
                    } else {
                        self.write_batch_atomic(job, rb, &new_watermark, n)
                            .await
                            .with_context(|| {
                                format!(
                                    "Write batch (cursor={cursor_value}, offset={offset}) \
                                 for job '{}'",
                                    job.name
                                )
                            })?;
                    }

                    total_rows += n;
                    info!(
                        rows = n,
                        total_rows,
                        write_mode = ?job.write_mode,
                        "Batch committed"
                    );

                    if n < job.batch_size {
                        break;
                    }
                }
            }
        }

        info!(
            total_rows,
            write_mode = ?job.write_mode,
            "Job complete"
        );

        Ok(RunSummary {
            job_name: job.name.clone(),
            rows_written: total_rows,
            new_watermark,
        })
    }

    // ── Atomic batch write ────────────────────────────────────────────────────

    async fn write_batch_atomic(
        &self,
        job: &SyncJob,
        batch: RecordBatch,
        watermark: &Option<DateTime<Utc>>,
        rows_written: usize,
    ) -> Result<()> {
        let ident = table_ident(&job.namespace, &job.table)?;

        // For merge_into, `_op` is a CDC routing signal that must never land in
        // Iceberg.  Derive the Iceberg table schema from a version of the batch
        // that has `_op` removed so it is never auto-created as a column.
        // The full batch (including `_op`) is passed to plan_merge_into so that
        // split_by_op can route rows; split_by_op strips `_op` from each
        // sub-batch before any file is written.
        let batch_for_schema = if job.write_mode == WriteMode::MergeInto {
            strip_column_if_present(batch.clone(), "_op")?
        } else {
            batch.clone()
        };

        self.ensure_table(&ident, &batch_for_schema, job).await?;
        if job.schema_evolution.allow_add_columns {
            self.evolve_schema(&ident, &batch_for_schema, &job.schema_evolution)
                .await?;
        }

        let table = self
            .catalog
            .load_table(&ident)
            .await
            .with_context(|| format!("Failed to load Iceberg table '{}'", ident))?;
        let schema = table.metadata().current_schema().clone();

        // Inject Iceberg field IDs into the batch columns.  For merge_into we
        // inject IDs only into columns that exist in the Iceberg schema (i.e.
        // skip `_op`); plan_merge_into receives the annotated batch, calls
        // split_by_op to route rows by `_op`, and strips it from every
        // sub-batch before write_parquet / emit_equality_deletes are called.
        let batch = inject_field_ids_lenient(batch, &schema)
            .context("inject Iceberg field IDs into batch")?;

        // ── Dispatch to write strategy ────────────────────────────────────────
        let plan = match &job.write_mode {
            WriteMode::Append => plan_append(job, &table, batch)
                .await
                .context("Failed during Append planning"),
            WriteMode::Overwrite => plan_overwrite(job, &table, batch)
                .await
                .context("Failed during Overwrite planning"),
            WriteMode::Upsert => plan_upsert(job, &table, batch)
                .await
                .context("Failed during Upsert planning"),
            WriteMode::MergeInto => plan_merge_into(job, &table, batch)
                .await
                .context("Failed during MergeInto planning"),
        }?;

        let rows_appended = plan.rows_appended;
        let rows_deleted = plan.rows_deleted;

        // ── COW snapshot replacement ───────────────────────────────────────────
        //
        // `fast_append` always stacks new manifest entries *on top of* the
        // existing snapshot's manifest list.  For copy-on-write modes the plan
        // has already merged surviving + new rows into fresh Parquet files.
        // If we just fast-appended those files the old data files would remain
        // in the snapshot, causing duplicate rows.
        //
        // Work-around within the iceberg-rust 0.9 API (which exposes only
        // `fast_append`): drop the table and recreate it with the same schema
        // and properties before committing, so the new snapshot starts from an
        // empty manifest list.  The plan's DataFiles then become the complete
        // set of files in the fresh snapshot.
        //
        // ## Native-partition overwrite exception
        //
        // When `plan_overwrite` runs with a real Iceberg partition spec it
        // returns the COMPLETE set of DataFiles needed for the new snapshot:
        //   - the freshly written target-partition file(s), AND
        //   - the existing DataFile pointers for all other partitions
        //     (collected from the manifest without reading their rows).
        //
        // This means drop+recreate is still required to clear the old snapshot,
        // but the other-partition files are passed through as opaque DataFile
        // pointers — no row reads — so the partition layout is fully preserved.
        //
        // For upsert and merge_into the plan always contains all surviving rows
        // rewritten as a single file, so the same drop+recreate logic applies.
        //
        // Append never needs drop+recreate (it stacks on top correctly).
        let needs_cow_replace =
            job.write_mode != WriteMode::Append && table.metadata().current_snapshot().is_some();

        let table = if needs_cow_replace {
            // Preserve schema and existing metadata properties (watermarks, etc.)
            // across the drop+recreate so we don't lose commit history metadata.
            let preserved_schema = table.metadata().current_schema().clone();
            let existing_props: std::collections::HashMap<String, String> = table
                .metadata()
                .properties()
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();

            // Preserve the existing partition spec so it survives the
            // drop+recreate cycle.  On the very first run the spec is
            // unpartitioned (empty fields) and we fall back to building one
            // from the job's `iceberg_partition` config if present.
            let existing_spec = table.metadata().default_partition_spec();
            let preserved_spec: Option<UnboundPartitionSpec> = if !existing_spec.fields().is_empty()
            {
                // Reconstruct the UnboundPartitionSpec manually rather than
                // using `From<PartitionSpec>`.  The `From` impl sets each
                // field's `field_id` to `None`, which serde serialises as
                // `"field-id": null`.  The Iceberg REST OpenAPI spec defines
                // field-id as optional-but-not-nullable, so the Java REST
                // server (Jackson) rejects `null` with:
                //   JsonMappingException: Cannot parse to an integer value: field-id: null
                // Preserving the existing field_id as `Some(id)` keeps the
                // JSON well-formed.  We also set spec_id for the same reason
                // (same `skip_serializing_if` omission on that field).
                let unbound = unbound_spec_preserving_ids(existing_spec.as_ref());
                Some(unbound)
            } else {
                // No existing spec — build one from job config if available.
                job.iceberg_partition.as_ref().and_then(|ip| {
                    build_partition_spec(&preserved_schema, &ip.column, &ip.transform).ok()
                })
            };

            // Drop the stale table.
            self.catalog
                .drop_table(&ident)
                .await
                .with_context(|| format!("COW replace: failed to drop table '{}'", ident))?;

            // Rebuild an Arrow schema from the Iceberg schema so we can call
            // ensure_table, which takes a RecordBatch for schema inference.
            // We recreate the table directly to preserve the exact Iceberg
            // schema (including field IDs) rather than re-inferring from Arrow.
            let builder = TableCreation::builder()
                .name(ident.name().to_string())
                .schema((*preserved_schema).clone())
                .properties(existing_props);

            let creation = if let Some(spec) = preserved_spec {
                builder.partition_spec(spec).build()
            } else {
                builder.build()
            };
            let ns = ident.namespace();
            self.catalog
                .create_table(ns, creation)
                .await
                .with_context(|| format!("COW replace: failed to recreate table '{}'", ident))?
        } else {
            table
        };

        // ── Commit data + metadata atomically ─────────────────────────────────
        let meta_updates =
            build_metadata_updates(job.watermark_column.as_deref(), *watermark, rows_written);

        let tx = Transaction::new(&table);
        let tx = apply_plan_to_transaction(&job.write_mode, plan, tx)?;

        let mut props_action = tx.update_table_properties();
        for (k, v) in meta_updates {
            props_action = props_action.set(k, v);
        }
        let tx = props_action.apply(tx)?;
        tx.commit(self.catalog)
            .await
            .with_context(|| format!("Failed to commit transaction for job '{}'", job.name))?;

        info!(
            write_mode = ?job.write_mode,
            rows_appended,
            rows_deleted,
            "Atomic batch committed"
        );

        Ok(())
    }

    // ── Schema evolution ──────────────────────────────────────────────────────

    async fn evolve_schema(
        &self,
        ident: &TableIdent,
        batch: &RecordBatch,
        _cfg: &SchemaEvolutionConfig,
    ) -> Result<()> {
        let table = self
            .catalog
            .load_table(ident)
            .await
            .with_context(|| format!("load_table for schema evolution: {ident}"))?;
        let iceberg_schema = table.metadata().current_schema();

        let batch_schema = batch.schema();
        let new_column_names: Vec<String> = batch_schema
            .fields()
            .iter()
            .filter(|f| {
                iceberg_schema
                    .as_struct()
                    .fields()
                    .iter()
                    .all(|icf| icf.name != *f.name())
            })
            .map(|f| f.name().clone())
            .collect();

        if new_column_names.is_empty() {
            return Ok(());
        }

        warn!(
            table = %ident,
            columns = ?new_column_names,
            "Schema evolution: new source columns detected. \
             Apply DDL manually and re-run to include them."
        );
        Ok(())
    }

    // ── Table auto-creation ───────────────────────────────────────────────────

    async fn ensure_table(
        &self,
        ident: &TableIdent,
        batch: &RecordBatch,
        job: &SyncJob,
    ) -> Result<()> {
        if self.catalog.table_exists(ident).await? {
            return Ok(());
        }

        warn!(table = %ident, "Table not found — creating from batch schema");

        let ns = ident.namespace();
        if !self.catalog.namespace_exists(ns).await? {
            self.catalog.create_namespace(ns, HashMap::new()).await?;
        }

        let schema = arrow_schema_to_iceberg(batch.schema_ref())?;
        // Attach a real Iceberg partition spec when `iceberg_partition` is set.
        // This ensures the table is created with a native day/month/year/hour
        // transform rather than being left unpartitioned and relying on the
        // legacy `partition_column` row-filter path.
        let resolved_spec =
            job.iceberg_partition.as_ref().and_then(|ip| {
                match build_partition_spec(&schema, &ip.column, &ip.transform) {
                    Ok(spec) => {
                        tracing::info!(
                            table = %ident,
                            column = %ip.column,
                            transform = %ip.transform,
                            "Attaching Iceberg partition spec to new table"
                        );
                        Some(spec)
                    }
                    Err(e) => {
                        tracing::warn!(
                            table = %ident,
                            column = %ip.column,
                            error = %e,
                            "Failed to build partition spec; creating table unpartitioned"
                        );
                        None
                    }
                }
            });

        // 2. Set up the base builder
        let builder = TableCreation::builder()
            .name(ident.name().to_string())
            .schema(schema.clone());

        // 3. Build it based on whether we successfully resolved a spec
        let creation = if let Some(spec) = resolved_spec {
            builder.partition_spec(spec).build()
        } else {
            builder.build()
        };
        self.catalog.create_table(ns, creation).await?;
        Ok(())
    }
}

// ── Parallel execution ────────────────────────────────────────────────────────

pub async fn run_jobs_parallel<C: Catalog + Sync>(
    engine: &SyncEngine<'_, C>,
    jobs: &[&SyncJob],
    source_dsn_map: &HashMap<String, String>,
    retry_map: &HashMap<String, RetryConfig>,
    parallelism: usize,
) -> Vec<(String, Result<RunSummary>)> {
    use std::sync::Arc;
    use tokio::sync::Semaphore;

    let sem = Arc::new(Semaphore::new(parallelism));
    let mut results = Vec::new();

    for &job in jobs {
        let _permit = sem.acquire().await;
        let dsn = match source_dsn_map.get(&job.source) {
            Some(d) => d.as_str(),
            None => {
                results.push((
                    job.name.clone(),
                    Err(anyhow::anyhow!("Source '{}' not found", job.source)),
                ));
                continue;
            }
        };
        let retry = retry_map.get(&job.name).cloned().unwrap_or_default();
        let r = engine.run_job(job, dsn, None, &retry).await;
        results.push((job.name.clone(), r));
    }

    results
}

/// Removes the internal `_pgcursor` column from a RecordBatch if it exists.
/// This is used to strip the synthetic cursor sentinel before writing to Iceberg.
fn drop_sentinel_column(batch: RecordBatch) -> Result<RecordBatch> {
    let schema = batch.schema();

    // Check if the sentinel column exists in the schema
    match schema.index_of("_pgcursor") {
        Ok(idx) => {
            let mut fields = schema.fields().to_vec();
            fields.remove(idx);

            let mut columns = batch.columns().to_vec();
            columns.remove(idx);

            // Reconstruct the schema and the batch without the sentinel column
            let new_schema = std::sync::Arc::new(
                ArrowSchema::new(fields).with_metadata(schema.metadata().clone()),
            );

            RecordBatch::try_new(new_schema, columns)
                .context("Failed to rebuild RecordBatch after dropping sentinel column")
        }
        // If the column isn't found, just return the batch as-is
        Err(_) => Ok(batch),
    }
}

/// Remove a named column from a [`RecordBatch`] if it exists, returning the
/// batch unchanged if the column is absent.
///
/// Used to strip CDC-only columns (e.g. `_op` for `merge_into`) from the
/// batch before table auto-creation and schema injection, so they never
/// become permanent Iceberg columns.
fn strip_column_if_present(batch: RecordBatch, col_name: &str) -> Result<RecordBatch> {
    let schema = batch.schema();
    match schema.index_of(col_name) {
        Ok(idx) => {
            let mut fields = schema.fields().to_vec();
            fields.remove(idx);
            let mut columns = batch.columns().to_vec();
            columns.remove(idx);
            let new_schema = std::sync::Arc::new(
                ArrowSchema::new(fields).with_metadata(schema.metadata().clone()),
            );
            RecordBatch::try_new(new_schema, columns)
                .context("Failed to rebuild RecordBatch after stripping column")
        }
        Err(_) => Ok(batch),
    }
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    // use crate::config::MergeConfig;

    fn _base_job() -> SyncJob {
        SyncJob {
            name: "test_job".into(),
            source: "pg".into(),
            destination: "wh".into(),
            namespace: "ns".into(),
            table: "tbl".into(),
            group: None,
            sql: "SELECT id, val FROM events WHERE ts > :watermark ORDER BY ts".into(),
            watermark_column: Some("ts".into()),
            watermark_type: crate::config::WatermarkType::Timestamptz,
            cursor_column: None,
            cursor_type: crate::config::CursorType::Int,
            depends_on: None,
            batch_size: 100,
            mode: SyncMode::Incremental,
            schema_evolution: Default::default(),
            retry: None,
            write_mode: WriteMode::Append,
            partition_column: None,
            iceberg_partition: None,
            merge: None,
        }
    }
}

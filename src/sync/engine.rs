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
    Catalog, NamespaceIdent, TableCreation, TableIdent,
    spec::{
        // DataFileFormat,
        NestedField,
        PrimitiveType,
        Schema as IcebergSchema,
        Transform,
        Type,
        UnboundPartitionField,
        UnboundPartitionSpec,
    },
    transaction::{ApplyTransactionAction, Transaction},
};
use tracing::{info, warn};

use crate::config::{
    // IcebergPartitionConfig,
    RetryConfig,
    SchemaEvolutionConfig,
    SyncJob,
    SyncMode,
    WriteMode,
};
use crate::sync::{
    // file_name::ProductionFileNameGenerator,
    metadata::{RunSummary, build_metadata_updates, read_watermark},
    postgres::{
        SqlValue, connect as pg_connect, max_int_in_batch, max_timestamp_in_batch, query_to_batch,
    },
    write_strategies::{
        apply_plan_to_transaction, plan_append, plan_merge_into, plan_overwrite, plan_upsert,
    },
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
        let mut last_err = None;
        let mut delay_ms = retry.initial_delay_ms as f64;

        for attempt in 1..=retry.max_attempts {
            match self.run_job_once(job, pg_dsn, extra_params.clone()).await {
                Ok(summary) => return Ok(summary),
                Err(e) => {
                    last_err = Some(e);
                    if attempt < retry.max_attempts {
                        warn!(
                            job = %job.name,
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
            job = %job.name,
            mode = ?job.mode,
            write_mode = ?job.write_mode,
            watermark = ?watermark,
            dry_run = self.dry_run,
            "Starting sync job"
        );

        // ── 2. Build SQL parameters ───────────────────────────────────────────
        let mut params: HashMap<String, SqlValue> = extra_params.unwrap_or_default();
        if let Some(wm) = watermark {
            params.insert("watermark".to_string(), SqlValue::Timestamp(wm));
        } else {
            params.insert(
                "watermark".to_string(),
                SqlValue::Timestamp(DateTime::UNIX_EPOCH),
            );
        }

        // ── 3. Fetch & write in batches ───────────────────────────────────────
        let mut total_rows: usize = 0;
        let mut new_watermark: Option<DateTime<Utc>> = watermark;

        let mut cursor_value: i64 = i64::MIN;
        let mut offset: usize = 0;

        loop {
            if job.cursor_column.is_some() {
                params.insert("_cursor".to_string(), SqlValue::Int(cursor_value));
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
                        if let Some(max_id) = max_int_in_batch(&rb, "_pgcursor") {
                            cursor_value = max_id;
                        }
                        // Strip the _pgcursor sentinel before writing to Iceberg;
                        // it is not part of the destination schema.
                        rb = drop_sentinel_column(rb)?;
                    } else {
                        offset += n;
                    }

                    if self.dry_run {
                        info!(
                            job = %job.name,
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
                        job = %job.name,
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
            job = %job.name,
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
        // has already merged surviving + new rows into a single fresh Parquet
        // file.  If we just fast-appended that file the old data files would
        // remain in the snapshot, causing duplicate rows.
        //
        // Work-around within the iceberg-rust 0.9 API (which exposes only
        // `fast_append`): drop the table and recreate it with the same schema
        // and properties before committing, so the new snapshot starts from an
        // empty manifest list.  The COW merged file then becomes the one-and-only
        // data file in the fresh snapshot.
        //
        // This is safe because the COW plan already contains all surviving rows.
        let table = if job.write_mode != WriteMode::Append
            && table.metadata().current_snapshot().is_some()
        {
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
                // Table already has a real partition spec — convert back to
                // unbound so TableCreation can accept it.  The
                // `From<PartitionSpec> for UnboundPartitionSpec` impl
                // retains source_id, name, and transform for each field
                // while setting field_id to None (catalog re-assigns).
                let unbound: UnboundPartitionSpec = existing_spec.as_ref().clone().into();
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
            job = %job.name,
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

// ── Pagination SQL builder ────────────────────────────────────────────────────

fn build_paged_sql(job: &SyncJob, offset: usize) -> String {
    if let Some(col) = &job.cursor_column {
        // Wrap the user SQL in a subquery that guarantees the cursor column is
        // present in the result set under the sentinel name `_pgcursor`.
        // This ensures keyset pagination works even when the user SQL does not
        // SELECT the cursor column explicitly (e.g. it appears only in ORDER BY).
        //
        // The outer wrapper filters and orders on `_pgcursor`, which is always
        // the cursor column value regardless of whether the user already SELECTed
        // it under its own name.
        format!(
            "SELECT * FROM (\
             SELECT _i.*, _i.{col} AS _pgcursor \
             FROM ({inner}) _i\
             ) _o \
             WHERE _pgcursor > :_cursor \
             ORDER BY _pgcursor \
             LIMIT {limit}",
            inner = job.sql,
            col = col,
            limit = job.batch_size,
        )
    } else {
        format!(
            "SELECT * FROM ({}) _q LIMIT {} OFFSET {}",
            job.sql, job.batch_size, offset
        )
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn table_ident(namespace: &str, table: &str) -> Result<TableIdent> {
    let ns = NamespaceIdent::from_strs(namespace.split('.').collect::<Vec<_>>())
        .map_err(|e| anyhow::anyhow!("Invalid namespace '{namespace}': {e}"))?;
    Ok(TableIdent::new(ns, table.to_string()))
}

// ── Partition spec builder ────────────────────────────────────────────────────

/// Build an [`UnboundPartitionSpec`] for a single temporal column.
///
/// Resolves the column name to its Iceberg field ID, selects the appropriate
/// [`Transform`], and returns the spec ready to pass to [`TableCreation`].
///
/// ## Supported transforms
///
/// | `transform_str` | [`Transform`] variant |
/// |-----------------|----------------------|
/// | `"day"`         | `Transform::Day`     |
/// | `"month"`       | `Transform::Month`   |
/// | `"year"`        | `Transform::Year`    |
/// | `"hour"`        | `Transform::Hour`    |
///
/// ## Errors
///
/// Returns an error if `column` is not present in `schema` or if
/// `transform_str` is not one of the supported values.
pub(crate) fn build_partition_spec(
    schema: &IcebergSchema,
    column: &str,
    transform_str: &str,
) -> Result<UnboundPartitionSpec> {
    // Resolve the column name to its Iceberg field ID.
    let field = schema.as_struct().field_by_name(column).ok_or_else(|| {
        anyhow::anyhow!(
            "iceberg_partition: column '{}' not found in schema \
                 (available: {})",
            column,
            schema
                .as_struct()
                .fields()
                .iter()
                .map(|f| f.name.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        )
    })?;
    let source_id = field.id;

    let transform = match transform_str {
        "day" => Transform::Day,
        "month" => Transform::Month,
        "year" => Transform::Year,
        "hour" => Transform::Hour,
        other => anyhow::bail!(
            "iceberg_partition: unsupported transform '{}'; \
             valid values are: day, month, year, hour",
            other
        ),
    };

    // Partition field name convention: "<column>_<transform>"
    // e.g. "sale_date_day", "event_ts_month".
    let field_name = format!("{column}_{transform_str}");

    let spec = UnboundPartitionSpec::builder()
        .add_partition_fields(vec![UnboundPartitionField {
            source_id,
            field_id: None, // assigned by the catalog
            name: field_name,
            transform,
        }])
        .context("Failed to build UnboundPartitionSpec")?
        .build();

    Ok(spec)
}

#[allow(dead_code)]
fn inject_field_ids(batch: RecordBatch, schema: &IcebergSchema) -> Result<RecordBatch> {
    use arrow_schema::Field;
    use std::sync::Arc;

    let old_arrow = batch.schema();
    let mut new_fields: Vec<Field> = Vec::with_capacity(old_arrow.fields().len());

    for arrow_field in old_arrow.fields().iter() {
        let iceberg_field = schema
            .as_struct()
            .fields()
            .iter()
            .find(|f| f.name == *arrow_field.name())
            .with_context(|| {
                format!(
                    "Column '{}' in query result has no matching field in Iceberg schema",
                    arrow_field.name()
                )
            })?;

        let mut meta = arrow_field.metadata().clone();
        meta.insert("PARQUET:field_id".to_string(), iceberg_field.id.to_string());
        new_fields.push(arrow_field.as_ref().clone().with_metadata(meta));
    }

    let new_schema =
        Arc::new(ArrowSchema::new(new_fields).with_metadata(old_arrow.metadata().clone()));
    RecordBatch::try_new(new_schema, batch.columns().to_vec())
        .context("Rebuild RecordBatch with injected field IDs")
}

/// Like [`inject_field_ids`] but skips columns not found in the Iceberg
/// schema instead of returning an error.
///
/// Used for `merge_into` batches that carry a transient `_op` routing column
/// which is deliberately absent from the Iceberg table schema.  The `_op`
/// column is stripped from every sub-batch inside `plan_merge_into` via
/// `split_by_op` before any file write occurs, so the missing field ID is
/// never observed by the Parquet or equality-delete writers.
fn inject_field_ids_lenient(batch: RecordBatch, schema: &IcebergSchema) -> Result<RecordBatch> {
    use arrow_schema::Field;
    use std::sync::Arc;

    let old_arrow = batch.schema();
    let mut new_fields: Vec<Field> = Vec::with_capacity(old_arrow.fields().len());

    for arrow_field in old_arrow.fields().iter() {
        match schema
            .as_struct()
            .fields()
            .iter()
            .find(|f| f.name == *arrow_field.name())
        {
            Some(iceberg_field) => {
                let mut meta = arrow_field.metadata().clone();
                meta.insert("PARQUET:field_id".to_string(), iceberg_field.id.to_string());
                new_fields.push(arrow_field.as_ref().clone().with_metadata(meta));
            }
            None => {
                // Column not in Iceberg schema — keep it as-is (no field_id).
                // Expected for transient columns like `_op` in merge_into mode.
                tracing::debug!(
                    column = %arrow_field.name(),
                    "inject_field_ids_lenient: skipping column absent from Iceberg schema"
                );
                new_fields.push(arrow_field.as_ref().clone());
            }
        }
    }

    let new_schema =
        Arc::new(ArrowSchema::new(new_fields).with_metadata(old_arrow.metadata().clone()));
    RecordBatch::try_new(new_schema, batch.columns().to_vec())
        .context("Rebuild RecordBatch with injected field IDs (lenient)")
}

fn arrow_type_to_iceberg(dt: &arrow_schema::DataType) -> Type {
    use arrow_schema::DataType;
    match dt {
        DataType::Int8 | DataType::Int16 | DataType::Int32 => Type::Primitive(PrimitiveType::Int),
        DataType::Int64 => Type::Primitive(PrimitiveType::Long),
        DataType::Float32 => Type::Primitive(PrimitiveType::Float),
        DataType::Float64 => Type::Primitive(PrimitiveType::Double),
        DataType::Boolean => Type::Primitive(PrimitiveType::Boolean),
        DataType::Date32 | DataType::Date64 => Type::Primitive(PrimitiveType::Date),
        DataType::Timestamp(_, Some(_)) => Type::Primitive(PrimitiveType::Timestamptz),
        DataType::Timestamp(_, None) => Type::Primitive(PrimitiveType::Timestamp),
        _ => Type::Primitive(PrimitiveType::String),
    }
}

fn arrow_schema_to_iceberg(arrow: &ArrowSchema) -> Result<IcebergSchema> {
    let mut fields = Vec::new();
    for (idx, f) in arrow.fields().iter().enumerate() {
        let field_id = (idx + 1) as i32;
        let iceberg_t = arrow_type_to_iceberg(f.data_type());
        fields.push(NestedField::optional(field_id, f.name(), iceberg_t).into());
    }
    IcebergSchema::builder()
        .with_fields(fields)
        .with_schema_id(1)
        .build()
        .context("Failed to build Iceberg schema from Arrow schema")
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

    fn base_job() -> SyncJob {
        SyncJob {
            name: "test_job".into(),
            source: "pg".into(),
            destination: "wh".into(),
            namespace: "ns".into(),
            table: "tbl".into(),
            group: None,
            sql: "SELECT id, val FROM events WHERE ts > :watermark ORDER BY ts".into(),
            watermark_column: Some("ts".into()),
            cursor_column: None,
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

    // ── build_paged_sql ──────────────────────────────────────────────────────

    #[test]
    fn paged_sql_cursor_mode_syntax() {
        let job = SyncJob {
            cursor_column: Some("id".into()),
            ..base_job()
        };
        let sql = build_paged_sql(&job, 0);
        assert!(
            sql.contains("WHERE _pgcursor > :_cursor"),
            "cursor filter: {sql}"
        );
        assert!(sql.contains("ORDER BY _pgcursor"), "cursor order: {sql}");
        assert!(sql.contains("LIMIT 100"), "limit: {sql}");
        assert!(!sql.contains("OFFSET"), "no OFFSET in cursor mode: {sql}");
    }

    #[test]
    fn paged_sql_cursor_mode_wraps_subquery() {
        let job = SyncJob {
            cursor_column: Some("id".into()),
            ..base_job()
        };
        let sql = build_paged_sql(&job, 0);
        assert!(
            sql.starts_with("SELECT * FROM ("),
            "subquery wrapper: {sql}"
        );
        assert!(sql.contains(") _o"), "subquery alias: {sql}");
    }

    #[test]
    fn paged_sql_offset_mode_zero() {
        let sql = build_paged_sql(&base_job(), 0);
        assert!(sql.contains("LIMIT 100"), "limit: {sql}");
        assert!(sql.contains("OFFSET 0"), "offset zero: {sql}");
        assert!(!sql.contains(":_cursor"), "no cursor: {sql}");
    }

    #[test]
    fn paged_sql_offset_mode_advances() {
        let sql = build_paged_sql(&base_job(), 300);
        assert!(sql.contains("OFFSET 300"), "offset advances: {sql}");
    }

    // ── arrow_type_to_iceberg ────────────────────────────────────────────────

    #[test]
    fn type_mapping_integers() {
        use arrow_schema::DataType;
        assert!(matches!(
            arrow_type_to_iceberg(&DataType::Int32),
            Type::Primitive(PrimitiveType::Int)
        ));
        assert!(matches!(
            arrow_type_to_iceberg(&DataType::Int16),
            Type::Primitive(PrimitiveType::Int)
        ));
        assert!(matches!(
            arrow_type_to_iceberg(&DataType::Int64),
            Type::Primitive(PrimitiveType::Long)
        ));
    }

    #[test]
    fn type_mapping_floats() {
        use arrow_schema::DataType;
        assert!(matches!(
            arrow_type_to_iceberg(&DataType::Float32),
            Type::Primitive(PrimitiveType::Float)
        ));
        assert!(matches!(
            arrow_type_to_iceberg(&DataType::Float64),
            Type::Primitive(PrimitiveType::Double)
        ));
    }

    #[test]
    fn type_mapping_bool_date_timestamp() {
        use arrow_schema::{DataType, TimeUnit};
        assert!(matches!(
            arrow_type_to_iceberg(&DataType::Boolean),
            Type::Primitive(PrimitiveType::Boolean)
        ));
        assert!(matches!(
            arrow_type_to_iceberg(&DataType::Date32),
            Type::Primitive(PrimitiveType::Date)
        ));
        assert!(matches!(
            arrow_type_to_iceberg(&DataType::Timestamp(TimeUnit::Microsecond, None)),
            Type::Primitive(PrimitiveType::Timestamp)
        ));
        assert!(matches!(
            arrow_type_to_iceberg(&DataType::Timestamp(
                TimeUnit::Microsecond,
                Some("UTC".into())
            )),
            Type::Primitive(PrimitiveType::Timestamptz)
        ));
    }

    #[test]
    fn type_mapping_unknown_falls_back_to_string() {
        use arrow_schema::DataType;
        assert!(matches!(
            arrow_type_to_iceberg(&DataType::Binary),
            Type::Primitive(PrimitiveType::String)
        ));
        assert!(matches!(
            arrow_type_to_iceberg(&DataType::LargeBinary),
            Type::Primitive(PrimitiveType::String)
        ));
    }

    // ── arrow_schema_to_iceberg ──────────────────────────────────────────────

    #[test]
    fn schema_to_iceberg_assigns_sequential_ids() {
        use arrow_schema::{DataType, Field, Schema};

        let arrow = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("score", DataType::Float64, true),
        ]);
        let iceberg = arrow_schema_to_iceberg(&arrow).unwrap();
        let fields: Vec<_> = iceberg.as_struct().fields().iter().collect();

        assert_eq!(fields.len(), 3);
        assert_eq!(fields[0].id, 1, "id field");
        assert_eq!(fields[1].id, 2, "name field");
        assert_eq!(fields[2].id, 3, "score field");
        assert_eq!(fields[0].name, "id");
        assert_eq!(fields[1].name, "name");
        assert_eq!(fields[2].name, "score");
    }

    #[test]
    fn schema_to_iceberg_single_field() {
        use arrow_schema::{DataType, Field, Schema};
        let arrow = Schema::new(vec![Field::new("x", DataType::Boolean, true)]);
        let iceberg = arrow_schema_to_iceberg(&arrow).unwrap();
        assert_eq!(iceberg.as_struct().fields().len(), 1);
        assert_eq!(iceberg.as_struct().fields()[0].id, 1);
    }

    // ── inject_field_ids ─────────────────────────────────────────────────────

    #[test]
    fn inject_field_ids_adds_parquet_metadata() {
        use arrow_array::{Int64Array, RecordBatch, StringArray};
        use arrow_schema::{DataType, Field, Schema};
        use iceberg::spec::{NestedField, Schema as IcebergSchema, Type};
        use std::sync::Arc;

        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            arrow_schema,
            vec![
                Arc::new(Int64Array::from(vec![1i64, 2])) as _,
                Arc::new(StringArray::from(vec!["a", "b"])) as _,
            ],
        )
        .unwrap();

        let iceberg_schema = IcebergSchema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::optional(42, "id", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::optional(43, "name", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .unwrap();

        let result = inject_field_ids(batch, &iceberg_schema).unwrap();
        assert_eq!(
            result
                .schema()
                .field_with_name("id")
                .unwrap()
                .metadata()
                .get("PARQUET:field_id")
                .unwrap(),
            "42"
        );
        assert_eq!(
            result
                .schema()
                .field_with_name("name")
                .unwrap()
                .metadata()
                .get("PARQUET:field_id")
                .unwrap(),
            "43"
        );
    }

    #[test]
    fn inject_field_ids_preserves_row_data() {
        use arrow_array::{Int64Array, RecordBatch};
        use arrow_schema::{DataType, Field, Schema};
        use iceberg::spec::{NestedField, Schema as IcebergSchema, Type};
        use std::sync::Arc;

        let arrow_schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            arrow_schema,
            vec![Arc::new(Int64Array::from(vec![10i64, 20, 30])) as _],
        )
        .unwrap();

        let iceberg_schema = IcebergSchema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::optional(1, "v", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .unwrap();

        let result = inject_field_ids(batch, &iceberg_schema).unwrap();
        assert_eq!(result.num_rows(), 3);
        let arr = result
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(arr.value(0), 10);
        assert_eq!(arr.value(1), 20);
        assert_eq!(arr.value(2), 30);
    }

    #[test]
    fn inject_field_ids_errors_on_missing_column() {
        use arrow_array::{Int64Array, RecordBatch};
        use arrow_schema::{DataType, Field, Schema};
        use iceberg::spec::{NestedField, Schema as IcebergSchema, Type};
        use std::sync::Arc;

        let arrow_schema = Arc::new(Schema::new(vec![Field::new(
            "ghost",
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            arrow_schema,
            vec![Arc::new(Int64Array::from(vec![1i64])) as _],
        )
        .unwrap();

        // Iceberg schema has no "ghost" field.
        let iceberg_schema = IcebergSchema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .unwrap();

        assert!(
            inject_field_ids(batch, &iceberg_schema).is_err(),
            "should error when column has no Iceberg field"
        );
    }

    // ── build_partition_spec ─────────────────────────────────────────────────

    fn schema_with_fields(fields: Vec<(&str, arrow_schema::DataType)>) -> IcebergSchema {
        use arrow_schema::{Field, Schema};
        let arrow = Schema::new(
            fields
                .into_iter()
                .map(|(name, dt)| Field::new(name, dt, true))
                .collect::<Vec<_>>(),
        );
        arrow_schema_to_iceberg(&arrow).unwrap()
    }

    #[test]
    fn partition_spec_day_on_date_column() {
        use arrow_schema::DataType;
        let schema = schema_with_fields(vec![
            ("id", DataType::Int64),
            ("sale_date", DataType::Date32),
            ("amount", DataType::Float64),
        ]);

        let spec = build_partition_spec(&schema, "sale_date", "day").unwrap();
        let fields = spec.fields();

        assert_eq!(fields.len(), 1, "one partition field");
        assert_eq!(fields[0].name, "sale_date_day");
        assert_eq!(fields[0].source_id, 2, "sale_date is field id 2 (1-based)");
        assert!(
            matches!(fields[0].transform, Transform::Day),
            "transform should be Day, got {:?}",
            fields[0].transform
        );
    }

    #[test]
    fn partition_spec_month_on_timestamp_column() {
        use arrow_schema::{DataType, TimeUnit};
        let schema = schema_with_fields(vec![
            ("event_ts", DataType::Timestamp(TimeUnit::Microsecond, None)),
            ("user_id", DataType::Int64),
        ]);

        let spec = build_partition_spec(&schema, "event_ts", "month").unwrap();
        let fields = spec.fields();

        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].name, "event_ts_month");
        assert_eq!(fields[0].source_id, 1, "event_ts is field id 1");
        assert!(matches!(fields[0].transform, Transform::Month));
    }

    #[test]
    fn partition_spec_year_on_timestamptz_column() {
        use arrow_schema::{DataType, TimeUnit};
        let schema = schema_with_fields(vec![(
            "created_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        )]);

        let spec = build_partition_spec(&schema, "created_at", "year").unwrap();
        assert_eq!(spec.fields()[0].name, "created_at_year");
        assert!(matches!(spec.fields()[0].transform, Transform::Year));
    }

    #[test]
    fn partition_spec_hour_transform() {
        use arrow_schema::{DataType, TimeUnit};
        let schema = schema_with_fields(vec![(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        )]);

        let spec = build_partition_spec(&schema, "ts", "hour").unwrap();
        assert_eq!(spec.fields()[0].name, "ts_hour");
        assert!(matches!(spec.fields()[0].transform, Transform::Hour));
    }

    #[test]
    fn partition_spec_field_id_is_correct_for_third_column() {
        // Ensures source_id = positional field ID (1-based from arrow_schema_to_iceberg)
        use arrow_schema::DataType;
        let schema = schema_with_fields(vec![
            ("a", DataType::Int64),
            ("b", DataType::Utf8),
            ("event_date", DataType::Date32),
            ("d", DataType::Float64),
        ]);

        let spec = build_partition_spec(&schema, "event_date", "day").unwrap();
        assert_eq!(
            spec.fields()[0].source_id,
            3,
            "event_date is the 3rd field, so source_id should be 3"
        );
    }

    #[test]
    fn partition_spec_error_on_missing_column() {
        use arrow_schema::DataType;
        let schema = schema_with_fields(vec![("id", DataType::Int64)]);
        let err = build_partition_spec(&schema, "nonexistent", "day").unwrap_err();
        assert!(
            err.to_string().contains("nonexistent"),
            "error should name the missing column: {err}"
        );
    }

    #[test]
    fn partition_spec_error_on_unsupported_transform() {
        use arrow_schema::DataType;
        let schema = schema_with_fields(vec![("dt", DataType::Date32)]);
        let err = build_partition_spec(&schema, "dt", "bucket").unwrap_err();
        assert!(
            err.to_string().contains("bucket"),
            "error should name the bad transform: {err}"
        );
    }

    #[test]
    fn partition_spec_field_name_convention() {
        // Field name must be "<column>_<transform>" for the partition-scoped
        // scan to find it correctly via Reference::new(partition_field_name).
        use arrow_schema::DataType;
        let schema = schema_with_fields(vec![("order_date", DataType::Date32)]);
        let spec = build_partition_spec(&schema, "order_date", "month").unwrap();
        assert_eq!(
            spec.fields()[0].name,
            "order_date_month",
            "partition field name must follow <column>_<transform> convention"
        );
    }
}

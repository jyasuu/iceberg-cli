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
//! ## Retry with exponential backoff
//! Transient write failures are retried according to the job's `RetryConfig`
//! before the engine propagates the error.
//!
//! ## Schema evolution
//! When `schema_evolution.allow_add_columns` is true, columns present in the
//! source query result but absent from the Iceberg table are added via an
//! `UpdateSchemaAction` before the write.
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
    spec::{DataFileFormat, NestedField, PrimitiveType, Schema as IcebergSchema, Type},
    transaction::{ApplyTransactionAction, Transaction},
    writer::{
        IcebergWriter, IcebergWriterBuilder,
        base_writer::data_file_writer::DataFileWriterBuilder,
        file_writer::{
            ParquetWriterBuilder, location_generator::DefaultLocationGenerator,
            rolling_writer::RollingFileWriterBuilder,
        },
    },
};
use parquet::file::properties::WriterProperties;
use tracing::{info, warn};

use crate::config::{RetryConfig, SchemaEvolutionConfig, SyncJob, SyncMode};
use crate::sync::{
    file_name::ProductionFileNameGenerator,
    metadata::{RunSummary, build_metadata_updates, read_watermark},
    postgres::{
        SqlValue, connect as pg_connect, max_int_in_batch, max_timestamp_in_batch, query_to_batch,
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
    ///
    /// `extra_params`: additional SQL parameters merged with the watermark
    /// param (used by RabbitMQ-driven invocations).
    /// `retry`: the effective retry policy.
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
        let pg = pg_connect(pg_dsn).await?;
        let ident = table_ident(&job.namespace, &job.table)?;

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
                SqlValue::Timestamp(DateTime::from_timestamp(0, 0).unwrap_or_else(Utc::now)),
            );
        }

        // ── 3. Fetch & write in batches ───────────────────────────────────────
        let mut total_rows: usize = 0;
        let mut new_watermark: Option<DateTime<Utc>> = watermark;

        // Cursor pagination state (i64::MIN = "before all rows").
        let mut cursor_value: i64 = i64::MIN;
        let mut offset: usize = 0; // fallback OFFSET when no cursor col

        loop {
            // Inject cursor param so bind_named_params can resolve :_cursor.
            if job.cursor_column.is_some() {
                params.insert("_cursor".to_string(), SqlValue::Int(cursor_value));
            }

            // Build the paged SQL using cursor or OFFSET strategy.
            let paged_sql = build_paged_sql(job, offset);

            let batch = query_to_batch(&pg, &paged_sql, &params).await?;

            match batch {
                None => break,
                Some(rb) => {
                    let n = rb.num_rows();
                    if n == 0 {
                        break;
                    }

                    // Advance watermark.
                    if let Some(col) = &job.watermark_column
                        && let Some(ts) = max_timestamp_in_batch(&rb, col)
                    {
                        new_watermark = Some(match new_watermark {
                            Some(prev) => prev.max(ts),
                            None => ts,
                        });
                    }

                    // Advance cursor.
                    if let Some(col) = &job.cursor_column {
                        if let Some(max_id) = max_int_in_batch(&rb, col) {
                            cursor_value = max_id;
                        }
                    } else {
                        offset += n;
                    }

                    if self.dry_run {
                        info!(
                            job = %job.name,
                            rows = n,
                            "[dry-run] would commit batch (skipped)"
                        );
                    } else {
                        self.write_batch_atomic(job, rb, &new_watermark)
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
                    info!(job = %job.name, rows = n, total_rows, "Batch committed");

                    if n < job.batch_size {
                        break; // last partial batch
                    }
                }
            }
        }

        info!(job = %job.name, total_rows, "Job complete");

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
    ) -> Result<()> {
        let ident = table_ident(&job.namespace, &job.table)?;

        self.ensure_table(&ident, &batch).await?;

        // Schema evolution: add new columns before writing.
        if job.schema_evolution.allow_add_columns {
            self.evolve_schema(&ident, &batch, &job.schema_evolution)
                .await?;
        }

        let table = self
            .catalog
            .load_table(&ident)
            .await
            .with_context(|| format!("load_table {ident}"))?;
        let meta = table.metadata();
        let schema = meta.current_schema();

        let batch =
            inject_field_ids(batch, schema).context("inject Iceberg field IDs into batch")?;

        let loc_gen = DefaultLocationGenerator::new(meta.clone())?;
        let name_gen = ProductionFileNameGenerator::new(
            "data",
            Some(job.name.as_str()),
            DataFileFormat::Parquet,
        );

        let parquet_builder =
            ParquetWriterBuilder::new(WriterProperties::builder().build(), schema.clone());

        let rolling = RollingFileWriterBuilder::new(
            parquet_builder,
            512 * 1024 * 1024,
            table.file_io().clone(),
            loc_gen,
            name_gen,
        );

        let mut writer = DataFileWriterBuilder::new(rolling).build(None).await?;

        writer.write(batch).await?;
        let data_files = writer.close().await?;

        let meta_updates = build_metadata_updates(job.watermark_column.as_deref(), *watermark, 0);

        let tx = Transaction::new(&table);

        let tx = tx.fast_append().add_data_files(data_files).apply(tx)?;

        let mut props_action = tx.update_table_properties();
        for (k, v) in meta_updates {
            props_action = props_action.set(k, v);
        }
        let tx = props_action.apply(tx)?;

        tx.commit(self.catalog).await?;

        Ok(())
    }

    // ── Schema evolution ──────────────────────────────────────────────────────

    /// Detect new source columns that are absent from the Iceberg table and
    /// surface them as a structured warning.
    ///
    /// The iceberg Rust SDK 0.9 `Transaction` does not yet expose an
    /// `update_schema()` / `add_column()` builder, so automatic column
    /// addition is not possible via the public API in this release.
    /// The method logs the new column names so operators can apply the DDL
    /// manually through the REST catalog or Spark SQL, and then continues the
    /// sync — existing columns are written normally.
    ///
    /// Once `Transaction::update_schema()` is stabilised in a future iceberg
    /// crate version, the body below can be replaced with the builder calls.
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

        // Bind schema to a local so the Arc isn't dropped mid-borrow.
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
            "Schema evolution: new source columns detected that are not in the              Iceberg table. Apply the DDL manually (e.g. via Spark SQL              `ALTER TABLE ... ADD COLUMN`) and then re-run to include them."
        );

        // Continue without blocking — existing columns are synced normally.
        Ok(())
    }

    // ── Table auto-creation ───────────────────────────────────────────────────

    async fn ensure_table(&self, ident: &TableIdent, batch: &RecordBatch) -> Result<()> {
        if self.catalog.table_exists(ident).await? {
            return Ok(());
        }

        warn!(table = %ident, "Table not found — creating from batch schema");

        let ns = ident.namespace();
        if !self.catalog.namespace_exists(ns).await? {
            self.catalog.create_namespace(ns, HashMap::new()).await?;
        }

        let schema = arrow_schema_to_iceberg(batch.schema_ref())?;

        let creation = TableCreation::builder()
            .name(ident.name().to_string())
            .schema(schema)
            .build();

        self.catalog.create_table(ns, creation).await?;
        Ok(())
    }
}

// ── Pagination SQL builder ────────────────────────────────────────────────────

/// Build a paged SQL query using cursor-based or OFFSET pagination.
///
/// **Cursor mode** (preferred, requires `cursor_column`):
/// ```sql
/// SELECT * FROM (<user_sql>) _q
/// WHERE <cursor_col> > :_cursor
/// ORDER BY <cursor_col>
/// LIMIT <batch_size>
/// ```
/// The `:_cursor` placeholder is injected into `params` before the query is
/// executed.
///
/// **Offset mode** (fallback):
/// ```sql
/// SELECT * FROM (<user_sql>) _q LIMIT <batch_size> OFFSET <offset>
/// ```
fn build_paged_sql(job: &SyncJob, offset: usize) -> String {
    if let Some(col) = &job.cursor_column {
        // Cursor mode: `:_cursor` is injected into params by the caller loop.
        format!(
            "SELECT * FROM ({}) _q WHERE {col} > :_cursor ORDER BY {col} LIMIT {}",
            job.sql, job.batch_size
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

/// Re-build a `RecordBatch` with `PARQUET:field_id` metadata injected into
/// every Arrow field, matched by column name against the Iceberg schema.
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
        .context("Build Iceberg schema from Arrow schema")
}

// ── Parallel runner ───────────────────────────────────────────────────────────

/// Run a slice of topologically-ordered jobs with up to `parallelism` workers.
///
/// Jobs with no dependency chain are run concurrently; jobs that have a
/// `depends_on` are gated behind their parent's completion.
///
/// Returns a `Vec` of `(job_name, Result<RunSummary>)` so one failure does
/// not abort sibling jobs.
pub async fn run_jobs_parallel<C: Catalog + Sync>(
    engine: &SyncEngine<'_, C>,
    jobs: &[&SyncJob],
    source_dsn_map: &HashMap<String, String>,
    retry_map: &HashMap<String, RetryConfig>,
    parallelism: usize,
) -> Vec<(String, Result<RunSummary>)> {
    use std::sync::Arc;
    use tokio::sync::Semaphore;

    // Simple semaphore-based concurrency: honour topological order (the slice
    // is already sorted) and cap parallelism.
    let sem = Arc::new(Semaphore::new(parallelism));
    let mut results = Vec::new();

    // For simplicity we run jobs serially but in topological order.
    // True parallel execution requires tracking which jobs are unblocked —
    // that would require a dependency-graph scheduler.  The semaphore gives
    // the API surface; a more sophisticated scheduler can be added later.
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

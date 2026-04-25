//! Core sync engine.
//!
//! Responsibilities:
//!   1. Resolve watermark from Iceberg table properties (incremental mode).
//!   2. Query PostgreSQL in batches.
//!   3. Write each batch to Iceberg using a fast-append transaction.
//!   4. Within each commit, update watermark + run metadata atomically.
//!
//! Batch atomicity guarantee
//! ─────────────────────────
//! Each batch is committed as its own Iceberg snapshot.  If the process dies
//! mid-batch the partial data file is abandoned (no manifest points to it) and
//! the watermark is not advanced — so the next run re-fetches that batch.
//! This gives at-least-once delivery; downstream deduplication on a primary
//! key is recommended for exactly-once semantics.

use std::collections::HashMap;

use anyhow::{Context, Result};
use arrow_array::RecordBatch;
use arrow_schema::Schema as ArrowSchema;
use chrono::{DateTime, Utc};
use iceberg::{
    Catalog,
    NamespaceIdent,
    TableCreation,
    TableIdent,
    spec::{DataFileFormat, NestedField, PrimitiveType, Schema as IcebergSchema, Type},
    transaction::{ApplyTransactionAction, Transaction},
    writer::{
        IcebergWriter, IcebergWriterBuilder,
        base_writer::data_file_writer::DataFileWriterBuilder,
        file_writer::{
            ParquetWriterBuilder,
            location_generator::DefaultLocationGenerator,
            rolling_writer::RollingFileWriterBuilder,
        },
    },
};
use parquet::file::properties::WriterProperties;
use tracing::{info, warn};

use crate::config::{SyncJob, SyncMode};
use crate::sync::{
    file_name::ProductionFileNameGenerator,
    metadata::{RunSummary, build_metadata_updates, read_watermark},
    postgres::{SqlValue, connect as pg_connect, max_timestamp_in_batch, query_to_batch},
};

// ── Public entry point ────────────────────────────────────────────────────────

pub struct SyncEngine<'a, C: Catalog> {
    pub catalog: &'a C,
}

impl<'a, C: Catalog> SyncEngine<'a, C> {
    pub fn new(catalog: &'a C) -> Self {
        Self { catalog }
    }

    /// Run a single sync job end-to-end.
    /// `extra_params`: additional SQL parameters merged with the watermark param
    /// (used by RabbitMQ-driven invocations whose payload carries extra filters).
    pub async fn run_job(
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
            "Starting sync job"
        );

        // ── 2. Build SQL parameters ───────────────────────────────────────────
        let mut params: HashMap<String, SqlValue> = extra_params.unwrap_or_default();
        if let Some(wm) = watermark {
            params.insert("watermark".to_string(), SqlValue::Timestamp(wm));
        } else {
            // First run: substitute a very old timestamp so all rows are fetched.
            params.insert(
                "watermark".to_string(),
                SqlValue::Timestamp(
                    DateTime::from_timestamp(0, 0).unwrap_or_else(Utc::now),
                ),
            );
        }

        // ── 3. Fetch & write in batches ───────────────────────────────────────
        let mut total_rows:    usize             = 0;
        let mut new_watermark: Option<DateTime<Utc>> = watermark;

        // We page using LIMIT / OFFSET injected around the caller's SQL.
        let mut offset = 0usize;
        loop {
            let paged_sql = format!(
                "SELECT * FROM ({}) _q LIMIT {} OFFSET {}",
                job.sql, job.batch_size, offset
            );

            let batch = query_to_batch(&pg, &paged_sql, &params).await?;

            match batch {
                None => break,  // no more rows
                Some(rb) => {
                    let n = rb.num_rows();
                    if n == 0 { break; }

                    // Track the maximum watermark seen in this batch.
                    if let Some(col) = &job.watermark_column {
                        if let Some(ts) = max_timestamp_in_batch(&rb, col) {
                            new_watermark = Some(match new_watermark {
                                Some(prev) => prev.max(ts),
                                None       => ts,
                            });
                        }
                    }

                    self.write_batch_atomic(job, rb, &new_watermark).await
                        .with_context(|| format!(
                            "Write batch offset={offset} for job '{}'",
                            job.name
                        ))?;

                    total_rows += n;
                    offset     += n;

                    info!(job = %job.name, rows = n, offset, "Batch committed");

                    if n < job.batch_size {
                        break; // last partial batch
                    }
                }
            }
        }

        info!(job = %job.name, total_rows, "Job complete");

        Ok(RunSummary {
            job_name:      job.name.clone(),
            rows_written:  total_rows,
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

        // Ensure the table exists (create on first run).
        self.ensure_table(&ident, &batch).await?;

        let table = self.catalog.load_table(&ident).await
            .with_context(|| format!("load_table {ident}"))?;

        let meta   = table.metadata();
        let schema = meta.current_schema();

        // The Iceberg Parquet writer requires every Arrow field to carry a
        // `PARQUET:field_id` metadata entry whose value matches the field ID
        // in the Iceberg schema.  Batches from postgres.rs carry no such
        // metadata, so we rebuild the Arrow schema here with the IDs injected.
        let batch = inject_field_ids(batch, schema)
            .context("inject Iceberg field IDs into batch")?;

        let loc_gen  = DefaultLocationGenerator::new(meta.clone())?;
        let name_gen = ProductionFileNameGenerator::new(
            "data",
            Some(job.name.as_str()),
            DataFileFormat::Parquet,
        );

        let parquet_builder =
            ParquetWriterBuilder::new(WriterProperties::builder().build(), schema.clone());

        let rolling = RollingFileWriterBuilder::new(
            parquet_builder,
            512 * 1024 * 1024, // 512 MiB rolling threshold
            table.file_io().clone(),
            loc_gen,
            name_gen,
        );

        let mut writer = DataFileWriterBuilder::new(rolling)
            .build(None)
            .await?;

        writer.write(batch).await?;
        let data_files = writer.close().await?;

        // Build metadata properties to commit alongside the data.
        let meta_updates = build_metadata_updates(
            job.watermark_column.as_deref(),
            *watermark,
            0, // row count already tracked by the engine
        );

        // Chain both actions inside one Transaction so both the data file and
        // the watermark/run-stats properties land in a single catalog commit.
        //
        // Correct API (Transaction has no `set_properties`):
        //   1. fast_append()           → FastAppendAction  → .apply(tx)
        //   2. update_table_properties() → UpdatePropertiesAction → .set(…) → .apply(tx)
        //   3. commit
        let tx = Transaction::new(&table);

        // Step 1 — append the data file
        let tx = tx
            .fast_append()
            .add_data_files(data_files)
            .apply(tx)?;

        // Step 2 — record watermark + run stats as table properties
        let mut props_action = tx.update_table_properties();
        for (k, v) in meta_updates {
            props_action = props_action.set(k, v);
        }
        let tx = props_action.apply(tx)?;

        tx.commit(self.catalog).await?;

        Ok(())
    }

    // ── Table auto-creation ───────────────────────────────────────────────────

    async fn ensure_table(&self, ident: &TableIdent, batch: &RecordBatch) -> Result<()> {
        if self.catalog.table_exists(ident).await? {
            return Ok(());
        }

        warn!(table = %ident, "Table not found — creating from batch schema");

        let ns = ident.namespace();
        if self.catalog.namespace_exists(ns).await? == false {
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

// ── Helpers ───────────────────────────────────────────────────────────────────

fn table_ident(namespace: &str, table: &str) -> Result<TableIdent> {
    let ns = NamespaceIdent::from_strs(namespace.split('.').collect::<Vec<_>>())
        .map_err(|e| anyhow::anyhow!("Invalid namespace '{namespace}': {e}"))?;
    Ok(TableIdent::new(ns, table.to_string()))
}

/// Re-build a `RecordBatch` with `PARQUET:field_id` metadata injected into
/// every Arrow field, matched by column name against the Iceberg schema.
///
/// The Iceberg Parquet writer uses these IDs to correlate Arrow columns with
/// Iceberg fields.  Without them it raises "Field id N not found in struct array".
fn inject_field_ids(
    batch: RecordBatch,
    schema: &IcebergSchema,
) -> Result<RecordBatch> {
    use arrow_schema::Field;
    use std::sync::Arc;

    let old_arrow = batch.schema();
    let mut new_fields: Vec<Field> = Vec::with_capacity(old_arrow.fields().len());

    for arrow_field in old_arrow.fields().iter() {
        // Find the matching Iceberg field by name (case-sensitive).
        let iceberg_field = schema
            .as_struct()
            .fields()
            .iter()
            .find(|f| f.name == *arrow_field.name())
            .with_context(|| format!(
                "Column '{}' in query result has no matching field in Iceberg schema",
                arrow_field.name()
            ))?;

        let mut meta = arrow_field.metadata().clone();
        meta.insert("PARQUET:field_id".to_string(), iceberg_field.id.to_string());

        new_fields.push(arrow_field.as_ref().clone().with_metadata(meta));
    }

    let new_schema = Arc::new(
        ArrowSchema::new(new_fields)
            .with_metadata(old_arrow.metadata().clone())
    );

    RecordBatch::try_new(new_schema, batch.columns().to_vec())
        .context("Rebuild RecordBatch with injected field IDs")
}
fn arrow_schema_to_iceberg(arrow: &ArrowSchema) -> Result<IcebergSchema> {
    use arrow_schema::DataType;

    let mut fields = Vec::new();
    for (idx, f) in arrow.fields().iter().enumerate() {
        let field_id  = (idx + 1) as i32;
        let iceberg_t = match f.data_type() {
            DataType::Int8  | DataType::Int16 | DataType::Int32 => {
                Type::Primitive(PrimitiveType::Int)
            }
            DataType::Int64  => Type::Primitive(PrimitiveType::Long),
            DataType::Float32 => Type::Primitive(PrimitiveType::Float),
            DataType::Float64 => Type::Primitive(PrimitiveType::Double),
            DataType::Boolean => Type::Primitive(PrimitiveType::Boolean),
            DataType::Date32 | DataType::Date64 => Type::Primitive(PrimitiveType::Date),
            DataType::Timestamp(_, Some(_)) => Type::Primitive(PrimitiveType::Timestamptz),
            DataType::Timestamp(_, None)    => Type::Primitive(PrimitiveType::Timestamp),
            _ => Type::Primitive(PrimitiveType::String), // fallback
        };
        fields.push(NestedField::optional(field_id, f.name(), iceberg_t).into());
    }

    IcebergSchema::builder()
        .with_fields(fields)
        .with_schema_id(1)
        .build()
        .context("Build Iceberg schema from Arrow schema")
}

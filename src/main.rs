//! iceberg-cli — minimal CLI to list, query and write Iceberg tables
//! via a REST catalog backed by S3.
//!
//! Usage examples
//! ──────────────
//!   # List namespaces
//!   iceberg-cli --uri http://localhost:8181 list-namespaces
//!
//!   # List tables inside a namespace
//!   iceberg-cli --uri http://localhost:8181 list-tables --namespace my_db
//!
//!   # Show table schema / metadata
//!   iceberg-cli --uri http://localhost:8181 describe --table my_db.products
//!
//!   # Scan / query a table (optional column projection + limit)
//!   iceberg-cli --uri http://localhost:8181 scan \
//!       --table my_db.products \
//!       --columns id,name,price \
//!       --limit 20
//!
//!   # Write rows (JSON array) into a table via fast-append
//!   iceberg-cli --uri http://localhost:8181 write \
//!       --table my_db.products \
//!       --json '[{"id":1,"name":"Widget","price":9.99}]'
//!
//!   # Create a namespace
//!   iceberg-cli --uri http://localhost:8181 create-namespace --namespace my_db
//!
//!   # Create a table  (id LONG, name STRING, price DOUBLE)
//!   iceberg-cli --uri http://localhost:8181 create-table \
//!       --table my_db.products \
//!       --schema "id:long,name:string,price:double"

mod config;
mod sync;

use std::{collections::HashMap, sync::Arc};

use anyhow::{Context, Result, anyhow, bail};
use arrow_array::{
    ArrayRef, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array, RecordBatch,
    StringArray,
};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use clap::{Parser, Subcommand};
use comfy_table::{Table, presets::UTF8_FULL};
use config::SyncConfig;
use futures::TryStreamExt;
use iceberg::{
    Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent,
    spec::{
        DataFileFormat,
        NestedField,
        PartitionKey,
        // PartitionSpec,
        PrimitiveType,
        Schema,
        Struct,
        Type,
    },
    transaction::{ApplyTransactionAction, Transaction},
    writer::{
        IcebergWriter, IcebergWriterBuilder,
        base_writer::data_file_writer::DataFileWriterBuilder,
        file_writer::{ParquetWriterBuilder, location_generator::DefaultLocationGenerator},
    },
};
use iceberg_catalog_rest::{REST_CATALOG_PROP_URI, RestCatalogBuilder};
use iceberg_storage_opendal::OpenDalStorageFactory;
use parquet::file::properties::WriterProperties;
use sync::engine::SyncEngine;
use sync::file_name::ProductionFileNameGenerator;

// ─── CLI definition ──────────────────────────────────────────────────────────

#[derive(Parser)]
#[command(
    name = "iceberg-cli",
    about = "Minimal Iceberg CLI (REST catalog + S3)",
    version
)]
struct Cli {
    /// REST catalog URI (e.g. http://localhost:8181)
    #[arg(long, env = "ICEBERG_URI", default_value = "http://localhost:8181")]
    uri: String,

    #[arg(long, env = "S3_ENDPOINT")]
    s3_endpoint: String,

    /// S3 URL scheme used by the storage backend.
    /// Use 's3a' for Hadoop-compatible stores (MinIO, EMR) — default.
    /// Use 's3'  when the REST catalog rewrites URLs itself.
    #[arg(long, env = "S3_SCHEME", default_value = "s3a")]
    s3_scheme: String,

    /// AWS region for S3 (or set AWS_DEFAULT_REGION / AWS_REGION)
    #[arg(long, env = "AWS_DEFAULT_REGION", default_value = "us-east-1")]
    region: String,

    /// AWS access key ID (or set AWS_ACCESS_KEY_ID)
    #[arg(long, env = "AWS_ACCESS_KEY_ID")]
    access_key_id: Option<String>,

    /// AWS secret access key (or set AWS_SECRET_ACCESS_KEY)
    #[arg(long, env = "AWS_SECRET_ACCESS_KEY")]
    secret_access_key: Option<String>,

    /// AWS session token for temporary credentials (or set AWS_SESSION_TOKEN)
    #[arg(long, env = "AWS_SESSION_TOKEN")]
    session_token: Option<String>,

    /// Optional catalog name
    #[arg(long, default_value = "default")]
    catalog: String,

    #[command(subcommand)]
    cmd: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// List namespaces in the catalog
    ListNamespaces,

    /// List tables inside a namespace
    ListTables {
        /// Namespace (e.g. my_db)
        #[arg(long)]
        namespace: String,
    },

    /// Show table schema and metadata
    Describe {
        /// Fully-qualified table: namespace.table
        #[arg(long)]
        table: String,
    },

    /// Scan / read a table into the terminal
    Scan {
        /// Fully-qualified table: namespace.table
        #[arg(long)]
        table: String,

        /// Comma-separated column names to project (default: all)
        #[arg(long)]
        columns: Option<String>,

        /// Max rows to display  (default: 50)
        #[arg(long, default_value = "50")]
        limit: usize,
    },

    /// Write rows (JSON array) into a table via fast-append
    Write {
        /// Fully-qualified table: namespace.table
        #[arg(long)]
        table: String,

        /// JSON array of objects, e.g. '[{"id":1,"name":"x","price":1.5}]'
        #[arg(long)]
        json: String,

        #[arg(long)]
        partition: Option<String>,
    },

    /// Create a new namespace
    CreateNamespace {
        #[arg(long)]
        namespace: String,
    },

    /// Run sync jobs from a YAML config file
    Sync {
        /// Path to sync config YAML
        #[arg(long)]
        config: String,

        /// Only run this specific job (default: all jobs in dependency order)
        #[arg(long)]
        job: Option<String>,

        /// Only run jobs belonging to this group tag
        #[arg(long)]
        group: Option<String>,

        /// Log what would be done without writing any data
        #[arg(long, default_value_t = false)]
        dry_run: bool,

        /// Maximum number of independent jobs to run concurrently (default: 1)
        #[arg(long, default_value_t = 1)]
        parallel: usize,
    },

    /// Start a RabbitMQ consumer loop (blocks)
    SyncConsume {
        /// Path to sync config YAML
        #[arg(long)]
        config: String,
    },
    /// Create a new table with a simple schema
    CreateTable {
        /// Fully-qualified table: namespace.table
        #[arg(long)]
        table: String,

        /// Schema: comma-separated "col:type" pairs.
        /// Supported types: long, string, double, boolean, int, float, date, timestamp
        /// Example: "id:long,name:string,price:double"
        #[arg(long)]
        schema: String,
    },
}

// ─── Entry point ─────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    let catalog = connect(
        &cli.uri,
        &cli.catalog,
        &cli.region,
        &cli.s3_endpoint,
        &cli.s3_scheme,
        cli.access_key_id.as_deref(),
        cli.secret_access_key.as_deref(),
        cli.session_token.as_deref(),
    )
    .await?;

    // SyncConsume needs ownership of the catalog (Arc<C: Catalog + Send + Sync + 'static>).
    // Handle it before the general match so we can move `catalog` into it.
    if let Commands::SyncConsume { config } = &cli.cmd {
        cmd_sync_consume(catalog, config).await?;
        return Ok(());
    }

    match &cli.cmd {
        Commands::ListNamespaces => cmd_list_namespaces(&catalog).await?,
        Commands::ListTables { namespace } => cmd_list_tables(&catalog, namespace).await?,
        Commands::Describe { table } => cmd_describe(&catalog, table).await?,
        Commands::Scan {
            table,
            columns,
            limit,
        } => cmd_scan(&catalog, table, columns.as_deref(), *limit).await?,
        Commands::Write {
            table,
            json,
            partition,
        } => cmd_write(&catalog, table, json, partition.as_deref()).await?,
        Commands::CreateNamespace { namespace } => {
            cmd_create_namespace(&catalog, namespace).await?
        }
        Commands::CreateTable { table, schema } => {
            cmd_create_table(&catalog, table, schema).await?
        }
        Commands::Sync {
            config,
            job,
            group,
            dry_run,
            parallel,
        } => {
            cmd_sync(
                &catalog,
                config,
                job.as_deref(),
                group.as_deref(),
                *dry_run,
                *parallel,
            )
            .await?
        }
        Commands::SyncConsume { .. } => unreachable!(),
    }

    Ok(())
}

// ─── Catalog connection ───────────────────────────────────────────────────────

#[allow(clippy::too_many_arguments)]
async fn connect(
    uri: &str,
    name: &str,
    region: &str,
    s3_endpoint: &str,
    s3_scheme: &str,
    access_key_id: Option<&str>,
    secret_access_key: Option<&str>,
    session_token: Option<&str>,
) -> Result<impl Catalog + use<>> {
    let mut props = HashMap::from([
        (REST_CATALOG_PROP_URI.to_string(), uri.to_string()),
        ("s3.region".to_string(), region.to_string()),
        ("s3.endpoint".to_string(), s3_endpoint.to_string()),
        ("client.region".to_string(), region.to_string()),
    ]);

    if let Some(k) = access_key_id {
        props.insert("s3.access-key-id".to_string(), k.to_string());
    }
    if let Some(s) = secret_access_key {
        props.insert("s3.secret-access-key".to_string(), s.to_string());
    }
    if let Some(t) = session_token {
        props.insert("s3.session-token".to_string(), t.to_string());
    }

    let catalog = RestCatalogBuilder::default()
        .with_storage_factory(Arc::new(OpenDalStorageFactory::S3 {
            configured_scheme: s3_scheme.to_string(),
            customized_credential_load: None,
        }))
        .load(name, props)
        .await
        .with_context(|| format!("Failed to connect to REST catalog at {uri}"))?;

    Ok(catalog)
}

// ─── Commands ─────────────────────────────────────────────────────────────────

async fn cmd_list_namespaces(catalog: &impl Catalog) -> Result<()> {
    let namespaces = catalog
        .list_namespaces(None)
        .await
        .context("list_namespaces")?;

    if namespaces.is_empty() {
        println!("(no namespaces)");
        return Ok(());
    }

    let mut tbl = Table::new();
    tbl.load_preset(UTF8_FULL).set_header(vec!["Namespace"]);
    for ns in &namespaces {
        tbl.add_row(vec![ns.to_url_string()]);
    }
    println!("{tbl}");
    Ok(())
}

async fn cmd_list_tables(catalog: &impl Catalog, namespace: &str) -> Result<()> {
    let ns = parse_namespace(namespace)?;
    let tables = catalog
        .list_tables(&ns)
        .await
        .with_context(|| format!("list_tables({namespace})"))?;

    if tables.is_empty() {
        println!("(no tables in namespace '{namespace}')");
        return Ok(());
    }

    let mut tbl = Table::new();
    tbl.load_preset(UTF8_FULL)
        .set_header(vec!["Namespace", "Table"]);
    for t in &tables {
        tbl.add_row(vec![t.namespace().to_url_string(), t.name().to_string()]);
    }
    println!("{tbl}");
    Ok(())
}

async fn cmd_describe(catalog: &impl Catalog, table_str: &str) -> Result<()> {
    let ident = parse_table(table_str)?;
    let table = catalog
        .load_table(&ident)
        .await
        .with_context(|| format!("load_table({table_str})"))?;
    let meta = table.metadata();

    println!("━━━  Table: {table_str}  ━━━");
    println!("  Format version : {:?}", meta.format_version());
    println!("  Location       : {}", meta.location());

    if let Some(snap) = meta.current_snapshot() {
        println!("  Snapshot ID    : {}", snap.snapshot_id());
        println!("  Manifest list  : {}", snap.manifest_list());
    } else {
        println!("  Snapshot       : (none — empty table)");
    }

    println!("\nSchema:");
    let mut tbl = Table::new();
    tbl.load_preset(UTF8_FULL)
        .set_header(vec!["Field ID", "Name", "Type", "Required"]);
    for field in meta.current_schema().as_struct().fields() {
        tbl.add_row(vec![
            field.id.to_string(),
            field.name.clone(),
            format!("{:?}", field.field_type),
            field.required.to_string(),
        ]);
    }
    println!("{tbl}");

    let props = meta.properties();
    if !props.is_empty() {
        println!("\nProperties:");
        let mut ptbl = Table::new();
        ptbl.load_preset(UTF8_FULL).set_header(vec!["Key", "Value"]);
        for (k, v) in props {
            ptbl.add_row(vec![k, v]);
        }
        println!("{ptbl}");
    }

    Ok(())
}

async fn cmd_scan(
    catalog: &impl Catalog,
    table_str: &str,
    columns: Option<&str>,
    limit: usize,
) -> Result<()> {
    let ident = parse_table(table_str)?;
    let table = catalog
        .load_table(&ident)
        .await
        .with_context(|| format!("load_table({table_str})"))?;

    let mut scan_builder = table.scan();

    if let Some(cols) = columns {
        let col_list: Vec<&str> = cols.split(',').map(str::trim).collect();
        scan_builder = scan_builder.select(col_list);
    }

    let stream = scan_builder
        .build()
        .context("build scan")?
        .to_arrow()
        .await
        .context("to_arrow")?;

    let batches: Vec<RecordBatch> = stream.try_collect().await.context("collect batches")?;

    if batches.is_empty() {
        println!("(no data)");
        return Ok(());
    }

    let schema = batches[0].schema();
    let headers: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();

    let mut tbl = Table::new();
    tbl.load_preset(UTF8_FULL).set_header(headers);

    let mut row_count = 0usize;
    'outer: for batch in &batches {
        for row in 0..batch.num_rows() {
            if row_count >= limit {
                break 'outer;
            }
            let cells: Vec<String> = (0..batch.num_columns())
                .map(|col| arrow_value_to_string(batch.column(col), row))
                .collect();
            tbl.add_row(cells);
            row_count += 1;
        }
    }
    println!("{tbl}");
    println!("({row_count} rows shown, limit={limit})");
    Ok(())
}

async fn cmd_write(
    catalog: &impl Catalog,
    table_str: &str,
    json: &str,
    _partition_override: Option<&str>,
) -> Result<()> {
    let ident = parse_table(table_str)?;
    let table = catalog.load_table(&ident).await?;

    let meta = table.metadata();
    let schema = meta.current_schema();
    let partition_spec = meta.default_partition_spec();

    let rows: Vec<serde_json::Value> = serde_json::from_str(json)?;
    if rows.is_empty() {
        return Ok(());
    }

    let batch = json_rows_to_record_batch(&rows, schema)?;

    let partition_key = if partition_spec.is_unpartitioned() {
        None
    } else {
        let mut literals = Vec::new();

        for field in partition_spec.fields() {
            let source_field = schema
                .field_by_id(field.source_id)
                .with_context(|| format!("Field ID {} not found in schema", field.source_id))?;

            let col_name = &source_field.name;

            let val = rows[0]
                .get(col_name)
                .with_context(|| format!("Partition column '{}' missing in JSON", col_name))?;

            let lit = match &source_field.field_type.as_primitive_type() {
                Some(PrimitiveType::String) => {
                    iceberg::spec::Literal::string(val.as_str().unwrap_or(""))
                }
                Some(PrimitiveType::Long) => {
                    iceberg::spec::Literal::long(val.as_i64().unwrap_or(0))
                }
                Some(PrimitiveType::Int) => {
                    iceberg::spec::Literal::int(val.as_i64().unwrap_or(0) as i32)
                }
                _ => bail!("Unsupported partition type for column {}", col_name),
            };
            literals.push(Some(lit));
        }

        let partition_data = Struct::from_iter(literals);
        Some(PartitionKey::new(
            partition_spec.as_ref().clone(),
            schema.clone(),
            partition_data,
        ))
    };

    let location_gen = DefaultLocationGenerator::new(meta.clone())?;
    let file_name_gen =
        ProductionFileNameGenerator::new("data", None::<&str>, DataFileFormat::Parquet);

    let parquet_builder =
        ParquetWriterBuilder::new(WriterProperties::builder().build(), schema.clone());

    let rolling_builder =
        iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder::new(
            parquet_builder,
            536_870_912usize,
            table.file_io().clone(),
            location_gen,
            file_name_gen,
        );

    let mut writer = DataFileWriterBuilder::new(rolling_builder)
        .build(partition_key)
        .await?;

    writer.write(batch).await?;
    let data_files = writer.close().await?;

    let tx = Transaction::new(&table);
    let action = tx.fast_append().add_data_files(data_files);
    let tx = action.apply(tx)?;
    let updated = tx.commit(catalog).await?;

    println!(
        "Committed. New snapshot ID: {:?}",
        updated
            .metadata()
            .current_snapshot()
            .map(|s| s.snapshot_id())
    );
    Ok(())
}

async fn cmd_create_namespace(catalog: &impl Catalog, namespace: &str) -> Result<()> {
    let ns = parse_namespace(namespace)?;
    match catalog.create_namespace(&ns, HashMap::new()).await {
        Ok(_) => println!("Created namespace '{namespace}'."),
        Err(e) => {
            let msg = e.to_string().to_lowercase();
            if msg.contains("already exists") || msg.contains("already exist") {
                println!("Namespace '{namespace}' already exists — skipping.");
            } else {
                return Err(e).with_context(|| format!("create_namespace({namespace})"));
            }
        }
    }
    Ok(())
}

async fn cmd_create_table(catalog: &impl Catalog, table_str: &str, schema_str: &str) -> Result<()> {
    let ident = parse_table(table_str)?;
    let schema = parse_schema(schema_str).context("parse --schema")?;

    let creation = TableCreation::builder()
        .name(ident.name().to_string())
        .schema(schema)
        .build();

    catalog
        .create_table(ident.namespace(), creation)
        .await
        .with_context(|| format!("create_table({table_str})"))?;

    println!("Created table '{table_str}'.");
    Ok(())
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

fn parse_namespace(s: &str) -> Result<NamespaceIdent> {
    NamespaceIdent::from_strs(s.split('.').collect::<Vec<_>>())
        .map_err(|e| anyhow!("Invalid namespace '{s}': {e}"))
}

fn parse_table(s: &str) -> Result<TableIdent> {
    let parts: Vec<&str> = s.rsplitn(2, '.').collect();
    match parts.as_slice() {
        [name, namespace] => {
            let ns = parse_namespace(namespace)?;
            Ok(TableIdent::new(ns, name.to_string()))
        }
        _ => bail!("Expected 'namespace.table', got '{s}'"),
    }
}

fn parse_schema(spec: &str) -> Result<Schema> {
    let mut fields = Vec::new();
    for (idx, part) in spec.split(',').enumerate() {
        let kv: Vec<&str> = part.trim().splitn(2, ':').collect();
        if kv.len() != 2 {
            bail!("Bad field spec '{part}' — expected 'name:type'");
        }
        let name = kv[0].trim();
        let ptype = match kv[1].trim().to_lowercase().as_str() {
            "long" | "int64" => PrimitiveType::Long,
            "int" | "int32" => PrimitiveType::Int,
            "string" | "str" => PrimitiveType::String,
            "double" | "float64" => PrimitiveType::Double,
            "float" | "float32" => PrimitiveType::Float,
            "boolean" | "bool" => PrimitiveType::Boolean,
            "date" => PrimitiveType::Date,
            "timestamp" => PrimitiveType::Timestamp,
            other => bail!("Unknown type '{other}'"),
        };
        let field_id = (idx + 1) as i32;
        fields.push(NestedField::optional(field_id, name, Type::Primitive(ptype)).into());
    }

    Schema::builder()
        .with_fields(fields)
        .with_schema_id(1)
        .build()
        .context("build schema")
}

fn arrow_value_to_string(array: &ArrayRef, row: usize) -> String {
    use arrow_array::Array;
    if array.is_null(row) {
        return "NULL".to_string();
    }
    macro_rules! downcast_fmt {
        ($ty:ty) => {
            if let Some(a) = array.as_any().downcast_ref::<$ty>() {
                return format!("{}", a.value(row)).to_string();
            }
        };
    }
    // downcast_fmt!(arrow_array::BinaryArray);
    // downcast_fmt!(arrow_array::BinaryViewArray);
    downcast_fmt!(arrow_array::Date32Array);
    downcast_fmt!(arrow_array::Date64Array);
    downcast_fmt!(arrow_array::Decimal32Array);
    downcast_fmt!(arrow_array::Decimal64Array);
    downcast_fmt!(arrow_array::Decimal128Array);
    downcast_fmt!(arrow_array::Decimal256Array);
    downcast_fmt!(arrow_array::DurationMicrosecondArray);
    downcast_fmt!(arrow_array::DurationMillisecondArray);
    downcast_fmt!(arrow_array::DurationNanosecondArray);
    downcast_fmt!(arrow_array::DurationSecondArray);
    downcast_fmt!(arrow_array::Float16Array);
    downcast_fmt!(arrow_array::Float32Array);
    downcast_fmt!(arrow_array::Float64Array);
    // downcast_fmt!(arrow_array::GenericStringArray);
    downcast_fmt!(arrow_array::Int8Array);
    // downcast_fmt!(arrow_array::Int8DictionaryArray);
    downcast_fmt!(arrow_array::Int16Array);
    // downcast_fmt!(arrow_array::Int16DictionaryArray);
    // downcast_fmt!(arrow_array::Int16RunArray);
    downcast_fmt!(arrow_array::Int32Array);
    // downcast_fmt!(arrow_array::Int32DictionaryArray);
    // downcast_fmt!(arrow_array::Int32RunArray);
    downcast_fmt!(arrow_array::Int64Array);
    // downcast_fmt!(arrow_array::Int64DictionaryArray);
    // downcast_fmt!(arrow_array::Int64RunArray);
    // downcast_fmt!(arrow_array::IntervalDayTimeArray);
    // downcast_fmt!(arrow_array::IntervalMonthDayNanoArray);
    // downcast_fmt!(arrow_array::IntervalYearMonthArray);
    // downcast_fmt!(arrow_array::LargeBinaryArray);
    // downcast_fmt!(arrow_array::LargeListArray);
    // downcast_fmt!(arrow_array::LargeListViewArray);
    // downcast_fmt!(arrow_array::LargeStringArray);
    // downcast_fmt!(arrow_array::ListArray);
    // downcast_fmt!(arrow_array::ListViewArray);
    // downcast_fmt!(arrow_array::StringArray);
    // downcast_fmt!(arrow_array::StringViewArray);
    downcast_fmt!(arrow_array::Time32MillisecondArray);
    downcast_fmt!(arrow_array::Time32SecondArray);
    downcast_fmt!(arrow_array::Time64MicrosecondArray);
    downcast_fmt!(arrow_array::Time64NanosecondArray);
    downcast_fmt!(arrow_array::TimestampMicrosecondArray);
    downcast_fmt!(arrow_array::TimestampMillisecondArray);
    downcast_fmt!(arrow_array::TimestampNanosecondArray);
    downcast_fmt!(arrow_array::TimestampSecondArray);
    downcast_fmt!(arrow_array::UInt8Array);
    // downcast_fmt!(arrow_array::UInt8DictionaryArray);
    downcast_fmt!(arrow_array::UInt16Array);
    // downcast_fmt!(arrow_array::UInt16DictionaryArray);
    downcast_fmt!(arrow_array::UInt32Array);
    // downcast_fmt!(arrow_array::UInt32DictionaryArray);
    downcast_fmt!(arrow_array::UInt64Array);
    // downcast_fmt!(arrow_array::UInt64DictionaryArray);

    if let Some(a) = array.as_any().downcast_ref::<arrow_array::StringArray>() {
        return a.value(row).to_string();
    }
    if let Some(a) = array
        .as_any()
        .downcast_ref::<arrow_array::LargeStringArray>()
    {
        return a.value(row).to_string();
    }
    "(unsupported)".to_string()
}

fn json_rows_to_record_batch(rows: &[serde_json::Value], schema: &Schema) -> Result<RecordBatch> {
    let mut arrow_fields: Vec<Field> = Vec::new();
    let mut columns: Vec<ArrayRef> = Vec::new();

    for field in schema.as_struct().fields() {
        let col = field.name.as_str();

        // Build metadata with the Iceberg field ID — this is what the writer uses
        // to resolve "Field id N not found in struct array"
        let mut metadata = HashMap::new();
        metadata.insert("PARQUET:field_id".to_string(), field.id.to_string());

        match &*field.field_type {
            Type::Primitive(PrimitiveType::Long) => {
                let vals: Vec<Option<i64>> = rows
                    .iter()
                    .map(|r| r.get(col).and_then(|v| v.as_i64()))
                    .collect();
                arrow_fields.push(Field::new(col, DataType::Int64, true).with_metadata(metadata));
                columns.push(Arc::new(Int64Array::from(vals)) as ArrayRef);
            }
            Type::Primitive(PrimitiveType::Int) => {
                let vals: Vec<Option<i32>> = rows
                    .iter()
                    .map(|r| r.get(col).and_then(|v| v.as_i64()).map(|i| i as i32))
                    .collect();
                arrow_fields.push(Field::new(col, DataType::Int32, true).with_metadata(metadata));
                columns.push(Arc::new(Int32Array::from(vals)) as ArrayRef);
            }
            Type::Primitive(PrimitiveType::Double) => {
                let vals: Vec<Option<f64>> = rows
                    .iter()
                    .map(|r| r.get(col).and_then(|v| v.as_f64()))
                    .collect();
                arrow_fields.push(Field::new(col, DataType::Float64, true).with_metadata(metadata));
                columns.push(Arc::new(Float64Array::from(vals)) as ArrayRef);
            }
            Type::Primitive(PrimitiveType::Float) => {
                let vals: Vec<Option<f32>> = rows
                    .iter()
                    .map(|r| r.get(col).and_then(|v| v.as_f64()).map(|f| f as f32))
                    .collect();
                arrow_fields.push(Field::new(col, DataType::Float32, true).with_metadata(metadata));
                columns.push(Arc::new(Float32Array::from(vals)) as ArrayRef);
            }
            Type::Primitive(PrimitiveType::String) => {
                let vals: Vec<Option<&str>> = rows
                    .iter()
                    .map(|r| r.get(col).and_then(|v| v.as_str()))
                    .collect();
                arrow_fields.push(Field::new(col, DataType::Utf8, true).with_metadata(metadata));
                columns.push(Arc::new(StringArray::from(vals)) as ArrayRef);
            }
            Type::Primitive(PrimitiveType::Boolean) => {
                let vals: Vec<Option<bool>> = rows
                    .iter()
                    .map(|r| r.get(col).and_then(|v| v.as_bool()))
                    .collect();
                arrow_fields.push(Field::new(col, DataType::Boolean, true).with_metadata(metadata));
                columns.push(Arc::new(BooleanArray::from(vals)) as ArrayRef);
            }
            Type::Primitive(PrimitiveType::Date) => {
                // JSON representation: "YYYY-MM-DD" string or null.
                // Arrow Date32 = days since Unix epoch.
                use arrow_array::Date32Array;
                let vals: Vec<Option<i32>> = rows
                    .iter()
                    .map(|r| {
                        r.get(col)
                            .and_then(|v| v.as_str())
                            .and_then(|s| chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").ok())
                            .map(|d| {
                                d.signed_duration_since(
                                    chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap(),
                                )
                                .num_days() as i32
                            })
                    })
                    .collect();
                arrow_fields.push(Field::new(col, DataType::Date32, true).with_metadata(metadata));
                columns.push(Arc::new(Date32Array::from(vals)) as ArrayRef);
            }
            Type::Primitive(PrimitiveType::Timestamp)
            | Type::Primitive(PrimitiveType::Timestamptz) => {
                // JSON representation: RFC-3339 string or null.
                use arrow_array::TimestampMicrosecondArray;
                let vals: Vec<Option<i64>> = rows
                    .iter()
                    .map(|r| {
                        r.get(col)
                            .and_then(|v| v.as_str())
                            .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                            .map(|dt| dt.timestamp_micros())
                    })
                    .collect();
                // No timezone annotation — Iceberg PrimitiveType::Timestamp maps to
                // Arrow Timestamp(µs) without tz.  Timestamptz would need "UTC" but
                // the auto-create schema uses Timestamp, so keep them consistent.
                arrow_fields.push(
                    Field::new(
                        col,
                        DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
                        true,
                    )
                    .with_metadata(metadata),
                );
                columns.push(Arc::new(TimestampMicrosecondArray::from(vals)) as ArrayRef);
            }
            _ => {
                let vals: Vec<Option<String>> = rows
                    .iter()
                    .map(|r| r.get(col).map(|v| v.to_string()))
                    .collect();
                arrow_fields.push(Field::new(col, DataType::Utf8, true).with_metadata(metadata));
                let arr: StringArray = vals.iter().map(|v| v.as_deref()).collect();
                columns.push(Arc::new(arr) as ArrayRef);
            }
        }
    }

    let arrow_schema = Arc::new(ArrowSchema::new(arrow_fields));
    RecordBatch::try_new(arrow_schema, columns).context("build RecordBatch")
}

// ─── Sync commands ────────────────────────────────────────────────────────────

async fn cmd_sync(
    catalog: &impl Catalog,
    config_path: &str,
    only_job: Option<&str>,
    only_group: Option<&str>,
    dry_run: bool,
    parallel: usize,
) -> Result<()> {
    use std::collections::HashMap;
    use sync::engine::run_jobs_parallel;

    let cfg = SyncConfig::from_file(config_path)?;
    let engine = if dry_run {
        SyncEngine::with_dry_run(catalog)
    } else {
        SyncEngine::new(catalog)
    };

    // Build the ordered job list, applying filters.
    let all_jobs = cfg.ordered_jobs()?;
    let jobs: Vec<_> = all_jobs
        .into_iter()
        .filter(|j| only_job.is_none_or(|n| j.name == n))
        .filter(|j| only_group.is_none_or(|g| j.group.as_deref() == Some(g)))
        .collect();

    if jobs.is_empty() {
        println!("No jobs to run.");
        return Ok(());
    }

    if dry_run {
        println!("⚠  dry-run mode — no data will be written");
    }

    // Build DSN and retry maps for the parallel runner.
    let source_dsn_map: HashMap<String, String> = cfg
        .sources
        .iter()
        .map(|(k, v)| (k.clone(), v.dsn.clone()))
        .collect();
    let retry_map: HashMap<String, crate::config::RetryConfig> = jobs
        .iter()
        .map(|j| (j.name.clone(), cfg.retry_for(j)))
        .collect();

    let results = run_jobs_parallel(&engine, &jobs, &source_dsn_map, &retry_map, parallel).await;

    let mut had_error = false;
    for (name, result) in results {
        match result {
            Ok(summary) => println!(
                "✓  {}  rows={}  watermark={:?}",
                summary.job_name, summary.rows_written, summary.new_watermark
            ),
            Err(e) => {
                eprintln!("✗  {}  error: {e:#}", name);
                had_error = true;
            }
        }
    }

    if had_error {
        anyhow::bail!("One or more sync jobs failed");
    }
    Ok(())
}

async fn cmd_sync_consume(catalog: impl Catalog + 'static, config_path: &str) -> Result<()> {
    let cfg = Arc::new(SyncConfig::from_file(config_path)?);
    let rmq = cfg
        .rabbitmq
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("No 'rabbitmq' section in config"))?;

    println!("Starting RabbitMQ consumer loop.  Press Ctrl-C to stop.");
    sync::rabbitmq::run_consumers(rmq, Arc::clone(&cfg), Arc::new(catalog)).await?;
    Ok(())
}

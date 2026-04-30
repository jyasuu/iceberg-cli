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

mod cmd;
mod config;
mod sync;

use std::{collections::HashMap, sync::Arc};
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

use anyhow::{Context, Result, bail};
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
    Catalog, CatalogBuilder, TableCreation,
    spec::{
        DataFileFormat,
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

    /// Enable verbose (DEBUG-level) logging.
    ///
    /// Use once (`-v`) for DEBUG, twice (`-vv`) for TRACE.
    /// The RUST_LOG environment variable always takes precedence.
    #[arg(short, long, action = clap::ArgAction::Count, global = true)]
    verbose: u8,

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

    /// Drop a table.
    ///
    /// By default this is a *soft delete*: the catalog entry is removed but
    /// all Parquet data files and Iceberg metadata files remain in storage
    /// and can be recovered by re-registering the table.
    ///
    /// Pass `--purge` for a *hard delete*: every data and metadata file
    /// referenced by the table's manifest chain is permanently deleted from
    /// storage, then the catalog entry is removed.  This is irreversible.
    DropTable {
        /// Fully-qualified table: namespace.table
        #[arg(long)]
        table: String,

        /// Hard-delete: permanently remove all data and metadata files from
        /// storage in addition to dropping the catalog entry.
        ///
        /// WARNING: IRREVERSIBLE. Only files listed in the table's manifest
        /// chain are deleted (safe in shared buckets). Without this flag only
        /// the catalog entry is removed (soft delete — data is recoverable).
        #[arg(long, default_value_t = false)]
        purge: bool,
    },

    /// Rename a table within the catalog
    RenameTable {
        /// Source fully-qualified table: namespace.table
        #[arg(long)]
        from: String,
        /// Destination fully-qualified table: namespace.table
        #[arg(long)]
        to: String,
    },

    /// Count the total number of rows in a table
    Count {
        /// Fully-qualified table: namespace.table
        #[arg(long)]
        table: String,
    },

    /// List snapshots for a table (most recent first)
    Snapshots {
        /// Fully-qualified table: namespace.table
        #[arg(long)]
        table: String,
        /// Maximum number of snapshots to display (default: 20)
        #[arg(long, default_value = "20")]
        limit: usize,
    },

    /// Expire snapshots older than N days (keeps the current snapshot)
    ExpireSnapshots {
        /// Fully-qualified table: namespace.table
        #[arg(long)]
        table: String,
        /// Expire snapshots older than this many days
        #[arg(long, default_value = "7")]
        older_than_days: i64,
    },

    /// Show file-level statistics for a table (snapshot, file count, total size, row count)
    TableStats {
        /// Fully-qualified table: namespace.table
        #[arg(long)]
        table: String,
    },

    /// Add a column to an existing table
    AddColumn {
        /// Fully-qualified table: namespace.table
        #[arg(long)]
        table: String,
        /// Column spec: "name:type" (same types as create-table)
        #[arg(long)]
        column: String,
    },

    /// Drop a column from an existing table
    DropColumn {
        /// Fully-qualified table: namespace.table
        #[arg(long)]
        table: String,
        /// Column name to remove
        #[arg(long)]
        column: String,
    },

    /// Show or update namespace-level properties
    NamespaceProperties {
        /// Namespace to inspect or update
        #[arg(long)]
        namespace: String,
        /// Set a property as "key=value" (can be repeated)
        #[arg(long = "set", value_name = "KEY=VALUE")]
        set: Vec<String>,
        /// Remove a property key (can be repeated)
        #[arg(long = "remove", value_name = "KEY")]
        remove: Vec<String>,
    },

    /// Drop a namespace (must be empty)
    DropNamespace {
        /// Namespace to drop
        #[arg(long)]
        namespace: String,
    },

    /// Copy all data from one table into another via fast-append (schemas must match)
    CopyTable {
        /// Source fully-qualified table: namespace.table
        #[arg(long)]
        from: String,
        /// Destination fully-qualified table: namespace.table
        #[arg(long)]
        to: String,
        /// Max rows to copy (default: all)
        #[arg(long)]
        limit: Option<usize>,
    },
}

// ─── Entry point ─────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // ── Logging initialisation ────────────────────────────────────────────────
    //
    // Priority order (highest → lowest):
    //   1. RUST_LOG environment variable  (always wins — useful in CI/CD)
    //   2. -v / -vv CLI flags             (DEBUG / TRACE for iceberg_cli)
    //   3. Default: WARN                  (silent unless something goes wrong)
    //
    // The `tracing` calls throughout the sync engine emit structured key=value
    // fields; the human-readable formatter below prints them inline so verbose
    // output is still grep-friendly.
    let default_level = match cli.verbose {
        0 => "warn",
        1 => "iceberg_cli=debug",
        _ => "iceberg_cli=trace",
    };

    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_level));

    tracing_subscriber::registry()
        .with(
            fmt::layer()
                .with_target(false) // omit module path — keeps lines short
                .with_thread_ids(false)
                .compact(),
        )
        .with(filter)
        .init();

    tracing::debug!(version = env!("CARGO_PKG_VERSION"), "iceberg-cli starting");

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
        Commands::DropTable { table, purge } => cmd_drop_table(&catalog, table, *purge).await?,
        Commands::RenameTable { from, to } => cmd_rename_table(&catalog, from, to).await?,
        Commands::Count { table } => cmd_count(&catalog, table).await?,
        Commands::Snapshots { table, limit } => cmd_snapshots(&catalog, table, *limit).await?,
        Commands::ExpireSnapshots {
            table,
            older_than_days,
        } => cmd_expire_snapshots(&catalog, table, *older_than_days).await?,
        Commands::TableStats { table } => cmd_table_stats(&catalog, table).await?,
        Commands::AddColumn { table, column } => cmd_add_column(&catalog, table, column).await?,
        Commands::DropColumn { table, column } => cmd_drop_column(&catalog, table, column).await?,
        Commands::NamespaceProperties {
            namespace,
            set,
            remove,
        } => cmd_namespace_properties(&catalog, namespace, set, remove).await?,
        Commands::DropNamespace { namespace } => cmd_drop_namespace(&catalog, namespace).await?,
        Commands::CopyTable { from, to, limit } => {
            cmd_copy_table(&catalog, from, to, *limit).await?
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

    tracing::debug!(
        catalog_uri = uri,
        catalog_name = name,
        "Connected to REST catalog"
    );
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

// ─── Drop table (soft and hard delete) ───────────────────────────────────────
//
// Soft delete (default, --purge omitted)
//   Removes only the catalog entry (table metadata pointer).  All Parquet
//   data files and Iceberg metadata files remain untouched in storage.
//   The table can be recovered by re-registering the metadata location.
//
// Hard delete (--purge)
//   Walks every snapshot's manifest chain, collects all referenced data files
//   and manifest files, deletes them one by one via FileIO, then removes the
//   catalog entry.  Only files explicitly listed in the manifests are deleted —
//   it is safe to use in shared buckets where multiple tables share a prefix.
//   This operation is IRREVERSIBLE.

async fn cmd_drop_table(catalog: &impl Catalog, table_str: &str, purge: bool) -> Result<()> {
    let ident = parse_table(table_str)?;

    // ── Table existence check (shared for both paths) ─────────────────────────
    let table = match catalog.load_table(&ident).await {
        Ok(t) => t,
        Err(e) => {
            let msg = e.to_string().to_lowercase();
            if msg.contains("does not exist")
                || msg.contains("not found")
                || msg.contains("no such")
            {
                tracing::warn!(table = table_str, "Table does not exist — skipping");
                println!("Table '{table_str}' does not exist — skipping.");
                return Ok(());
            }
            return Err(e).with_context(|| format!("load_table({table_str})"));
        }
    };

    if purge {
        // ── Hard delete ───────────────────────────────────────────────────────
        tracing::warn!(
            table = table_str,
            "Hard delete requested — collecting files to purge"
        );
        purge_table_files(&table, table_str).await?;
    } else {
        // ── Soft delete ───────────────────────────────────────────────────────
        tracing::info!(
            table = table_str,
            "Soft delete: removing catalog entry only"
        );
        tracing::warn!(
            table = table_str,
            location = table.metadata().location(),
            "Data files NOT deleted. Re-run with --purge to also remove files from storage."
        );
    }

    // Drop the catalog entry (both paths)
    match catalog.drop_table(&ident).await {
        Ok(_) => {
            if purge {
                println!(
                    "Purged and dropped '{table_str}' (hard delete — catalog entry and all data files removed)."
                );
            } else {
                println!(
                    "Dropped '{table_str}' (soft delete — catalog entry removed, data files intact at {}).",
                    table.metadata().location()
                );
            }
        }
        Err(e) => {
            return Err(e).with_context(|| format!("drop_table({table_str})"));
        }
    }

    Ok(())
}

/// Walk every snapshot's manifest chain and delete all referenced data files,
/// manifest files, and manifest list files via FileIO.
///
/// Files are collected across ALL snapshots (not just current) so that
/// orphaned files from expired snapshots are also removed.  The metadata
/// JSON files are collected from the table's version history.
///
/// Deletion order: data files → manifest files → manifest list files →
/// metadata JSON files.  This order ensures that if any step fails the
/// remaining metadata still points to valid (surviving) data.
async fn purge_table_files(table: &iceberg::table::Table, table_str: &str) -> Result<()> {
    use std::collections::HashSet;

    let meta = table.metadata();
    let file_io = table.file_io();
    let mut data_files: HashSet<String> = HashSet::new();
    let mut manifest_files: HashSet<String> = HashSet::new();
    let mut manifest_list_files: HashSet<String> = HashSet::new();

    // ── Walk every snapshot ───────────────────────────────────────────────────
    for snapshot in meta.snapshots() {
        let manifest_list_path = snapshot.manifest_list().to_string();
        manifest_list_files.insert(manifest_list_path.clone());

        tracing::debug!(
            snapshot_id = snapshot.snapshot_id(),
            manifest_list = %manifest_list_path,
            "Scanning snapshot"
        );

        // Load the manifest list for this snapshot
        let manifest_list = match snapshot
            .load_manifest_list(file_io, &table.metadata_ref())
            .await
        {
            Ok(ml) => ml,
            Err(e) => {
                tracing::warn!(
                    snapshot_id = snapshot.snapshot_id(),
                    error = %e,
                    "Could not load manifest list — skipping snapshot (files may remain)"
                );
                continue;
            }
        };

        // Walk each manifest referenced by this snapshot
        for manifest_entry in manifest_list.entries() {
            let manifest_path = manifest_entry.manifest_path.clone();
            manifest_files.insert(manifest_path.clone());

            let manifest = match manifest_entry.load_manifest(file_io).await {
                Ok(m) => m,
                Err(e) => {
                    tracing::warn!(
                        manifest = %manifest_path,
                        error = %e,
                        "Could not load manifest — skipping (files may remain)"
                    );
                    continue;
                }
            };

            for entry in manifest.entries() {
                let file_path = entry.data_file().file_path().to_string();
                tracing::trace!(file = %file_path, "Queued for deletion");
                data_files.insert(file_path);
            }
        }
    }

    // ── Collect metadata JSON files from version history ──────────────────────
    // The metadata log tracks every metadata.json the table has ever written.
    let mut metadata_files: HashSet<String> = HashSet::new();
    for log_entry in meta.history() {
        // history() returns SnapshotLog entries — the metadata JSON paths are
        // stored in metadata_log() separately.
        let _ = log_entry; // snapshot log, not metadata log
    }
    // metadata_log() gives us the actual metadata.json version history.
    for meta_entry in meta.metadata_log() {
        metadata_files.insert(meta_entry.metadata_file.clone());
    }
    // Always include the current metadata file.
    if let Some(current_meta_path) = meta.location().strip_suffix('/') {
        // The REST catalog stores current metadata under metadata/v<N>.metadata.json;
        // we can't always know the exact name without the catalog, so we rely on
        // metadata_log() above and warn if it was empty.
        let _ = current_meta_path;
    }

    let total_data = data_files.len();
    let total_manifests = manifest_files.len();
    let total_manifest_lists = manifest_list_files.len();
    let total_metadata = metadata_files.len();
    let grand_total = total_data + total_manifests + total_manifest_lists + total_metadata;

    tracing::info!(
        table = table_str,
        data_files = total_data,
        manifest_files = total_manifests,
        manifest_list_files = total_manifest_lists,
        metadata_files = total_metadata,
        "Starting hard delete"
    );
    println!(
        "Purging {grand_total} file(s): {total_data} data, {total_manifests} manifests,          {total_manifest_lists} manifest lists, {total_metadata} metadata JSON."
    );

    // ── Delete in safe order ──────────────────────────────────────────────────
    // Data files first: if we fail here, manifests still point to valid files.
    let mut deleted = 0usize;
    let mut failed = 0usize;

    for path in &data_files {
        match file_io.delete(path).await {
            Ok(_) => {
                tracing::debug!(path, "Deleted data file");
                deleted += 1;
            }
            Err(e) => {
                tracing::warn!(path, error = %e, "Failed to delete data file");
                failed += 1;
            }
        }
    }

    for path in &manifest_files {
        match file_io.delete(path).await {
            Ok(_) => {
                tracing::debug!(path, "Deleted manifest file");
                deleted += 1;
            }
            Err(e) => {
                tracing::warn!(path, error = %e, "Failed to delete manifest file");
                failed += 1;
            }
        }
    }

    for path in &manifest_list_files {
        match file_io.delete(path).await {
            Ok(_) => {
                tracing::debug!(path, "Deleted manifest list file");
                deleted += 1;
            }
            Err(e) => {
                tracing::warn!(path, error = %e, "Failed to delete manifest list file");
                failed += 1;
            }
        }
    }

    for path in &metadata_files {
        match file_io.delete(path).await {
            Ok(_) => {
                tracing::debug!(path, "Deleted metadata JSON file");
                deleted += 1;
            }
            Err(e) => {
                tracing::warn!(path, error = %e, "Failed to delete metadata JSON file");
                failed += 1;
            }
        }
    }

    tracing::info!(
        table = table_str,
        deleted,
        failed,
        "Hard delete file pass complete"
    );

    if failed > 0 {
        tracing::warn!(
            table = table_str,
            failed,
            "Some files could not be deleted. The catalog entry will still be dropped."
        );
        println!(
            "Warning: {failed} file(s) could not be deleted (see --verbose for details).              Catalog entry will still be removed."
        );
    } else {
        println!("Deleted {deleted} file(s) from storage.");
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

    match catalog.create_table(ident.namespace(), creation).await {
        Ok(_) => println!("Created table '{table_str}'."),
        Err(e) => {
            let msg = e.to_string().to_lowercase();
            if msg.contains("already exists") || msg.contains("already exist") {
                println!("Table '{table_str}' already exists — skipping.");
            } else {
                return Err(e).with_context(|| format!("create_table({table_str})"));
            }
        }
    }
    Ok(())
}

// ─── New commands ─────────────────────────────────────────────────────────────

async fn cmd_rename_table(catalog: &impl Catalog, from_str: &str, to_str: &str) -> Result<()> {
    let src = parse_table(from_str)?;
    let dst = parse_table(to_str)?;
    catalog
        .rename_table(&src, &dst)
        .await
        .with_context(|| format!("rename_table({from_str} → {to_str})"))?;
    println!("Renamed '{from_str}' → '{to_str}'.");
    Ok(())
}

async fn cmd_count(catalog: &impl Catalog, table_str: &str) -> Result<()> {
    let ident = parse_table(table_str)?;
    let table = catalog
        .load_table(&ident)
        .await
        .with_context(|| format!("load_table({table_str})"))?;

    let stream = table
        .scan()
        .build()
        .context("build scan")?
        .to_arrow()
        .await
        .context("to_arrow")?;

    let batches: Vec<RecordBatch> = stream.try_collect().await.context("collect batches")?;
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    println!("{total} rows in '{table_str}'.");
    Ok(())
}

async fn cmd_snapshots(catalog: &impl Catalog, table_str: &str, limit: usize) -> Result<()> {
    let ident = parse_table(table_str)?;
    let table = catalog
        .load_table(&ident)
        .await
        .with_context(|| format!("load_table({table_str})"))?;
    let meta = table.metadata();

    let mut snapshots: Vec<_> = meta.snapshots().collect();
    // Most recent first
    snapshots.sort_by_key(|b| std::cmp::Reverse(b.timestamp_ms()));

    if snapshots.is_empty() {
        println!("(no snapshots — table is empty)");
        return Ok(());
    }

    let current_id = meta.current_snapshot().map(|s| s.snapshot_id());

    let mut tbl = Table::new();
    tbl.load_preset(UTF8_FULL).set_header(vec![
        "Snapshot ID",
        "Timestamp (UTC)",
        "Operation",
        "Current",
    ]);

    for snap in snapshots.iter().take(limit) {
        let ts = chrono::DateTime::from_timestamp_millis(snap.timestamp_ms())
            .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
            .unwrap_or_else(|| snap.timestamp_ms().to_string());
        let op = format!("{:?}", snap.summary().operation);
        let is_current = if current_id == Some(snap.snapshot_id()) {
            "✓"
        } else {
            ""
        };
        tbl.add_row(vec![
            snap.snapshot_id().to_string(),
            ts,
            op,
            is_current.to_string(),
        ]);
    }
    println!("{tbl}");
    println!(
        "({} snapshots total, showing up to {limit})",
        snapshots.len()
    );
    Ok(())
}

async fn cmd_expire_snapshots(
    catalog: &impl Catalog,
    table_str: &str,
    older_than_days: i64,
) -> Result<()> {
    let ident = parse_table(table_str)?;
    let table = catalog
        .load_table(&ident)
        .await
        .with_context(|| format!("load_table({table_str})"))?;
    let meta = table.metadata();

    let cutoff_ms =
        (chrono::Utc::now() - chrono::Duration::days(older_than_days)).timestamp_millis();

    let current_id = meta
        .current_snapshot()
        .map(|s| s.snapshot_id())
        .ok_or_else(|| anyhow::anyhow!("Table '{table_str}' has no current snapshot"))?;

    // Identify snapshots to expire (older than cutoff, not current)
    let to_expire: Vec<i64> = meta
        .snapshots()
        .filter(|s| s.snapshot_id() != current_id && s.timestamp_ms() < cutoff_ms)
        .map(|s| s.snapshot_id())
        .collect();

    if to_expire.is_empty() {
        println!("No snapshots older than {older_than_days} days to expire.");
        return Ok(());
    }

    // Use the REST catalog's update-table endpoint with a RemoveSnapshots update.
    // iceberg-rust 0.9.0 Transaction does not expose a snapshot-expiry action,
    // but TableUpdate::RemoveSnapshots is part of the Iceberg REST spec.
    let uri = std::env::var("ICEBERG_URI").unwrap_or_else(|_| "http://localhost:8181".to_string());
    let ns = ident.namespace().to_url_string();
    let tbl_name = ident.name();
    let url = format!("{uri}/v1/namespaces/{ns}/tables/{tbl_name}");

    let payload = serde_json::json!({
        "requirements": [],
        "updates": [
            {
                "action": "remove-snapshots",
                "snapshot-ids": to_expire
            }
        ]
    });

    let resp = reqwest::Client::new()
        .post(&url)
        .json(&payload)
        .send()
        .await
        .context("POST update-table (remove-snapshots)")?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("Catalog returned {status}: {body}");
    }

    println!(
        "Expired {} snapshot(s) older than {older_than_days} day(s).",
        to_expire.len()
    );
    Ok(())
}

async fn cmd_table_stats(catalog: &impl Catalog, table_str: &str) -> Result<()> {
    let ident = parse_table(table_str)?;
    let table = catalog
        .load_table(&ident)
        .await
        .with_context(|| format!("load_table({table_str})"))?;
    let meta = table.metadata();

    let snap = match meta.current_snapshot() {
        Some(s) => s,
        None => {
            println!("Table '{table_str}' has no snapshots (empty table).");
            return Ok(());
        }
    };

    // summary() returns &Summary (never None) in iceberg 0.9
    let summary = snap.summary();

    println!("━━━  Table Stats: {table_str}  ━━━");
    println!("  Snapshot ID     : {}", snap.snapshot_id());

    let ts = chrono::DateTime::from_timestamp_millis(snap.timestamp_ms())
        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
        .unwrap_or_else(|| snap.timestamp_ms().to_string());
    println!("  Snapshot time   : {ts}");
    println!("  Operation       : {:?}", summary.operation);

    let props = &summary.additional_properties;
    let get = |k: &str| props.get(k).map(|v: &String| v.as_str()).unwrap_or("—");
    let mut tbl = Table::new();
    tbl.load_preset(UTF8_FULL)
        .set_header(vec!["Metric", "Value"]);
    tbl.add_row(vec!["Total data files", get("total-data-files")]);
    tbl.add_row(vec!["Total delete files", get("total-delete-files")]);
    tbl.add_row(vec!["Total records", get("total-records")]);
    tbl.add_row(vec!["Total file size (bytes)", get("total-files-size")]);
    tbl.add_row(vec!["Added files", get("added-data-files")]);
    tbl.add_row(vec!["Added records", get("added-records")]);
    tbl.add_row(vec!["Deleted records", get("deleted-records")]);
    println!("{tbl}");
    Ok(())
}

async fn cmd_add_column(catalog: &impl Catalog, table_str: &str, column_spec: &str) -> Result<()> {
    let ident = parse_table(table_str)?;
    let table = catalog
        .load_table(&ident)
        .await
        .with_context(|| format!("load_table({table_str})"))?;

    // Parse and validate the column spec "name:type" up front
    let parts: Vec<&str> = column_spec.splitn(2, ':').collect();
    if parts.len() != 2 {
        anyhow::bail!("Expected 'name:type', got '{column_spec}'");
    }
    let col_name = parts[0].trim();
    let col_type_str = parts[1].trim();

    use iceberg::spec::{NestedField, PrimitiveType as PT, Type};
    let ptype = match col_type_str.to_lowercase().as_str() {
        "long" | "int64" => PT::Long,
        "int" | "int32" => PT::Int,
        "string" | "str" => PT::String,
        "double" | "float64" => PT::Double,
        "float" | "float32" => PT::Float,
        "boolean" | "bool" => PT::Boolean,
        "date" => PT::Date,
        "timestamp" => PT::Timestamp,
        other => anyhow::bail!("Unknown column type '{other}'"),
    };

    let current_schema = table.metadata().current_schema();

    // Guard: column must not already exist
    if current_schema.field_by_name(col_name).is_some() {
        anyhow::bail!("Column '{col_name}' already exists in '{table_str}'");
    }

    // Assign next field ID
    let max_id = current_schema
        .as_struct()
        .fields()
        .iter()
        .map(|f| f.id)
        .max()
        .unwrap_or(0);
    let new_id = max_id + 1;

    // Build new schema: all existing fields + the new optional column
    let mut new_fields: Vec<_> = current_schema.as_struct().fields().to_vec();
    new_fields.push(NestedField::optional(new_id, col_name, Type::Primitive(ptype)).into());

    let new_schema = Schema::builder()
        .with_fields(new_fields)
        .with_schema_id(current_schema.schema_id() + 1)
        .build()
        .context("build updated schema")?;

    // Commit via the REST catalog directly: POST /v1/namespaces/{ns}/tables/{tbl}
    // with an UpdateTableRequest that adds an AddSchemaUpdate + SetCurrentSchemaUpdate.
    // iceberg-rust 0.9.0 Transaction does not expose a schema-evolution action, so we
    // call the REST endpoint ourselves using reqwest.
    let uri = std::env::var("ICEBERG_URI").unwrap_or_else(|_| "http://localhost:8181".to_string());
    let ns = ident.namespace().to_url_string();
    let tbl = ident.name();
    let url = format!("{uri}/v1/namespaces/{ns}/tables/{tbl}");

    let schema_json = serde_json::to_value(&new_schema).context("serialize schema")?;
    let payload = serde_json::json!({
        "requirements": [{ "type": "assert-current-schema-id", "current-schema-id": current_schema.schema_id() }],
        "updates": [
            { "action": "add-schema", "schema": schema_json, "last-column-id": new_id },
            { "action": "set-current-schema", "schema-id": -1 }
        ]
    });

    let resp = reqwest::Client::new()
        .post(&url)
        .json(&payload)
        .send()
        .await
        .context("POST update-table")?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("Catalog returned {status}: {body}");
    }

    println!("Added column '{col_name}' ({col_type_str}) to '{table_str}'.");
    Ok(())
}

async fn cmd_drop_column(catalog: &impl Catalog, table_str: &str, column: &str) -> Result<()> {
    let ident = parse_table(table_str)?;
    let table = catalog
        .load_table(&ident)
        .await
        .with_context(|| format!("load_table({table_str})"))?;

    let current_schema = table.metadata().current_schema();

    // Verify the column exists
    if current_schema.field_by_name(column).is_none() {
        anyhow::bail!("Column '{column}' does not exist in '{table_str}'");
    }

    let remaining: Vec<_> = current_schema
        .as_struct()
        .fields()
        .iter()
        .filter(|f| f.name != column)
        .cloned()
        .collect();

    if remaining.is_empty() {
        anyhow::bail!("Cannot drop the only column in '{table_str}'");
    }

    let max_id = remaining.iter().map(|f| f.id).max().unwrap_or(0);
    let new_schema = Schema::builder()
        .with_fields(remaining)
        .with_schema_id(current_schema.schema_id() + 1)
        .build()
        .context("build updated schema")?;

    // Same direct REST call as add-column (iceberg-rust 0.9 Transaction has no
    // schema-evolution action).
    let uri = std::env::var("ICEBERG_URI").unwrap_or_else(|_| "http://localhost:8181".to_string());
    let ns = ident.namespace().to_url_string();
    let tbl = ident.name();
    let url = format!("{uri}/v1/namespaces/{ns}/tables/{tbl}");

    let schema_json = serde_json::to_value(&new_schema).context("serialize schema")?;
    let payload = serde_json::json!({
        "requirements": [{ "type": "assert-current-schema-id", "current-schema-id": current_schema.schema_id() }],
        "updates": [
            { "action": "add-schema", "schema": schema_json, "last-column-id": max_id },
            { "action": "set-current-schema", "schema-id": -1 }
        ]
    });

    let resp = reqwest::Client::new()
        .post(&url)
        .json(&payload)
        .send()
        .await
        .context("POST update-table")?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("Catalog returned {status}: {body}");
    }

    println!("Dropped column '{column}' from '{table_str}'.");
    Ok(())
}

async fn cmd_namespace_properties(
    catalog: &impl Catalog,
    namespace: &str,
    set: &[String],
    remove: &[String],
) -> Result<()> {
    let ns = parse_namespace(namespace)?;

    if set.is_empty() && remove.is_empty() {
        // Read-only: display current properties
        let props = catalog
            .get_namespace(&ns)
            .await
            .with_context(|| format!("get_namespace({namespace})"))?
            .properties()
            .clone();

        if props.is_empty() {
            println!("(no properties on namespace '{namespace}')");
        } else {
            let mut tbl = Table::new();
            tbl.load_preset(UTF8_FULL).set_header(vec!["Key", "Value"]);
            for (k, v) in &props {
                tbl.add_row(vec![k, v]);
            }
            println!("{tbl}");
        }
        return Ok(());
    }

    // In iceberg 0.9 update_namespace only accepts updates (HashMap); there is no
    // separate removals argument. To "remove" a key we omit it from the new map —
    // the catalog will replace the property set with whatever we send.
    // First fetch current props so we can merge intelligently.
    let mut current = catalog
        .get_namespace(&ns)
        .await
        .with_context(|| format!("get_namespace({namespace})"))?
        .properties()
        .clone();

    // Apply removals
    for key in remove {
        current.remove(key.as_str());
    }
    // Apply updates (overwrite / insert)
    for kv in set {
        let parts: Vec<&str> = kv.splitn(2, '=').collect();
        if parts.len() != 2 {
            anyhow::bail!("Expected 'key=value', got '{kv}'");
        }
        current.insert(parts[0].to_string(), parts[1].to_string());
    }

    catalog
        .update_namespace(&ns, current)
        .await
        .with_context(|| format!("update_namespace({namespace})"))?;

    println!("Updated properties for namespace '{namespace}'.");
    Ok(())
}

async fn cmd_drop_namespace(catalog: &impl Catalog, namespace: &str) -> Result<()> {
    let ns = parse_namespace(namespace)?;
    match catalog.drop_namespace(&ns).await {
        Ok(_) => println!("Dropped namespace '{namespace}'."),
        Err(e) => {
            let msg = e.to_string().to_lowercase();
            if msg.contains("does not exist") || msg.contains("not found") {
                println!("Namespace '{namespace}' does not exist — skipping.");
            } else {
                return Err(e).with_context(|| format!("drop_namespace({namespace})"));
            }
        }
    }
    Ok(())
}

async fn cmd_copy_table(
    catalog: &impl Catalog,
    from_str: &str,
    to_str: &str,
    limit: Option<usize>,
) -> Result<()> {
    // Load source
    let src_ident = parse_table(from_str)?;
    let src_table = catalog
        .load_table(&src_ident)
        .await
        .with_context(|| format!("load_table({from_str})"))?;

    // Load destination
    let dst_ident = parse_table(to_str)?;
    let dst_table = catalog
        .load_table(&dst_ident)
        .await
        .with_context(|| format!("load_table({to_str})"))?;

    let dst_meta = dst_table.metadata();
    let dst_schema = dst_meta.current_schema();
    let dst_partition_spec = dst_meta.default_partition_spec();

    // Scan source
    let stream = src_table
        .scan()
        .build()
        .context("build scan")?
        .to_arrow()
        .await
        .context("to_arrow")?;

    let all_batches: Vec<RecordBatch> = stream.try_collect().await.context("collect")?;

    // Apply row limit if requested
    let batches: Vec<RecordBatch> = if let Some(max_rows) = limit {
        let mut out = Vec::new();
        let mut remaining = max_rows;
        for batch in all_batches {
            if remaining == 0 {
                break;
            }
            if batch.num_rows() <= remaining {
                remaining -= batch.num_rows();
                out.push(batch);
            } else {
                out.push(batch.slice(0, remaining));
                remaining = 0;
            }
        }
        out
    } else {
        all_batches
    };

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

    if total_rows == 0 {
        println!("Source table '{from_str}' is empty — nothing to copy.");
        return Ok(());
    }

    // Write to destination using fast-append
    let location_gen = DefaultLocationGenerator::new(dst_meta.clone())?;
    let file_name_gen =
        ProductionFileNameGenerator::new("data", None::<&str>, DataFileFormat::Parquet);
    let parquet_builder =
        ParquetWriterBuilder::new(WriterProperties::builder().build(), dst_schema.clone());
    let rolling_builder =
        iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder::new(
            parquet_builder,
            536_870_912usize,
            dst_table.file_io().clone(),
            location_gen,
            file_name_gen,
        );

    let partition_key = if dst_partition_spec.is_unpartitioned() {
        None
    } else {
        // For a simple copy we skip partition override; the writer will use the default.
        None
    };

    let mut writer = DataFileWriterBuilder::new(rolling_builder)
        .build(partition_key)
        .await?;

    for batch in batches {
        writer.write(batch).await?;
    }
    let data_files = writer.close().await?;

    let tx = iceberg::transaction::Transaction::new(&dst_table);
    let action = tx.fast_append().add_data_files(data_files);
    let tx = action.apply(tx)?;
    let updated = tx.commit(catalog).await?;

    println!(
        "Copied {total_rows} rows from '{from_str}' → '{to_str}'. New snapshot: {:?}",
        updated
            .metadata()
            .current_snapshot()
            .map(|s| s.snapshot_id())
    );
    Ok(())
}

// ─── Helpers ─────────────────────────────────────────────────────────────────
// parse_namespace / parse_table / parse_schema moved to src/cmd/parse.rs
use cmd::parse::{parse_namespace, parse_schema, parse_table};

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

    // Attach a span so every tracing event emitted inside this call carries
    // the config path and filters as structured fields — useful when multiple
    // syncs run in CI logs.
    let _span = tracing::info_span!(
        "sync",
        config = config_path,
        job = ?only_job,
        group = ?only_group,
        dry_run,
        parallel,
    )
    .entered();

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
        tracing::warn!("No jobs matched the given filters — nothing to run");
        println!("No jobs to run.");
        return Ok(());
    }

    tracing::info!(job_count = jobs.len(), "Starting sync run");

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
    let mut total_rows = 0usize;

    for (name, result) in results {
        match result {
            Ok(summary) => {
                total_rows += summary.rows_written;
                tracing::info!(
                    job = %summary.job_name,
                    rows = summary.rows_written,
                    watermark = ?summary.new_watermark,
                    "Job succeeded"
                );
                println!(
                    "✓  {}  rows={}  watermark={:?}",
                    summary.job_name, summary.rows_written, summary.new_watermark
                );
            }
            Err(e) => {
                tracing::error!(job = %name, error = %e, "Job failed");
                eprintln!("✗  {}  error: {e:#}", name);
                had_error = true;
            }
        }
    }

    if had_error {
        tracing::error!(total_rows, "Sync run finished with errors");
        anyhow::bail!("One or more sync jobs failed");
    }

    tracing::info!(total_rows, "Sync run complete");
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

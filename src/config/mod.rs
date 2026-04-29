//! Sync configuration — loaded from a YAML file.
//!
//! ## New features
//!
//! ### Cursor-based pagination (`cursor_column`)
//! Instead of LIMIT/OFFSET (which drifts when rows are inserted mid-run), the
//! engine wraps the caller's SQL as:
//!
//! ```sql
//! SELECT * FROM (<user_sql>) _q
//! WHERE <cursor_column> > :_cursor
//! ORDER BY <cursor_column>
//! LIMIT <batch_size>
//! ```
//!
//! The cursor is advanced in memory each batch; the watermark handles
//! across-run resumption.
//!
//! ### Retry config (`retry`)
//! Global or per-job exponential-backoff retry.
//!
//! ### Schema evolution (`schema_evolution`)
//! With `allow_add_columns: true`, new columns in the source are appended to
//! the Iceberg table automatically.
//!
//! ### Write modes (`write_mode`)
//! Controls how each batch is committed to the Iceberg table:
//!
//! | Mode        | Iceberg behaviour                              | Typical use-case              |
//! |-------------|------------------------------------------------|-------------------------------|
//! | `append`    | `fast_append` — only adds new data files       | Events, logs, immutable CDC   |
//! | `overwrite` | Replace-files by partition                     | Full-refresh dimensions        |
//! | `upsert`    | Position-delete matching rows, append new rows | PK-keyed CDC updates           |
//! | `merge_into`| Route rows by `_op` column (I / U / D)         | Full MERGE semantics from CDC  |

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub(crate) mod validation;

// ── Top-level ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SyncConfig {
    pub sources: HashMap<String, SourceConfig>,
    pub destinations: HashMap<String, DestinationConfig>,
    pub sync_jobs: Vec<SyncJob>,
    #[serde(default)]
    pub retry: RetryConfig,
    #[serde(default)]
    pub rabbitmq: Option<RabbitMqConfig>,
}

impl SyncConfig {
    pub fn from_file(path: &str) -> anyhow::Result<Self> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("Cannot read config '{}': {}", path, e))?;
        let cfg: SyncConfig = serde_yaml::from_str(&text)
            .map_err(|e| anyhow::anyhow!("Invalid YAML in '{}': {}", path, e))?;
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn ordered_jobs(&self) -> anyhow::Result<Vec<&SyncJob>> {
        let mut result: Vec<&SyncJob> = Vec::new();
        let mut visited: std::collections::HashSet<&str> = Default::default();

        fn visit<'a>(
            name: &'a str,
            jobs: &'a [SyncJob],
            visited: &mut std::collections::HashSet<&'a str>,
            result: &mut Vec<&'a SyncJob>,
        ) -> anyhow::Result<()> {
            if visited.contains(name) {
                return Ok(());
            }
            let job = jobs
                .iter()
                .find(|j| j.name == name)
                .ok_or_else(|| anyhow::anyhow!("Job '{}' not found", name))?;
            if let Some(dep) = &job.depends_on {
                visit(dep, jobs, visited, result)?;
            }
            visited.insert(name);
            result.push(job);
            Ok(())
        }

        for name in self.sync_jobs.iter().map(|j| j.name.as_str()) {
            visit(name, &self.sync_jobs, &mut visited, &mut result)?;
        }
        Ok(result)
    }

    #[allow(dead_code)]
    pub fn ordered_jobs_for_group<'a>(&'a self, group: &str) -> anyhow::Result<Vec<&'a SyncJob>> {
        let all = self.ordered_jobs()?;
        Ok(all
            .into_iter()
            .filter(|j| j.group.as_deref() == Some(group))
            .collect())
    }

    pub fn retry_for(&self, job: &SyncJob) -> RetryConfig {
        job.retry.clone().unwrap_or_else(|| self.retry.clone())
    }
}

// ── Source ────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SourceConfig {
    #[serde(rename = "type")]
    pub source_type: SourceType,
    pub dsn: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SourceType {
    Postgres,
}

// ── Destination ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DestinationConfig {
    pub catalog_uri: String,
    pub s3_endpoint: String,
    #[serde(default = "default_region")]
    pub region: String,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub session_token: Option<String>,
    #[serde(default = "default_catalog_name")]
    pub catalog_name: String,
}

fn default_region() -> String {
    "us-east-1".to_string()
}
fn default_catalog_name() -> String {
    "default".to_string()
}

// ── Retry policy ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RetryConfig {
    #[serde(default = "default_max_attempts")]
    pub max_attempts: u32,
    #[serde(default = "default_initial_delay_ms")]
    pub initial_delay_ms: u64,
    #[serde(default = "default_backoff_multiplier")]
    pub backoff_multiplier: f64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: default_max_attempts(),
            initial_delay_ms: default_initial_delay_ms(),
            backoff_multiplier: default_backoff_multiplier(),
        }
    }
}

fn default_max_attempts() -> u32 {
    3
}
fn default_initial_delay_ms() -> u64 {
    500
}
fn default_backoff_multiplier() -> f64 {
    2.0
}

// ── Schema evolution ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct SchemaEvolutionConfig {
    #[serde(default)]
    pub allow_add_columns: bool,
}

// ── Write mode ────────────────────────────────────────────────────────────────

/// How the engine commits each batch to the Iceberg table.
///
/// ## Choosing the right mode
///
/// ```text
/// Source produces only new rows?           → append   (default, fastest)
/// Daily full-refresh of a partition?       → overwrite
/// CDC stream with row updates (PK-keyed)?  → upsert
/// CDC stream with I/U/D in _op column?     → merge_into
/// ```
#[derive(Debug, Clone, Deserialize, Serialize, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WriteMode {
    /// Only add new data files. Never touches existing files. (default)
    ///
    /// Uses Iceberg `fast_append`. Produces one new data file per batch.
    /// Cheapest operation — no read-before-write.
    #[default]
    Append,

    /// Drop and replace the matching partition(s) before writing new rows.
    ///
    /// Requires `partition_column`. Uses Iceberg `replace_data_files`.
    /// Old data files in the matched partition are marked as deleted in the
    /// same snapshot that adds the new ones — atomic from a reader's view.
    ///
    /// Suitable for full-refresh dimension tables or daily snapshot partitions.
    Overwrite,

    /// Match existing rows by `merge.key_columns` using position deletes,
    /// then append all rows from the source batch.
    ///
    /// Produces position-delete files plus a new data file per batch.
    /// Requires periodic compaction (`REWRITE_DATA_FILES + EXPIRE_SNAPSHOTS`).
    ///
    /// Suitable for CDC streams that emit the full updated row without an
    /// explicit operation-type column.
    Upsert,

    /// Route rows by the `_op` column value:
    ///   - `'I'` (insert)  → append to a new data file
    ///   - `'U'` (update)  → position-delete the old row, append new row
    ///   - `'D'` (delete)  → position-delete only (no new row appended)
    ///
    /// Requires `merge.key_columns`. Produces position-delete files for U/D
    /// rows and data files for I/U rows.
    ///
    /// Suitable for Debezium, pglogical, or similar CDC sources that emit the
    /// operation type as a column.
    MergeInto,
}

// ── Iceberg partition config ──────────────────────────────────────────────────

/// True Iceberg partition spec attached to the table at creation time.
///
/// When set on an `overwrite` job, the table is created with a native
/// `day(column)` (or `month` / `year` / `hour`) transform rather than using
/// `partition_column` as a row-level filter.  This enables:
///
/// - **Scan pruning** — readers skip files outside the queried partition.
/// - **Atomic per-day overwrite** — only files in matching day-buckets are
///   replaced; other partitions are never touched.
/// - **Correct manifest layout** — each Parquet file lands in its own
///   `<column>_<transform>=<value>/` directory.
///
/// The field names in the `UnboundPartitionSpec` sent to the catalog follow
/// the convention `<column>_<transform>` (e.g. `sale_date_day`).
///
/// ## Supported transforms
///
/// | `transform` value | Iceberg transform | Source type            |
/// |-------------------|-------------------|------------------------|
/// | `"day"`  (default)| `day(col)`        | Timestamp / Date       |
/// | `"month"`         | `month(col)`      | Timestamp / Date       |
/// | `"year"`          | `year(col)`       | Timestamp / Date       |
/// | `"hour"`          | `hour(col)`       | Timestamp only         |
///
/// ## Compatibility with `partition_column`
///
/// `partition_column` is still accepted for backward compatibility and drives
/// the legacy COW row-filter path.  When `iceberg_partition` is present it
/// takes precedence: the table is created with a real partition spec, and the
/// overwrite plan uses the partition values from `iceberg_partition.column`
/// rather than `partition_column`.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct IcebergPartitionConfig {
    /// Name of the source column to partition on.
    /// Must exist in the SQL result set and resolve to an Iceberg field.
    pub column: String,

    /// Temporal transform to apply.  Defaults to `"day"`.
    #[serde(default = "default_partition_transform")]
    pub transform: String,
}

fn default_partition_transform() -> String {
    "day".to_string()
}

// ── Merge config ──────────────────────────────────────────────────────────────

/// Configuration for [`WriteMode::Upsert`] and [`WriteMode::MergeInto`].
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MergeConfig {
    /// Column(s) that uniquely identify a logical row in the target table.
    ///
    /// Used to locate existing Iceberg rows that must be position-deleted
    /// before the incoming row is appended.
    ///
    /// Typically the primary key: `[id]` or composite `[tenant_id, user_id]`.
    pub key_columns: Vec<String>,

    /// When `true`, deletes are permanent within the active snapshot.
    /// When `false` (default), deleted rows remain in historical snapshots.
    ///
    /// Set `true` for GDPR right-to-erasure workflows, then run
    /// `EXPIRE_SNAPSHOTS` to physically purge the old data files.
    #[serde(default)]
    pub hard_delete: bool,
}

// ── Sync job ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SyncJob {
    pub name: String,
    pub source: String,
    pub destination: String,
    pub namespace: String,
    pub table: String,

    pub group: Option<String>,

    /// Custom SQL with named placeholders:
    /// - `:watermark`  — last committed watermark (incremental)
    /// - `:_cursor`    — injected by engine when `cursor_column` is set
    /// - `:param_name` — any key from a RabbitMQ payload
    /// - `_op`         — operation type column required by `merge_into`
    pub sql: String,

    pub watermark_column: Option<String>,
    /// Postgres column type for the watermark parameter binding.
    ///
    /// - `timestamptz` (default) — sends `DateTime<Utc>` for a `TIMESTAMPTZ` column.
    /// - `timestamp`             — sends `NaiveDateTime` for a plain `TIMESTAMP` column.
    ///
    /// Error symptom when wrong: `cannot convert between the Rust type
    /// chrono::datetime::DateTime<Utc> and the Postgres type 'timestamp'`
    #[serde(default)]
    pub watermark_type: WatermarkType,

    pub cursor_column: Option<String>,
    /// Postgres column type for the `:_cursor` parameter binding.
    ///
    /// - `int` (default) — sends `i64`; use for `BIGINT`/`INTEGER` cursor columns.
    /// - `text`          — sends `String`; use for `TEXT`/`VARCHAR` cursor columns.
    ///
    /// Error symptom when wrong: `cannot convert between the Rust type i64
    /// and the Postgres type 'text'`
    #[serde(default)]
    pub cursor_type: CursorType,
    pub depends_on: Option<String>,

    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    #[serde(default)]
    pub mode: SyncMode,

    #[serde(default)]
    pub schema_evolution: SchemaEvolutionConfig,

    pub retry: Option<RetryConfig>,

    // ── Write strategy ────────────────────────────────────────────────────────
    /// How the engine commits each batch to the Iceberg table.
    /// Defaults to `append` (additive, no read-before-write).
    #[serde(default)]
    pub write_mode: WriteMode,

    /// Partition column for `write_mode: overwrite` (legacy row-filter path).
    ///
    /// When `iceberg_partition` is also set, this field is superseded.
    /// Kept for backward compatibility; new jobs should use `iceberg_partition`.
    pub partition_column: Option<String>,

    /// True Iceberg partition spec for `write_mode: overwrite`.
    ///
    /// When set, the table is created (or recreated after COW) with a real
    /// `UnboundPartitionSpec` using the specified temporal transform.  This
    /// enables scan pruning and correct per-partition atomic overwrite.
    ///
    /// Example YAML:
    /// ```yaml
    /// iceberg_partition:
    ///   column: sale_date
    ///   transform: day   # default; can be month / year / hour
    /// ```
    pub iceberg_partition: Option<IcebergPartitionConfig>,

    /// Merge config for `write_mode: upsert` and `write_mode: merge_into`.
    pub merge: Option<MergeConfig>,
}

fn default_batch_size() -> usize {
    500
}

#[derive(Debug, Clone, Deserialize, Serialize, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SyncMode {
    #[default]
    Incremental,
    Full,
}

/// How the `:watermark` SQL parameter is bound to Postgres.
///
/// Choose based on the column type in your source table:
/// - `TIMESTAMPTZ` → `timestamptz` (default)
/// - `TIMESTAMP` (no tz) → `timestamp`
#[derive(Debug, Clone, Deserialize, Serialize, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum WatermarkType {
    #[default]
    Timestamptz,
    Timestamp,
}

/// How the `:_cursor` SQL parameter is bound to Postgres.
///
/// Choose based on the cursor column type in your source table:
/// - `BIGINT`/`INTEGER` → `int` (default)
/// - `TEXT`/`VARCHAR`   → `text`
#[derive(Debug, Clone, Deserialize, Serialize, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum CursorType {
    #[default]
    Int,
    Text,
}

// ── RabbitMQ ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RabbitMqConfig {
    pub uri: String,
    pub queues: Vec<QueueBinding>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct QueueBinding {
    pub queue: String,
    pub job: String,
    pub dead_letter_exchange: Option<String>,
}

// ── Public helpers ─────────────────────────────────────────────────────────────

/// Parse and validate a YAML configuration string into a [`SyncConfig`].
#[allow(dead_code)]
pub fn parse(yaml: &str) -> anyhow::Result<SyncConfig> {
    let cfg: SyncConfig = serde_yaml::from_str(yaml)?;
    cfg.validate()?;
    Ok(cfg)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn minimal_config(extra_job_yaml: &str) -> String {
        // Re-indent every line of `extra_job_yaml` with 4 spaces so that
        // multi-line blocks (merge:, cursor_column:, etc.) are parsed as
        // children of the job list item rather than falling to the top level.
        let indented = extra_job_yaml
            .lines()
            .enumerate()
            .map(|(i, line)| {
                if i == 0 {
                    line.to_string() // first line already sits after "    " in the template
                } else {
                    format!("    {line}")
                }
            })
            .collect::<Vec<_>>()
            .join("\n");

        format!(
            r#"
sources:
  pg:
    type: postgres
    dsn: "host=localhost dbname=test user=test password=test"
destinations:
  wh:
    catalog_uri: "http://localhost:8181"
    s3_endpoint: "http://localhost:9000"
    access_key_id: minioadmin
    secret_access_key: minioadmin
sync_jobs:
  - name: test_job
    source: pg
    destination: wh
    namespace: shop
    table: test
    sql: "SELECT 1"
    {indented}
"#
        )
    }

    fn parse(yaml: &str) -> anyhow::Result<SyncConfig> {
        let cfg: SyncConfig = serde_yaml::from_str(yaml)?;
        cfg.validate()?;
        Ok(cfg)
    }

    // ── WriteMode defaults ────────────────────────────────────────────────────

    #[test]
    fn write_mode_defaults_to_append() {
        let cfg = parse(&minimal_config("")).unwrap();
        assert_eq!(cfg.sync_jobs[0].write_mode, WriteMode::Append);
    }

    #[test]
    fn write_mode_append_explicit() {
        let cfg = parse(&minimal_config("write_mode: append")).unwrap();
        assert_eq!(cfg.sync_jobs[0].write_mode, WriteMode::Append);
    }

    // ── Overwrite ────────────────────────────────────────────────────────────

    #[test]
    fn overwrite_requires_partition_column_or_iceberg_partition() {
        let err = parse(&minimal_config("write_mode: overwrite")).unwrap_err();
        assert!(
            err.to_string().contains("partition_column")
                || err.to_string().contains("iceberg_partition"),
            "expected partition_column or iceberg_partition error, got: {err}"
        );
    }

    #[test]
    fn overwrite_with_partition_column_is_valid() {
        let cfg = parse(&minimal_config(
            "write_mode: overwrite\npartition_column: sale_date",
        ))
        .unwrap();
        assert_eq!(cfg.sync_jobs[0].write_mode, WriteMode::Overwrite);
        assert_eq!(
            cfg.sync_jobs[0].partition_column.as_deref(),
            Some("sale_date")
        );
    }

    #[test]
    fn overwrite_with_iceberg_partition_is_valid() {
        let cfg = parse(&minimal_config(
            "write_mode: overwrite\niceberg_partition:\n  column: sale_date\n  transform: day",
        ))
        .unwrap();
        assert_eq!(cfg.sync_jobs[0].write_mode, WriteMode::Overwrite);
        let ip = cfg.sync_jobs[0].iceberg_partition.as_ref().unwrap();
        assert_eq!(ip.column, "sale_date");
        assert_eq!(ip.transform, "day");
    }

    #[test]
    fn iceberg_partition_transform_defaults_to_day() {
        let cfg = parse(&minimal_config(
            "write_mode: overwrite\niceberg_partition:\n  column: created_at",
        ))
        .unwrap();
        let ip = cfg.sync_jobs[0].iceberg_partition.as_ref().unwrap();
        assert_eq!(ip.transform, "day");
    }

    #[test]
    fn iceberg_partition_month_transform_is_valid() {
        let cfg = parse(&minimal_config(
            "write_mode: overwrite\niceberg_partition:\n  column: event_ts\n  transform: month",
        ))
        .unwrap();
        let ip = cfg.sync_jobs[0].iceberg_partition.as_ref().unwrap();
        assert_eq!(ip.transform, "month");
    }

    // ── Upsert ───────────────────────────────────────────────────────────────
    #[test]
    fn upsert_requires_merge_block() {
        let err = parse(&minimal_config("write_mode: upsert")).unwrap_err();
        assert!(
            err.to_string().contains("merge"),
            "expected merge error, got: {err}"
        );
    }

    #[test]
    fn upsert_requires_non_empty_key_columns() {
        let err = parse(&minimal_config(
            "write_mode: upsert\nmerge:\n  key_columns: []",
        ))
        .unwrap_err();
        assert!(
            err.to_string().contains("key_columns"),
            "expected key_columns error, got: {err}"
        );
    }

    #[test]
    fn upsert_with_key_columns_is_valid() {
        let cfg = parse(&minimal_config(
            "write_mode: upsert\nmerge:\n  key_columns: [id]",
        ))
        .unwrap();
        assert_eq!(cfg.sync_jobs[0].write_mode, WriteMode::Upsert);
        let merge = cfg.sync_jobs[0].merge.as_ref().unwrap();
        assert_eq!(merge.key_columns, vec!["id"]);
        assert!(!merge.hard_delete);
    }

    #[test]
    fn upsert_hard_delete_flag() {
        let cfg = parse(&minimal_config(
            "write_mode: upsert\nmerge:\n  key_columns: [id]\n  hard_delete: true",
        ))
        .unwrap();
        assert!(cfg.sync_jobs[0].merge.as_ref().unwrap().hard_delete);
    }

    // ── MergeInto ────────────────────────────────────────────────────────────

    #[test]
    fn merge_into_requires_merge_block() {
        let err = parse(&minimal_config("write_mode: merge_into")).unwrap_err();
        assert!(
            err.to_string().contains("merge"),
            "expected merge error, got: {err}"
        );
    }

    #[test]
    fn merge_into_with_composite_key_is_valid() {
        let cfg = parse(&minimal_config(
            "write_mode: merge_into\nmerge:\n  key_columns: [tenant_id, user_id]",
        ))
        .unwrap();
        assert_eq!(cfg.sync_jobs[0].write_mode, WriteMode::MergeInto);
        assert_eq!(
            cfg.sync_jobs[0].merge.as_ref().unwrap().key_columns,
            vec!["tenant_id", "user_id"]
        );
    }

    // ── Existing validation invariants ────────────────────────────────────────

    #[test]
    fn cursor_column_rejected_with_full_mode() {
        let yaml = minimal_config("mode: full\ncursor_column: id");
        let err = parse(&yaml).unwrap_err();
        assert!(err.to_string().contains("cursor_column"));
    }

    #[test]
    fn unknown_source_rejected() {
        let yaml = minimal_config("").replace("source: pg", "source: nonexistent_src");
        let err = parse(&yaml).unwrap_err();
        assert!(err.to_string().contains("nonexistent_src"));
    }

    #[test]
    fn dependency_cycle_rejected() {
        let yaml = r#"
sources:
  pg:
    type: postgres
    dsn: "host=localhost dbname=test user=test password=test"
destinations:
  wh:
    catalog_uri: "http://localhost:8181"
    s3_endpoint: "http://localhost:9000"
    access_key_id: minioadmin
    secret_access_key: minioadmin
sync_jobs:
  - name: a
    source: pg
    destination: wh
    namespace: x
    table: a
    sql: "SELECT 1"
    depends_on: b
  - name: b
    source: pg
    destination: wh
    namespace: x
    table: b
    sql: "SELECT 1"
    depends_on: a
"#;
        let err = parse(yaml).unwrap_err();
        assert!(err.to_string().to_lowercase().contains("cycle"));
    }

    // ── Serialisation round-trip ──────────────────────────────────────────────

    #[test]
    fn write_mode_round_trips_through_yaml() {
        for (mode_str, expected) in [
            ("append", WriteMode::Append),
            ("overwrite", WriteMode::Overwrite),
            ("upsert", WriteMode::Upsert),
            ("merge_into", WriteMode::MergeInto),
        ] {
            let mode: WriteMode = serde_yaml::from_str(mode_str).unwrap();
            assert_eq!(mode, expected, "failed for {mode_str}");
            let back = serde_yaml::to_string(&mode).unwrap();
            assert!(
                back.trim() == mode_str,
                "round-trip failed for {mode_str}: got {back}"
            );
        }
    }

    #[test]
    fn merge_config_hard_delete_defaults_false() {
        let mc: MergeConfig = serde_yaml::from_str("key_columns: [id]").unwrap();
        assert!(!mc.hard_delete);
    }
}

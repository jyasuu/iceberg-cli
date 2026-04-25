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
//! Global or per-job exponential-backoff retry:
//! ```yaml
//! retry:
//!   max_attempts: 3
//!   initial_delay_ms: 500
//!   backoff_multiplier: 2.0
//! ```
//!
//! ### Schema evolution (`schema_evolution`)
//! With `allow_add_columns: true`, new columns in the source are appended to
//! the Iceberg table automatically.
//!
//! ### Dry-run (`--dry-run` CLI flag)
//! Logs what the engine *would* do without writing any data.
//!
//! ### Parallel execution (`parallel`)
//! Independent jobs (no shared dependency chain) run concurrently up to
//! `parallel` workers (default 1).
//!
//! ### Dead-letter queue (`dead_letter_exchange`)
//! RabbitMQ bindings can specify a DLX for nacked messages.
//!
//! ### Job groups (`group`)
//! Tag jobs with a group name to run just that cohort:
//! `iceberg-cli sync --group orders_pipeline`

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// ── Top-level ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SyncConfig {
    pub sources: HashMap<String, SourceConfig>,
    pub destinations: HashMap<String, DestinationConfig>,
    pub sync_jobs: Vec<SyncJob>,
    /// Global retry policy — can be overridden per job.
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

    fn validate(&self) -> anyhow::Result<()> {
        for job in &self.sync_jobs {
            anyhow::ensure!(
                self.sources.contains_key(&job.source),
                "Job '{}' references unknown source '{}'",
                job.name,
                job.source
            );
            anyhow::ensure!(
                self.destinations.contains_key(&job.destination),
                "Job '{}' references unknown destination '{}'",
                job.name,
                job.destination
            );
            if let Some(dep) = &job.depends_on {
                anyhow::ensure!(
                    self.sync_jobs.iter().any(|j| &j.name == dep),
                    "Job '{}' depends_on unknown job '{}'",
                    job.name,
                    dep
                );
            }
            if job.cursor_column.is_some() && job.mode == SyncMode::Full {
                anyhow::bail!(
                    "Job '{}': cursor_column cannot be used with mode: full",
                    job.name
                );
            }
        }
        self.detect_cycles()?;
        Ok(())
    }

    fn detect_cycles(&self) -> anyhow::Result<()> {
        use std::collections::HashSet;
        let mut visiting: HashSet<&str> = HashSet::new();
        let mut visited: HashSet<&str> = HashSet::new();

        fn dfs<'a>(
            name: &'a str,
            jobs: &'a [SyncJob],
            visiting: &mut HashSet<&'a str>,
            visited: &mut HashSet<&'a str>,
        ) -> anyhow::Result<()> {
            if visited.contains(name) {
                return Ok(());
            }
            anyhow::ensure!(
                !visiting.contains(name),
                "Dependency cycle detected involving job '{}'",
                name
            );
            visiting.insert(name);
            if let Some(job) = jobs.iter().find(|j| j.name == name) {
                if let Some(dep) = &job.depends_on {
                    dfs(dep, jobs, visiting, visited)?;
                }
            }
            visiting.remove(name);
            visited.insert(name);
            Ok(())
        }

        for name in self.sync_jobs.iter().map(|j| j.name.as_str()) {
            dfs(name, &self.sync_jobs, &mut visiting, &mut visited)?;
        }
        Ok(())
    }

    /// Jobs sorted so dependencies run before dependents (topological).
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

    /// Jobs filtered by group name, topologically ordered.
    pub fn ordered_jobs_for_group<'a>(&'a self, group: &str) -> anyhow::Result<Vec<&'a SyncJob>> {
        let all = self.ordered_jobs()?;
        Ok(all
            .into_iter()
            .filter(|j| j.group.as_deref() == Some(group))
            .collect())
    }

    /// Effective retry policy for a job (job-level overrides global).
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
    /// Total attempts including the first (1 = no retry).
    #[serde(default = "default_max_attempts")]
    pub max_attempts: u32,
    /// Delay before first retry in milliseconds.
    #[serde(default = "default_initial_delay_ms")]
    pub initial_delay_ms: u64,
    /// Backoff multiplier applied after each failure (1.0 = constant delay).
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
    /// When true, columns present in the source but absent from the Iceberg
    /// table are appended automatically.  Removed columns are left nullable.
    #[serde(default)]
    pub allow_add_columns: bool,
}

// ── Sync job ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SyncJob {
    pub name: String,
    pub source: String,
    pub destination: String,
    pub namespace: String,
    pub table: String,

    /// Optional group tag — run cohorts with `--group <name>`.
    pub group: Option<String>,

    /// Custom SQL with named placeholders:
    /// - `:watermark`  — last committed watermark (incremental)
    /// - `:_cursor`    — injected by engine when `cursor_column` is set
    /// - `:param_name` — any key from a RabbitMQ payload
    pub sql: String,

    /// Watermark column for incremental tracking (e.g. `updated_at`).
    pub watermark_column: Option<String>,

    /// Stable, monotonically increasing column for cursor-based pagination
    /// (e.g. primary key `id`).  Avoids LIMIT/OFFSET drift.
    ///
    /// When set the engine wraps the SQL as:
    /// ```sql
    /// SELECT * FROM (<user_sql>) _q
    /// WHERE <cursor_column> > :_cursor
    /// ORDER BY <cursor_column>
    /// LIMIT <batch_size>
    /// ```
    pub cursor_column: Option<String>,

    /// Run only after the named job has committed successfully.
    pub depends_on: Option<String>,

    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    #[serde(default)]
    pub mode: SyncMode,

    #[serde(default)]
    pub schema_evolution: SchemaEvolutionConfig,

    /// Per-job retry override.
    pub retry: Option<RetryConfig>,
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
    /// Dead-letter exchange for nacked messages.
    pub dead_letter_exchange: Option<String>,
}

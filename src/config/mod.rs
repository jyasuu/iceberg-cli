//! Sync configuration — loaded from a YAML file.
//!
//! Example config:
//! ```yaml
//! sources:
//!   main_pg:
//!     type: postgres
//!     dsn: "host=localhost port=5432 dbname=shop user=app password=secret"
//!
//! destinations:
//!   warehouse:
//!     catalog_uri: "http://localhost:8181"
//!     s3_endpoint:  "http://localhost:9000"
//!     region:       "us-east-1"
//!     access_key_id: minioadmin
//!     secret_access_key: minioadmin
//!
//! sync_jobs:
//!   - name: orders
//!     source: main_pg
//!     destination: warehouse
//!     namespace: shop
//!     table: orders
//!     # Full-table SQL (incremental mode uses the watermark column automatically)
//!     sql: |
//!       SELECT o.id, o.user_id, o.total, o.created_at
//!       FROM   orders o
//!       WHERE  o.created_at > :watermark
//!       ORDER  BY o.created_at
//!     watermark_column: created_at
//!     batch_size: 500
//!     mode: incremental          # incremental | full
//!
//!   - name: order_items
//!     source: main_pg
//!     destination: warehouse
//!     namespace: shop
//!     table: order_items
//!     # N+1-safe: join pulled into SQL
//!     sql: |
//!       SELECT oi.id, oi.order_id, oi.product_id, oi.qty, oi.unit_price
//!       FROM   order_items oi
//!       JOIN   orders      o  ON o.id = oi.order_id
//!       WHERE  o.created_at > :watermark
//!     watermark_column: ~         # no direct watermark; driven by parent job
//!     depends_on: orders          # run after parent
//!     batch_size: 1000
//!     mode: incremental
//!
//! rabbitmq:
//!   uri: "amqp://guest:guest@localhost:5672/%2f"
//!   queues:
//!     - queue: user_sync_requests
//!       job: users_by_id
//!       # payload {"user_id": 42} → SELECT … WHERE user_id = :user_id
//! ```

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// ── Top-level ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SyncConfig {
    pub sources: HashMap<String, SourceConfig>,
    pub destinations: HashMap<String, DestinationConfig>,
    pub sync_jobs: Vec<SyncJob>,
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
        }
        Ok(())
    }

    /// Returns jobs sorted so dependencies run before dependents (topological).
    pub fn ordered_jobs(&self) -> anyhow::Result<Vec<&SyncJob>> {
        let mut result: Vec<&SyncJob> = Vec::new();
        let mut visited: std::collections::HashSet<&str> = std::collections::HashSet::new();

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

        let names: Vec<&str> = self.sync_jobs.iter().map(|j| j.name.as_str()).collect();
        for name in names {
            visit(name, &self.sync_jobs, &mut visited, &mut result)?;
        }
        Ok(result)
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

// ── Sync job ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SyncJob {
    pub name: String,
    pub source: String,
    pub destination: String,
    pub namespace: String,
    pub table: String,

    /// Custom SQL for the query. Use `:watermark` as the placeholder for
    /// incremental mode and `:param_name` for RabbitMQ-driven queries.
    pub sql: String,

    /// Column used to track incremental progress (e.g. `updated_at`).
    pub watermark_column: Option<String>,

    /// Run this job only after the named job has committed.
    pub depends_on: Option<String>,

    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    #[serde(default)]
    pub mode: SyncMode,
}

fn default_batch_size() -> usize {
    500
}

#[derive(Debug, Clone, Deserialize, Serialize, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SyncMode {
    /// Only fetch rows newer than the last watermark.
    #[default]
    Incremental,
    /// Truncate and reload the full table every run.
    Full,
}

// ── RabbitMQ ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RabbitMqConfig {
    pub uri: String,
    pub queues: Vec<QueueBinding>,
}

/// Maps a RabbitMQ queue to a sync job.
/// The message payload (JSON object) is merged with the SQL parameters —
/// e.g. `{"user_id": 42}` fills `:user_id` in the job's SQL.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct QueueBinding {
    pub queue: String,
    pub job: String,
}

//! RabbitMQ-driven sync consumer.
//!
//! Each message on a bound queue carries a JSON payload whose fields are
//! merged as named SQL parameters.  Example:
//!
//!   Queue message: `{"user_id": 42}`
//!   Job SQL:       `SELECT … WHERE user_id = :user_id`
//!
//! ## New features
//!
//! ### Dead-letter exchange (DLX)
//! When `dead_letter_exchange` is set in the queue binding the consumer
//! declares the queue with `x-dead-letter-exchange` so nacked messages are
//! routed to the DLX instead of being dropped.
//!
//! ### Per-job retry
//! Failed jobs are retried according to the effective `RetryConfig` before
//! being nacked.

use std::{collections::HashMap, sync::Arc};

use anyhow::{Context, Result};
use futures::StreamExt;
use iceberg::Catalog;
use lapin::{
    Channel, Connection, ConnectionProperties,
    options::{
        BasicAckOptions, BasicConsumeOptions, BasicNackOptions, BasicQosOptions,
        QueueDeclareOptions,
    },
    types::{AMQPValue, FieldTable},
};
use tracing::{error, info, warn};

use crate::config::{QueueBinding, RabbitMqConfig, SyncConfig};
use crate::sync::{engine::SyncEngine, postgres::SqlValue};

// ── Connection ────────────────────────────────────────────────────────────────

pub async fn connect(uri: &str) -> Result<Connection> {
    Connection::connect(uri, ConnectionProperties::default())
        .await
        .with_context(|| format!("Connect to RabbitMQ: {uri}"))
}

// ── Consumer loop ─────────────────────────────────────────────────────────────

/// Start one consumer per queue binding and drive them concurrently.
/// Blocks until all consumers exit.
pub async fn run_consumers<C: Catalog + Send + Sync + 'static>(
    rmq_cfg: &RabbitMqConfig,
    sync_cfg: Arc<SyncConfig>,
    catalog: Arc<C>,
) -> Result<()> {
    let conn = connect(&rmq_cfg.uri).await?;

    let mut handles = Vec::new();
    for binding in &rmq_cfg.queues {
        let channel = conn.create_channel().await?;
        // Prefetch = 1 so a slow job doesn't starve other consumers.
        channel.basic_qos(1, BasicQosOptions::default()).await?;

        let binding = binding.clone();
        let sync_cfg = Arc::clone(&sync_cfg);
        let catalog_ref = Arc::clone(&catalog);

        let handle = tokio::spawn(async move {
            if let Err(e) = consume_queue(channel, &binding, &sync_cfg, &*catalog_ref).await {
                error!(queue = %binding.queue, "Consumer error: {e:#}");
            }
        });
        handles.push(handle);
    }

    for h in handles {
        let _ = h.await;
    }
    Ok(())
}

// ── Single queue consumer ─────────────────────────────────────────────────────

async fn consume_queue<C: Catalog>(
    channel: Channel,
    binding: &QueueBinding,
    sync_cfg: &SyncConfig,
    catalog: &C,
) -> Result<()> {
    let job = sync_cfg
        .sync_jobs
        .iter()
        .find(|j| j.name == binding.job)
        .with_context(|| {
            format!(
                "Queue '{}' binds to unknown job '{}'",
                binding.queue, binding.job
            )
        })?;

    let source = sync_cfg
        .sources
        .get(&job.source)
        .with_context(|| format!("Source '{}' not found", job.source))?;

    let retry = sync_cfg.retry_for(job);
    let engine = SyncEngine::new(catalog);

    // Declare queue with optional dead-letter exchange.
    let mut args = FieldTable::default();
    if let Some(dlx) = &binding.dead_letter_exchange {
        args.insert(
            "x-dead-letter-exchange".into(),
            AMQPValue::LongString(dlx.clone().into()),
        );
        info!(queue = %binding.queue, dlx = %dlx, "Declared queue with dead-letter exchange");
    }

    channel
        .queue_declare(
            &binding.queue,
            QueueDeclareOptions {
                durable: true,
                passive: false,
                exclusive: false,
                auto_delete: false,
                nowait: false,
            },
            args,
        )
        .await
        .with_context(|| format!("Declare queue '{}'", binding.queue))?;

    let mut consumer = channel
        .basic_consume(
            &binding.queue,
            "iceberg-sync",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .with_context(|| format!("Consume queue '{}'", binding.queue))?;

    info!(queue = %binding.queue, job = %job.name, "Consumer started");

    while let Some(delivery) = consumer.next().await {
        let delivery = match delivery {
            Ok(d) => d,
            Err(e) => {
                warn!("Delivery error: {e}");
                continue;
            }
        };

        let payload_result = parse_payload(&delivery.data);

        match payload_result {
            Err(e) => {
                error!(queue = %binding.queue, "Bad payload: {e:#}");
                delivery
                    .nack(BasicNackOptions {
                        requeue: false,
                        multiple: false,
                    })
                    .await
                    .ok();
                continue;
            }
            Ok(extra_params) => {
                match engine
                    .run_job(job, &source.dsn, Some(extra_params), &retry)
                    .await
                {
                    Ok(summary) => {
                        info!(
                            job   = %summary.job_name,
                            rows  = summary.rows_written,
                            "RabbitMQ-triggered job succeeded"
                        );
                        delivery.ack(BasicAckOptions::default()).await.ok();
                    }
                    Err(e) => {
                        error!(job = %job.name, "Job error after retries: {e:#}");
                        delivery
                            .nack(BasicNackOptions {
                                requeue: false,
                                multiple: false,
                            })
                            .await
                            .ok();
                    }
                }
            }
        }
    }

    Ok(())
}

// ── Payload parsing ───────────────────────────────────────────────────────────

/// Parse a JSON object payload into SQL parameters.
///
/// ```json
/// {"user_id": 42, "status": "active"}
/// ```
/// → `{"user_id" => SqlValue::Int(42), "status" => SqlValue::Text("active")}`
fn parse_payload(data: &[u8]) -> Result<HashMap<String, SqlValue>> {
    let json: serde_json::Value =
        serde_json::from_slice(data).context("Payload is not valid JSON")?;

    let obj = json.as_object().context("Payload must be a JSON object")?;

    let mut params = HashMap::new();
    for (key, val) in obj {
        let sql_val = match val {
            serde_json::Value::Bool(b) => SqlValue::Bool(*b),
            serde_json::Value::Number(n) if n.is_i64() => SqlValue::Int(n.as_i64().unwrap()),
            serde_json::Value::Number(n) => SqlValue::Float(n.as_f64().unwrap_or(0.0)),
            serde_json::Value::String(s) => SqlValue::Text(s.clone()),
            serde_json::Value::Null => SqlValue::Null,
            other => SqlValue::Text(other.to_string()),
        };
        params.insert(key.clone(), sql_val);
    }

    Ok(params)
}

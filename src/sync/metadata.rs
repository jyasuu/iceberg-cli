//! Sync metadata — stores watermarks and run state as Iceberg table properties.
//!
//! Keys written to the table:
//!   sync.watermark.<column>  = ISO-8601 timestamp  (incremental jobs)
//!   sync.last_run_at         = ISO-8601 timestamp
//!   sync.last_run_rows       = integer

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use iceberg::{Catalog, TableIdent};
use std::collections::HashMap;

const KEY_LAST_RUN_AT:    &str = "sync.last_run_at";
const KEY_LAST_RUN_ROWS:  &str = "sync.last_run_rows";

fn watermark_key(column: &str) -> String {
    format!("sync.watermark.{column}")
}

/// Read the stored watermark for `column` from table properties.
/// Returns `None` if no watermark is recorded yet (first run → full initial load).
/// Also returns `None` if the table doesn't exist yet — `ensure_table` will
/// create it on the first write, so this is safe to call before the table exists.
pub async fn read_watermark(
    catalog: &impl Catalog,
    ident: &TableIdent,
    column: &str,
) -> Result<Option<DateTime<Utc>>> {
    let table = match catalog.load_table(ident).await {
        Ok(t) => t,
        Err(e) => {
            let msg = e.to_string().to_lowercase();
            if msg.contains("does not exist") || msg.contains("not found") || msg.contains("no such") {
                return Ok(None); // first run — table will be created on write
            }
            return Err(e).with_context(|| format!("load_table for watermark read: {ident}"));
        }
    };

    let props = table.metadata().properties();
    let key   = watermark_key(column);

    match props.get(&key) {
        Some(raw) => {
            let ts = raw.parse::<DateTime<Utc>>()
                .with_context(|| format!("Invalid watermark value '{raw}' for key '{key}'"))?;
            Ok(Some(ts))
        }
        None => Ok(None),
    }
}

/// Commit updated watermark + run stats into table properties.
///
/// This is done inside the same Iceberg transaction as the data write, so
/// the metadata update is atomic with the data commit.
pub fn build_metadata_updates(
    column: Option<&str>,
    watermark: Option<DateTime<Utc>>,
    rows_written: usize,
) -> HashMap<String, String> {
    let mut updates = HashMap::new();

    updates.insert(
        KEY_LAST_RUN_AT.to_string(),
        Utc::now().to_rfc3339(),
    );
    updates.insert(
        KEY_LAST_RUN_ROWS.to_string(),
        rows_written.to_string(),
    );

    if let (Some(col), Some(ts)) = (column, watermark) {
        updates.insert(watermark_key(col), ts.to_rfc3339());
    }

    updates
}

/// Summary returned after a sync run completes.
#[derive(Debug, Clone)]
pub struct RunSummary {
    pub job_name:     String,
    pub rows_written: usize,
    pub new_watermark: Option<DateTime<Utc>>,
}

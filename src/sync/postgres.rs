//! PostgreSQL data source.
//!
//! Converts named SQL placeholders (`:param_name`) to the `$N` positional
//! syntax that `tokio-postgres` expects, then executes the query and streams
//! back rows as Arrow `RecordBatch`es.
//!
//! N+1 avoidance
//! ─────────────
//! Rather than querying related tables row-by-row, callers write a single
//! JOIN in their config SQL that fetches all needed columns in one round-trip.
//! This module simply executes whatever SQL is given.

use anyhow::{Context, Result};
use arrow_array::{
    Array, ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray,
    TimestampMicrosecondArray,
};
use arrow_schema::{DataType, Field, Schema as ArrowSchema, TimeUnit};
use chrono::{DateTime, NaiveDateTime, Utc};
use std::{collections::HashMap, sync::Arc};
use tokio_postgres::{Client, NoTls, Row, types::Type};

// ── Connection ────────────────────────────────────────────────────────────────

pub async fn connect(dsn: &str) -> Result<Client> {
    let (client, conn) = tokio_postgres::connect(dsn, NoTls).await
        .with_context(|| format!("Connect to Postgres: {dsn}"))?;

    // Drive the connection on a background task.
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            tracing::error!("Postgres connection error: {e}");
        }
    });

    Ok(client)
}

// ── SQL parameter substitution ────────────────────────────────────────────────

/// Substitutes named `:placeholder` markers with `$1`, `$2`, … in order of
/// first appearance and returns the rewritten SQL + ordered value list.
///
/// Supported value types: string, i64, f64, DateTime<Utc>.
pub fn bind_named_params(
    sql: &str,
    params: &HashMap<String, SqlValue>,
) -> Result<(String, Vec<SqlValue>)> {
    let mut out_sql    = sql.to_string();
    let mut ordered    = Vec::new();
    let mut name_to_idx: HashMap<String, usize> = HashMap::new();

    // Find all `:name` tokens.  We scan left-to-right so $N is assigned in
    // first-encounter order, which is stable across calls.
    // Skip `::` — that is PostgreSQL cast syntax, not a named parameter.
    let mut remaining = sql;
    while let Some(pos) = remaining.find(':') {
        // Peek at the character immediately after the colon.
        let after = &remaining[pos + 1..];

        // `::cast` — skip both colons and continue scanning after them.
        if after.starts_with(':') {
            remaining = &after[1..];
            continue;
        }

        let end = after
            .find(|c: char| !c.is_alphanumeric() && c != '_')
            .unwrap_or(after.len());
        let name = &after[..end];
        if name.is_empty() {
            remaining = &remaining[pos + 1..];
            continue;
        }
        if !name_to_idx.contains_key(name) {
            let idx = ordered.len() + 1; // 1-based
            let val = params
                .get(name)
                .with_context(|| format!("SQL uses :{name} but no value supplied"))?
                .clone();
            name_to_idx.insert(name.to_string(), idx);
            ordered.push(val);
        }
        remaining = &after[end..];
    }

    // Replace `:name` → `$N` in the SQL string.
    // Use a negative-lookbehind equivalent: replace only `:name` that is NOT
    // preceded by another colon.  We do this by temporarily replacing `::` with
    // a sentinel, doing the substitutions, then restoring.
    const SENTINEL: &str = "\x00DBLCOLON\x00";
    out_sql = out_sql.replace("::", SENTINEL);
    for (name, idx) in &name_to_idx {
        out_sql = out_sql.replace(&format!(":{name}"), &format!("${idx}"));
    }
    out_sql = out_sql.replace(SENTINEL, "::");

    Ok((out_sql, ordered))
}

// ── Value types ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum SqlValue {
    Text(String),
    Int(i64),
    Float(f64),
    Timestamp(DateTime<Utc>),
    Null,
}

// ── Query execution ───────────────────────────────────────────────────────────

/// Execute a query and return all rows as a single `RecordBatch`.
/// Returns `None` when the result set is empty.
pub async fn query_to_batch(
    client: &Client,
    sql: &str,
    params: &HashMap<String, SqlValue>,
) -> Result<Option<RecordBatch>> {
    let (bound_sql, ordered_vals) = bind_named_params(sql, params)?;

    // All concrete types inside SqlValue are Send, so we can box them as
    // `Box<dyn ToSql + Sync + Send>`.  This makes the vec — and therefore
    // the enclosing async fn — Send, satisfying tokio::spawn's bound.
    let pg_params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> =
        ordered_vals.iter().map(sql_value_to_pg).collect();
    // client.query wants &[&(dyn ToSql + Sync)]; coerce explicitly.
    let pg_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
        pg_params.iter().map(|b| b.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync)).collect();

    let rows = client
        .query(bound_sql.as_str(), pg_refs.as_slice())
        .await
        .with_context(|| format!("Query failed:\n{bound_sql}"))?;

    if rows.is_empty() {
        return Ok(None);
    }

    rows_to_batch(&rows)
}

fn sql_value_to_pg(
    v: &SqlValue,
) -> Box<dyn tokio_postgres::types::ToSql + Sync + Send> {
    match v {
        SqlValue::Text(s)      => Box::new(s.clone()),
        SqlValue::Int(i)       => Box::new(*i),
        SqlValue::Float(f)     => Box::new(*f),
        SqlValue::Timestamp(t) => Box::new(*t),
        SqlValue::Null         => Box::new(Option::<String>::None),
    }
}

// ── Row → RecordBatch ─────────────────────────────────────────────────────────

fn rows_to_batch(rows: &[Row]) -> Result<Option<RecordBatch>> {
    if rows.is_empty() {
        return Ok(None);
    }

    let stmt    = rows[0].columns();
    let mut fields:  Vec<Field>    = Vec::with_capacity(stmt.len());
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(stmt.len());

    for (col_idx, col) in stmt.iter().enumerate() {
        let name = col.name().to_string();
        let pg_type = col.type_();

        match pg_type {
            &Type::INT2 | &Type::INT4 | &Type::INT8 => {
                let vals: Vec<Option<i64>> = rows
                    .iter()
                    .map(|r| r.try_get::<_, Option<i64>>(col_idx).ok().flatten())
                    .collect();
                fields.push(Field::new(&name, DataType::Int64, true));
                columns.push(Arc::new(Int64Array::from(vals)) as ArrayRef);
            }
            &Type::FLOAT4 | &Type::FLOAT8 | &Type::NUMERIC => {
                let vals: Vec<Option<f64>> = rows
                    .iter()
                    .map(|r| r.try_get::<_, Option<f64>>(col_idx).ok().flatten())
                    .collect();
                fields.push(Field::new(&name, DataType::Float64, true));
                columns.push(Arc::new(Float64Array::from(vals)) as ArrayRef);
            }
            &Type::TIMESTAMPTZ | &Type::TIMESTAMP => {
                let vals: Vec<Option<i64>> = rows
                    .iter()
                    .map(|r| {
                        r.try_get::<_, Option<NaiveDateTime>>(col_idx)
                            .ok()
                            .flatten()
                            .map(|ts| ts.and_utc().timestamp_micros())
                    })
                    .collect();
                // No timezone annotation: Iceberg Timestamp (non-tz) maps to
                // Arrow Timestamp(µs, None).  inject_field_ids will add the
                // field_id; keep the Arrow type consistent with the auto-created
                // Iceberg schema so the writer doesn't reject the batch.
                fields.push(Field::new(
                    &name,
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    true,
                ));
                columns.push(Arc::new(TimestampMicrosecondArray::from(vals)) as ArrayRef);
            }
            _ => {
                // Catch-all: stringify everything else.
                let vals: Vec<Option<String>> = rows
                    .iter()
                    .map(|r| {
                        r.try_get::<_, Option<String>>(col_idx)
                            .ok()
                            .flatten()
                    })
                    .collect();
                fields.push(Field::new(&name, DataType::Utf8, true));
                let arr: StringArray = vals.iter().map(|v| v.as_deref()).collect();
                columns.push(Arc::new(arr) as ArrayRef);
            }
        }
    }

    let schema = Arc::new(ArrowSchema::new(fields));
    let batch  = RecordBatch::try_new(schema, columns)
        .context("Build RecordBatch from Postgres rows")?;
    Ok(Some(batch))
}

/// Extract the maximum value of a timestamp column from a batch.
/// Used to advance the watermark after a successful write.
pub fn max_timestamp_in_batch(
    batch: &RecordBatch,
    column: &str,
) -> Option<DateTime<Utc>> {
    let idx   = batch.schema().index_of(column).ok()?;
    let array = batch.column(idx);
    let ts_arr = array
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()?;

    let max_us = (0..ts_arr.len())
        .filter_map(|i| if ts_arr.is_null(i) { None } else { Some(ts_arr.value(i)) })
        .max()?;

    DateTime::from_timestamp_micros(max_us)
}

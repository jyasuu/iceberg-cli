//! PostgreSQL data source.
//!
//! Converts named SQL placeholders (`:param_name`) to the `$N` positional
//! syntax that `tokio-postgres` expects, then executes the query and streams
//! back rows as Arrow `RecordBatch`es.
//!
//! ## N+1 avoidance
//! Callers write a single JOIN in their config SQL.  This module executes
//! whatever SQL is given without additional per-row queries.
//!
//! ## New type support
//! - `BOOL`     → Arrow `Boolean`
//! - `UUID`     → Arrow `Utf8` (stringified)
//! - `DATE`     → Arrow `Date32`
//! - `JSONB/JSON` → Arrow `Utf8`
//! - `INT2`     → Arrow `Int32` (upcast from smallint)
//! - `NUMERIC`  → Arrow `Float64` (best-effort; use explicit CAST for precision)

use anyhow::{Context, Result};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Date32Array, Float64Array, Int32Array, Int64Array,
    RecordBatch, StringArray, TimestampMicrosecondArray,
};
use arrow_schema::{DataType, Field, Schema as ArrowSchema, TimeUnit};
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use std::{collections::HashMap, sync::Arc};
use tokio_postgres::{Client, NoTls, Row, types::Type};

// ── Connection ────────────────────────────────────────────────────────────────

pub async fn connect(dsn: &str) -> Result<Client> {
    let (client, conn) = tokio_postgres::connect(dsn, NoTls).await
        .with_context(|| format!("Connect to Postgres: {dsn}"))?;

    tokio::spawn(async move {
        if let Err(e) = conn.await {
            tracing::error!("Postgres connection error: {e}");
        }
    });

    Ok(client)
}

// ── SQL parameter substitution ────────────────────────────────────────────────

/// Substitutes named `:placeholder` markers with `$1`, `$2`, … in order of
/// first appearance.  Skips `::` (PostgreSQL cast syntax).
pub fn bind_named_params(
    sql: &str,
    params: &HashMap<String, SqlValue>,
) -> Result<(String, Vec<SqlValue>)> {
    let mut out_sql   = sql.to_string();
    let mut ordered   = Vec::new();
    let mut name_to_idx: HashMap<String, usize> = HashMap::new();

    let mut remaining = sql;
    while let Some(pos) = remaining.find(':') {
        let after = &remaining[pos + 1..];

        // `::cast` — skip both colons.
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
            let idx = ordered.len() + 1;
            let val = params
                .get(name)
                .with_context(|| format!("SQL uses :{name} but no value supplied"))?
                .clone();
            name_to_idx.insert(name.to_string(), idx);
            ordered.push(val);
        }
        remaining = &after[end..];
    }

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
    Bool(bool),
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

    let pg_params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> =
        ordered_vals.iter().map(sql_value_to_pg).collect();
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

fn sql_value_to_pg(v: &SqlValue) -> Box<dyn tokio_postgres::types::ToSql + Sync + Send> {
    match v {
        SqlValue::Text(s)      => Box::new(s.clone()),
        SqlValue::Int(i)       => Box::new(*i),
        SqlValue::Float(f)     => Box::new(*f),
        SqlValue::Timestamp(t) => Box::new(*t),
        SqlValue::Bool(b)      => Box::new(*b),
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
        let name    = col.name().to_string();
        let pg_type = col.type_();

        match pg_type {
            // ── Integers ──────────────────────────────────────────────────────
            &Type::INT2 | &Type::INT4 => {
                // Store smallint / int as Int32 to save space.
                let vals: Vec<Option<i32>> = rows
                    .iter()
                    .map(|r| r.try_get::<_, Option<i32>>(col_idx).ok().flatten())
                    .collect();
                fields.push(Field::new(&name, DataType::Int32, true));
                columns.push(Arc::new(Int32Array::from(vals)) as ArrayRef);
            }
            &Type::INT8 | &Type::OID => {
                let vals: Vec<Option<i64>> = rows
                    .iter()
                    .map(|r| r.try_get::<_, Option<i64>>(col_idx).ok().flatten())
                    .collect();
                fields.push(Field::new(&name, DataType::Int64, true));
                columns.push(Arc::new(Int64Array::from(vals)) as ArrayRef);
            }

            // ── Floats / Numeric ──────────────────────────────────────────────
            &Type::FLOAT4 | &Type::FLOAT8 | &Type::NUMERIC => {
                let vals: Vec<Option<f64>> = rows
                    .iter()
                    .map(|r| r.try_get::<_, Option<f64>>(col_idx).ok().flatten())
                    .collect();
                fields.push(Field::new(&name, DataType::Float64, true));
                columns.push(Arc::new(Float64Array::from(vals)) as ArrayRef);
            }

            // ── Boolean ───────────────────────────────────────────────────────
            &Type::BOOL => {
                let vals: Vec<Option<bool>> = rows
                    .iter()
                    .map(|r| r.try_get::<_, Option<bool>>(col_idx).ok().flatten())
                    .collect();
                fields.push(Field::new(&name, DataType::Boolean, true));
                columns.push(Arc::new(BooleanArray::from(vals)) as ArrayRef);
            }

            // ── Date ──────────────────────────────────────────────────────────
            &Type::DATE => {
                let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                let vals: Vec<Option<i32>> = rows
                    .iter()
                    .map(|r| {
                        r.try_get::<_, Option<NaiveDate>>(col_idx)
                            .ok()
                            .flatten()
                            .map(|d| d.signed_duration_since(epoch).num_days() as i32)
                    })
                    .collect();
                fields.push(Field::new(&name, DataType::Date32, true));
                columns.push(Arc::new(Date32Array::from(vals)) as ArrayRef);
            }

            // ── Timestamps ────────────────────────────────────────────────────
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
                fields.push(Field::new(
                    &name,
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    true,
                ));
                columns.push(Arc::new(TimestampMicrosecondArray::from(vals)) as ArrayRef);
            }

            // ── UUID → string ─────────────────────────────────────────────────
            // Read as &str to avoid uuid crate version conflicts between
            // iceberg-cli and tokio-postgres's bundled uuid dependency.
            &Type::UUID => {
                let vals: Vec<Option<String>> = rows
                    .iter()
                    .map(|r| r.try_get::<_, Option<String>>(col_idx).ok().flatten())
                    .collect();
                fields.push(Field::new(&name, DataType::Utf8, true));
                let arr: StringArray = vals.iter().map(|v| v.as_deref()).collect();
                columns.push(Arc::new(arr) as ArrayRef);
            }

            // ── JSON / JSONB → string ─────────────────────────────────────────
            &Type::JSON | &Type::JSONB => {
                let vals: Vec<Option<String>> = rows
                    .iter()
                    .map(|r| {
                        r.try_get::<_, Option<serde_json::Value>>(col_idx)
                            .ok()
                            .flatten()
                            .map(|v| v.to_string())
                    })
                    .collect();
                fields.push(Field::new(&name, DataType::Utf8, true));
                let arr: StringArray = vals.iter().map(|v| v.as_deref()).collect();
                columns.push(Arc::new(arr) as ArrayRef);
            }

            // ── Catch-all: stringify ───────────────────────────────────────────
            _ => {
                let vals: Vec<Option<String>> = rows
                    .iter()
                    .map(|r| r.try_get::<_, Option<String>>(col_idx).ok().flatten())
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
pub fn max_timestamp_in_batch(batch: &RecordBatch, column: &str) -> Option<DateTime<Utc>> {
    let idx    = batch.schema().index_of(column).ok()?;
    let array  = batch.column(idx);
    let ts_arr = array.as_any().downcast_ref::<TimestampMicrosecondArray>()?;

    let max_us = (0..ts_arr.len())
        .filter_map(|i| if ts_arr.is_null(i) { None } else { Some(ts_arr.value(i)) })
        .max()?;

    DateTime::from_timestamp_micros(max_us)
}

/// Extract the last (maximum) Int64 or Int32 value from a column.
/// Used for cursor-based pagination advancement.
pub fn max_int_in_batch(batch: &RecordBatch, column: &str) -> Option<i64> {
    let idx   = batch.schema().index_of(column).ok()?;
    let array = batch.column(idx);

    if let Some(arr) = array.as_any().downcast_ref::<Int64Array>() {
        return (0..arr.len())
            .filter_map(|i| if arr.is_null(i) { None } else { Some(arr.value(i)) })
            .max();
    }
    if let Some(arr) = array.as_any().downcast_ref::<Int32Array>() {
        return (0..arr.len())
            .filter_map(|i| if arr.is_null(i) { None } else { Some(arr.value(i) as i64) })
            .max();
    }
    None
}

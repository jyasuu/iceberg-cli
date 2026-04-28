//! Production-grade file name and location generators.
//!
//! `DefaultFileNameGenerator` from iceberg-rust uses a simple counter, which
//! is unsafe when multiple writers run concurrently or a process restarts:
//! two writers can produce the same filename and silently overwrite each other.
//!
//! `ProductionFileNameGenerator` fixes this by combining:
//!   <prefix>-<timestamp_ms>-<uuid_v4>[-<suffix>].<ext>
//!
//! ## SafeLocationGenerator
//!
//! iceberg-rust 0.9.0 has a bug in `Datum::to_human_string` where the
//! `Timestamp` and `Timestamptz` variants hit `unreachable!()`, causing a
//! panic whenever a table with a temporal partition spec writes its first file:
//!
//! ```
//! thread 'main' panicked at iceberg-0.9.0/.../datum.rs:334:17:
//! internal error: entered unreachable code
//! stack backtrace:
//!   ...
//!   iceberg::spec::values::datum::Datum::to_human_string
//!   iceberg::spec::transform::Transform::to_human_string
//!   iceberg::spec::partition::PartitionSpec::partition_to_path
//!   DefaultLocationGenerator::generate_location
//! ```
//!
//! `SafeLocationGenerator` wraps `DefaultLocationGenerator` and overrides
//! `generate_location` to compute the partition sub-directory manually when the
//! table has temporal partition fields, bypassing the broken library path.

use iceberg::spec::{DataFileFormat, PartitionKey, TableMetadata, Transform};
use iceberg::writer::file_writer::location_generator::{
    DefaultLocationGenerator, FileNameGenerator, LocationGenerator,
};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct ProductionFileNameGenerator {
    prefix: String,
    suffix: Option<String>,
    format: DataFileFormat,
}

impl ProductionFileNameGenerator {
    pub fn new(
        prefix: impl Into<String>,
        suffix: Option<impl Into<String>>,
        format: DataFileFormat,
    ) -> Self {
        Self {
            prefix: prefix.into(),
            suffix: suffix.map(|s| s.into()),
            format,
        }
    }

    fn extension(&self) -> &'static str {
        match self.format {
            DataFileFormat::Parquet => "parquet",
            DataFileFormat::Avro => "avro",
            DataFileFormat::Orc => "orc",
            DataFileFormat::Puffin => "puffin",
        }
    }
}

// Correct trait method name is `generate_file_name`, not `generate`.
impl FileNameGenerator for ProductionFileNameGenerator {
    fn generate_file_name(&self) -> String {
        let ts = chrono::Utc::now().timestamp_millis();
        let uid = Uuid::new_v4().simple();

        match &self.suffix {
            Some(suf) => format!(
                "{}-{}-{}-{}.{}",
                self.prefix,
                ts,
                uid,
                suf,
                self.extension()
            ),
            None => format!("{}-{}-{}.{}", self.prefix, ts, uid, self.extension()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn names_are_unique() {
        // `gen` is a reserved keyword in Rust edition 2024 — use `fng`.
        let fng = ProductionFileNameGenerator::new("data", None::<&str>, DataFileFormat::Parquet);
        let names: HashSet<String> = (0..1000).map(|_| fng.generate_file_name()).collect();
        assert_eq!(names.len(), 1000, "collision detected");
    }

    #[test]
    fn name_contains_extension() {
        let fng = ProductionFileNameGenerator::new("data", None::<&str>, DataFileFormat::Parquet);
        assert!(fng.generate_file_name().ends_with(".parquet"));
    }

    #[test]
    fn name_with_suffix() {
        let fng = ProductionFileNameGenerator::new("part", Some("attempt_0"), DataFileFormat::Avro);
        let name = fng.generate_file_name();
        assert!(name.contains("attempt_0"));
        assert!(name.ends_with(".avro"));
    }
}

// ── SafeLocationGenerator ─────────────────────────────────────────────────────

/// A location generator that avoids the `Datum::to_human_string` panic in
/// iceberg-rust 0.9.0 for `Timestamp`/`Timestamptz` partition columns.
///
/// iceberg-rust 0.9 `DefaultLocationGenerator` calls
/// `PartitionSpec::partition_to_path` → `Datum::to_human_string`, but the
/// Timestamp variants of `Datum` hit `unreachable!()` — a library bug.
///
/// `SafeLocationGenerator` replicates the path-construction logic, computing
/// the partition directory from the raw `i64` microsecond value directly for
/// temporal transforms, and delegates to `DefaultLocationGenerator` for
/// non-temporal (identity, bucket, truncate) specs.
///
/// ## Path format
///
/// Matches the Hive-style convention: `<table>/data/<part_name>=<value>/`
///
/// | Transform | Example                              |
/// |-----------|--------------------------------------|
/// | `hour`    | `event_ts_hour=2024-03-15-14`        |
/// | `day`     | `event_ts_day=2024-03-15`            |
/// | `month`   | `event_ts_month=2024-03`             |
/// | `year`    | `event_ts_year=2024`                 |
#[derive(Clone)]
pub struct SafeLocationGenerator {
    /// Fallback for non-temporal or unpartitioned tables.
    inner: DefaultLocationGenerator,
    /// Pre-computed partition sub-path (may be empty for unpartitioned tables).
    partition_segment: String,
    /// Base table location from Iceberg metadata.
    table_location: String,
}

impl SafeLocationGenerator {
    /// Build a `SafeLocationGenerator` from Iceberg table metadata and the
    /// raw microsecond value of the partition column's first row.
    ///
    /// `partition_value_us` is ignored for non-temporal specs; pass `0` when
    /// the table is unpartitioned.
    pub fn new(
        meta: &TableMetadata,
        partition_value_us: i64,
    ) -> anyhow::Result<Self> {
        use iceberg::spec::PrimitiveType;

        let table_location = meta.location().to_string();
        let spec = meta.default_partition_spec();
        let schema = meta.current_schema();

        // For unpartitioned tables fall straight through to DefaultLocationGenerator.
        if spec.is_unpartitioned() {
            return Ok(Self {
                inner: DefaultLocationGenerator::new(meta.clone())
                    .map_err(|e| anyhow::anyhow!("DefaultLocationGenerator::new: {e}"))?,
                partition_segment: String::new(),
                table_location,
            });
        }

        // Check whether any partition field is a temporal transform on a
        // Timestamp/Timestamptz source column.  If any are, we compute all
        // partition path segments ourselves to avoid the library bug.
        let has_temporal_timestamp = spec.fields().iter().any(|f| {
            let is_temporal = matches!(
                f.transform,
                Transform::Hour | Transform::Day | Transform::Month | Transform::Year
            );
            if !is_temporal {
                return false;
            }
            schema
                .field_by_id(f.source_id)
                .map(|sf| {
                    matches!(
                        sf.field_type.as_primitive_type(),
                        Some(PrimitiveType::Timestamp) | Some(PrimitiveType::Timestamptz)
                    )
                })
                .unwrap_or(false)
        });

        if !has_temporal_timestamp {
            // Safe to use the standard generator (no broken Datum path).
            return Ok(Self {
                inner: DefaultLocationGenerator::new(meta.clone())
                    .map_err(|e| anyhow::anyhow!("DefaultLocationGenerator::new: {e}"))?,
                partition_segment: String::new(),
                table_location,
            });
        }

        // Build the partition directory string manually.
        // For a composite partition spec each field contributes one segment;
        // they are joined with `/`.
        let mut parts: Vec<String> = Vec::new();
        for field in spec.fields() {
            let segment = compute_temporal_segment(&field.name, field.transform, partition_value_us);
            parts.push(segment);
        }
        let partition_segment = parts.join("/");

        tracing::debug!(
            partition_segment = %partition_segment,
            "SafeLocationGenerator: computed temporal partition path"
        );

        Ok(Self {
            inner: DefaultLocationGenerator::new(meta.clone())
                .map_err(|e| anyhow::anyhow!("DefaultLocationGenerator::new: {e}"))?,
            partition_segment,
            table_location,
        })
    }
}

impl LocationGenerator for SafeLocationGenerator {
    fn generate_location(&self, partition_key: Option<&PartitionKey>, file_name: &str) -> String {
        if self.partition_segment.is_empty() {
            // Unpartitioned or non-temporal spec: delegate to the standard generator.
            // This also handles the case where inner already computed a safe path.
            // If the spec is genuinely non-temporal we can call inner safely;
            // otherwise partition_segment is set and we use the manual path.
            self.inner.generate_location(partition_key, file_name)
        } else {
            format!("{}/data/{}/{}", self.table_location, self.partition_segment, file_name)
        }
    }
}

/// Compute one Hive-style partition path segment for a temporal transform.
///
/// `partition_value_us` is the **source column value** in microseconds since
/// Unix epoch (Arrow `TimestampMicrosecondArray` native unit).
///
/// The returned string is `"<field_name>=<formatted_value>"`.
fn compute_temporal_segment(field_name: &str, transform: Transform, value_us: i64) -> String {
    use chrono::{DateTime, NaiveDate, Utc};

    match transform {
        Transform::Hour => {
            // Iceberg hour transform: hours since epoch.
            let hours = value_us / 3_600_000_000;
            // Format as YYYY-MM-DD-HH for human-readability (Hive convention).
            let dt = DateTime::<Utc>::from_timestamp(hours * 3600, 0)
                .unwrap_or_default()
                .naive_utc();
            format!("{}={}", field_name, dt.format("%Y-%m-%d-%H"))
        }
        Transform::Day => {
            let days = (value_us / 86_400_000_000) as i32;
            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            let d = epoch + chrono::Duration::days(days as i64);
            format!("{}={}", field_name, d.format("%Y-%m-%d"))
        }
        Transform::Month => {
            // Iceberg month transform: months since epoch (1970-01).
            let days = value_us / 86_400_000_000;
            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            let d = epoch + chrono::Duration::days(days);
            format!("{}={}", field_name, d.format("%Y-%m"))
        }
        Transform::Year => {
            let days = value_us / 86_400_000_000;
            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            let d = epoch + chrono::Duration::days(days);
            format!("{}={}", field_name, d.format("%Y"))
        }
        // For non-temporal transforms fall back to a raw string representation.
        _ => format!("{field_name}=unknown"),
    }
}

#[cfg(test)]
mod safe_location_tests {
    use super::*;

    #[test]
    fn compute_day_segment() {
        // 2024-03-15 00:00:00 UTC in microseconds
        let us = 1710460800i64 * 1_000_000;
        let seg = compute_temporal_segment("event_ts_day", Transform::Day, us);
        assert_eq!(seg, "event_ts_day=2024-03-15");
    }

    #[test]
    fn compute_month_segment() {
        let us = 1710460800i64 * 1_000_000; // 2024-03-15
        let seg = compute_temporal_segment("event_ts_month", Transform::Month, us);
        assert_eq!(seg, "event_ts_month=2024-03");
    }

    #[test]
    fn compute_year_segment() {
        let us = 1710460800i64 * 1_000_000; // 2024-03-15
        let seg = compute_temporal_segment("event_ts_year", Transform::Year, us);
        assert_eq!(seg, "event_ts_year=2024");
    }

    #[test]
    fn compute_hour_segment() {
        // 2024-03-15 14:00:00 UTC
        let us = (1710460800i64 + 14 * 3600) * 1_000_000;
        let seg = compute_temporal_segment("event_ts_hour", Transform::Hour, us);
        assert_eq!(seg, "event_ts_hour=2024-03-15-14");
    }
}
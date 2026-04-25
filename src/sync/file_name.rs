//! Production-grade file name generator.
//!
//! `DefaultFileNameGenerator` from iceberg-rust uses a simple counter, which
//! is unsafe when multiple writers run concurrently or a process restarts:
//! two writers can produce the same filename and silently overwrite each other.
//!
//! `ProductionFileNameGenerator` fixes this by combining:
//!   <prefix>-<timestamp_ms>-<uuid_v4>[-<suffix>].<ext>

use iceberg::spec::DataFileFormat;
use iceberg::writer::file_writer::location_generator::FileNameGenerator;
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

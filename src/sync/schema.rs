//! Arrow ↔ Iceberg schema conversion utilities.
//!
//! Extracted from `engine.rs` to keep each file focused on one concern.
//! Rule of thumb: anything that touches only type mapping or schema building
//! lives here; anything that uses a live `Catalog` or `Table` stays in `engine`.

use anyhow::{Context, Result};
use arrow_array::RecordBatch;
use arrow_schema::Schema as ArrowSchema;
use iceberg::spec::{NestedField, PrimitiveType, Schema as IcebergSchema, Type};

// ── Type mapping ──────────────────────────────────────────────────────────────

/// Map an Arrow primitive type to its Iceberg equivalent.
///
/// Unknown or complex Arrow types fall back to `String` so the pipeline
/// always produces a valid schema rather than hard-failing on an exotic
/// source type.
pub fn arrow_type_to_iceberg(dt: &arrow_schema::DataType) -> Type {
    use arrow_schema::DataType;
    match dt {
        DataType::Int8 | DataType::Int16 | DataType::Int32 => Type::Primitive(PrimitiveType::Int),
        DataType::Int64 => Type::Primitive(PrimitiveType::Long),
        DataType::Float32 => Type::Primitive(PrimitiveType::Float),
        DataType::Float64 => Type::Primitive(PrimitiveType::Double),
        DataType::Boolean => Type::Primitive(PrimitiveType::Boolean),
        DataType::Date32 | DataType::Date64 => Type::Primitive(PrimitiveType::Date),
        DataType::Timestamp(_, Some(_)) => Type::Primitive(PrimitiveType::Timestamptz),
        DataType::Timestamp(_, None) => Type::Primitive(PrimitiveType::Timestamp),
        _ => Type::Primitive(PrimitiveType::String),
    }
}

/// Convert an Arrow schema to an Iceberg schema, assigning sequential field IDs
/// starting at 1.
pub fn arrow_schema_to_iceberg(arrow: &ArrowSchema) -> Result<IcebergSchema> {
    let mut fields = Vec::new();
    for (idx, f) in arrow.fields().iter().enumerate() {
        let field_id = (idx + 1) as i32;
        let iceberg_t = arrow_type_to_iceberg(f.data_type());
        fields.push(NestedField::optional(field_id, f.name(), iceberg_t).into());
    }
    IcebergSchema::builder()
        .with_fields(fields)
        .with_schema_id(1)
        .build()
        .context("Failed to build Iceberg schema from Arrow schema")
}

// ── Field-ID injection ────────────────────────────────────────────────────────

/// Inject Iceberg field IDs as `PARQUET:field_id` Arrow metadata on each column.
///
/// Returns an error if any column in `batch` has no matching field in `schema`.
/// Use [`inject_field_ids_lenient`] when the batch may carry transient columns
/// (e.g. `_op` for CDC routing) that are intentionally absent from the schema.
#[allow(dead_code)]
pub fn inject_field_ids(batch: RecordBatch, schema: &IcebergSchema) -> Result<RecordBatch> {
    use arrow_schema::Field;
    use std::sync::Arc;

    let old_arrow = batch.schema();
    let mut new_fields: Vec<Field> = Vec::with_capacity(old_arrow.fields().len());

    for arrow_field in old_arrow.fields().iter() {
        let iceberg_field = schema
            .as_struct()
            .fields()
            .iter()
            .find(|f| f.name == *arrow_field.name())
            .with_context(|| {
                format!(
                    "Column '{}' in query result has no matching field in Iceberg schema",
                    arrow_field.name()
                )
            })?;

        let mut meta = arrow_field.metadata().clone();
        meta.insert("PARQUET:field_id".to_string(), iceberg_field.id.to_string());
        new_fields.push(arrow_field.as_ref().clone().with_metadata(meta));
    }

    let new_schema =
        Arc::new(ArrowSchema::new(new_fields).with_metadata(old_arrow.metadata().clone()));
    RecordBatch::try_new(new_schema, batch.columns().to_vec())
        .context("Rebuild RecordBatch with injected field IDs")
}

/// Like [`inject_field_ids`] but silently skips columns absent from the schema.
///
/// Needed for `merge_into` batches that carry `_op`, which is stripped inside
/// `plan_merge_into` before any file write occurs.
pub fn inject_field_ids_lenient(batch: RecordBatch, schema: &IcebergSchema) -> Result<RecordBatch> {
    use arrow_schema::Field;
    use std::sync::Arc;

    let old_arrow = batch.schema();
    let mut new_fields: Vec<Field> = Vec::with_capacity(old_arrow.fields().len());

    for arrow_field in old_arrow.fields().iter() {
        let meta_entry = schema
            .as_struct()
            .fields()
            .iter()
            .find(|f| f.name == *arrow_field.name())
            .map(|f| f.id.to_string());

        let mut meta = arrow_field.metadata().clone();
        if let Some(id) = meta_entry {
            meta.insert("PARQUET:field_id".to_string(), id);
        }
        new_fields.push(arrow_field.as_ref().clone().with_metadata(meta));
    }

    let new_schema =
        Arc::new(ArrowSchema::new(new_fields).with_metadata(old_arrow.metadata().clone()));
    RecordBatch::try_new(new_schema, batch.columns().to_vec())
        .context("Rebuild RecordBatch with injected field IDs (lenient)")
}

// ─────────────────────────────────────────────────────────────────────────────
#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    // ── arrow_type_to_iceberg ─────────────────────────────────────────────────

    #[test]
    fn type_mapping_integers() {
        assert_eq!(
            arrow_type_to_iceberg(&DataType::Int32),
            Type::Primitive(PrimitiveType::Int)
        );
        assert_eq!(
            arrow_type_to_iceberg(&DataType::Int8),
            Type::Primitive(PrimitiveType::Int)
        );
        assert_eq!(
            arrow_type_to_iceberg(&DataType::Int64),
            Type::Primitive(PrimitiveType::Long)
        );
    }

    #[test]
    fn type_mapping_floats() {
        assert_eq!(
            arrow_type_to_iceberg(&DataType::Float32),
            Type::Primitive(PrimitiveType::Float)
        );
        assert_eq!(
            arrow_type_to_iceberg(&DataType::Float64),
            Type::Primitive(PrimitiveType::Double)
        );
    }

    #[test]
    fn type_mapping_bool_date_timestamp() {
        assert_eq!(
            arrow_type_to_iceberg(&DataType::Boolean),
            Type::Primitive(PrimitiveType::Boolean)
        );
        assert_eq!(
            arrow_type_to_iceberg(&DataType::Date32),
            Type::Primitive(PrimitiveType::Date)
        );
        use arrow_schema::TimeUnit;
        assert_eq!(
            arrow_type_to_iceberg(&DataType::Timestamp(TimeUnit::Microsecond, None)),
            Type::Primitive(PrimitiveType::Timestamp)
        );
        assert_eq!(
            arrow_type_to_iceberg(&DataType::Timestamp(
                TimeUnit::Microsecond,
                Some("UTC".into())
            )),
            Type::Primitive(PrimitiveType::Timestamptz)
        );
    }

    #[test]
    fn type_mapping_unknown_falls_back_to_string() {
        // LargeUtf8 is not explicitly handled → String fallback
        assert_eq!(
            arrow_type_to_iceberg(&DataType::LargeUtf8),
            Type::Primitive(PrimitiveType::String)
        );
    }

    // ── arrow_schema_to_iceberg ───────────────────────────────────────────────

    fn schema_with_fields(fields: Vec<(&str, DataType)>) -> ArrowSchema {
        Schema::new(
            fields
                .into_iter()
                .map(|(name, dt)| Field::new(name, dt, true))
                .collect::<Vec<_>>(),
        )
    }

    #[test]
    fn schema_to_iceberg_assigns_sequential_ids() {
        let arrow = schema_with_fields(vec![
            ("id", DataType::Int64),
            ("name", DataType::Utf8),
            ("score", DataType::Float64),
        ]);
        let iceberg = arrow_schema_to_iceberg(&arrow).unwrap();
        let ids: Vec<i32> = iceberg.as_struct().fields().iter().map(|f| f.id).collect();
        assert_eq!(ids, vec![1, 2, 3]);
    }

    #[test]
    fn schema_to_iceberg_single_field() {
        let arrow = schema_with_fields(vec![("ts", DataType::Date32)]);
        let iceberg = arrow_schema_to_iceberg(&arrow).unwrap();
        let f = &iceberg.as_struct().fields()[0];
        assert_eq!(f.id, 1);
        assert_eq!(f.field_type, Box::new(Type::Primitive(PrimitiveType::Date)));
    }

    // ── inject_field_ids ─────────────────────────────────────────────────────

    fn make_schema(fields: Vec<(&str, DataType)>) -> ArrowSchema {
        schema_with_fields(fields)
    }

    fn make_iceberg(fields: Vec<(&str, DataType)>) -> IcebergSchema {
        arrow_schema_to_iceberg(&make_schema(fields)).unwrap()
    }

    #[test]
    fn inject_field_ids_adds_parquet_metadata() {
        let arrow = make_schema(vec![("id", DataType::Int64), ("name", DataType::Utf8)]);
        let iceberg = make_iceberg(vec![("id", DataType::Int64), ("name", DataType::Utf8)]);

        let batch = RecordBatch::try_new(
            Arc::new(arrow),
            vec![
                Arc::new(Int64Array::from(vec![1i64])),
                Arc::new(StringArray::from(vec!["x"])),
            ],
        )
        .unwrap();

        let out = inject_field_ids(batch, &iceberg).unwrap();
        for field in out.schema().fields().iter() {
            assert!(
                field.metadata().contains_key("PARQUET:field_id"),
                "field '{}' missing PARQUET:field_id",
                field.name()
            );
        }
    }

    #[test]
    fn inject_field_ids_errors_on_missing_column() {
        // Iceberg schema has "id" but batch has "wrong_col"
        let arrow = make_schema(vec![("wrong_col", DataType::Int64)]);
        let iceberg = make_iceberg(vec![("id", DataType::Int64)]);

        let batch = RecordBatch::try_new(
            Arc::new(arrow),
            vec![Arc::new(Int64Array::from(vec![1i64]))],
        )
        .unwrap();

        assert!(inject_field_ids(batch, &iceberg).is_err());
    }

    #[test]
    fn inject_lenient_skips_unknown_columns() {
        // batch has "_op" which is not in the iceberg schema
        let arrow = make_schema(vec![("id", DataType::Int64), ("_op", DataType::Utf8)]);
        let iceberg = make_iceberg(vec![("id", DataType::Int64)]);

        let batch = RecordBatch::try_new(
            Arc::new(arrow),
            vec![
                Arc::new(Int64Array::from(vec![1i64])),
                Arc::new(StringArray::from(vec!["U"])),
            ],
        )
        .unwrap();

        let out = inject_field_ids_lenient(batch, &iceberg).unwrap();
        let schema = out.schema();
        // "id" gets a field_id; "_op" does not
        assert!(
            schema
                .field_with_name("id")
                .unwrap()
                .metadata()
                .contains_key("PARQUET:field_id")
        );
        assert!(
            !schema
                .field_with_name("_op")
                .unwrap()
                .metadata()
                .contains_key("PARQUET:field_id")
        );
    }

    #[test]
    fn inject_field_ids_preserves_row_data() {
        let arrow = make_schema(vec![("id", DataType::Int32)]);
        let iceberg = make_iceberg(vec![("id", DataType::Int32)]);

        let batch =
            RecordBatch::try_new(Arc::new(arrow), vec![Arc::new(Int32Array::from(vec![42]))])
                .unwrap();

        let out = inject_field_ids(batch, &iceberg).unwrap();
        let col = out.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(col.value(0), 42);
    }
}

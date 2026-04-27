//! CLI parsing helpers for the `iceberg-cli` binary.
//!
//! Extracted from `main.rs` so the command-dispatch match stays thin and
//! this pure logic is independently testable without spinning up a catalog.

use anyhow::{Context, Result, bail};
use iceberg::{
    NamespaceIdent, TableIdent,
    spec::{NestedField, PrimitiveType, Schema, Type},
};

// ─────────────────────────────────────────────────────────────────────────────

/// Parse a dotted namespace string (e.g. `"my_db"` or `"catalog.db"`) into
/// a [`NamespaceIdent`].
pub fn parse_namespace(s: &str) -> Result<NamespaceIdent> {
    NamespaceIdent::from_strs(s.split('.').collect::<Vec<_>>())
        .map_err(|e| anyhow::anyhow!("Invalid namespace '{s}': {e}"))
}

/// Parse a fully-qualified `"namespace.table"` string into a [`TableIdent`].
///
/// The rightmost `.`-delimited segment is the table name; everything to the
/// left is the namespace.
pub fn parse_table(s: &str) -> Result<TableIdent> {
    let parts: Vec<&str> = s.rsplitn(2, '.').collect();
    match parts.as_slice() {
        [name, namespace] => {
            let ns = parse_namespace(namespace)?;
            Ok(TableIdent::new(ns, name.to_string()))
        }
        _ => bail!("Expected 'namespace.table', got '{s}'"),
    }
}

/// Parse a comma-separated `"col:type"` schema spec into an Iceberg [`Schema`].
///
/// Supported type tokens: `long`/`int64`, `int`/`int32`, `string`/`str`,
/// `double`/`float64`, `float`/`float32`, `boolean`/`bool`, `date`,
/// `timestamp`.
pub fn parse_schema(spec: &str) -> Result<Schema> {
    let mut fields = Vec::new();
    for (idx, part) in spec.split(',').enumerate() {
        let kv: Vec<&str> = part.trim().splitn(2, ':').collect();
        if kv.len() != 2 {
            bail!("Bad field spec '{part}' — expected 'name:type'");
        }
        let name = kv[0].trim();
        let ptype = match kv[1].trim().to_lowercase().as_str() {
            "long" | "int64" => PrimitiveType::Long,
            "int" | "int32" => PrimitiveType::Int,
            "string" | "str" => PrimitiveType::String,
            "double" | "float64" => PrimitiveType::Double,
            "float" | "float32" => PrimitiveType::Float,
            "boolean" | "bool" => PrimitiveType::Boolean,
            "date" => PrimitiveType::Date,
            "timestamp" => PrimitiveType::Timestamp,
            other => bail!("Unknown type '{other}'"),
        };
        let field_id = (idx + 1) as i32;
        fields.push(NestedField::optional(field_id, name, Type::Primitive(ptype)).into());
    }

    Schema::builder()
        .with_fields(fields)
        .with_schema_id(1)
        .build()
        .context("build schema")
}

// ─────────────────────────────────────────────────────────────────────────────
#[cfg(test)]
mod tests {
    use super::*;

    // ── parse_namespace ──────────────────────────────────────────────────────

    #[test]
    fn namespace_single_segment() {
        let ns = parse_namespace("mydb").unwrap();
        assert_eq!(ns.to_url_string(), "mydb");
    }

    #[test]
    fn namespace_dotted() {
        let ns = parse_namespace("catalog.db").unwrap();
        // two segments
        assert_eq!(ns.len(), 2);
    }

    // ── parse_table ──────────────────────────────────────────────────────────

    #[test]
    fn table_simple() {
        let t = parse_table("mydb.orders").unwrap();
        assert_eq!(t.name(), "orders");
    }

    #[test]
    fn table_dotted_namespace() {
        let t = parse_table("catalog.db.sales").unwrap();
        assert_eq!(t.name(), "sales");
        assert_eq!(t.namespace().to_url_string(), "catalog\u{1f}db");
    }

    #[test]
    fn table_missing_dot_errors() {
        assert!(parse_table("orders").is_err());
    }

    // ── parse_schema ─────────────────────────────────────────────────────────

    #[test]
    fn schema_basic_types() {
        let s = parse_schema("id:long,name:string,price:double").unwrap();
        assert_eq!(s.as_struct().fields().len(), 3);
    }

    #[test]
    fn schema_aliases_accepted() {
        parse_schema("a:int64,b:str,c:float64,d:float32,e:bool,f:int32").unwrap();
    }

    #[test]
    fn schema_date_and_timestamp() {
        let s = parse_schema("dt:date,ts:timestamp").unwrap();
        let fields = s.as_struct().fields();
        assert_eq!(
            fields[0].field_type,
            Box::new(Type::Primitive(PrimitiveType::Date))
        );
        assert_eq!(
            fields[1].field_type,
            Box::new(Type::Primitive(PrimitiveType::Timestamp))
        );
    }

    #[test]
    fn schema_sequential_field_ids() {
        let s = parse_schema("a:long,b:long,c:long").unwrap();
        let ids: Vec<i32> = s.as_struct().fields().iter().map(|f| f.id).collect();
        assert_eq!(ids, vec![1, 2, 3]);
    }

    #[test]
    fn schema_unknown_type_errors() {
        assert!(parse_schema("x:blob").is_err());
    }

    #[test]
    fn schema_malformed_spec_errors() {
        assert!(parse_schema("nocolon").is_err());
    }
}

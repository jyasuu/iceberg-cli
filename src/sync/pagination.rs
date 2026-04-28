//! SQL pagination helpers.
//!
//! Constructs the paged SQL variants used by the sync engine to batch-read
//! from PostgreSQL — either keyset/cursor pagination or classic LIMIT/OFFSET.

use anyhow::Result;
use iceberg::{NamespaceIdent, TableIdent};

use crate::config::SyncJob;

// ─────────────────────────────────────────────────────────────────────────────

/// Build the SQL for a single page of results.
///
/// **Cursor mode** (when `job.cursor_column` is set): wraps the user SQL in a
/// subquery that exposes the cursor column as `_pgcursor`, then filters and
/// orders by it.  This avoids OFFSET-drift (rows inserted mid-run shifting
/// subsequent pages).
///
/// **Offset mode** (fallback): simple `LIMIT / OFFSET`.
pub(crate) fn build_paged_sql(job: &SyncJob, offset: usize) -> String {
    if let Some(col) = &job.cursor_column {
        // The inner subquery always aliases the cursor column to `_pgcursor` so
        // the outer filter works even when the user SQL does not SELECT that
        // column by name (e.g. it only appears in ORDER BY).
        format!(
            "SELECT * FROM (\
             SELECT _i.*, _i.{col} AS _pgcursor \
             FROM ({inner}) _i\
             ) _o \
             WHERE _pgcursor > :_cursor \
             ORDER BY _pgcursor \
             LIMIT {limit}",
            inner = job.sql,
            col = col,
            limit = job.batch_size,
        )
    } else {
        format!(
            "SELECT * FROM ({}) _q LIMIT {} OFFSET {}",
            job.sql, job.batch_size, offset
        )
    }
}

/// Parse `"namespace.table"` into an Iceberg [`TableIdent`].
pub(crate) fn table_ident(namespace: &str, table: &str) -> Result<TableIdent> {
    let ns = NamespaceIdent::from_strs(namespace.split('.').collect::<Vec<_>>())
        .map_err(|e| anyhow::anyhow!("Invalid namespace '{namespace}': {e}"))?;
    Ok(TableIdent::new(ns, table.to_string()))
}

// ─────────────────────────────────────────────────────────────────────────────
#[cfg(test)]
mod tests {
    use super::*;

    /// Minimal `SyncJob` stub for pagination tests.
    fn base_job() -> SyncJob {
        use crate::config::{CursorType, SyncMode, WatermarkType, WriteMode};
        SyncJob {
            name: "test_job".into(),
            source: "src".into(),
            destination: "dst".into(),
            namespace: "ns".into(),
            table: "tbl".into(),
            sql: "SELECT * FROM orders".into(),
            mode: SyncMode::Incremental,
            batch_size: 100,
            write_mode: WriteMode::Append,
            watermark_column: None,
            watermark_type: WatermarkType::Timestamptz,
            cursor_column: None,
            cursor_type: CursorType::Int,
            partition_column: None,
            iceberg_partition: None,
            merge: None,
            schema_evolution: Default::default(),
            depends_on: None,
            retry: None,
            group: None,
        }
    }

    // ── cursor mode ──────────────────────────────────────────────────────────

    #[test]
    fn paged_sql_cursor_mode_syntax() {
        let mut job = base_job();
        job.cursor_column = Some("updated_at".into());

        let sql = build_paged_sql(&job, 0);

        assert!(
            sql.contains("WHERE _pgcursor > :_cursor"),
            "missing cursor filter"
        );
        assert!(sql.contains("ORDER BY _pgcursor"), "missing order by");
        assert!(sql.contains("LIMIT 100"), "missing limit");
        assert!(sql.contains("_i.updated_at AS _pgcursor"), "missing alias");
    }

    #[test]
    fn paged_sql_cursor_mode_wraps_subquery() {
        let mut job = base_job();
        job.cursor_column = Some("id".into());

        let sql = build_paged_sql(&job, 999); // offset ignored in cursor mode
        assert!(sql.contains("SELECT * FROM orders"), "inner SQL missing");
        assert!(!sql.contains("OFFSET"), "cursor mode must not use OFFSET");
    }

    // ── offset mode ──────────────────────────────────────────────────────────

    #[test]
    fn paged_sql_offset_mode_zero() {
        let job = base_job(); // no cursor_column
        let sql = build_paged_sql(&job, 0);
        assert_eq!(
            sql,
            "SELECT * FROM (SELECT * FROM orders) _q LIMIT 100 OFFSET 0"
        );
    }

    #[test]
    fn paged_sql_offset_mode_advances() {
        let job = base_job();
        let sql = build_paged_sql(&job, 200);
        assert!(sql.contains("OFFSET 200"));
        assert!(sql.contains("LIMIT 100"));
    }

    // ── table_ident ──────────────────────────────────────────────────────────

    #[test]
    fn table_ident_parses_simple_namespace() {
        let ident = table_ident("mydb", "orders").unwrap();
        assert_eq!(ident.name(), "orders");
    }

    #[test]
    fn table_ident_parses_dotted_namespace() {
        let ident = table_ident("catalog.db", "sales").unwrap();
        assert_eq!(ident.name(), "sales");
    }
}
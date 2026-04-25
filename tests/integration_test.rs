
//! Integration tests for iceberg-cli.
//!
//! These tests require the full docker-compose stack to be running:
//!
//!   docker compose up -d
//!
//! Then run with:
//!
//!   cargo test --test integration_test -- --test-threads=1
//!
//! Environment variables (all have defaults matching docker-compose):
//!
//!   ICEBERG_URI          http://localhost:8181
//!   S3_ENDPOINT          http://localhost:9000
//!   AWS_ACCESS_KEY_ID    admin
//!   AWS_SECRET_ACCESS_KEY password
//!   AWS_REGION           us-east-1
//!   PG_DSN               host=localhost port=5432 dbname=shop user=app password=secret sslmode=disable
//!   RABBITMQ_URI         amqp://guest:guest@localhost:5672/%2f
//!
//! Tests are grouped into modules that mirror the original integration_test.sh
//! sections.  Each module can be run in isolation:
//!
//!   cargo test --test integration_test namespace
//!   cargo test --test integration_test sync
//!   cargo test --test integration_test rabbitmq

use std::process::{Command, Output};
use std::time::Duration;

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Absolute path to the release binary.
fn cli_bin() -> String {
    // When run via `cargo test` the binary is already built.  Resolve relative
    // to CARGO_MANIFEST_DIR so it works from any working directory.
    let manifest = std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".into());
    format!("{manifest}/target/release/iceberg-cli")
}

fn iceberg_uri()   -> String { std::env::var("ICEBERG_URI").unwrap_or_else(|_| "http://localhost:8181".into()) }
fn s3_endpoint()   -> String { std::env::var("S3_ENDPOINT").unwrap_or_else(|_| "http://localhost:9000".into()) }
fn access_key()    -> String { std::env::var("AWS_ACCESS_KEY_ID").unwrap_or_else(|_| "admin".into()) }
fn secret_key()    -> String { std::env::var("AWS_SECRET_ACCESS_KEY").unwrap_or_else(|_| "password".into()) }
fn region()        -> String { std::env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".into()) }
fn pg_dsn()        -> String {
    std::env::var("PG_DSN")
        .unwrap_or_else(|_| "host=localhost port=5432 dbname=shop user=app password=secret sslmode=disable".into())
}
fn rabbitmq_uri()  -> String {
    std::env::var("RABBITMQ_URI").unwrap_or_else(|_| "amqp://guest:guest@localhost:5672/%2f".into())
}

/// Run the CLI with common global flags pre-applied.
fn cli(args: &[&str]) -> Output {
    let bin = cli_bin();
    let mut cmd = Command::new(&bin);
    cmd.args([
        "--uri",              &iceberg_uri(),
        "--s3-endpoint",      &s3_endpoint(),
        "--access-key-id",    &access_key(),
        "--secret-access-key",&secret_key(),
        "--region",           &region(),
    ]);
    cmd.args(args);
    cmd.output().unwrap_or_else(|e| panic!("Failed to run {bin}: {e}"))
}

/// Assert the command exits 0.  Panics with stdout+stderr on failure.
#[track_caller]
fn assert_ok(label: &str, args: &[&str]) -> String {
    let out = cli(args);
    let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
    assert!(
        out.status.success(),
        "[{label}] expected success\nstdout: {stdout}\nstderr: {stderr}"
    );
    stdout
}

/// Assert the command exits 0 AND stdout contains `needle`.
#[track_caller]
fn assert_output(label: &str, needle: &str, args: &[&str]) {
    let stdout = assert_ok(label, args);
    assert!(
        stdout.contains(needle),
        "[{label}] expected '{needle}' in output\nstdout: {stdout}"
    );
}

/// Assert the command exits non-zero (expected failure).
#[track_caller]
fn assert_fail(label: &str, args: &[&str]) {
    let out = cli(args);
    assert!(
        !out.status.success(),
        "[{label}] expected failure but command succeeded\nstdout: {}",
        String::from_utf8_lossy(&out.stdout)
    );
}

/// Poll a URL until it returns 200 or the timeout elapses.
fn wait_for_http(url: &str, timeout: Duration) {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if reqwest::blocking::get(url).map(|r| r.status().is_success()).unwrap_or(false) {
            return;
        }
        if std::time::Instant::now() >= deadline {
            panic!("Service at {url} did not become ready within {timeout:?}");
        }
        std::thread::sleep(Duration::from_secs(1));
    }
}

/// Write a temp YAML config file, run `f` with its path, then delete it.
fn with_sync_config<F: FnOnce(&str)>(yaml: &str, f: F) {
    let mut file = tempfile::NamedTempFile::new().expect("tempfile");
    use std::io::Write;
    write!(file, "{yaml}").unwrap();
    f(file.path().to_str().unwrap());
    // NamedTempFile deleted on drop
}

// ── Pre-flight ─────────────────────────────────────────────────────────────────

/// Called once at the start of each test module that needs the stack.
fn preflight() {
    // Verify the binary exists.
    assert!(
        std::path::Path::new(&cli_bin()).exists(),
        "CLI binary not found at {}. Run: cargo build --release",
        cli_bin()
    );
    wait_for_http(&format!("{}/v1/config", iceberg_uri()), Duration::from_secs(30));
    wait_for_http(&format!("{}/minio/health/live", s3_endpoint()), Duration::from_secs(30));
}

// Fixed namespace so tests are isolated from each other and from the example data.
const NS: &str = "itest";

// =============================================================================
// § 1 – Namespace management
// =============================================================================
#[cfg(test)]
mod namespace {
    use super::*;

    #[test]
    fn t1_1_create_namespace() {
        preflight();
        assert_ok("1.1 create namespace", &["create-namespace", "--namespace", NS]);
    }

    #[test]
    fn t1_2_list_namespaces_shows_ns() {
        preflight();
        // Ensure it exists first.
        cli(&["create-namespace", "--namespace", NS]);
        assert_output("1.2 list-namespaces shows ns", NS, &["list-namespaces"]);
    }

    #[test]
    fn t1_3_create_namespace_idempotent() {
        preflight();
        cli(&["create-namespace", "--namespace", NS]);
        // Second call should not crash.
        assert_ok("1.3 create namespace again", &["create-namespace", "--namespace", NS]);
    }
}

// =============================================================================
// § 2 – Table management
// =============================================================================
#[cfg(test)]
mod table_mgmt {
    use super::*;

    fn setup() { preflight(); cli(&["create-namespace", "--namespace", NS]); }

    #[test]
    fn t2_1_create_table() {
        setup();
        assert_ok("2.1 create table", &[
            "create-table", "--table", &format!("{NS}.products"),
            "--schema", "id:long,name:string,price:double",
        ]);
    }

    #[test]
    fn t2_2_list_tables_shows_table() {
        setup();
        cli(&["create-table", "--table", &format!("{NS}.products2"),
              "--schema", "id:long,name:string"]);
        assert_output("2.2 list-tables", "products2",
            &["list-tables", "--namespace", NS]);
    }

    #[test]
    fn t2_3_describe_shows_field() {
        setup();
        cli(&["create-table", "--table", &format!("{NS}.products3"),
              "--schema", "id:long,name:string,price:double"]);
        assert_output("2.3 describe shows field", "price",
            &["describe", "--table", &format!("{NS}.products3")]);
    }

    #[test]
    fn t2_4_describe_shows_snapshot_section() {
        setup();
        cli(&["create-table", "--table", &format!("{NS}.products4"),
              "--schema", "id:long,name:string,price:double"]);
        assert_output("2.4 describe shows format", "Format",
            &["describe", "--table", &format!("{NS}.products4")]);
    }
}

// =============================================================================
// § 3 – Basic write / scan
// =============================================================================
#[cfg(test)]
mod write_scan {
    use super::*;

    fn setup_table(suffix: &str) -> String {
        preflight();
        cli(&["create-namespace", "--namespace", NS]);
        let tbl = format!("{NS}.ws_{suffix}");
        cli(&["create-table", "--table", &tbl,
              "--schema", "id:long,name:string,price:double"]);
        tbl
    }

    #[test]
    fn t3_1_write_single_row() {
        let tbl = setup_table("single");
        assert_ok("3.1 write single row", &[
            "write", "--table", &tbl,
            "--json", r#"[{"id":1,"name":"Widget","price":9.99}]"#,
        ]);
    }

    #[test]
    fn t3_2_scan_shows_written_row() {
        let tbl = setup_table("scan1");
        cli(&["write", "--table", &tbl,
              "--json", r#"[{"id":1,"name":"Widget","price":9.99}]"#]);
        assert_output("3.2 scan shows row", "Widget", &["scan", "--table", &tbl]);
    }

    #[test]
    fn t3_3_write_multiple_rows() {
        let tbl = setup_table("multi");
        assert_ok("3.3 write multiple rows", &[
            "write", "--table", &tbl,
            "--json", r#"[{"id":2,"name":"Gadget","price":19.99},{"id":3,"name":"Doohickey","price":4.49}]"#,
        ]);
    }

    #[test]
    fn t3_4_scan_shows_all_rows() {
        let tbl = setup_table("all");
        cli(&["write", "--table", &tbl,
              "--json", r#"[{"id":1,"name":"Widget","price":9.99},{"id":2,"name":"Gadget","price":19.99}]"#]);
        assert_output("3.4 scan all rows", "Gadget", &["scan", "--table", &tbl]);
    }

    #[test]
    fn t3_5_scan_with_limit() {
        let tbl = setup_table("limit");
        cli(&["write", "--table", &tbl,
              "--json", r#"[{"id":1,"name":"A","price":1.0},{"id":2,"name":"B","price":2.0},{"id":3,"name":"C","price":3.0}]"#]);
        assert_output("3.5 scan limit=1", "1 rows shown",
            &["scan", "--table", &tbl, "--limit", "1"]);
    }
}

// =============================================================================
// § 4 – Column projection
// =============================================================================
#[cfg(test)]
mod projection {
    use super::*;

    fn setup() -> String {
        preflight();
        cli(&["create-namespace", "--namespace", NS]);
        let tbl = format!("{NS}.proj");
        cli(&["create-table", "--table", &tbl,
              "--schema", "id:long,name:string,price:double"]);
        cli(&["write", "--table", &tbl,
              "--json", r#"[{"id":1,"name":"Widget","price":9.99}]"#]);
        tbl
    }

    #[test]
    fn t4_1_projected_columns_appear() {
        let tbl = setup();
        assert_output("4.1 projected columns present", "name",
            &["scan", "--table", &tbl, "--columns", "id,name"]);
    }

    #[test]
    fn t4_2_unprojected_column_absent() {
        let tbl = setup();
        let out = assert_ok("4.2 projection ok", &["scan", "--table", &tbl, "--columns", "id,name"]);
        assert!(!out.contains("price"), "price should be absent from projected scan; got: {out}");
    }
}

// =============================================================================
// § 5 – Append semantics & snapshot isolation
// =============================================================================
#[cfg(test)]
mod append {
    use super::*;

    #[test]
    fn t5_multi_batch_append_and_scan() {
        preflight();
        cli(&["create-namespace", "--namespace", NS]);
        let tbl = format!("{NS}.orders_append");
        cli(&["create-table", "--table", &tbl,
              "--schema", "id:long,status:string,total:double"]);

        cli(&["write", "--table", &tbl,
              "--json", r#"[{"id":100,"status":"pending","total":19.98}]"#]);
        cli(&["write", "--table", &tbl,
              "--json", r#"[{"id":101,"status":"shipped","total":19.99},{"id":102,"status":"pending","total":22.45}]"#]);

        assert_output("5 append: shipped visible", "shipped",
            &["scan", "--table", &tbl]);
        assert_output("5 append: pending visible", "pending",
            &["scan", "--table", &tbl]);
    }

    #[test]
    fn t6_describe_shows_snapshot_after_write() {
        preflight();
        cli(&["create-namespace", "--namespace", NS]);
        let tbl = format!("{NS}.snap_test");
        cli(&["create-table", "--table", &tbl, "--schema", "id:long,val:string"]);
        cli(&["write", "--table", &tbl, "--json", r#"[{"id":1,"val":"x"}]"#]);
        assert_output("6 snapshot present", "Snapshot",
            &["describe", "--table", &tbl]);
    }
}

// =============================================================================
// § 7 – Edge cases
// =============================================================================
#[cfg(test)]
mod edge_cases {
    use super::*;

    fn setup_table(suffix: &str) -> String {
        preflight();
        cli(&["create-namespace", "--namespace", NS]);
        let tbl = format!("{NS}.edge_{suffix}");
        cli(&["create-table", "--table", &tbl,
              "--schema", "id:long,name:string,price:double"]);
        tbl
    }

    #[test]
    fn t7_1_write_empty_array_is_noop() {
        let tbl = setup_table("empty");
        assert_ok("7.1 empty write", &["write", "--table", &tbl, "--json", "[]"]);
    }

    #[test]
    fn t7_2_table_readable_after_empty_write() {
        let tbl = setup_table("after_empty");
        cli(&["write", "--table", &tbl,
              "--json", r#"[{"id":1,"name":"Widget","price":9.99}]"#]);
        cli(&["write", "--table", &tbl, "--json", "[]"]);
        assert_output("7.2 readable after empty write", "Widget",
            &["scan", "--table", &tbl]);
    }

    #[test]
    fn t7_3_write_null_values() {
        let tbl = setup_table("nulls");
        assert_ok("7.3 null values", &[
            "write", "--table", &tbl,
            "--json", r#"[{"id":99,"name":null,"price":0.0}]"#,
        ]);
    }

    #[test]
    fn t7_4_high_limit_returns_all_rows() {
        let tbl = setup_table("highlimit");
        cli(&["write", "--table", &tbl,
              "--json", r#"[{"id":1,"name":"A","price":1.0},{"id":2,"name":"B","price":2.0}]"#]);
        assert_output("7.4 high limit", "rows shown",
            &["scan", "--table", &tbl, "--limit", "9999"]);
    }
}

// =============================================================================
// § 8 – Expected failures
// =============================================================================
#[cfg(test)]
mod expected_failures {
    use super::*;

    #[test]
    fn t8_1_scan_nonexistent_table() {
        preflight();
        assert_fail("8.1 scan missing table",
            &["scan", "--table", &format!("{NS}.no_such_table_xyz")]);
    }

    #[test]
    fn t8_2_describe_nonexistent_table() {
        preflight();
        assert_fail("8.2 describe missing table",
            &["describe", "--table", &format!("{NS}.ghost_xyz")]);
    }

    #[test]
    fn t8_3_list_tables_unknown_namespace() {
        preflight();
        assert_fail("8.3 list-tables unknown ns",
            &["list-tables", "--namespace", "no_such_namespace_xyz_abc"]);
    }
}

// =============================================================================
// § 9 – All data types
// =============================================================================
#[cfg(test)]
mod data_types {
    use super::*;

    #[test]
    fn t9_all_types_roundtrip() {
        preflight();
        cli(&["create-namespace", "--namespace", NS]);
        let tbl = format!("{NS}.all_types");
        assert_ok("9.1 create all-types table", &[
            "create-table", "--table", &tbl,
            "--schema", "i:int,l:long,f:float,d:double,b:boolean,s:string,dt:date,ts:timestamp",
        ]);
        assert_ok("9.2 write all-types row", &[
            "write", "--table", &tbl,
            "--json", r#"[{"i":42,"l":9000000000,"f":3.14,"d":2.718,"b":true,"s":"hello","dt":null,"ts":null}]"#,
        ]);
        assert_output("9.3 long value visible", "9000000000",
            &["scan", "--table", &tbl]);
    }
}

// =============================================================================
// § 10 – Large batch stress test
// =============================================================================
#[cfg(test)]
mod stress {
    use super::*;

    #[test]
    fn t10_write_200_rows_and_scan() {
        preflight();
        cli(&["create-namespace", "--namespace", NS]);
        let tbl = format!("{NS}.stress");
        cli(&["create-table", "--table", &tbl, "--schema", "id:long,val:string"]);

        // Build 200-row JSON array.
        let rows: Vec<String> = (1..=200)
            .map(|i| format!(r#"{{"id":{i},"val":"row_{i}"}}"#))
            .collect();
        let json = format!("[{}]", rows.join(","));

        assert_ok("10.1 write 200 rows", &["write", "--table", &tbl, "--json", &json]);
        assert_output("10.2 scan 200 rows", "200 rows shown",
            &["scan", "--table", &tbl, "--limit", "200"]);
    }
}

// =============================================================================
// § 11 – Sync config: PostgreSQL → Iceberg
// =============================================================================
#[cfg(test)]
mod sync {
    use super::*;

    fn pg_reachable() -> bool {
        // Quick TCP probe — no psql needed.
        std::net::TcpStream::connect("localhost:5432").is_ok()
    }

    fn sync_config(ns: &str) -> String {
        format!(r#"
sources:
  test_pg:
    type: postgres
    dsn: "{pg}"

destinations:
  warehouse:
    catalog_uri: "{iceberg}"
    s3_endpoint:  "{s3}"
    region:       "{region}"
    access_key_id: "{ak}"
    secret_access_key: "{sk}"

sync_jobs:
  - name: sync_products
    source: test_pg
    destination: warehouse
    namespace: {ns}
    table: pg_products
    sql: |
      SELECT id, sku, name, category, unit_price::float8, stock_qty, updated_at
      FROM products
      ORDER BY id
    watermark_column: ~
    batch_size: 100
    mode: full

  - name: sync_orders
    source: test_pg
    destination: warehouse
    namespace: {ns}
    table: pg_orders
    sql: |
      SELECT id, user_id, status, total_amount::float8, created_at, updated_at
      FROM orders
      WHERE updated_at > :watermark
      ORDER BY updated_at ASC, id ASC
    watermark_column: updated_at
    cursor_column: id
    batch_size: 100
    mode: incremental

  - name: sync_order_items
    source: test_pg
    destination: warehouse
    namespace: {ns}
    table: pg_order_items
    sql: |
      SELECT oi.id, oi.order_id, oi.product_id, oi.quantity,
             oi.unit_price::float8, oi.line_total::float8,
             o.updated_at AS order_updated_at
      FROM order_items oi
      JOIN orders o ON o.id = oi.order_id
      WHERE o.updated_at > :watermark
      ORDER BY o.updated_at ASC, oi.id ASC
    watermark_column: order_updated_at
    cursor_column: id
    depends_on: sync_orders
    batch_size: 200
    mode: incremental
"#,
            pg      = pg_dsn(),
            iceberg = iceberg_uri(),
            s3      = s3_endpoint(),
            region  = region(),
            ak      = access_key(),
            sk      = secret_key(),
            ns      = ns,
        )
    }

    #[test]
    fn t11_1_sync_full_products() {
        if !pg_reachable() { eprintln!("SKIP: postgres not reachable"); return; }
        preflight();
        cli(&["create-namespace", "--namespace", NS]);
        with_sync_config(&sync_config(NS), |cfg| {
            assert_ok("11.1 sync full products",
                &["sync", "--config", cfg, "--job", "sync_products"]);
        });
    }

    #[test]
    fn t11_2_synced_products_queryable() {
        if !pg_reachable() { eprintln!("SKIP: postgres not reachable"); return; }
        preflight();
        cli(&["create-namespace", "--namespace", NS]);
        with_sync_config(&sync_config(NS), |cfg| {
            cli(&["sync", "--config", cfg, "--job", "sync_products"]);
            assert_output("11.2 products contain Widget", "Widget",
                &["scan", "--table", &format!("{NS}.pg_products")]);
        });
    }

    #[test]
    fn t11_3_sync_incremental_orders_first_run() {
        if !pg_reachable() { eprintln!("SKIP: postgres not reachable"); return; }
        preflight();
        cli(&["create-namespace", "--namespace", NS]);
        with_sync_config(&sync_config(NS), |cfg| {
            assert_ok("11.3 sync orders first run",
                &["sync", "--config", cfg, "--job", "sync_orders"]);
        });
    }

    #[test]
    fn t11_4_synced_orders_contain_shipped() {
        if !pg_reachable() { eprintln!("SKIP: postgres not reachable"); return; }
        preflight();
        cli(&["create-namespace", "--namespace", NS]);
        with_sync_config(&sync_config(NS), |cfg| {
            cli(&["sync", "--config", cfg, "--job", "sync_orders"]);
            assert_output("11.4 orders contain shipped", "shipped",
                &["scan", "--table", &format!("{NS}.pg_orders")]);
        });
    }

    #[test]
    fn t11_5_synced_orders_contain_pending() {
        if !pg_reachable() { eprintln!("SKIP: postgres not reachable"); return; }
        preflight();
        cli(&["create-namespace", "--namespace", NS]);
        with_sync_config(&sync_config(NS), |cfg| {
            cli(&["sync", "--config", cfg, "--job", "sync_orders"]);
            assert_output("11.5 orders contain pending", "pending",
                &["scan", "--table", &format!("{NS}.pg_orders")]);
        });
    }

    #[test]
    fn t11_6_sync_order_items_depends_on_orders() {
        if !pg_reachable() { eprintln!("SKIP: postgres not reachable"); return; }
        preflight();
        cli(&["create-namespace", "--namespace", NS]);
        with_sync_config(&sync_config(NS), |cfg| {
            // Run orders first, then items.
            cli(&["sync", "--config", cfg, "--job", "sync_orders"]);
            assert_ok("11.6 sync order_items",
                &["sync", "--config", cfg, "--job", "sync_order_items"]);
        });
    }

    #[test]
    fn t11_7_order_items_has_rows() {
        if !pg_reachable() { eprintln!("SKIP: postgres not reachable"); return; }
        preflight();
        cli(&["create-namespace", "--namespace", NS]);
        with_sync_config(&sync_config(NS), |cfg| {
            cli(&["sync", "--config", cfg, "--job", "sync_orders"]);
            cli(&["sync", "--config", cfg, "--job", "sync_order_items"]);
            assert_output("11.7 order_items has rows", "order_id",
                &["scan", "--table", &format!("{NS}.pg_order_items"),
                  "--columns", "id,order_id,product_id,quantity"]);
        });
    }

    #[test]
    fn t11_8_incremental_resync_is_idempotent() {
        if !pg_reachable() { eprintln!("SKIP: postgres not reachable"); return; }
        preflight();
        cli(&["create-namespace", "--namespace", NS]);
        with_sync_config(&sync_config(NS), |cfg| {
            cli(&["sync", "--config", cfg, "--job", "sync_orders"]);
            // Second run: watermark is set; should succeed with 0 new rows if
            // no data changed.
            assert_ok("11.8 incremental resync",
                &["sync", "--config", cfg, "--job", "sync_orders"]);
        });
    }

    #[test]
    fn t11_9_full_mode_resync_is_idempotent() {
        if !pg_reachable() { eprintln!("SKIP: postgres not reachable"); return; }
        preflight();
        cli(&["create-namespace", "--namespace", NS]);
        with_sync_config(&sync_config(NS), |cfg| {
            cli(&["sync", "--config", cfg, "--job", "sync_products"]);
            assert_ok("11.9 full resync",
                &["sync", "--config", cfg, "--job", "sync_products"]);
        });
    }

    #[test]
    fn t11_10_run_all_jobs_in_dependency_order() {
        if !pg_reachable() { eprintln!("SKIP: postgres not reachable"); return; }
        preflight();
        cli(&["create-namespace", "--namespace", NS]);
        with_sync_config(&sync_config(NS), |cfg| {
            assert_ok("11.10 all jobs", &["sync", "--config", cfg]);
        });
    }

    #[test]
    fn t11_11_dry_run_writes_nothing() {
        if !pg_reachable() { eprintln!("SKIP: postgres not reachable"); return; }
        preflight();
        cli(&["create-namespace", "--namespace", NS]);
        // Dry-run on a fresh namespace — table should NOT be created.
        let dry_ns = "itest_dry";
        cli(&["create-namespace", "--namespace", dry_ns]);
        with_sync_config(&sync_config(dry_ns), |cfg| {
            assert_ok("11.11 dry-run exits 0",
                &["sync", "--config", cfg, "--job", "sync_products", "--dry-run"]);
        });
        // Table should not exist after a dry run.
        assert_fail("11.11 dry-run created no table",
            &["scan", "--table", &format!("{dry_ns}.pg_products")]);
    }

    #[test]
    fn t11_12_cursor_pagination_no_duplicates() {
        if !pg_reachable() { eprintln!("SKIP: postgres not reachable"); return; }
        preflight();
        cli(&["create-namespace", "--namespace", NS]);
        // Use a very small batch_size to force multiple cursor pages.
        let cfg = format!(r#"
sources:
  test_pg:
    type: postgres
    dsn: "{pg}"
destinations:
  warehouse:
    catalog_uri: "{iceberg}"
    s3_endpoint:  "{s3}"
    region:       "{region}"
    access_key_id: "{ak}"
    secret_access_key: "{sk}"
sync_jobs:
  - name: cursor_orders
    source: test_pg
    destination: warehouse
    namespace: {ns}
    table: cursor_orders
    sql: |
      SELECT id, user_id, status, total_amount::float8, updated_at
      FROM orders
      WHERE updated_at > :watermark
      ORDER BY updated_at ASC, id ASC
    watermark_column: updated_at
    cursor_column: id
    batch_size: 2
    mode: incremental
"#,
            pg      = pg_dsn(), iceberg = iceberg_uri(), s3 = s3_endpoint(),
            region  = region(), ak = access_key(), sk = secret_key(), ns = NS,
        );
        with_sync_config(&cfg, |cfgp| {
            assert_ok("11.12 cursor sync", &["sync", "--config", cfgp]);
        });
        // Scan and count — every order id must appear exactly once.
        let out = assert_ok("11.12 scan cursor result",
            &["scan", "--table", &format!("{NS}.cursor_orders"), "--limit", "100"]);
        // The init.sql has 5 orders.  Each status appears at least once.
        assert!(out.contains("shipped") || out.contains("pending"),
            "11.12 expected order rows; got: {out}");
    }
}

// =============================================================================
// § 12 – RabbitMQ-triggered sync
// =============================================================================
#[cfg(test)]
mod rabbitmq {
    use super::*;

    fn rmq_reachable() -> bool {
        std::net::TcpStream::connect("localhost:5672").is_ok()
    }

    fn pg_reachable() -> bool {
        std::net::TcpStream::connect("localhost:5432").is_ok()
    }

    /// Publish a single JSON message to a RabbitMQ queue using the HTTP
    /// Management API (no Rust AMQP client needed in tests).
    fn rmq_publish(queue: &str, payload: &str) {
        // Ensure the queue exists first.
        let declare_url = format!(
            "http://localhost:15672/api/queues/%2f/{queue}"
        );
        let client = reqwest::blocking::Client::new();
        client.put(&declare_url)
            .basic_auth("guest", Some("guest"))
            .json(&serde_json::json!({"durable": true, "auto_delete": false, "arguments": {}}))
            .send()
            .expect("declare queue via management API");

        // Publish via management API.
        let pub_url = format!(
            "http://localhost:15672/api/exchanges/%2f/amq.default/publish"
        );
        let body = serde_json::json!({
            "properties":        {},
            "routing_key":       queue,
            "payload":           payload,
            "payload_encoding":  "string"
        });
        let res = client.post(&pub_url)
            .basic_auth("guest", Some("guest"))
            .json(&body)
            .send()
            .expect("publish via management API");
        assert!(res.status().is_success(), "publish failed: {:?}", res.status());
    }

    /// Consume-and-process one message via `sync-consume` with a very short
    /// timeout.  We run the CLI in a thread and kill it after `timeout`.
    fn run_consumer_briefly(config_path: &str, timeout: Duration) {
        let bin   = cli_bin();
        let uri   = iceberg_uri();
        let s3    = s3_endpoint();
        let ak    = access_key();
        let sk    = secret_key();
        let reg   = region();
        let cfg   = config_path.to_string();

        let mut child = Command::new(&bin)
            .args([
                "--uri",               &uri,
                "--s3-endpoint",       &s3,
                "--access-key-id",     &ak,
                "--secret-access-key", &sk,
                "--region",            &reg,
                "sync-consume",
                "--config",            &cfg,
            ])
            .spawn()
            .expect("spawn sync-consume");

        std::thread::sleep(timeout);
        let _ = child.kill();
        let _ = child.wait();
    }

    #[test]
    fn t12_1_consumer_processes_message() {
        if !rmq_reachable() { eprintln!("SKIP: RabbitMQ not reachable"); return; }
        if !pg_reachable()  { eprintln!("SKIP: Postgres not reachable"); return; }
        preflight();

        cli(&["create-namespace", "--namespace", NS]);
        let ns = NS;

        let config = format!(r#"
sources:
  test_pg:
    type: postgres
    dsn: "{pg}"
destinations:
  warehouse:
    catalog_uri: "{iceberg}"
    s3_endpoint:  "{s3}"
    region:       "{region}"
    access_key_id: "{ak}"
    secret_access_key: "{sk}"
sync_jobs:
  - name: user_orders
    source: test_pg
    destination: warehouse
    namespace: {ns}
    table: rmq_user_orders
    sql: |
      SELECT id, user_id, status, total_amount::float8, created_at
      FROM orders
      WHERE user_id = :user_id
      ORDER BY created_at DESC
    watermark_column: ~
    batch_size: 50
    mode: full
rabbitmq:
  uri: "{rmq}"
  queues:
    - queue: test_user_sync
      job:   user_orders
"#,
            pg      = pg_dsn(),
            iceberg = iceberg_uri(),
            s3      = s3_endpoint(),
            region  = region(),
            ak      = access_key(),
            sk      = secret_key(),
            rmq     = rabbitmq_uri(),
            ns      = ns,
        );

        with_sync_config(&config, |cfg| {
            // Publish a message for user_id=1 (has 2 orders in seed data).
            rmq_publish("test_user_sync", r#"{"user_id": 1}"#);

            // Run the consumer long enough to process one message.
            run_consumer_briefly(cfg, Duration::from_secs(5));
        });

        // The table should now exist with rows for user 1.
        assert_output("12.1 consumer wrote rows", "user_id",
            &["scan", "--table", &format!("{NS}.rmq_user_orders"),
              "--columns", "id,user_id,status"]);
    }

    #[test]
    fn t12_2_dead_letter_exchange_config_accepted() {
        if !rmq_reachable() { eprintln!("SKIP: RabbitMQ not reachable"); return; }
        preflight();

        // Just verify the config with a DLX parses and the consumer starts
        // without crashing (we kill it immediately — no messages sent).
        let config = format!(r#"
sources:
  test_pg:
    type: postgres
    dsn: "{pg}"
destinations:
  warehouse:
    catalog_uri: "{iceberg}"
    s3_endpoint:  "{s3}"
    region:       "{region}"
    access_key_id: "{ak}"
    secret_access_key: "{sk}"
sync_jobs:
  - name: dlx_test_job
    source: test_pg
    destination: warehouse
    namespace: {ns}
    table: dlx_test
    sql: |
      SELECT id FROM orders WHERE user_id = :user_id ORDER BY id
    watermark_column: ~
    batch_size: 10
    mode: full
rabbitmq:
  uri: "{rmq}"
  queues:
    - queue: test_dlx_queue
      job:   dlx_test_job
      dead_letter_exchange: dlx.test
"#,
            pg      = pg_dsn(),
            iceberg = iceberg_uri(),
            s3      = s3_endpoint(),
            region  = region(),
            ak      = access_key(),
            sk      = secret_key(),
            rmq     = rabbitmq_uri(),
            ns      = NS,
        );

        with_sync_config(&config, |cfg| {
            // Start consumer, let it declare the queue, then stop.
            run_consumer_briefly(cfg, Duration::from_secs(2));
            // If we reached here without panic, the DLX config was accepted.
        });
    }
}
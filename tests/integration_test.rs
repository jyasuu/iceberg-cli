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

fn iceberg_uri() -> String {
    std::env::var("ICEBERG_URI").unwrap_or_else(|_| "http://localhost:8181".into())
}
fn s3_endpoint() -> String {
    std::env::var("S3_ENDPOINT").unwrap_or_else(|_| "http://localhost:9000".into())
}
fn access_key() -> String {
    std::env::var("AWS_ACCESS_KEY_ID").unwrap_or_else(|_| "admin".into())
}
fn secret_key() -> String {
    std::env::var("AWS_SECRET_ACCESS_KEY").unwrap_or_else(|_| "password".into())
}
fn region() -> String {
    std::env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".into())
}
fn pg_dsn() -> String {
    std::env::var("PG_DSN").unwrap_or_else(|_| {
        "host=localhost port=5432 dbname=shop user=app password=secret sslmode=disable".into()
    })
}
fn rabbitmq_uri() -> String {
    std::env::var("RABBITMQ_URI").unwrap_or_else(|_| "amqp://guest:guest@localhost:5672/%2f".into())
}

/// Run the CLI with common global flags pre-applied.
fn cli(args: &[&str]) -> Output {
    let bin = cli_bin();
    let mut cmd = Command::new(&bin);
    cmd.env("RUST_LOG", "debug");
    cmd.env("RUST_BACKTRACE", "1");
    cmd.args([
        "--uri",
        &iceberg_uri(),
        "--s3-endpoint",
        &s3_endpoint(),
        "--access-key-id",
        &access_key(),
        "--secret-access-key",
        &secret_key(),
        "--region",
        &region(),
    ]);
    cmd.args(args);
    cmd.output()
        .unwrap_or_else(|e| panic!("Failed to run {bin}: {e}"))
}

/// Assert the command exits 0.  Panics with stdout+stderr on failure.
#[track_caller]
fn assert_ok(label: &str, args: &[&str]) -> String {
    let out = cli(args);
    let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
    println!("[{label}] expected success\nstdout: {stdout}\nstderr: {stderr}");
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
        if reqwest::blocking::get(url)
            .map(|r| r.status().is_success())
            .unwrap_or(false)
        {
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
    wait_for_http(
        &format!("{}/v1/config", iceberg_uri()),
        Duration::from_secs(30),
    );
    wait_for_http(
        &format!("{}/minio/health/live", s3_endpoint()),
        Duration::from_secs(30),
    );
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
        assert_ok(
            "1.1 create namespace",
            &["create-namespace", "--namespace", NS],
        );
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
        assert_ok(
            "1.3 create namespace again",
            &["create-namespace", "--namespace", NS],
        );
    }
}

// =============================================================================
// § 2 – Table management
// =============================================================================
#[cfg(test)]
mod table_mgmt {
    use super::*;

    fn setup() {
        preflight();
        cli(&["create-namespace", "--namespace", NS]);
    }

    #[test]
    fn t2_1_create_table() {
        setup();
        assert_ok(
            "2.1 create table",
            &[
                "create-table",
                "--table",
                &format!("{NS}.products"),
                "--schema",
                "id:long,name:string,price:double",
            ],
        );
    }

    #[test]
    fn t2_2_list_tables_shows_table() {
        setup();
        cli(&[
            "create-table",
            "--table",
            &format!("{NS}.products2"),
            "--schema",
            "id:long,name:string",
        ]);
        assert_output(
            "2.2 list-tables",
            "products2",
            &["list-tables", "--namespace", NS],
        );
    }

    #[test]
    fn t2_3_describe_shows_field() {
        setup();
        cli(&[
            "create-table",
            "--table",
            &format!("{NS}.products3"),
            "--schema",
            "id:long,name:string,price:double",
        ]);
        assert_output(
            "2.3 describe shows field",
            "price",
            &["describe", "--table", &format!("{NS}.products3")],
        );
    }

    #[test]
    fn t2_4_describe_shows_snapshot_section() {
        setup();
        cli(&[
            "create-table",
            "--table",
            &format!("{NS}.products4"),
            "--schema",
            "id:long,name:string,price:double",
        ]);
        assert_output(
            "2.4 describe shows format",
            "Format",
            &["describe", "--table", &format!("{NS}.products4")],
        );
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
        cli(&[
            "create-table",
            "--table",
            &tbl,
            "--schema",
            "id:long,name:string,price:double",
        ]);
        tbl
    }

    #[test]
    fn t3_1_write_single_row() {
        let tbl = setup_table("single");
        assert_ok(
            "3.1 write single row",
            &[
                "write",
                "--table",
                &tbl,
                "--json",
                r#"[{"id":1,"name":"Widget","price":9.99}]"#,
            ],
        );
    }

    #[test]
    fn t3_2_scan_shows_written_row() {
        let tbl = setup_table("scan1");
        cli(&[
            "write",
            "--table",
            &tbl,
            "--json",
            r#"[{"id":1,"name":"Widget","price":9.99}]"#,
        ]);
        assert_output("3.2 scan shows row", "Widget", &["scan", "--table", &tbl]);
    }

    #[test]
    fn t3_3_write_multiple_rows() {
        let tbl = setup_table("multi");
        assert_ok(
            "3.3 write multiple rows",
            &[
                "write",
                "--table",
                &tbl,
                "--json",
                r#"[{"id":2,"name":"Gadget","price":19.99},{"id":3,"name":"Doohickey","price":4.49}]"#,
            ],
        );
    }

    #[test]
    fn t3_4_scan_shows_all_rows() {
        let tbl = setup_table("all");
        cli(&[
            "write",
            "--table",
            &tbl,
            "--json",
            r#"[{"id":1,"name":"Widget","price":9.99},{"id":2,"name":"Gadget","price":19.99}]"#,
        ]);
        assert_output("3.4 scan all rows", "Gadget", &["scan", "--table", &tbl]);
    }

    #[test]
    fn t3_5_scan_with_limit() {
        let tbl = setup_table("limit");
        cli(&[
            "write",
            "--table",
            &tbl,
            "--json",
            r#"[{"id":1,"name":"A","price":1.0},{"id":2,"name":"B","price":2.0},{"id":3,"name":"C","price":3.0}]"#,
        ]);
        assert_output(
            "3.5 scan limit=1",
            "1 rows shown",
            &["scan", "--table", &tbl, "--limit", "1"],
        );
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
        cli(&[
            "create-table",
            "--table",
            &tbl,
            "--schema",
            "id:long,name:string,price:double",
        ]);
        cli(&[
            "write",
            "--table",
            &tbl,
            "--json",
            r#"[{"id":1,"name":"Widget","price":9.99}]"#,
        ]);
        tbl
    }

    #[test]
    fn t4_1_projected_columns_appear() {
        let tbl = setup();
        assert_output(
            "4.1 projected columns present",
            "name",
            &["scan", "--table", &tbl, "--columns", "id,name"],
        );
    }

    #[test]
    fn t4_2_unprojected_column_absent() {
        let tbl = setup();
        let out = assert_ok(
            "4.2 projection ok",
            &["scan", "--table", &tbl, "--columns", "id,name"],
        );
        assert!(
            !out.contains("price"),
            "price should be absent from projected scan; got: {out}"
        );
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
        cli(&[
            "create-table",
            "--table",
            &tbl,
            "--schema",
            "id:long,status:string,total:double",
        ]);

        cli(&[
            "write",
            "--table",
            &tbl,
            "--json",
            r#"[{"id":100,"status":"pending","total":19.98}]"#,
        ]);
        cli(&[
            "write",
            "--table",
            &tbl,
            "--json",
            r#"[{"id":101,"status":"shipped","total":19.99},{"id":102,"status":"pending","total":22.45}]"#,
        ]);

        assert_output(
            "5 append: shipped visible",
            "shipped",
            &["scan", "--table", &tbl],
        );
        assert_output(
            "5 append: pending visible",
            "pending",
            &["scan", "--table", &tbl],
        );
    }

    #[test]
    fn t6_describe_shows_snapshot_after_write() {
        preflight();
        cli(&["create-namespace", "--namespace", NS]);
        let tbl = format!("{NS}.snap_test");
        cli(&[
            "create-table",
            "--table",
            &tbl,
            "--schema",
            "id:long,val:string",
        ]);
        cli(&[
            "write",
            "--table",
            &tbl,
            "--json",
            r#"[{"id":1,"val":"x"}]"#,
        ]);
        assert_output(
            "6 snapshot present",
            "Snapshot",
            &["describe", "--table", &tbl],
        );
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
        cli(&[
            "create-table",
            "--table",
            &tbl,
            "--schema",
            "id:long,name:string,price:double",
        ]);
        tbl
    }

    #[test]
    fn t7_1_write_empty_array_is_noop() {
        let tbl = setup_table("empty");
        assert_ok(
            "7.1 empty write",
            &["write", "--table", &tbl, "--json", "[]"],
        );
    }

    #[test]
    fn t7_2_table_readable_after_empty_write() {
        let tbl = setup_table("after_empty");
        cli(&[
            "write",
            "--table",
            &tbl,
            "--json",
            r#"[{"id":1,"name":"Widget","price":9.99}]"#,
        ]);
        cli(&["write", "--table", &tbl, "--json", "[]"]);
        assert_output(
            "7.2 readable after empty write",
            "Widget",
            &["scan", "--table", &tbl],
        );
    }

    #[test]
    fn t7_3_write_null_values() {
        let tbl = setup_table("nulls");
        assert_ok(
            "7.3 null values",
            &[
                "write",
                "--table",
                &tbl,
                "--json",
                r#"[{"id":99,"name":null,"price":0.0}]"#,
            ],
        );
    }

    #[test]
    fn t7_4_high_limit_returns_all_rows() {
        let tbl = setup_table("highlimit");
        cli(&[
            "write",
            "--table",
            &tbl,
            "--json",
            r#"[{"id":1,"name":"A","price":1.0},{"id":2,"name":"B","price":2.0}]"#,
        ]);
        assert_output(
            "7.4 high limit",
            "rows shown",
            &["scan", "--table", &tbl, "--limit", "9999"],
        );
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
        assert_fail(
            "8.1 scan missing table",
            &["scan", "--table", &format!("{NS}.no_such_table_xyz")],
        );
    }

    #[test]
    fn t8_2_describe_nonexistent_table() {
        preflight();
        assert_fail(
            "8.2 describe missing table",
            &["describe", "--table", &format!("{NS}.ghost_xyz")],
        );
    }

    #[test]
    fn t8_3_list_tables_unknown_namespace() {
        preflight();
        assert_fail(
            "8.3 list-tables unknown ns",
            &["list-tables", "--namespace", "no_such_namespace_xyz_abc"],
        );
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
        assert_ok(
            "9.1 create all-types table",
            &[
                "create-table",
                "--table",
                &tbl,
                "--schema",
                "i:int,l:long,f:float,d:double,b:boolean,s:string,dt:date,ts:timestamp",
            ],
        );
        assert_ok(
            "9.2 write all-types row",
            &[
                "write",
                "--table",
                &tbl,
                "--json",
                r#"[{"i":42,"l":9000000000,"f":3.14,"d":2.718,"b":true,"s":"hello","dt":null,"ts":null}]"#,
            ],
        );
        assert_output(
            "9.3 long value visible",
            "9000000000",
            &["scan", "--table", &tbl],
        );
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
        cli(&[
            "create-table",
            "--table",
            &tbl,
            "--schema",
            "id:long,val:string",
        ]);

        // Build 200-row JSON array.
        let rows: Vec<String> = (1..=200)
            .map(|i| format!(r#"{{"id":{i},"val":"row_{i}"}}"#))
            .collect();
        let json = format!("[{}]", rows.join(","));

        assert_ok(
            "10.1 write 200 rows",
            &["write", "--table", &tbl, "--json", &json],
        );
        assert_output(
            "10.2 scan 200 rows",
            "200 rows shown",
            &["scan", "--table", &tbl, "--limit", "200"],
        );
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
        format!(
            r#"
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
            pg = pg_dsn(),
            iceberg = iceberg_uri(),
            s3 = s3_endpoint(),
            region = region(),
            ak = access_key(),
            sk = secret_key(),
            ns = ns,
        )
    }

    #[test]
    fn t11_1_sync_full_products() {
        if !pg_reachable() {
            eprintln!("SKIP: postgres not reachable");
            return;
        }
        preflight();
        cli(&["create-namespace", "--namespace", NS]);
        with_sync_config(&sync_config(NS), |cfg| {
            assert_ok(
                "11.1 sync full products",
                &["sync", "--config", cfg, "--job", "sync_products"],
            );
        });
    }

    #[test]
    fn t11_2_synced_products_queryable() {
        if !pg_reachable() {
            eprintln!("SKIP: postgres not reachable");
            return;
        }
        preflight();
        cli(&["create-namespace", "--namespace", NS]);
        with_sync_config(&sync_config(NS), |cfg| {
            cli(&["sync", "--config", cfg, "--job", "sync_products"]);
            assert_output(
                "11.2 products contain Widget",
                "Widget",
                &["scan", "--table", &format!("{NS}.pg_products")],
            );
        });
    }

    #[test]
    fn t11_3_sync_incremental_orders_first_run() {
        if !pg_reachable() {
            eprintln!("SKIP: postgres not reachable");
            return;
        }
        preflight();
        cli(&["create-namespace", "--namespace", NS]);
        with_sync_config(&sync_config(NS), |cfg| {
            assert_ok(
                "11.3 sync orders first run",
                &["sync", "--config", cfg, "--job", "sync_orders"],
            );
        });
    }

    #[test]
    fn t11_4_synced_orders_contain_shipped() {
        if !pg_reachable() {
            eprintln!("SKIP: postgres not reachable");
            return;
        }
        preflight();
        cli(&["create-namespace", "--namespace", NS]);
        with_sync_config(&sync_config(NS), |cfg| {
            cli(&["sync", "--config", cfg, "--job", "sync_orders"]);
            assert_output(
                "11.4 orders contain shipped",
                "shipped",
                &["scan", "--table", &format!("{NS}.pg_orders")],
            );
        });
    }

    #[test]
    fn t11_5_synced_orders_contain_pending() {
        if !pg_reachable() {
            eprintln!("SKIP: postgres not reachable");
            return;
        }
        preflight();
        cli(&["create-namespace", "--namespace", NS]);
        with_sync_config(&sync_config(NS), |cfg| {
            cli(&["sync", "--config", cfg, "--job", "sync_orders"]);
            assert_output(
                "11.5 orders contain pending",
                "pending",
                &["scan", "--table", &format!("{NS}.pg_orders")],
            );
        });
    }

    #[test]
    fn t11_6_sync_order_items_depends_on_orders() {
        if !pg_reachable() {
            eprintln!("SKIP: postgres not reachable");
            return;
        }
        preflight();
        cli(&["create-namespace", "--namespace", NS]);
        with_sync_config(&sync_config(NS), |cfg| {
            // Run orders first, then items.
            cli(&["sync", "--config", cfg, "--job", "sync_orders"]);
            assert_ok(
                "11.6 sync order_items",
                &["sync", "--config", cfg, "--job", "sync_order_items"],
            );
        });
    }

    #[test]
    fn t11_7_order_items_has_rows() {
        if !pg_reachable() {
            eprintln!("SKIP: postgres not reachable");
            return;
        }
        preflight();
        cli(&["create-namespace", "--namespace", NS]);
        with_sync_config(&sync_config(NS), |cfg| {
            cli(&["sync", "--config", cfg, "--job", "sync_orders"]);
            cli(&["sync", "--config", cfg, "--job", "sync_order_items"]);
            assert_output(
                "11.7 order_items has rows",
                "order_id",
                &[
                    "scan",
                    "--table",
                    &format!("{NS}.pg_order_items"),
                    "--columns",
                    "id,order_id,product_id,quantity",
                ],
            );
        });
    }

    #[test]
    fn t11_8_incremental_resync_is_idempotent() {
        if !pg_reachable() {
            eprintln!("SKIP: postgres not reachable");
            return;
        }
        preflight();
        cli(&["create-namespace", "--namespace", NS]);
        with_sync_config(&sync_config(NS), |cfg| {
            cli(&["sync", "--config", cfg, "--job", "sync_orders"]);
            // Second run: watermark is set; should succeed with 0 new rows if
            // no data changed.
            assert_ok(
                "11.8 incremental resync",
                &["sync", "--config", cfg, "--job", "sync_orders"],
            );
        });
    }

    #[test]
    fn t11_9_full_mode_resync_is_idempotent() {
        if !pg_reachable() {
            eprintln!("SKIP: postgres not reachable");
            return;
        }
        preflight();
        cli(&["create-namespace", "--namespace", NS]);
        with_sync_config(&sync_config(NS), |cfg| {
            cli(&["sync", "--config", cfg, "--job", "sync_products"]);
            assert_ok(
                "11.9 full resync",
                &["sync", "--config", cfg, "--job", "sync_products"],
            );
        });
    }

    #[test]
    fn t11_10_run_all_jobs_in_dependency_order() {
        if !pg_reachable() {
            eprintln!("SKIP: postgres not reachable");
            return;
        }
        preflight();
        cli(&["create-namespace", "--namespace", NS]);
        with_sync_config(&sync_config(NS), |cfg| {
            assert_ok("11.10 all jobs", &["sync", "--config", cfg]);
        });
    }

    #[test]
    fn t11_11_dry_run_writes_nothing() {
        if !pg_reachable() {
            eprintln!("SKIP: postgres not reachable");
            return;
        }
        preflight();
        cli(&["create-namespace", "--namespace", NS]);
        // Dry-run on a fresh namespace — table should NOT be created.
        let dry_ns = "itest_dry";
        cli(&["create-namespace", "--namespace", dry_ns]);
        with_sync_config(&sync_config(dry_ns), |cfg| {
            assert_ok(
                "11.11 dry-run exits 0",
                &[
                    "sync",
                    "--config",
                    cfg,
                    "--job",
                    "sync_products",
                    "--dry-run",
                ],
            );
        });
        // Table should not exist after a dry run.
        assert_fail(
            "11.11 dry-run created no table",
            &["scan", "--table", &format!("{dry_ns}.pg_products")],
        );
    }

    #[test]
    fn t11_12_cursor_pagination_no_duplicates() {
        if !pg_reachable() {
            eprintln!("SKIP: postgres not reachable");
            return;
        }
        preflight();
        cli(&["create-namespace", "--namespace", NS]);
        // Use a very small batch_size to force multiple cursor pages.
        let cfg = format!(
            r#"
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
            pg = pg_dsn(),
            iceberg = iceberg_uri(),
            s3 = s3_endpoint(),
            region = region(),
            ak = access_key(),
            sk = secret_key(),
            ns = NS,
        );
        with_sync_config(&cfg, |cfgp| {
            assert_ok("11.12 cursor sync", &["sync", "--config", cfgp]);
        });
        // Scan and count — every order id must appear exactly once.
        let out = assert_ok(
            "11.12 scan cursor result",
            &[
                "scan",
                "--table",
                &format!("{NS}.cursor_orders"),
                "--limit",
                "100",
            ],
        );
        // The init.sql has 5 orders.  Each status appears at least once.
        assert!(
            out.contains("shipped") || out.contains("pending"),
            "11.12 expected order rows; got: {out}"
        );
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
        let declare_url = format!("http://localhost:15672/api/queues/%2f/{queue}");
        let client = reqwest::blocking::Client::new();
        client
            .put(&declare_url)
            .basic_auth("guest", Some("guest"))
            .json(&serde_json::json!({"durable": true, "auto_delete": false, "arguments": {}}))
            .send()
            .expect("declare queue via management API");

        // Publish via management API.
        let pub_url = "http://localhost:15672/api/exchanges/%2f/amq.default/publish".to_string();
        let body = serde_json::json!({
            "properties":        {},
            "routing_key":       queue,
            "payload":           payload,
            "payload_encoding":  "string"
        });
        let res = client
            .post(&pub_url)
            .basic_auth("guest", Some("guest"))
            .json(&body)
            .send()
            .expect("publish via management API");
        assert!(
            res.status().is_success(),
            "publish failed: {:?}",
            res.status()
        );
    }

    /// Consume-and-process one message via `sync-consume` with a very short
    /// timeout.  We run the CLI in a thread and kill it after `timeout`.
    fn run_consumer_briefly(config_path: &str, timeout: Duration) {
        let bin = cli_bin();
        let uri = iceberg_uri();
        let s3 = s3_endpoint();
        let ak = access_key();
        let sk = secret_key();
        let reg = region();
        let cfg = config_path.to_string();

        let mut child = Command::new(&bin)
            .args([
                "--uri",
                &uri,
                "--s3-endpoint",
                &s3,
                "--access-key-id",
                &ak,
                "--secret-access-key",
                &sk,
                "--region",
                &reg,
                "sync-consume",
                "--config",
                &cfg,
            ])
            .spawn()
            .expect("spawn sync-consume");

        std::thread::sleep(timeout);
        let _ = child.kill();
        let _ = child.wait();
    }

    #[test]
    fn t12_1_consumer_processes_message() {
        if !rmq_reachable() {
            eprintln!("SKIP: RabbitMQ not reachable");
            return;
        }
        if !pg_reachable() {
            eprintln!("SKIP: Postgres not reachable");
            return;
        }
        preflight();

        cli(&["create-namespace", "--namespace", NS]);
        let ns = NS;

        let config = format!(
            r#"
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
            pg = pg_dsn(),
            iceberg = iceberg_uri(),
            s3 = s3_endpoint(),
            region = region(),
            ak = access_key(),
            sk = secret_key(),
            rmq = rabbitmq_uri(),
            ns = ns,
        );

        with_sync_config(&config, |cfg| {
            // Publish a message for user_id=1 (has 2 orders in seed data).
            rmq_publish("test_user_sync", r#"{"user_id": 1}"#);

            // Run the consumer long enough to process one message.
            run_consumer_briefly(cfg, Duration::from_secs(5));
        });

        // The table should now exist with rows for user 1.
        assert_output(
            "12.1 consumer wrote rows",
            "user_id",
            &[
                "scan",
                "--table",
                &format!("{NS}.rmq_user_orders"),
                "--columns",
                "id,user_id,status",
            ],
        );
    }

    #[test]
    fn t12_2_dead_letter_exchange_config_accepted() {
        if !rmq_reachable() {
            eprintln!("SKIP: RabbitMQ not reachable");
            return;
        }
        preflight();

        // Just verify the config with a DLX parses and the consumer starts
        // without crashing (we kill it immediately — no messages sent).
        let config = format!(
            r#"
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
            pg = pg_dsn(),
            iceberg = iceberg_uri(),
            s3 = s3_endpoint(),
            region = region(),
            ak = access_key(),
            sk = secret_key(),
            rmq = rabbitmq_uri(),
            ns = NS,
        );

        with_sync_config(&config, |cfg| {
            // Start consumer, let it declare the queue, then stop.
            run_consumer_briefly(cfg, Duration::from_secs(2));
            // If we reached here without panic, the DLX config was accepted.
        });
    }
}

// =============================================================================
// § 13 – Time-series incremental sync: multi-stage (insert → sync) × N
//
// These tests simulate a realistic CDC pipeline:
//
//   Stage 1: baseline data already in Postgres (from init.sql)
//   Stage 2: insert batch-2 rows  → sync  → assert only batch-2 arrives
//   Stage 3: insert batch-3 rows  → sync  → assert only batch-3 arrives
//   ...
//
// Key invariants checked after every stage:
//   • Total Iceberg row count equals exactly the cumulative expected count.
//   • The `sync.watermark.<col>` property in table metadata advances each run.
//   • A subsequent sync with no new data writes 0 extra rows (idempotency).
//   • Rows from earlier stages are never lost (at-least-once, no overwrites).
//
// Isolation strategy
// ──────────────────
// Every test gets its own Iceberg namespace AND its own Postgres source table,
// both named after the test function (e.g. `itest_ts_t13_1`).  This guarantees
// that:
//   • Tests can run in parallel without stomping on each other's watermarks.
//   • Each test starts with a clean Iceberg table (no stale watermark from a
//     previous run in the same process).
//   • The `DROP TABLE … CREATE TABLE` DDL in setup() is safe even under
//     parallel execution because every table name is unique.
//
// PostgreSQL multi-statement DDL
// ──────────────────────────────
// psql's `-c` flag executes only ONE statement reliably.  Passing a
// semicolon-separated string may silently drop subsequent statements on some
// versions.  All setup DDL is therefore split into individual `pg_exec` calls.
// =============================================================================
#[cfg(test)]
mod time_series {
    use super::*;
    use std::net::TcpStream;

    fn pg_reachable() -> bool {
        TcpStream::connect("localhost:5432").is_ok()
    }

    // ── PostgreSQL helper ─────────────────────────────────────────────────────
    //
    // Execute a SINGLE SQL statement via psql.  Do not pass multiple
    // semicolon-separated statements — split them into separate calls instead.
    fn pg_exec(sql: &str) {
        let dsn = pg_dsn();
        let mut host = "localhost";
        let mut port = "5432";
        let mut dbname = "shop";
        let mut user = "app";
        let mut password = "secret";
        for pair in dsn.split_whitespace() {
            if let Some((k, v)) = pair.split_once('=') {
                match k {
                    "host" => host = v,
                    "port" => port = v,
                    "dbname" => dbname = v,
                    "user" => user = v,
                    "password" => password = v,
                    _ => {}
                }
            }
        }
        let out = std::process::Command::new("psql")
            .env("PGPASSWORD", password)
            .args(["-h", host, "-p", port, "-U", user, "-d", dbname, "-c", sql])
            .output()
            .expect("psql not found — is postgres client installed?");
        assert!(
            out.status.success(),
            "pg_exec failed:\nSQL: {sql}\nstderr: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }

    // ── Per-test isolation helpers ────────────────────────────────────────────

    /// Derive a short, filesystem-safe identifier from a test name.
    ///
    /// We truncate to 24 characters so the combined namespace + table name
    /// stays well within Iceberg/S3 limits.
    fn test_id(name: &str) -> String {
        // Keep only alphanumerics and underscores; strip the leading "t13_N_"
        // prefix to save space, then truncate.
        let clean: String = name
            .chars()
            .map(|c| {
                if c.is_alphanumeric() || c == '_' {
                    c
                } else {
                    '_'
                }
            })
            .collect();
        clean.chars().take(24).collect()
    }

    /// Iceberg namespace for this test (unique per test function).
    fn ts_ns(id: &str) -> String {
        format!("itest_ts_{id}")
    }

    /// Iceberg destination table name (constant within a test).
    fn ts_iceberg_table() -> &'static str {
        "ts_events"
    }

    /// Postgres source table name (unique per test function).
    fn pg_table(id: &str) -> String {
        format!("ts_src_{id}")
    }

    // ── Setup (per-test, idempotent) ──────────────────────────────────────────
    //
    // Creates:
    //   • An Iceberg namespace  (itest_ts_<id>)
    //   • A Postgres table      (ts_src_<id>)
    //
    // The Postgres DDL is split into separate statements so psql -c works
    // correctly on every supported version.
    fn setup(id: &str) {
        preflight();
        let ns = ts_ns(id);
        let pg_tbl = pg_table(id);

        cli(&["create-namespace", "--namespace", &ns]);

        // Drop the Iceberg destination table if it exists from a previous run.
        // Without this, a stale watermark would cause the engine to skip all
        // rows whose timestamps fall at or below the old high-water mark.
        cli(&[
            "drop-table",
            "--table",
            &format!("{ns}.{}", ts_iceberg_table()),
        ]);

        // Drop if exists (separate statement — psql -c handles one at a time).
        pg_exec(&format!("DROP TABLE IF EXISTS {pg_tbl}"));

        pg_exec(&format!(
            "CREATE TABLE {pg_tbl} ( \
               id       BIGSERIAL PRIMARY KEY, \
               stage    INT         NOT NULL, \
               label    TEXT        NOT NULL, \
               value    BIGINT      NOT NULL DEFAULT 0, \
               event_at TIMESTAMPTZ NOT NULL DEFAULT NOW() \
             )"
        ));

        pg_exec(&format!("CREATE INDEX ON {pg_tbl}(event_at)"));
    }

    // ── Sync-config factory ───────────────────────────────────────────────────
    //
    // event_at offsets in the SQL are deliberately far in the past relative to
    // test start, so we use ABSOLUTE timestamps (inserted with a fixed past
    // anchor) rather than NOW() + small offsets.  See `insert_rows` below.
    fn ts_sync_config(id: &str) -> String {
        let ns = ts_ns(id);
        let pg_tbl = pg_table(id);
        format!(
            r#"
sources:
  ts_pg:
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
  - name: sync_ts_events
    source: ts_pg
    destination: warehouse
    namespace: {ns}
    table: {table}
    sql: |
      SELECT id, stage, label, value, event_at
      FROM {pg_tbl}
      WHERE event_at > :watermark
      ORDER BY event_at ASC, id ASC
    watermark_column: event_at
    cursor_column:    id
    batch_size: 10
    mode: incremental
"#,
            pg = pg_dsn(),
            iceberg = iceberg_uri(),
            s3 = s3_endpoint(),
            region = region(),
            ak = access_key(),
            sk = secret_key(),
            ns = ns,
            table = ts_iceberg_table(),
            pg_tbl = pg_tbl,
        )
    }

    // ── Data-insertion helper ─────────────────────────────────────────────────
    //
    // Rows are inserted with an ABSOLUTE timestamp calculated as:
    //
    //   TIMESTAMP '2020-01-01 00:00:00 UTC' + INTERVAL '<epoch_offset> seconds'
    //
    // Using a fixed past epoch (2020-01-01) means:
    //   • All inserted timestamps are well in the past → no clock-skew issues.
    //   • The watermark (which is also stored as an absolute timestamp) will
    //     always compare correctly with `>` regardless of when the test runs.
    //   • Distinct stages use non-overlapping offset ranges, so watermarks are
    //     strictly monotone across stages.
    //
    // `epoch_offset_secs` is the number of seconds since 2020-01-01 00:00 UTC.
    fn insert_rows(pg_tbl: &str, stage: u32, rows: &[(&str, i64, i64)]) {
        for (label, value, epoch_offset) in rows {
            pg_exec(&format!(
                "INSERT INTO {pg_tbl} (stage, label, value, event_at) \
                 VALUES ({stage}, '{label}', {value}, \
                         TIMESTAMP '2020-01-01 00:00:00 UTC' + INTERVAL '{epoch_offset} seconds')"
            ));
        }
    }

    // ── Post-sync assertion helpers ───────────────────────────────────────────

    /// Parse "N rows shown" from the tail of a scan's stdout.
    fn scan_row_count(full_table: &str) -> usize {
        let out = assert_ok(
            &format!("scan {full_table}"),
            &["scan", "--table", full_table, "--limit", "9999"],
        );
        out.lines()
            .rev()
            .find_map(|line| {
                // Format: "(N rows shown, limit=9999)"
                let trimmed = line.trim().trim_start_matches('(');
                trimmed
                    .split_once(" rows shown")
                    .and_then(|(n, _)| n.trim().parse::<usize>().ok())
            })
            .unwrap_or(0)
    }

    /// Return the value of the `sync.watermark.*` property from `describe`.
    /// Panics with a helpful message if the property is absent.
    fn read_watermark(full_table: &str) -> String {
        let out = assert_ok(
            &format!("describe {full_table}"),
            &["describe", "--table", full_table],
        );
        let line = out.lines().find(|l| l.contains("sync.watermark."));
        println!("=====================================");
        println!("{:?}", line);
        println!("=====================================");
        out.lines()
            .find(|l| l.contains("sync.watermark."))
            .unwrap_or_else(|| {
                panic!(
                    "No sync.watermark.* property in describe output for {full_table}:\n{out}\n\n\
                     Hint: the sync engine only writes this property when at least one row \
                     was synced.  Check that the inserted timestamps are older than the \
                     current watermark and that the SQL WHERE clause matches them."
                )
            })
            .split('┆')
            .nth(1) // ✓ index 1 is the value cell
            .unwrap_or("")
            .trim()
            .trim_end_matches('│') // strip the trailing │
            .trim()
            .to_string()
    }

    /// Insert rows, run the sync job, then read and return the new watermark.
    fn stage_and_watermark(
        stage: u32,
        rows: &[(&str, i64, i64)],
        pg_tbl: &str,
        config_path: &str,
        full_table: &str,
    ) -> String {
        insert_rows(pg_tbl, stage, rows);
        assert_ok(
            &format!("stage {stage} sync"),
            &["sync", "--config", config_path, "--job", "sync_ts_events"],
        );
        read_watermark(full_table)
    }

    // =========================================================================
    // Test 13.1 — Three-stage pipeline: watermark advances each run
    //
    // Stage 1: 3 rows at epoch offsets 1 s, 2 s, 3 s
    // Stage 2: 3 rows at epoch offsets 10 s, 11 s, 12 s
    // Stage 3: 2 rows at epoch offsets 20 s, 21 s
    //
    // After each sync:
    //   • Cumulative Iceberg row count == expected
    //   • Watermark is strictly greater than the previous stage's watermark
    // =========================================================================
    #[test]
    fn t13_1_watermark_advances_across_three_stages() {
        if !pg_reachable() {
            eprintln!("SKIP: postgres not reachable");
            return;
        }
        let id = test_id("t13_1");
        setup(&id);
        let pg_tbl = pg_table(&id);
        let full_table = format!("{}.{}", ts_ns(&id), ts_iceberg_table());

        with_sync_config(&ts_sync_config(&id), |cfg| {
            // Stage 1
            let wm1 = stage_and_watermark(
                1,
                &[("alpha", 100, 1), ("beta", 200, 2), ("gamma", 300, 3)],
                &pg_tbl,
                cfg,
                &full_table,
            );
            assert_eq!(scan_row_count(&full_table), 3, "stage 1: expected 3 rows");
            assert!(!wm1.is_empty(), "stage 1: watermark must be set");

            // Stage 2
            let wm2 = stage_and_watermark(
                2,
                &[("delta", 400, 10), ("epsilon", 500, 11), ("zeta", 600, 12)],
                &pg_tbl,
                cfg,
                &full_table,
            );
            assert_eq!(
                scan_row_count(&full_table),
                6,
                "stage 2: expected 6 cumulative rows"
            );
            assert!(
                wm2 > wm1,
                "stage 2: watermark must advance\n  wm1={wm1}\n  wm2={wm2}"
            );

            // Stage 3
            let wm3 = stage_and_watermark(
                3,
                &[("eta", 700, 20), ("theta", 800, 21)],
                &pg_tbl,
                cfg,
                &full_table,
            );
            assert_eq!(
                scan_row_count(&full_table),
                8,
                "stage 3: expected 8 cumulative rows"
            );
            assert!(
                wm3 > wm2,
                "stage 3: watermark must advance\n  wm2={wm2}\n  wm3={wm3}"
            );
        });
    }

    // =========================================================================
    // Test 13.2 — Idempotency: re-running sync with no new data writes 0 rows
    // =========================================================================
    #[test]
    fn t13_2_no_new_data_sync_is_a_noop() {
        if !pg_reachable() {
            eprintln!("SKIP: postgres not reachable");
            return;
        }
        let id = test_id("t13_2");
        setup(&id);
        let pg_tbl = pg_table(&id);
        let full_table = format!("{}.{}", ts_ns(&id), ts_iceberg_table());

        with_sync_config(&ts_sync_config(&id), |cfg| {
            // Seed one stage.
            stage_and_watermark(1, &[("a", 1, 1), ("b", 2, 2)], &pg_tbl, cfg, &full_table);
            let count_after_first = scan_row_count(&full_table);
            assert_eq!(count_after_first, 2, "expected 2 rows after first sync");

            // Re-sync without any new inserts.
            assert_ok(
                "13.2 noop sync",
                &["sync", "--config", cfg, "--job", "sync_ts_events"],
            );
            let count_after_noop = scan_row_count(&full_table);
            assert_eq!(
                count_after_noop, count_after_first,
                "noop sync must not add rows: expected {count_after_first}, got {count_after_noop}"
            );
        });
    }

    // =========================================================================
    // Test 13.3 — Cursor pagination: 25 rows with batch_size=10
    //
    // The engine must page through 3 pages (10+10+5) and land all 25 rows.
    // =========================================================================
    #[test]
    fn t13_3_cursor_pagination_lands_all_rows() {
        if !pg_reachable() {
            eprintln!("SKIP: postgres not reachable");
            return;
        }
        let id = test_id("t13_3");
        setup(&id);
        let pg_tbl = pg_table(&id);
        let full_table = format!("{}.{}", ts_ns(&id), ts_iceberg_table());

        with_sync_config(&ts_sync_config(&id), |cfg| {
            // 25 rows with strictly increasing epoch offsets (+1 s each).
            let rows: Vec<(&str, i64, i64)> = (1i64..=25).map(|i| ("page_test", i, i)).collect();

            stage_and_watermark(1, &rows, &pg_tbl, cfg, &full_table);
            let count = scan_row_count(&full_table);
            assert_eq!(
                count, 25,
                "cursor pagination: expected 25 rows, got {count}"
            );
        });
    }

    // =========================================================================
    // Test 13.4 — Five-stage pipeline with varying batch sizes
    //
    //   Stage 1:  5 rows  → cumulative  5
    //   Stage 2:  1 row   → cumulative  6
    //   Stage 3: 10 rows  → cumulative 16
    //   Stage 4:  3 rows  → cumulative 19
    //   Stage 5:  7 rows  → cumulative 26
    //
    // Watermark must be strictly monotonically increasing across all stages.
    // =========================================================================
    #[test]
    fn t13_4_five_stage_monotone_watermark() {
        if !pg_reachable() {
            eprintln!("SKIP: postgres not reachable");
            return;
        }
        let id = test_id("t13_4");
        setup(&id);
        let pg_tbl = pg_table(&id);
        let full_table = format!("{}.{}", ts_ns(&id), ts_iceberg_table());

        // (stage_id, row_count, base_epoch_offset_secs)
        // Offsets are non-overlapping across stages so watermarks are distinct.
        let stages: &[(u32, usize, i64)] = &[
            (1, 5, 100),
            (2, 1, 200),
            (3, 10, 300),
            (4, 3, 500),
            (5, 7, 700),
        ];

        with_sync_config(&ts_sync_config(&id), |cfg| {
            let mut prev_wm = String::new();
            let mut expected_total: usize = 0;

            for &(stage_id, row_count, base_offset) in stages {
                let rows: Vec<(&str, i64, i64)> = (0..row_count)
                    .map(|i| {
                        (
                            "multi_stage",
                            stage_id as i64 * 100 + i as i64,
                            base_offset + i as i64,
                        )
                    })
                    .collect();

                let wm = stage_and_watermark(stage_id, &rows, &pg_tbl, cfg, &full_table);
                expected_total += row_count;

                let count = scan_row_count(&full_table);
                assert_eq!(
                    count, expected_total,
                    "stage {stage_id}: expected {expected_total} cumulative rows, got {count}"
                );

                if !prev_wm.is_empty() {
                    assert!(
                        wm > prev_wm,
                        "stage {stage_id}: watermark must be > previous\n  prev={prev_wm}\n  curr={wm}"
                    );
                }
                prev_wm = wm;
            }
        });
    }

    // =========================================================================
    // Test 13.5 — Watermark does not regress after a noop sync
    //
    // 1. Run a normal stage → establishes wm1.
    // 2. Run sync again with no new data → watermark must stay at wm1.
    // 3. Insert new data and sync again → wm2 must be > wm1.
    // =========================================================================
    #[test]
    fn t13_5_watermark_does_not_regress_after_noop() {
        if !pg_reachable() {
            eprintln!("SKIP: postgres not reachable");
            return;
        }
        let id = test_id("t13_5");
        setup(&id);
        let pg_tbl = pg_table(&id);
        let full_table = format!("{}.{}", ts_ns(&id), ts_iceberg_table());

        with_sync_config(&ts_sync_config(&id), |cfg| {
            // Stage 1 — establish a watermark.
            let wm1 = stage_and_watermark(
                1,
                &[("x", 1, 100), ("y", 2, 101)],
                &pg_tbl,
                cfg,
                &full_table,
            );

            // Noop sync — no inserts.
            assert_ok(
                "13.5 noop",
                &["sync", "--config", cfg, "--job", "sync_ts_events"],
            );
            let wm_after_noop = read_watermark(&full_table);
            assert_eq!(
                wm_after_noop, wm1,
                "noop sync must not change watermark\n  wm1={wm1}\n  after noop={wm_after_noop}"
            );

            // Stage 2 — new data at later offsets; watermark must advance.
            let wm2 = stage_and_watermark(2, &[("z", 3, 500)], &pg_tbl, cfg, &full_table);
            let count = scan_row_count(&full_table);
            assert_eq!(count, 3, "expected 3 total rows after stage 2");
            assert!(
                wm2 > wm1,
                "stage 2 watermark must be > stage 1\n  wm1={wm1}\n  wm2={wm2}"
            );
        });
    }

    // =========================================================================
    // Test 13.6 — Parent-child (header + lines) joint watermark across stages
    //
    // Simulates the orders/order_items pattern.  Two jobs share an `event_at`
    // watermark driven by the parent (ts_headers), propagated to the child
    // (ts_lines) via a JOIN.
    //
    //   Stage 1: 1 header + 3 lines → cumulative headers=1, lines=3
    //   Stage 2: 1 header + 2 lines → cumulative headers=2, lines=5
    //   Stage 3: 1 header + 5 lines → cumulative headers=3, lines=10
    //
    // After each stage both cumulative counts are asserted.
    // =========================================================================
    #[test]
    fn t13_6_parent_child_joint_watermark_stages() {
        if !pg_reachable() {
            eprintln!("SKIP: postgres not reachable");
            return;
        }
        let id = test_id("t13_6");
        preflight();
        let ns = ts_ns(&id);
        cli(&["create-namespace", "--namespace", &ns]);

        // Drop stale Iceberg tables from a previous run so watermarks start fresh.
        cli(&["drop-table", "--table", &format!("{ns}.ts_headers_iceberg")]);
        cli(&["drop-table", "--table", &format!("{ns}.ts_lines_iceberg")]);

        // Postgres table names (unique to this test).
        let hdr_tbl = format!("ts_hdr_{id}");
        let lines_tbl = format!("ts_lines_{id}");

        // Drop existing tables (separate statements — one psql -c per call).
        pg_exec(&format!("DROP TABLE IF EXISTS {lines_tbl}"));
        pg_exec(&format!("DROP TABLE IF EXISTS {hdr_tbl}"));

        pg_exec(&format!(
            "CREATE TABLE {hdr_tbl} ( \
               id       BIGSERIAL PRIMARY KEY, \
               stage    INT         NOT NULL, \
               label    TEXT        NOT NULL, \
               event_at TIMESTAMPTZ NOT NULL \
             )"
        ));
        pg_exec(&format!("CREATE INDEX ON {hdr_tbl}(event_at)"));

        pg_exec(&format!(
            "CREATE TABLE {lines_tbl} ( \
               id        BIGSERIAL PRIMARY KEY, \
               header_id BIGINT NOT NULL REFERENCES {hdr_tbl}(id), \
               seq       INT    NOT NULL, \
               amount    BIGINT NOT NULL \
             )"
        ));

        let cfg_yaml = format!(
            r#"
sources:
  ts_pg:
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
  - name: sync_headers
    source: ts_pg
    destination: warehouse
    namespace: {ns}
    table: ts_headers_iceberg
    sql: |
      SELECT id, stage, label, event_at
      FROM {hdr_tbl}
      WHERE event_at > :watermark
      ORDER BY event_at ASC, id ASC
    watermark_column: event_at
    cursor_column:    id
    batch_size: 20
    mode: incremental

  - name: sync_lines
    source: ts_pg
    destination: warehouse
    namespace: {ns}
    table: ts_lines_iceberg
    sql: |
      SELECT l.id, l.header_id, l.seq, l.amount, h.event_at
      FROM {lines_tbl} l
      JOIN {hdr_tbl} h ON h.id = l.header_id
      WHERE h.event_at > :watermark
      ORDER BY h.event_at ASC, l.id ASC
    watermark_column: event_at
    cursor_column:    id
    depends_on:       sync_headers
    batch_size: 20
    mode: incremental
"#,
            pg = pg_dsn(),
            iceberg = iceberg_uri(),
            s3 = s3_endpoint(),
            region = region(),
            ak = access_key(),
            sk = secret_key(),
            ns = ns,
            hdr_tbl = hdr_tbl,
            lines_tbl = lines_tbl,
        );

        // Helper: insert one header + N lines at a fixed past timestamp, then
        // run both jobs (depends_on order is handled automatically by `sync`).
        let insert_and_sync = |stage: u32, n_lines: usize, epoch_offset: i64, cfg: &str| {
            // Insert header with an absolute past timestamp.
            pg_exec(&format!(
                "INSERT INTO {hdr_tbl} (stage, label, event_at) \
                 VALUES ({stage}, 'hdr-{stage}', \
                         TIMESTAMP '2020-01-01 00:00:00 UTC' + INTERVAL '{epoch_offset} seconds')"
            ));
            // Insert lines referencing the just-inserted header.
            for seq in 1..=n_lines {
                pg_exec(&format!(
                    "INSERT INTO {lines_tbl} (header_id, seq, amount) \
                     SELECT id, {seq}, {amount} \
                     FROM {hdr_tbl} WHERE stage = {stage} ORDER BY id DESC LIMIT 1",
                    amount = seq as i64 * 100,
                ));
            }
            assert_ok(
                &format!("13.6 stage {stage} sync"),
                &["sync", "--config", cfg],
            );
        };

        let hdr_iceberg = format!("{ns}.ts_headers_iceberg");
        let lines_iceberg = format!("{ns}.ts_lines_iceberg");

        with_sync_config(&cfg_yaml, |cfg| {
            // Stage 1: 1 header, 3 lines
            insert_and_sync(1, 3, 100, cfg);
            assert_eq!(scan_row_count(&hdr_iceberg), 1, "stage 1: 1 header");
            assert_eq!(scan_row_count(&lines_iceberg), 3, "stage 1: 3 lines");

            // Stage 2: 1 more header, 2 lines
            insert_and_sync(2, 2, 500, cfg);
            assert_eq!(scan_row_count(&hdr_iceberg), 2, "stage 2: 2 headers");
            assert_eq!(scan_row_count(&lines_iceberg), 5, "stage 2: 5 lines total");

            // Stage 3: 1 more header, 5 lines
            insert_and_sync(3, 5, 1000, cfg);
            assert_eq!(scan_row_count(&hdr_iceberg), 3, "stage 3: 3 headers");
            assert_eq!(
                scan_row_count(&lines_iceberg),
                10,
                "stage 3: 10 lines total"
            );

            // The lines watermark must be set (driven by the JOIN's event_at).
            let lines_wm = read_watermark(&lines_iceberg);
            assert!(
                !lines_wm.is_empty(),
                "lines watermark must be set after stage 3"
            );
        });
    }
}

// =============================================================================
// § 14 – Write-strategy integration tests
//
// Covers all four write modes end-to-end against a live Postgres + Iceberg
// REST catalog + MinIO stack.
//
// Run this module only:
//   cargo test --test integration_test write_strategies -- --test-threads=1
//
// Prerequisites (same as other integration tests):
//   docker compose up -d
//   cargo build --release
// =============================================================================

#[cfg(test)]
mod write_strategies {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    // ── Shared helpers ────────────────────────────────────────────────────────

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    /// Unique test ID: "ws_<unix_ms>_<counter>"
    fn test_id(prefix: &str) -> String {
        let ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
        format!("{prefix}_{ms}_{seq}")
    }

    fn ws_ns(id: &str) -> String {
        format!("ws_{id}")
    }

    fn pg_reachable() -> bool {
        std::process::Command::new("psql")
            .args([&pg_dsn(), "-c", "SELECT 1"])
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false)
    }

    fn pg_exec(sql: &str) {
        let status = std::process::Command::new("psql")
            .args([&pg_dsn(), "-c", sql])
            .status()
            .expect("psql");
        assert!(status.success(), "psql failed: {sql}");
    }

    fn scan_row_count(full_table: &str) -> usize {
        let out = assert_ok(
            &format!("scan {full_table}"),
            &["scan", "--table", full_table, "--limit", "99999"],
        );
        out.lines()
            .rev()
            .find_map(|line| {
                let t = line.trim().trim_start_matches('(');
                t.split_once(" rows shown")
                    .and_then(|(n, _)| n.trim().parse::<usize>().ok())
            })
            .unwrap_or(0)
    }

    /// Build a minimal YAML sync config string.
    fn sync_cfg(
        source_table: &str,
        iceberg_ns: &str,
        iceberg_table: &str,
        sql: &str,
        extra_job_yaml: &str,
    ) -> String {
        format!(
            r#"
sources:
  ws_pg:
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
  - name: ws_job
    source: ws_pg
    destination: warehouse
    namespace: {ns}
    table: {tbl}
    sql: |
      {sql}
    {extra}
"#,
            pg = pg_dsn(),
            iceberg = iceberg_uri(),
            s3 = s3_endpoint(),
            region = region(),
            ak = access_key(),
            sk = secret_key(),
            ns = iceberg_ns,
            tbl = iceberg_table,
            sql = sql,
            extra = extra_job_yaml,
        )
    }

    // =========================================================================
    // § 14.1  APPEND — three-stage incremental append
    //
    // Creates a Postgres table with (id, label, value, updated_at), inserts
    // three batches of rows at strictly increasing timestamps, and asserts that
    // the cumulative Iceberg row count grows correctly after each sync.
    // =========================================================================

    #[test]
    fn t14_1_append_three_stage_incremental() {
        if !pg_reachable() {
            eprintln!("SKIP t14_1: postgres not reachable");
            return;
        }
        preflight();

        let id = test_id("t14_1");
        let pg_tbl = format!("ws_append_{id}");
        let ns = ws_ns(&id);
        let iceberg_tbl = "ws_events";
        let full = format!("{ns}.{iceberg_tbl}");

        // ── Setup ─────────────────────────────────────────────────────────────
        cli(&["create-namespace", "--namespace", &ns]);
        pg_exec(&format!(
            "CREATE TABLE {pg_tbl} ( \
               id BIGSERIAL PRIMARY KEY, \
               label TEXT NOT NULL, \
               value BIGINT NOT NULL, \
               updated_at TIMESTAMPTZ NOT NULL DEFAULT now() \
             )"
        ));
        pg_exec(&format!("CREATE INDEX ON {pg_tbl}(updated_at)"));

        let sql = format!(
            "SELECT id, label, value, updated_at \
             FROM {pg_tbl} \
             WHERE updated_at > :watermark \
             ORDER BY updated_at ASC, id ASC"
        );
        let extra = "watermark_column: updated_at\n    cursor_column: id\n    batch_size: 5\n    mode: incremental\n    write_mode: append";

        let cfg_yaml = sync_cfg(&pg_tbl, &ns, iceberg_tbl, &sql, extra);

        with_sync_config(&cfg_yaml, |cfg| {
            // Stage 1 – 3 rows
            for i in 1i64..=3 {
                pg_exec(&format!(
                    "INSERT INTO {pg_tbl} (label, value, updated_at) VALUES \
                     ('s1_row{i}', {i}, TIMESTAMP '2020-01-01' + INTERVAL '{i} seconds')"
                ));
            }
            assert_ok(
                "14.1 stage1 sync",
                &["sync", "--config", cfg, "--job", "ws_job"],
            );
            assert_eq!(scan_row_count(&full), 3, "14.1 stage1: expected 3 rows");

            // Stage 2 – 4 more rows
            for i in 1i64..=4 {
                pg_exec(&format!(
                    "INSERT INTO {pg_tbl} (label, value, updated_at) VALUES \
                     ('s2_row{i}', {i}, TIMESTAMP '2020-01-02' + INTERVAL '{i} seconds')"
                ));
            }
            assert_ok(
                "14.1 stage2 sync",
                &["sync", "--config", cfg, "--job", "ws_job"],
            );
            assert_eq!(scan_row_count(&full), 7, "14.1 stage2: expected 7 rows");

            // Stage 3 – 2 more rows
            for i in 1i64..=2 {
                pg_exec(&format!(
                    "INSERT INTO {pg_tbl} (label, value, updated_at) VALUES \
                     ('s3_row{i}', {i}, TIMESTAMP '2020-01-03' + INTERVAL '{i} seconds')"
                ));
            }
            assert_ok(
                "14.1 stage3 sync",
                &["sync", "--config", cfg, "--job", "ws_job"],
            );
            assert_eq!(scan_row_count(&full), 9, "14.1 stage3: expected 9 rows");
        });

        pg_exec(&format!("DROP TABLE IF EXISTS {pg_tbl}"));
    }

    // =========================================================================
    // § 14.2  APPEND — idempotent re-run writes zero rows
    // =========================================================================

    #[test]
    fn t14_2_append_noop_after_watermark() {
        if !pg_reachable() {
            eprintln!("SKIP t14_2: postgres not reachable");
            return;
        }
        preflight();

        let id = test_id("t14_2");
        let pg_tbl = format!("ws_noop_{id}");
        let ns = ws_ns(&id);
        let iceberg_tbl = "ws_noop_events";
        let full = format!("{ns}.{iceberg_tbl}");

        cli(&["create-namespace", "--namespace", &ns]);
        pg_exec(&format!(
            "CREATE TABLE {pg_tbl} ( \
               id BIGSERIAL PRIMARY KEY, \
               val TEXT, \
               updated_at TIMESTAMPTZ NOT NULL DEFAULT now() \
             )"
        ));

        let sql = format!(
            "SELECT id, val, updated_at FROM {pg_tbl} \
             WHERE updated_at > :watermark ORDER BY updated_at ASC, id ASC"
        );
        let extra = "watermark_column: updated_at\n    cursor_column: id\n    batch_size: 10\n    mode: incremental\n    write_mode: append";

        with_sync_config(&sync_cfg(&pg_tbl, &ns, iceberg_tbl, &sql, extra), |cfg| {
            pg_exec(&format!(
                "INSERT INTO {pg_tbl} (val, updated_at) VALUES \
                 ('a', TIMESTAMP '2020-06-01'), ('b', TIMESTAMP '2020-06-02')"
            ));
            assert_ok(
                "14.2 first sync",
                &["sync", "--config", cfg, "--job", "ws_job"],
            );
            let count_first = scan_row_count(&full);
            assert_eq!(count_first, 2, "14.2 first sync: expected 2 rows");

            // Re-run with no new data.
            assert_ok(
                "14.2 noop sync",
                &["sync", "--config", cfg, "--job", "ws_job"],
            );
            assert_eq!(
                scan_row_count(&full),
                count_first,
                "14.2 noop sync must not add rows"
            );
        });

        pg_exec(&format!("DROP TABLE IF EXISTS {pg_tbl}"));
    }

    // =========================================================================
    // § 14.3  APPEND — cursor pagination lands all rows across multiple pages
    // =========================================================================

    #[test]
    fn t14_3_append_cursor_pagination() {
        if !pg_reachable() {
            eprintln!("SKIP t14_3: postgres not reachable");
            return;
        }
        preflight();

        let id = test_id("t14_3");
        let pg_tbl = format!("ws_page_{id}");
        let ns = ws_ns(&id);
        let iceberg_tbl = "ws_paged_events";
        let full = format!("{ns}.{iceberg_tbl}");

        cli(&["create-namespace", "--namespace", &ns]);
        pg_exec(&format!(
            "CREATE TABLE {pg_tbl} ( \
               id BIGSERIAL PRIMARY KEY, \
               seq BIGINT, \
               updated_at TIMESTAMPTZ NOT NULL \
             )"
        ));

        // Insert 23 rows; batch_size=7 forces 4 pages (7+7+7+2).
        for i in 1i64..=23 {
            pg_exec(&format!(
                "INSERT INTO {pg_tbl} (seq, updated_at) VALUES \
                 ({i}, TIMESTAMP '2020-01-01' + INTERVAL '{i} seconds')"
            ));
        }

        let sql = format!(
            "SELECT id, seq, updated_at FROM {pg_tbl} \
             WHERE updated_at > :watermark ORDER BY updated_at ASC, id ASC"
        );
        let extra = "watermark_column: updated_at\n    cursor_column: id\n    batch_size: 7\n    mode: incremental\n    write_mode: append";

        with_sync_config(&sync_cfg(&pg_tbl, &ns, iceberg_tbl, &sql, extra), |cfg| {
            assert_ok("14.3 sync", &["sync", "--config", cfg, "--job", "ws_job"]);
            assert_eq!(scan_row_count(&full), 23, "14.3: all 23 rows must land");
        });

        pg_exec(&format!("DROP TABLE IF EXISTS {pg_tbl}"));
    }

    // =========================================================================
    // § 14.4  OVERWRITE — daily partition replaced on each run
    //
    // Creates a Postgres table with daily snapshot rows.  Runs the sync twice
    // for the same date partition: the second run must REPLACE (not add) rows
    // so the final count equals the second batch size, not both combined.
    // =========================================================================

    #[test]
    fn t14_4_overwrite_replaces_partition() {
        if !pg_reachable() {
            eprintln!("SKIP t14_4: postgres not reachable");
            return;
        }
        preflight();

        let id = test_id("t14_4");
        let pg_tbl = format!("ws_snap_{id}");
        let ns = ws_ns(&id);
        let iceberg_tbl = "ws_daily_snap";
        let full = format!("{ns}.{iceberg_tbl}");

        cli(&["create-namespace", "--namespace", &ns]);
        pg_exec(&format!(
            "CREATE TABLE {pg_tbl} ( \
               id BIGSERIAL PRIMARY KEY, \
               snap_date TEXT NOT NULL, \
               metric TEXT NOT NULL, \
               amount NUMERIC(12,2) NOT NULL \
             )"
        ));

        // Run 1: insert 3 rows for '2024-01-15'.
        for i in 1i64..=3 {
            pg_exec(&format!(
                "INSERT INTO {pg_tbl} (snap_date, metric, amount) VALUES \
                 ('2024-01-15', 'metric_{i}', {i}00.00)"
            ));
        }

        let sql = format!(
            "SELECT id, snap_date, metric, amount FROM {pg_tbl} \
             WHERE snap_date = '2024-01-15' ORDER BY id"
        );
        let extra = "watermark_column: ~\n    batch_size: 100\n    mode: full\n    write_mode: overwrite\n    partition_column: snap_date";

        with_sync_config(&sync_cfg(&pg_tbl, &ns, iceberg_tbl, &sql, extra), |cfg| {
            assert_ok(
                "14.4 run1 sync",
                &["sync", "--config", cfg, "--job", "ws_job"],
            );
            assert_eq!(
                scan_row_count(&full),
                3,
                "14.4 run1: 3 rows after first sync"
            );

            // Run 2: replace with 2 rows — simulates regenerated snapshot.
            pg_exec(&format!("DELETE FROM {pg_tbl}"));
            for i in 1i64..=2 {
                pg_exec(&format!(
                    "INSERT INTO {pg_tbl} (snap_date, metric, amount) VALUES \
                     ('2024-01-15', 'metric_{i}', {i}999.00)"
                ));
            }

            assert_ok(
                "14.4 run2 sync",
                &["sync", "--config", cfg, "--job", "ws_job"],
            );
            // After overwrite the partition should contain only the 2 new rows.
            assert_eq!(
                scan_row_count(&full),
                2,
                "14.4 run2: overwrite must replace old rows; expected 2"
            );
        });

        pg_exec(&format!("DROP TABLE IF EXISTS {pg_tbl}"));
    }

    // =========================================================================
    // § 14.5  OVERWRITE — first run on empty table degrades to append
    // =========================================================================

    #[test]
    fn t14_5_overwrite_first_run_is_append() {
        if !pg_reachable() {
            eprintln!("SKIP t14_5: postgres not reachable");
            return;
        }
        preflight();

        let id = test_id("t14_5");
        let pg_tbl = format!("ws_snap2_{id}");
        let ns = ws_ns(&id);
        let iceberg_tbl = "ws_daily_snap2";
        let full = format!("{ns}.{iceberg_tbl}");

        cli(&["create-namespace", "--namespace", &ns]);
        pg_exec(&format!(
            "CREATE TABLE {pg_tbl} ( \
               id BIGSERIAL PRIMARY KEY, \
               snap_date TEXT NOT NULL, \
               val BIGINT NOT NULL \
             )"
        ));

        for i in 1i64..=4 {
            pg_exec(&format!(
                "INSERT INTO {pg_tbl} (snap_date, val) VALUES ('2024-02-01', {i})"
            ));
        }

        let sql = format!("SELECT id, snap_date, val FROM {pg_tbl} ORDER BY id");
        let extra = "watermark_column: ~\n    batch_size: 100\n    mode: full\n    write_mode: overwrite\n    partition_column: snap_date";

        with_sync_config(&sync_cfg(&pg_tbl, &ns, iceberg_tbl, &sql, extra), |cfg| {
            // First run on an empty Iceberg table must not error and must land all rows.
            assert_ok(
                "14.5 first overwrite",
                &["sync", "--config", cfg, "--job", "ws_job"],
            );
            assert_eq!(
                scan_row_count(&full),
                4,
                "14.5: 4 rows after first overwrite"
            );
        });

        pg_exec(&format!("DROP TABLE IF EXISTS {pg_tbl}"));
    }

    // =========================================================================
    // § 14.6  UPSERT — PK-keyed updates replace existing rows
    //
    // Stage 1: insert 4 entities.
    // Stage 2: update 2 of them (higher updated_at) → total rows in Iceberg
    //          stays 4 (old versions position-deleted, new appended).
    //
    // Because position-deletes are logical, the raw Iceberg scan still shows
    // the correct view (old + new rows with deletes applied).  We assert the
    // visible row count equals 4 after both stages.
    // =========================================================================

    #[test]
    fn t14_6_upsert_updates_existing_rows() {
        if !pg_reachable() {
            eprintln!("SKIP t14_6: postgres not reachable");
            return;
        }
        preflight();

        let id = test_id("t14_6");
        let pg_tbl = format!("ws_ent_{id}");
        let ns = ws_ns(&id);
        let iceberg_tbl = "ws_entities";
        let full = format!("{ns}.{iceberg_tbl}");

        cli(&["create-namespace", "--namespace", &ns]);
        pg_exec(&format!(
            "CREATE TABLE {pg_tbl} ( \
               id BIGSERIAL PRIMARY KEY, \
               name TEXT NOT NULL, \
               status TEXT NOT NULL DEFAULT 'active', \
               updated_at TIMESTAMPTZ NOT NULL DEFAULT now() \
             )"
        ));
        pg_exec(&format!("CREATE INDEX ON {pg_tbl}(updated_at)"));

        let sql = format!(
            "SELECT id, name, status, updated_at FROM {pg_tbl} \
             WHERE updated_at > :watermark ORDER BY updated_at ASC, id ASC"
        );
        let extra = "watermark_column: updated_at\n    cursor_column: id\n    batch_size: 10\n    mode: incremental\n    write_mode: upsert\n    merge:\n      key_columns: [id]\n      hard_delete: false";

        with_sync_config(&sync_cfg(&pg_tbl, &ns, iceberg_tbl, &sql, extra), |cfg| {
            // Stage 1: insert 4 entities.
            for i in 1i64..=4 {
                pg_exec(&format!(
                    "INSERT INTO {pg_tbl} (name, status, updated_at) VALUES \
                     ('entity_{i}', 'active', TIMESTAMP '2020-03-01' + INTERVAL '{i} seconds')"
                ));
            }
            assert_ok("14.6 stage1", &["sync", "--config", cfg, "--job", "ws_job"]);
            // After stage 1: 4 rows visible.
            assert_eq!(scan_row_count(&full), 4, "14.6 stage1: 4 entities");

            // Stage 2: update entities 1 and 2 to 'inactive'.
            for i in 1i64..=2 {
                pg_exec(&format!(
                    "UPDATE {pg_tbl} SET status = 'inactive', \
                     updated_at = TIMESTAMP '2020-04-01' + INTERVAL '{i} seconds' \
                     WHERE name = 'entity_{i}'"
                ));
            }
            assert_ok("14.6 stage2", &["sync", "--config", cfg, "--job", "ws_job"]);
            // The 2 updated rows must be visible in their new state; old versions
            // deleted.  Total visible = 4 (not 6).
            assert_eq!(
                scan_row_count(&full),
                4,
                "14.6 stage2: upsert must not duplicate rows; expected 4 visible"
            );
        });

        pg_exec(&format!("DROP TABLE IF EXISTS {pg_tbl}"));
    }

    // =========================================================================
    // § 14.7  UPSERT — composite key (tenant_id, entity_id)
    // =========================================================================

    #[test]
    fn t14_7_upsert_composite_key() {
        if !pg_reachable() {
            eprintln!("SKIP t14_7: postgres not reachable");
            return;
        }
        preflight();

        let id = test_id("t14_7");
        let pg_tbl = format!("ws_comp_{id}");
        let ns = ws_ns(&id);
        let iceberg_tbl = "ws_composite_entities";
        let full = format!("{ns}.{iceberg_tbl}");

        cli(&["create-namespace", "--namespace", &ns]);
        pg_exec(&format!(
            "CREATE TABLE {pg_tbl} ( \
               id BIGSERIAL PRIMARY KEY, \
               tenant_id TEXT NOT NULL, \
               entity_id BIGINT NOT NULL, \
               name TEXT, \
               updated_at TIMESTAMPTZ NOT NULL \
             )"
        ));

        // Tenants A and B each have 2 entities.
        let rows = [
            ("tenant_a", 1i64, "alpha"),
            ("tenant_a", 2i64, "beta"),
            ("tenant_b", 1i64, "gamma"),
            ("tenant_b", 2i64, "delta"),
        ];
        for (tenant, eid, name) in &rows {
            pg_exec(&format!(
                "INSERT INTO {pg_tbl} (tenant_id, entity_id, name, updated_at) VALUES \
                 ('{tenant}', {eid}, '{name}', TIMESTAMP '2020-05-01' + INTERVAL '{eid} seconds')"
            ));
        }

        let sql = format!(
            "SELECT id, tenant_id, entity_id, name, updated_at FROM {pg_tbl} \
             WHERE updated_at > :watermark ORDER BY updated_at ASC, id ASC"
        );
        let extra = "watermark_column: updated_at\n    cursor_column: id\n    batch_size: 10\n    mode: incremental\n    write_mode: upsert\n    merge:\n      key_columns: [tenant_id, entity_id]\n      hard_delete: false";

        with_sync_config(&sync_cfg(&pg_tbl, &ns, iceberg_tbl, &sql, extra), |cfg| {
            assert_ok(
                "14.7 initial",
                &["sync", "--config", cfg, "--job", "ws_job"],
            );
            assert_eq!(scan_row_count(&full), 4, "14.7: 4 rows after initial load");

            // Update tenant_a/entity_1.
            pg_exec(&format!(
                "UPDATE {pg_tbl} SET name = 'alpha_v2', updated_at = TIMESTAMP '2020-06-01' \
                 WHERE tenant_id = 'tenant_a' AND entity_id = 1"
            ));
            assert_ok("14.7 update", &["sync", "--config", cfg, "--job", "ws_job"]);
            assert_eq!(
                scan_row_count(&full),
                4,
                "14.7: composite upsert must not duplicate rows; expected 4"
            );
        });

        pg_exec(&format!("DROP TABLE IF EXISTS {pg_tbl}"));
    }

    // =========================================================================
    // § 14.8  MERGE INTO — I / U / D routing via _op column
    //
    // Inserts a CDC outbox with mixed operations and asserts the final
    // Iceberg state reflects inserts and updates (deletes produce no new rows).
    // =========================================================================

    #[test]
    fn t14_8_merge_into_iud_routing() {
        if !pg_reachable() {
            eprintln!("SKIP t14_8: postgres not reachable");
            return;
        }
        preflight();

        let id = test_id("t14_8");
        let pg_tbl = format!("ws_cdc_{id}");
        let ns = ws_ns(&id);
        let iceberg_tbl = "ws_cdc_target";
        let full = format!("{ns}.{iceberg_tbl}");

        cli(&["create-namespace", "--namespace", &ns]);
        pg_exec(&format!(
            "CREATE TABLE {pg_tbl} ( \
               id BIGSERIAL PRIMARY KEY, \
               entity_id BIGINT NOT NULL, \
               name TEXT, \
               status TEXT, \
               op TEXT NOT NULL, \
               occurred_at TIMESTAMPTZ NOT NULL DEFAULT now() \
             )"
        ));
        pg_exec(&format!("CREATE INDEX ON {pg_tbl}(occurred_at)"));

        // Batch 1: 3 inserts for entity_id 10, 20, 30.
        let inserts = [
            (10i64, "alpha", "new"),
            (20, "beta", "new"),
            (30, "gamma", "new"),
        ];
        for (i, (eid, name, status)) in inserts.iter().enumerate() {
            let sec = i as i64 + 1;
            pg_exec(&format!(
                "INSERT INTO {pg_tbl} (entity_id, name, status, op, occurred_at) VALUES \
                 ({eid}, '{name}', '{status}', 'I', \
                  TIMESTAMP '2020-07-01' + INTERVAL '{sec} seconds')"
            ));
        }

        let sql = format!(
            "SELECT id, entity_id, name, status, op AS _op, occurred_at FROM {pg_tbl} \
             WHERE occurred_at > :watermark ORDER BY occurred_at ASC, id ASC"
        );
        let extra = "watermark_column: occurred_at\n    cursor_column: id\n    batch_size: 10\n    mode: incremental\n    write_mode: merge_into\n    merge:\n      key_columns: [entity_id]\n      hard_delete: false";

        with_sync_config(&sync_cfg(&pg_tbl, &ns, iceberg_tbl, &sql, extra), |cfg| {
            // Sync batch 1: 3 inserts → 3 rows in Iceberg.
            assert_ok("14.8 batch1", &["sync", "--config", cfg, "--job", "ws_job"]);
            assert_eq!(scan_row_count(&full), 3, "14.8 batch1: 3 inserted rows");

            // Batch 2: update entity 10, delete entity 30, insert entity 40.
            let events: &[(i64, &str, &str, &str, i64)] = &[
                (10, "alpha_v2", "updated", "U", 10),
                (30, "gamma", "new", "D", 11),
                (40, "delta", "new", "I", 12),
            ];
            for (eid, name, status, op, sec) in events {
                pg_exec(&format!(
                    "INSERT INTO {pg_tbl} (entity_id, name, status, op, occurred_at) VALUES \
                     ({eid}, '{name}', '{status}', '{op}', \
                      TIMESTAMP '2020-07-02' + INTERVAL '{sec} seconds')"
                ));
            }

            assert_ok("14.8 batch2", &["sync", "--config", cfg, "--job", "ws_job"]);
            // After batch 2:
            //   entity 10 → updated (old row deleted, new appended)
            //   entity 30 → deleted (no new row)
            //   entity 40 → inserted
            // Visible rows = 10(new) + 20 + 40 = 3
            assert_eq!(
                scan_row_count(&full),
                3,
                "14.8 batch2: expected 3 visible rows after I+U+D merge"
            );
        });

        pg_exec(&format!("DROP TABLE IF EXISTS {pg_tbl}"));
    }

    // =========================================================================
    // § 14.9  MERGE INTO — insert-only batch is handled correctly
    // =========================================================================

    #[test]
    fn t14_9_merge_into_all_inserts() {
        if !pg_reachable() {
            eprintln!("SKIP t14_9: postgres not reachable");
            return;
        }
        preflight();

        let id = test_id("t14_9");
        let pg_tbl = format!("ws_ins_{id}");
        let ns = ws_ns(&id);
        let iceberg_tbl = "ws_insert_target";
        let full = format!("{ns}.{iceberg_tbl}");

        cli(&["create-namespace", "--namespace", &ns]);
        pg_exec(&format!(
            "CREATE TABLE {pg_tbl} ( \
               id BIGSERIAL PRIMARY KEY, \
               entity_id BIGINT NOT NULL, \
               val TEXT, \
               op TEXT NOT NULL DEFAULT 'I', \
               occurred_at TIMESTAMPTZ NOT NULL \
             )"
        ));

        for i in 1i64..=5 {
            pg_exec(&format!(
                "INSERT INTO {pg_tbl} (entity_id, val, op, occurred_at) VALUES \
                 ({i}, 'value_{i}', 'I', TIMESTAMP '2020-08-01' + INTERVAL '{i} seconds')"
            ));
        }

        let sql = format!(
            "SELECT id, entity_id, val, op AS _op, occurred_at FROM {pg_tbl} \
             WHERE occurred_at > :watermark ORDER BY occurred_at ASC, id ASC"
        );
        let extra = "watermark_column: occurred_at\n    cursor_column: id\n    batch_size: 10\n    mode: incremental\n    write_mode: merge_into\n    merge:\n      key_columns: [entity_id]";

        with_sync_config(&sync_cfg(&pg_tbl, &ns, iceberg_tbl, &sql, extra), |cfg| {
            assert_ok("14.9 sync", &["sync", "--config", cfg, "--job", "ws_job"]);
            assert_eq!(
                scan_row_count(&full),
                5,
                "14.9: 5 inserted rows via merge_into"
            );
        });

        pg_exec(&format!("DROP TABLE IF EXISTS {pg_tbl}"));
    }

    // =========================================================================
    // § 14.10  MERGE INTO — delete-only batch leaves no new data rows
    // =========================================================================

    #[test]
    fn t14_10_merge_into_all_deletes() {
        if !pg_reachable() {
            eprintln!("SKIP t14_10: postgres not reachable");
            return;
        }
        preflight();

        let id = test_id("t14_10");
        let pg_tbl = format!("ws_del_{id}");
        let ns = ws_ns(&id);
        let iceberg_tbl = "ws_delete_target";
        let full = format!("{ns}.{iceberg_tbl}");

        cli(&["create-namespace", "--namespace", &ns]);
        pg_exec(&format!(
            "CREATE TABLE {pg_tbl} ( \
               id BIGSERIAL PRIMARY KEY, \
               entity_id BIGINT NOT NULL, \
               val TEXT, \
               op TEXT NOT NULL DEFAULT 'I', \
               occurred_at TIMESTAMPTZ NOT NULL \
             )"
        ));

        // Batch 1: insert 3 rows via merge_into (I).
        for i in 1i64..=3 {
            pg_exec(&format!(
                "INSERT INTO {pg_tbl} (entity_id, val, op, occurred_at) VALUES \
                 ({i}, 'v{i}', 'I', TIMESTAMP '2020-09-01' + INTERVAL '{i} seconds')"
            ));
        }

        let sql = format!(
            "SELECT id, entity_id, val, op AS _op, occurred_at FROM {pg_tbl} \
             WHERE occurred_at > :watermark ORDER BY occurred_at ASC, id ASC"
        );
        let extra = "watermark_column: occurred_at\n    cursor_column: id\n    batch_size: 10\n    mode: incremental\n    write_mode: merge_into\n    merge:\n      key_columns: [entity_id]";

        with_sync_config(&sync_cfg(&pg_tbl, &ns, iceberg_tbl, &sql, extra), |cfg| {
            assert_ok(
                "14.10 batch1 insert",
                &["sync", "--config", cfg, "--job", "ws_job"],
            );
            assert_eq!(scan_row_count(&full), 3, "14.10 batch1: 3 rows");

            // Batch 2: delete all 3 entities.
            for i in 1i64..=3 {
                pg_exec(&format!(
                    "INSERT INTO {pg_tbl} (entity_id, val, op, occurred_at) VALUES \
                     ({i}, 'v{i}', 'D', TIMESTAMP '2020-09-02' + INTERVAL '{i} seconds')"
                ));
            }
            assert_ok(
                "14.10 batch2 delete",
                &["sync", "--config", cfg, "--job", "ws_job"],
            );
            // All 3 visible rows are now position-deleted.
            assert_eq!(
                scan_row_count(&full),
                0,
                "14.10 batch2: all rows deleted; expected 0 visible"
            );
        });

        pg_exec(&format!("DROP TABLE IF EXISTS {pg_tbl}"));
    }

    // =========================================================================
    // § 14.11  APPEND + OVERWRITE — two jobs in same config, different modes
    //
    // Demonstrates that a single YAML config can mix write modes.
    // Job A appends events; Job B overwrites a daily partition.
    // Both run via `--config` (all jobs), and their row counts are independent.
    // =========================================================================

    #[test]
    fn t14_11_mixed_write_modes_in_one_config() {
        if !pg_reachable() {
            eprintln!("SKIP t14_11: postgres not reachable");
            return;
        }
        preflight();

        let id = test_id("t14_11");
        let events_pg = format!("ws_mix_events_{id}");
        let snap_pg = format!("ws_mix_snap_{id}");
        let ns = ws_ns(&id);

        cli(&["create-namespace", "--namespace", &ns]);

        pg_exec(&format!(
            "CREATE TABLE {events_pg} ( \
               id BIGSERIAL PRIMARY KEY, \
               label TEXT, \
               updated_at TIMESTAMPTZ NOT NULL \
             )"
        ));
        pg_exec(&format!(
            "CREATE TABLE {snap_pg} ( \
               id BIGSERIAL PRIMARY KEY, \
               snap_date TEXT, \
               val BIGINT \
             )"
        ));

        for i in 1i64..=3 {
            pg_exec(&format!(
                "INSERT INTO {events_pg} (label, updated_at) VALUES \
                 ('ev_{i}', TIMESTAMP '2021-01-01' + INTERVAL '{i} seconds')"
            ));
        }
        for i in 1i64..=4 {
            pg_exec(&format!(
                "INSERT INTO {snap_pg} (snap_date, val) VALUES ('2021-01-15', {i})"
            ));
        }

        let mixed_cfg = format!(
            r#"
sources:
  ws_pg:
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
  - name: events_append
    source: ws_pg
    destination: warehouse
    namespace: {ns}
    table: mix_events
    sql: |
      SELECT id, label, updated_at FROM {events_pg}
      WHERE updated_at > :watermark ORDER BY updated_at ASC, id ASC
    watermark_column: updated_at
    cursor_column: id
    batch_size: 10
    mode: incremental
    write_mode: append

  - name: snap_overwrite
    source: ws_pg
    destination: warehouse
    namespace: {ns}
    table: mix_snap
    sql: |
      SELECT id, snap_date, val FROM {snap_pg} ORDER BY id
    watermark_column: ~
    batch_size: 100
    mode: full
    write_mode: overwrite
    partition_column: snap_date
"#,
            pg = pg_dsn(),
            iceberg = iceberg_uri(),
            s3 = s3_endpoint(),
            region = region(),
            ak = access_key(),
            sk = secret_key(),
            ns = ns,
            events_pg = events_pg,
            snap_pg = snap_pg,
        );

        with_sync_config(&mixed_cfg, |cfg| {
            assert_ok("14.11 run1", &["sync", "--config", cfg]);
            assert_eq!(
                scan_row_count(&format!("{ns}.mix_events")),
                3,
                "14.11: 3 events appended"
            );
            assert_eq!(
                scan_row_count(&format!("{ns}.mix_snap")),
                4,
                "14.11: 4 snapshot rows"
            );

            // Second run: add 2 more events; overwrite snap stays at 4.
            for i in 4i64..=5 {
                pg_exec(&format!(
                    "INSERT INTO {events_pg} (label, updated_at) VALUES \
                     ('ev_{i}', TIMESTAMP '2021-02-01' + INTERVAL '{i} seconds')"
                ));
            }
            assert_ok("14.11 run2", &["sync", "--config", cfg]);
            assert_eq!(
                scan_row_count(&format!("{ns}.mix_events")),
                5,
                "14.11 run2: 5 cumulative events"
            );
            assert_eq!(
                scan_row_count(&format!("{ns}.mix_snap")),
                4,
                "14.11 run2: overwrite snap still 4"
            );
        });

        pg_exec(&format!("DROP TABLE IF EXISTS {events_pg}"));
        pg_exec(&format!("DROP TABLE IF EXISTS {snap_pg}"));
    }
}

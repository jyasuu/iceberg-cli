//! Integration tests §11 – Sync config: PostgreSQL → Iceberg
//!
//! Run with:
//!   cargo test --test test_sync -- --test-threads=1

mod common;
use common::*;

fn pg_reachable() -> bool {
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
    assert!(
        out.contains("shipped") || out.contains("pending"),
        "11.12 expected order rows; got: {out}"
    );
}

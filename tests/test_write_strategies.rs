//! Integration tests §14 – Write-strategy integration tests
//!
//! Covers all four write modes end-to-end: append, overwrite, upsert, merge_into.
//!
//! Run with:
//!   cargo test --test test_write_strategies -- --test-threads=1

mod common;
use common::*;
use std::sync::atomic::{AtomicU64, Ordering};

static COUNTER: AtomicU64 = AtomicU64::new(0);

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

fn pg_exec_ws(sql: &str) {
    let status = std::process::Command::new("psql")
        .args([&pg_dsn(), "-c", sql])
        .status()
        .expect("psql");
    assert!(status.success(), "psql failed: {sql}");
}

fn scan_row_count_ws(full_table: &str) -> usize {
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

fn sync_cfg(
    _source_table: &str,
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

// =============================================================================
// § 14.1  APPEND — three-stage incremental append
// =============================================================================
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

    cli(&["create-namespace", "--namespace", &ns]);
    pg_exec_ws(&format!(
        "CREATE TABLE {pg_tbl} ( \
           id BIGSERIAL PRIMARY KEY, \
           label TEXT NOT NULL, \
           value BIGINT NOT NULL, \
           updated_at TIMESTAMPTZ NOT NULL DEFAULT now() \
         )"
    ));
    pg_exec_ws(&format!("CREATE INDEX ON {pg_tbl}(updated_at)"));

    let sql = format!(
        "SELECT id, label, value, updated_at \
         FROM {pg_tbl} \
         WHERE updated_at > :watermark \
         ORDER BY updated_at ASC, id ASC"
    );
    let extra = "watermark_column: updated_at\n    cursor_column: id\n    batch_size: 5\n    mode: incremental\n    write_mode: append";

    let cfg_yaml = sync_cfg(&pg_tbl, &ns, iceberg_tbl, &sql, extra);

    with_sync_config(&cfg_yaml, |cfg| {
        for i in 1i64..=3 {
            pg_exec_ws(&format!(
                "INSERT INTO {pg_tbl} (label, value, updated_at) VALUES \
                 ('s1_row{i}', {i}, TIMESTAMP '2020-01-01' + INTERVAL '{i} seconds')"
            ));
        }
        assert_ok(
            "14.1 stage1 sync",
            &["sync", "--config", cfg, "--job", "ws_job"],
        );
        assert_eq!(scan_row_count_ws(&full), 3, "14.1 stage1: expected 3 rows");

        for i in 1i64..=4 {
            pg_exec_ws(&format!(
                "INSERT INTO {pg_tbl} (label, value, updated_at) VALUES \
                 ('s2_row{i}', {i}, TIMESTAMP '2020-01-02' + INTERVAL '{i} seconds')"
            ));
        }
        assert_ok(
            "14.1 stage2 sync",
            &["sync", "--config", cfg, "--job", "ws_job"],
        );
        assert_eq!(scan_row_count_ws(&full), 7, "14.1 stage2: expected 7 rows");

        for i in 1i64..=2 {
            pg_exec_ws(&format!(
                "INSERT INTO {pg_tbl} (label, value, updated_at) VALUES \
                 ('s3_row{i}', {i}, TIMESTAMP '2020-01-03' + INTERVAL '{i} seconds')"
            ));
        }
        assert_ok(
            "14.1 stage3 sync",
            &["sync", "--config", cfg, "--job", "ws_job"],
        );
        assert_eq!(scan_row_count_ws(&full), 9, "14.1 stage3: expected 9 rows");
    });

    pg_exec_ws(&format!("DROP TABLE IF EXISTS {pg_tbl}"));
}

// =============================================================================
// § 14.2  APPEND — idempotent re-run writes zero rows
// =============================================================================
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
    pg_exec_ws(&format!(
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
        pg_exec_ws(&format!(
            "INSERT INTO {pg_tbl} (val, updated_at) VALUES \
             ('a', TIMESTAMP '2020-06-01'), ('b', TIMESTAMP '2020-06-02')"
        ));
        assert_ok(
            "14.2 first sync",
            &["sync", "--config", cfg, "--job", "ws_job"],
        );
        let count_first = scan_row_count_ws(&full);
        assert_eq!(count_first, 2, "14.2 first sync: expected 2 rows");

        assert_ok(
            "14.2 noop sync",
            &["sync", "--config", cfg, "--job", "ws_job"],
        );
        assert_eq!(
            scan_row_count_ws(&full),
            count_first,
            "14.2 noop sync must not add rows"
        );
    });

    pg_exec_ws(&format!("DROP TABLE IF EXISTS {pg_tbl}"));
}

// =============================================================================
// § 14.3  APPEND — cursor pagination lands all rows across multiple pages
// =============================================================================
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
    pg_exec_ws(&format!(
        "CREATE TABLE {pg_tbl} ( \
           id BIGSERIAL PRIMARY KEY, \
           seq BIGINT, \
           updated_at TIMESTAMPTZ NOT NULL \
         )"
    ));

    for i in 1i64..=23 {
        pg_exec_ws(&format!(
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
        assert_eq!(scan_row_count_ws(&full), 23, "14.3: all 23 rows must land");
    });

    pg_exec_ws(&format!("DROP TABLE IF EXISTS {pg_tbl}"));
}

// =============================================================================
// § 14.4  OVERWRITE — daily partition replaced on each run
// =============================================================================
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
    pg_exec_ws(&format!(
        "CREATE TABLE {pg_tbl} ( \
           id BIGSERIAL PRIMARY KEY, \
           snap_date TEXT NOT NULL, \
           metric TEXT NOT NULL, \
           amount NUMERIC(12,2) NOT NULL \
         )"
    ));

    for i in 1i64..=3 {
        pg_exec_ws(&format!(
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
            scan_row_count_ws(&full),
            3,
            "14.4 run1: 3 rows after first sync"
        );

        pg_exec_ws(&format!("DELETE FROM {pg_tbl}"));
        for i in 1i64..=2 {
            pg_exec_ws(&format!(
                "INSERT INTO {pg_tbl} (snap_date, metric, amount) VALUES \
                 ('2024-01-15', 'metric_{i}', {i}999.00)"
            ));
        }

        assert_ok(
            "14.4 run2 sync",
            &["sync", "--config", cfg, "--job", "ws_job"],
        );
        assert_eq!(
            scan_row_count_ws(&full),
            2,
            "14.4 run2: overwrite must replace old rows; expected 2"
        );
    });

    pg_exec_ws(&format!("DROP TABLE IF EXISTS {pg_tbl}"));
}

// =============================================================================
// § 14.5  OVERWRITE — first run on empty table degrades to append
// =============================================================================
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
    pg_exec_ws(&format!(
        "CREATE TABLE {pg_tbl} ( \
           id BIGSERIAL PRIMARY KEY, \
           snap_date TEXT NOT NULL, \
           val BIGINT NOT NULL \
         )"
    ));

    for i in 1i64..=4 {
        pg_exec_ws(&format!(
            "INSERT INTO {pg_tbl} (snap_date, val) VALUES ('2024-02-01', {i})"
        ));
    }

    let sql = format!("SELECT id, snap_date, val FROM {pg_tbl} ORDER BY id");
    let extra = "watermark_column: ~\n    batch_size: 100\n    mode: full\n    write_mode: overwrite\n    partition_column: snap_date";

    with_sync_config(&sync_cfg(&pg_tbl, &ns, iceberg_tbl, &sql, extra), |cfg| {
        assert_ok(
            "14.5 first overwrite",
            &["sync", "--config", cfg, "--job", "ws_job"],
        );
        assert_eq!(
            scan_row_count_ws(&full),
            4,
            "14.5: 4 rows after first overwrite"
        );
    });

    pg_exec_ws(&format!("DROP TABLE IF EXISTS {pg_tbl}"));
}

// =============================================================================
// § 14.6  UPSERT — PK-keyed updates replace existing rows
// =============================================================================
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
    pg_exec_ws(&format!(
        "CREATE TABLE {pg_tbl} ( \
           id BIGSERIAL PRIMARY KEY, \
           name TEXT NOT NULL, \
           status TEXT NOT NULL DEFAULT 'active', \
           updated_at TIMESTAMPTZ NOT NULL DEFAULT now() \
         )"
    ));
    pg_exec_ws(&format!("CREATE INDEX ON {pg_tbl}(updated_at)"));

    let sql = format!(
        "SELECT id, name, status, updated_at FROM {pg_tbl} \
         WHERE updated_at > :watermark ORDER BY updated_at ASC, id ASC"
    );
    let extra = "watermark_column: updated_at\n    cursor_column: id\n    batch_size: 10\n    mode: incremental\n    write_mode: upsert\n    merge:\n      key_columns: [id]\n      hard_delete: false";

    with_sync_config(&sync_cfg(&pg_tbl, &ns, iceberg_tbl, &sql, extra), |cfg| {
        for i in 1i64..=4 {
            pg_exec_ws(&format!(
                "INSERT INTO {pg_tbl} (name, status, updated_at) VALUES \
                 ('entity_{i}', 'active', TIMESTAMP '2020-03-01' + INTERVAL '{i} seconds')"
            ));
        }
        assert_ok("14.6 stage1", &["sync", "--config", cfg, "--job", "ws_job"]);
        assert_eq!(scan_row_count_ws(&full), 4, "14.6 stage1: 4 entities");

        for i in 1i64..=2 {
            pg_exec_ws(&format!(
                "UPDATE {pg_tbl} SET status = 'inactive', \
                 updated_at = TIMESTAMP '2020-04-01' + INTERVAL '{i} seconds' \
                 WHERE name = 'entity_{i}'"
            ));
        }
        assert_ok("14.6 stage2", &["sync", "--config", cfg, "--job", "ws_job"]);
        assert_eq!(
            scan_row_count_ws(&full),
            4,
            "14.6 stage2: upsert must not duplicate rows; expected 4 visible"
        );
    });

    pg_exec_ws(&format!("DROP TABLE IF EXISTS {pg_tbl}"));
}

// =============================================================================
// § 14.7  UPSERT — composite key (tenant_id, entity_id)
// =============================================================================
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
    pg_exec_ws(&format!(
        "CREATE TABLE {pg_tbl} ( \
           id BIGSERIAL PRIMARY KEY, \
           tenant_id TEXT NOT NULL, \
           entity_id BIGINT NOT NULL, \
           name TEXT, \
           updated_at TIMESTAMPTZ NOT NULL \
         )"
    ));

    let rows = [
        ("tenant_a", 1i64, "alpha"),
        ("tenant_a", 2i64, "beta"),
        ("tenant_b", 1i64, "gamma"),
        ("tenant_b", 2i64, "delta"),
    ];
    for (tenant, eid, name) in &rows {
        pg_exec_ws(&format!(
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
        assert_eq!(
            scan_row_count_ws(&full),
            4,
            "14.7: 4 rows after initial load"
        );

        pg_exec_ws(&format!(
            "UPDATE {pg_tbl} SET name = 'alpha_v2', updated_at = TIMESTAMP '2020-06-01' \
             WHERE tenant_id = 'tenant_a' AND entity_id = 1"
        ));
        assert_ok("14.7 update", &["sync", "--config", cfg, "--job", "ws_job"]);
        assert_eq!(
            scan_row_count_ws(&full),
            4,
            "14.7: composite upsert must not duplicate rows; expected 4"
        );
    });

    pg_exec_ws(&format!("DROP TABLE IF EXISTS {pg_tbl}"));
}

// =============================================================================
// § 14.8  MERGE INTO — I / U / D routing via _op column
// =============================================================================
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
    pg_exec_ws(&format!(
        "CREATE TABLE {pg_tbl} ( \
           id BIGSERIAL PRIMARY KEY, \
           entity_id BIGINT NOT NULL, \
           name TEXT, \
           status TEXT, \
           op TEXT NOT NULL, \
           occurred_at TIMESTAMPTZ NOT NULL DEFAULT now() \
         )"
    ));
    pg_exec_ws(&format!("CREATE INDEX ON {pg_tbl}(occurred_at)"));

    let inserts = [
        (10i64, "alpha", "new"),
        (20, "beta", "new"),
        (30, "gamma", "new"),
    ];
    for (i, (eid, name, status)) in inserts.iter().enumerate() {
        let sec = i as i64 + 1;
        pg_exec_ws(&format!(
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
        assert_ok("14.8 batch1", &["sync", "--config", cfg, "--job", "ws_job"]);
        assert_eq!(scan_row_count_ws(&full), 3, "14.8 batch1: 3 inserted rows");

        let events: &[(i64, &str, &str, &str, i64)] = &[
            (10, "alpha_v2", "updated", "U", 10),
            (30, "gamma", "new", "D", 11),
            (40, "delta", "new", "I", 12),
        ];
        for (eid, name, status, op, sec) in events {
            pg_exec_ws(&format!(
                "INSERT INTO {pg_tbl} (entity_id, name, status, op, occurred_at) VALUES \
                 ({eid}, '{name}', '{status}', '{op}', \
                  TIMESTAMP '2020-07-02' + INTERVAL '{sec} seconds')"
            ));
        }

        assert_ok("14.8 batch2", &["sync", "--config", cfg, "--job", "ws_job"]);
        assert_eq!(
            scan_row_count_ws(&full),
            3,
            "14.8 batch2: expected 3 visible rows after I+U+D merge"
        );
    });

    pg_exec_ws(&format!("DROP TABLE IF EXISTS {pg_tbl}"));
}

// =============================================================================
// § 14.9  MERGE INTO — insert-only batch
// =============================================================================
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
    pg_exec_ws(&format!(
        "CREATE TABLE {pg_tbl} ( \
           id BIGSERIAL PRIMARY KEY, \
           entity_id BIGINT NOT NULL, \
           val TEXT, \
           op TEXT NOT NULL DEFAULT 'I', \
           occurred_at TIMESTAMPTZ NOT NULL \
         )"
    ));

    for i in 1i64..=5 {
        pg_exec_ws(&format!(
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
            scan_row_count_ws(&full),
            5,
            "14.9: 5 inserted rows via merge_into"
        );
    });

    pg_exec_ws(&format!("DROP TABLE IF EXISTS {pg_tbl}"));
}

// =============================================================================
// § 14.10  MERGE INTO — delete-only batch leaves no new data rows
// =============================================================================
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
    pg_exec_ws(&format!(
        "CREATE TABLE {pg_tbl} ( \
           id BIGSERIAL PRIMARY KEY, \
           entity_id BIGINT NOT NULL, \
           val TEXT, \
           op TEXT NOT NULL DEFAULT 'I', \
           occurred_at TIMESTAMPTZ NOT NULL \
         )"
    ));

    for i in 1i64..=3 {
        pg_exec_ws(&format!(
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
        assert_eq!(scan_row_count_ws(&full), 3, "14.10 batch1: 3 rows");

        for i in 1i64..=3 {
            pg_exec_ws(&format!(
                "INSERT INTO {pg_tbl} (entity_id, val, op, occurred_at) VALUES \
                 ({i}, 'v{i}', 'D', TIMESTAMP '2020-09-02' + INTERVAL '{i} seconds')"
            ));
        }
        assert_ok(
            "14.10 batch2 delete",
            &["sync", "--config", cfg, "--job", "ws_job"],
        );
        assert_eq!(
            scan_row_count_ws(&full),
            0,
            "14.10 batch2: all rows deleted; expected 0 visible"
        );
    });

    pg_exec_ws(&format!("DROP TABLE IF EXISTS {pg_tbl}"));
}

// =============================================================================
// § 14.11  APPEND + OVERWRITE — two jobs in same config, different modes
// =============================================================================
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

    pg_exec_ws(&format!(
        "CREATE TABLE {events_pg} ( \
           id BIGSERIAL PRIMARY KEY, \
           label TEXT, \
           updated_at TIMESTAMPTZ NOT NULL \
         )"
    ));
    pg_exec_ws(&format!(
        "CREATE TABLE {snap_pg} ( \
           id BIGSERIAL PRIMARY KEY, \
           snap_date TEXT, \
           val BIGINT \
         )"
    ));

    for i in 1i64..=3 {
        pg_exec_ws(&format!(
            "INSERT INTO {events_pg} (label, updated_at) VALUES \
             ('ev_{i}', TIMESTAMP '2021-01-01' + INTERVAL '{i} seconds')"
        ));
    }
    for i in 1i64..=4 {
        pg_exec_ws(&format!(
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
            scan_row_count_ws(&format!("{ns}.mix_events")),
            3,
            "14.11: 3 events appended"
        );
        assert_eq!(
            scan_row_count_ws(&format!("{ns}.mix_snap")),
            4,
            "14.11: 4 snapshot rows"
        );

        for i in 4i64..=5 {
            pg_exec_ws(&format!(
                "INSERT INTO {events_pg} (label, updated_at) VALUES \
                 ('ev_{i}', TIMESTAMP '2021-02-01' + INTERVAL '{i} seconds')"
            ));
        }
        assert_ok("14.11 run2", &["sync", "--config", cfg]);
        assert_eq!(
            scan_row_count_ws(&format!("{ns}.mix_events")),
            5,
            "14.11 run2: 5 cumulative events"
        );
        assert_eq!(
            scan_row_count_ws(&format!("{ns}.mix_snap")),
            4,
            "14.11 run2: overwrite snap still 4"
        );
    });

    pg_exec_ws(&format!("DROP TABLE IF EXISTS {events_pg}"));
    pg_exec_ws(&format!("DROP TABLE IF EXISTS {snap_pg}"));
}

// =============================================================================
// § 14.12  OVERWRITE with real Iceberg day() partition spec
// =============================================================================
#[test]
fn t14_12_iceberg_partition_day_spec_overwrite_isolation() {
    if !pg_reachable() {
        eprintln!("SKIP t14_12: postgres not reachable");
        return;
    }
    preflight();

    let id = test_id("t14_12");
    let pg_tbl = format!("ip_sales_{id}");
    let ns = ws_ns(&id);
    let iceberg_tbl = "ip_daily_sales";
    let full = format!("{ns}.{iceberg_tbl}");

    cli(&["create-namespace", "--namespace", &ns]);
    pg_exec_ws(&format!(
        "CREATE TABLE {pg_tbl} ( \
           id BIGSERIAL PRIMARY KEY, \
           sale_date DATE NOT NULL, \
           product TEXT NOT NULL, \
           units INT NOT NULL \
         )"
    ));

    for i in 1i64..=3 {
        pg_exec_ws(&format!(
            "INSERT INTO {pg_tbl} (sale_date, product, units) VALUES \
             ('2024-03-01', 'widget_{i}', {i}0)"
        ));
    }
    for i in 1i64..=2 {
        pg_exec_ws(&format!(
            "INSERT INTO {pg_tbl} (sale_date, product, units) VALUES \
             ('2024-03-02', 'gadget_{i}', {i}00)"
        ));
    }

    let sql_day_a = format!(
        "SELECT id, sale_date, product, units FROM {pg_tbl} WHERE sale_date = '2024-03-01'"
    );
    let extra = "watermark_column: ~\n    batch_size: 100\n    mode: full\n    write_mode: overwrite\n    iceberg_partition:\n      column: sale_date\n      transform: day";

    with_sync_config(
        &sync_cfg(&pg_tbl, &ns, iceberg_tbl, &sql_day_a, extra),
        |cfg| {
            assert_ok(
                "14.12 run1: sync Day A",
                &["sync", "--config", cfg, "--job", "ws_job"],
            );
            assert_eq!(
                scan_row_count_ws(&full),
                3,
                "14.12 run1: 3 rows after syncing Day A"
            );

            let sql_day_b = format!(
                "SELECT id, sale_date, product, units FROM {pg_tbl} WHERE sale_date = '2024-03-02'"
            );
            let cfg_b = sync_cfg(&pg_tbl, &ns, iceberg_tbl, &sql_day_b, extra);
            with_sync_config(&cfg_b, |cfg2| {
                assert_ok(
                    "14.12 run1b: sync Day B",
                    &["sync", "--config", cfg2, "--job", "ws_job"],
                );
                assert_eq!(
                    scan_row_count_ws(&full),
                    5,
                    "14.12 after both days: 3+2=5 rows"
                );

                pg_exec_ws(&format!(
                    "DELETE FROM {pg_tbl} WHERE sale_date = '2024-03-01'"
                ));
                for i in 10i64..=11 {
                    pg_exec_ws(&format!(
                        "INSERT INTO {pg_tbl} (sale_date, product, units) VALUES \
                     ('2024-03-01', 'new_widget_{i}', {i})"
                    ));
                }

                assert_ok(
                    "14.12 run2: overwrite Day A with 2 new rows",
                    &["sync", "--config", cfg, "--job", "ws_job"],
                );
                assert_eq!(
                    scan_row_count_ws(&full),
                    4,
                    "14.12 run2: Day A replaced (2 new) + Day B untouched (2) = 4 total"
                );

                assert_output(
                    "14.12 run2: Day B row still present",
                    "gadget_1",
                    &["scan", "--table", &full, "--limit", "99999"],
                );
                assert_output(
                    "14.12 run2: new Day A row present",
                    "new_widget_10",
                    &["scan", "--table", &full, "--limit", "99999"],
                );
            });
        },
    );

    pg_exec_ws(&format!("DROP TABLE IF EXISTS {pg_tbl}"));
}

// =============================================================================
// § 14.13  OVERWRITE with iceberg_partition — spec survives COW recreate
// =============================================================================
#[test]
fn t14_13_iceberg_partition_spec_survives_cow_recreate() {
    if !pg_reachable() {
        eprintln!("SKIP t14_13: postgres not reachable");
        return;
    }
    preflight();

    let id = test_id("t14_13");
    let pg_tbl = format!("ip_snap_{id}");
    let ns = ws_ns(&id);
    let iceberg_tbl = "ip_snap";
    let full = format!("{ns}.{iceberg_tbl}");

    cli(&["create-namespace", "--namespace", &ns]);
    pg_exec_ws(&format!(
        "CREATE TABLE {pg_tbl} ( \
           id BIGSERIAL PRIMARY KEY, \
           snap_date DATE NOT NULL, \
           val INT NOT NULL \
         )"
    ));

    let sql = format!("SELECT id, snap_date, val FROM {pg_tbl}");
    let extra = "watermark_column: ~\n    batch_size: 100\n    mode: full\n    write_mode: overwrite\n    iceberg_partition:\n      column: snap_date\n      transform: day";

    for i in 1i64..=3 {
        pg_exec_ws(&format!(
            "INSERT INTO {pg_tbl} (snap_date, val) VALUES ('2025-01-10', {i})"
        ));
    }

    with_sync_config(&sync_cfg(&pg_tbl, &ns, iceberg_tbl, &sql, extra), |cfg| {
        assert_ok("14.13 run1", &["sync", "--config", cfg, "--job", "ws_job"]);
        assert_eq!(scan_row_count_ws(&full), 3, "14.13 run1: 3 rows");

        pg_exec_ws(&format!("DELETE FROM {pg_tbl}"));
        for i in 10i64..=12 {
            pg_exec_ws(&format!(
                "INSERT INTO {pg_tbl} (snap_date, val) VALUES ('2025-01-10', {i})"
            ));
        }

        assert_ok("14.13 run2", &["sync", "--config", cfg, "--job", "ws_job"]);
        assert_eq!(
            scan_row_count_ws(&full),
            3,
            "14.13 run2: partition spec survived COW recreate — still 3 rows"
        );

        pg_exec_ws(&format!("DELETE FROM {pg_tbl}"));
        pg_exec_ws(&format!(
            "INSERT INTO {pg_tbl} (snap_date, val) VALUES ('2025-01-10', 999)"
        ));

        assert_ok("14.13 run3", &["sync", "--config", cfg, "--job", "ws_job"]);
        assert_eq!(
            scan_row_count_ws(&full),
            1,
            "14.13 run3: spec still intact after two COW cycles — 1 row"
        );
    });

    pg_exec_ws(&format!("DROP TABLE IF EXISTS {pg_tbl}"));
}

//! Integration tests §13 – Time-series incremental sync: multi-stage (insert → sync) × N
//!
//! Tests simulate a realistic CDC pipeline across multiple stages, verifying:
//!   • Cumulative row counts grow correctly after each stage.
//!   • The `sync.watermark.<col>` property in table metadata advances each run.
//!   • Noop syncs with no new data write 0 extra rows (idempotency).
//!   • Rows from earlier stages are never lost.
//!
//! Run with:
//!   cargo test --test test_time_series -- --test-threads=1

mod common;
use common::*;
use std::net::TcpStream;

fn pg_reachable() -> bool {
    TcpStream::connect("localhost:5432").is_ok()
}

// ── Isolation helpers ─────────────────────────────────────────────────────────

fn test_id(name: &str) -> String {
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

fn ts_ns(id: &str) -> String {
    format!("itest_ts_{id}")
}

fn ts_iceberg_table() -> &'static str {
    "ts_events"
}

fn pg_table(id: &str) -> String {
    format!("ts_src_{id}")
}

// ── Setup ─────────────────────────────────────────────────────────────────────

fn setup(id: &str) {
    preflight();
    let ns = ts_ns(id);
    let pg_tbl = pg_table(id);

    cli(&["create-namespace", "--namespace", &ns]);
    cli(&[
        "drop-table",
        "--table",
        &format!("{ns}.{}", ts_iceberg_table()),
    ]);

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

// ── Sync-config factory ───────────────────────────────────────────────────────

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

// ── Data-insertion helper ─────────────────────────────────────────────────────

fn insert_rows(pg_tbl: &str, stage: u32, rows: &[(&str, i64, i64)]) {
    for (label, value, epoch_offset) in rows {
        pg_exec(&format!(
            "INSERT INTO {pg_tbl} (stage, label, value, event_at) \
             VALUES ({stage}, '{label}', {value}, \
                     TIMESTAMP '2020-01-01 00:00:00 UTC' + INTERVAL '{epoch_offset} seconds')"
        ));
    }
}

// ── Assertion helpers ─────────────────────────────────────────────────────────

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
                 was synced."
            )
        })
        .split('┆')
        .nth(1)
        .unwrap_or("")
        .trim()
        .trim_end_matches('│')
        .trim()
        .to_string()
}

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

// =============================================================================
// Test 13.1 — Three-stage pipeline: watermark advances each run
// =============================================================================
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
        let wm1 = stage_and_watermark(
            1,
            &[("alpha", 100, 1), ("beta", 200, 2), ("gamma", 300, 3)],
            &pg_tbl,
            cfg,
            &full_table,
        );
        assert_eq!(scan_row_count(&full_table), 3, "stage 1: expected 3 rows");
        assert!(!wm1.is_empty(), "stage 1: watermark must be set");

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

// =============================================================================
// Test 13.2 — Idempotency: re-running sync with no new data writes 0 rows
// =============================================================================
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
        stage_and_watermark(1, &[("a", 1, 1), ("b", 2, 2)], &pg_tbl, cfg, &full_table);
        let count_after_first = scan_row_count(&full_table);
        assert_eq!(count_after_first, 2, "expected 2 rows after first sync");

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

// =============================================================================
// Test 13.3 — Cursor pagination: 25 rows with batch_size=10
// =============================================================================
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
        let rows: Vec<(&str, i64, i64)> = (1i64..=25).map(|i| ("page_test", i, i)).collect();
        stage_and_watermark(1, &rows, &pg_tbl, cfg, &full_table);
        let count = scan_row_count(&full_table);
        assert_eq!(
            count, 25,
            "cursor pagination: expected 25 rows, got {count}"
        );
    });
}

// =============================================================================
// Test 13.4 — Five-stage pipeline with varying batch sizes
// =============================================================================
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

// =============================================================================
// Test 13.5 — Watermark does not regress after a noop sync
// =============================================================================
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
        let wm1 = stage_and_watermark(
            1,
            &[("x", 1, 100), ("y", 2, 101)],
            &pg_tbl,
            cfg,
            &full_table,
        );

        assert_ok(
            "13.5 noop",
            &["sync", "--config", cfg, "--job", "sync_ts_events"],
        );
        let wm_after_noop = read_watermark(&full_table);
        assert_eq!(
            wm_after_noop, wm1,
            "noop sync must not change watermark\n  wm1={wm1}\n  after noop={wm_after_noop}"
        );

        let wm2 = stage_and_watermark(2, &[("z", 3, 500)], &pg_tbl, cfg, &full_table);
        let count = scan_row_count(&full_table);
        assert_eq!(count, 3, "expected 3 total rows after stage 2");
        assert!(
            wm2 > wm1,
            "stage 2 watermark must be > stage 1\n  wm1={wm1}\n  wm2={wm2}"
        );
    });
}

// =============================================================================
// Test 13.6 — Parent-child (header + lines) joint watermark across stages
// =============================================================================
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

    cli(&["drop-table", "--table", &format!("{ns}.ts_headers_iceberg")]);
    cli(&["drop-table", "--table", &format!("{ns}.ts_lines_iceberg")]);

    let hdr_tbl = format!("ts_hdr_{id}");
    let lines_tbl = format!("ts_lines_{id}");

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

    let insert_and_sync = |stage: u32, n_lines: usize, epoch_offset: i64, cfg: &str| {
        pg_exec(&format!(
            "INSERT INTO {hdr_tbl} (stage, label, event_at) \
             VALUES ({stage}, 'hdr-{stage}', \
                     TIMESTAMP '2020-01-01 00:00:00 UTC' + INTERVAL '{epoch_offset} seconds')"
        ));
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
        insert_and_sync(1, 3, 100, cfg);
        assert_eq!(scan_row_count(&hdr_iceberg), 1, "stage 1: 1 header");
        assert_eq!(scan_row_count(&lines_iceberg), 3, "stage 1: 3 lines");

        insert_and_sync(2, 2, 500, cfg);
        assert_eq!(scan_row_count(&hdr_iceberg), 2, "stage 2: 2 headers");
        assert_eq!(scan_row_count(&lines_iceberg), 5, "stage 2: 5 lines total");

        insert_and_sync(3, 5, 1000, cfg);
        assert_eq!(scan_row_count(&hdr_iceberg), 3, "stage 3: 3 headers");
        assert_eq!(
            scan_row_count(&lines_iceberg),
            10,
            "stage 3: 10 lines total"
        );

        let lines_wm = read_watermark(&lines_iceberg);
        assert!(
            !lines_wm.is_empty(),
            "lines watermark must be set after stage 3"
        );
    });
}

//! Integration tests §15 – Bug-fix regression tests
//!
//! Covers three reported production bugs:
//!
//!   15.1  watermark_type: timestamp  — plain TIMESTAMP (no tz) watermark column
//!   15.2  cursor_type: text          — TEXT/VARCHAR cursor column (UUID v7 style)
//!   15.3  timestamp partition column — TIMESTAMPTZ column used as iceberg_partition
//!   15.4  Combined watermark_type + cursor_type used together
//!
//! Run with:
//!   cargo test --test test_bugfix -- --test-threads=1

mod common;
use common::*;
use std::sync::atomic::{AtomicU64, Ordering};

static BF_COUNTER: AtomicU64 = AtomicU64::new(0);

fn test_id(prefix: &str) -> String {
    let ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    let seq = BF_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}_{ms}_{seq}")
}

fn pg_reachable() -> bool {
    std::net::TcpStream::connect("localhost:5432").is_ok()
}

fn scan_row_count_bf(full_table: &str) -> usize {
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

fn sync_cfg_yaml(
    _pg_tbl: &str,
    ns: &str,
    iceberg_tbl: &str,
    sql: &str,
    extra_job_yaml: &str,
) -> String {
    format!(
        r#"
sources:
  bf_pg:
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
  - name: bf_job
    source: bf_pg
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
        ns = ns,
        tbl = iceberg_tbl,
        sql = sql,
        extra = extra_job_yaml,
    )
}

// =============================================================================
// § 15.1 — watermark_type: timestamp (plain TIMESTAMP, no tz)
// =============================================================================
#[test]
fn t15_1_watermark_type_timestamp_no_tz() {
    if !pg_reachable() {
        eprintln!("SKIP t15_1: postgres not reachable");
        return;
    }
    preflight();

    let id = test_id("bf15_1");
    let pg_tbl = format!("bf_ts_ntz_{id}");
    let ns = format!("bf_{id}");
    let iceberg_tbl = "bf_events_ntz";
    let full = format!("{ns}.{iceberg_tbl}");

    cli(&["create-namespace", "--namespace", &ns]);

    pg_exec(&format!(
        "CREATE TABLE {pg_tbl} ( \
           id       BIGSERIAL    PRIMARY KEY, \
           label    TEXT         NOT NULL, \
           value    BIGINT       NOT NULL, \
           evt_at   TIMESTAMP    NOT NULL \
         )"
    ));
    pg_exec(&format!("CREATE INDEX ON {pg_tbl}(evt_at)"));

    for i in 1i64..=3 {
        pg_exec(&format!(
            "INSERT INTO {pg_tbl} (label, value, evt_at) VALUES \
             ('stage1_row{i}', {i}, \
              TIMESTAMP '2021-06-01 00:00:00' + INTERVAL '{i} seconds')"
        ));
    }

    let sql = format!(
        "SELECT id, label, value, evt_at \
         FROM {pg_tbl} \
         WHERE evt_at > :watermark \
         ORDER BY evt_at ASC, id ASC"
    );
    let extra = "watermark_column: evt_at\n    watermark_type: timestamp\n    cursor_column: id\n    batch_size: 10\n    mode: incremental\n    write_mode: append";

    with_sync_config(
        &sync_cfg_yaml(&pg_tbl, &ns, iceberg_tbl, &sql, extra),
        |cfg| {
            assert_ok(
                "15.1 first sync (no-tz watermark)",
                &["sync", "--config", cfg, "--job", "bf_job"],
            );
            assert_eq!(
                scan_row_count_bf(&full),
                3,
                "15.1 first sync: expected 3 rows"
            );

            for i in 1i64..=4 {
                pg_exec(&format!(
                    "INSERT INTO {pg_tbl} (label, value, evt_at) VALUES \
                 ('stage2_row{i}', {i}00, \
                  TIMESTAMP '2021-07-01 00:00:00' + INTERVAL '{i} seconds')"
                ));
            }
            assert_ok(
                "15.1 second sync (incremental)",
                &["sync", "--config", cfg, "--job", "bf_job"],
            );
            assert_eq!(
                scan_row_count_bf(&full),
                7,
                "15.1 second sync: expected 7 total rows"
            );

            assert_ok(
                "15.1 noop sync",
                &["sync", "--config", cfg, "--job", "bf_job"],
            );
            assert_eq!(
                scan_row_count_bf(&full),
                7,
                "15.1 noop sync: expected still 7 rows"
            );
        },
    );

    pg_exec(&format!("DROP TABLE IF EXISTS {pg_tbl}"));
}

// =============================================================================
// § 15.2 — cursor_type: text (TEXT/VARCHAR cursor column, e.g. UUID v7)
// =============================================================================
#[test]
fn t15_2_cursor_type_text_uuid_v7_style() {
    if !pg_reachable() {
        eprintln!("SKIP t15_2: postgres not reachable");
        return;
    }
    preflight();

    let id = test_id("bf15_2");
    let pg_tbl = format!("bf_txt_cursor_{id}");
    let ns = format!("bf_{id}");
    let iceberg_tbl = "bf_txt_events";
    let full = format!("{ns}.{iceberg_tbl}");

    cli(&["create-namespace", "--namespace", &ns]);

    pg_exec(&format!(
        "CREATE TABLE {pg_tbl} ( \
           uid      TEXT         PRIMARY KEY, \
           label    TEXT         NOT NULL, \
           value    BIGINT       NOT NULL, \
           evt_at   TIMESTAMPTZ  NOT NULL DEFAULT now() \
         )"
    ));
    pg_exec(&format!("CREATE INDEX ON {pg_tbl}(uid)"));

    for i in 1i64..=8 {
        pg_exec(&format!(
            "INSERT INTO {pg_tbl} (uid, label, value, evt_at) VALUES \
             ('UID-{i:05}', 'label_{i}', {i}, \
              TIMESTAMP '2022-03-01' + INTERVAL '{i} seconds')"
        ));
    }

    let sql = format!(
        "SELECT uid, label, value, evt_at \
         FROM {pg_tbl} \
         WHERE evt_at > :watermark \
         ORDER BY evt_at ASC, uid ASC"
    );
    let extra = "watermark_column: evt_at\n    watermark_type: timestamptz\n    cursor_column: uid\n    cursor_type: text\n    batch_size: 3\n    mode: incremental\n    write_mode: append";

    with_sync_config(
        &sync_cfg_yaml(&pg_tbl, &ns, iceberg_tbl, &sql, extra),
        |cfg| {
            assert_ok(
                "15.2 first sync (text cursor)",
                &["sync", "--config", cfg, "--job", "bf_job"],
            );
            assert_eq!(
                scan_row_count_bf(&full),
                8,
                "15.2 first sync: expected all 8 rows"
            );

            for i in 9i64..=11 {
                pg_exec(&format!(
                    "INSERT INTO {pg_tbl} (uid, label, value, evt_at) VALUES \
                 ('UID-{i:05}', 'label_{i}', {i}, \
                  TIMESTAMP '2022-04-01' + INTERVAL '{i} seconds')"
                ));
            }

            assert_ok(
                "15.2 incremental sync (text cursor)",
                &["sync", "--config", cfg, "--job", "bf_job"],
            );
            assert_eq!(
                scan_row_count_bf(&full),
                11,
                "15.2 incremental sync: expected 11 total rows (8+3)"
            );

            assert_ok(
                "15.2 noop sync",
                &["sync", "--config", cfg, "--job", "bf_job"],
            );
            assert_eq!(
                scan_row_count_bf(&full),
                11,
                "15.2 noop sync: expected still 11 rows"
            );
        },
    );

    pg_exec(&format!("DROP TABLE IF EXISTS {pg_tbl}"));
}

// =============================================================================
// § 15.3a — TIMESTAMPTZ partition column: day transform
// =============================================================================
#[test]
fn t15_3a_timestamptz_partition_day_transform() {
    if !pg_reachable() {
        eprintln!("SKIP t15_3a: postgres not reachable");
        return;
    }
    preflight();

    let id = test_id("bf15_3a");
    let pg_tbl = format!("bf_tstz_day_{id}");
    let ns = format!("bf_{id}");
    let iceberg_tbl = "bf_tstz_day";
    let full = format!("{ns}.{iceberg_tbl}");

    cli(&["create-namespace", "--namespace", &ns]);

    pg_exec(&format!(
        "CREATE TABLE {pg_tbl} ( \
           id       BIGSERIAL   PRIMARY KEY, \
           label    TEXT        NOT NULL, \
           value    BIGINT      NOT NULL, \
           event_ts TIMESTAMPTZ NOT NULL \
         )"
    ));
    pg_exec(&format!("CREATE INDEX ON {pg_tbl}(event_ts)"));

    for i in 1i64..=3 {
        pg_exec(&format!(
            "INSERT INTO {pg_tbl} (label, value, event_ts) VALUES \
             ('day_a_row{i}', {i}00, \
              TIMESTAMPTZ '2024-05-10 10:00:00+00' + INTERVAL '{i} hours')"
        ));
    }
    for i in 1i64..=2 {
        pg_exec(&format!(
            "INSERT INTO {pg_tbl} (label, value, event_ts) VALUES \
             ('day_b_row{i}', {i}000, \
              TIMESTAMPTZ '2024-05-11 08:00:00+00' + INTERVAL '{i} hours')"
        ));
    }

    let sql_day_a = format!(
        "SELECT id, label, value, event_ts \
         FROM {pg_tbl} \
         WHERE event_ts::date = '2024-05-10' \
         ORDER BY event_ts ASC, id ASC"
    );
    let extra = "watermark_column: ~\n    batch_size: 100\n    mode: full\n    write_mode: overwrite\n    iceberg_partition:\n      column: event_ts\n      transform: day";

    with_sync_config(
        &sync_cfg_yaml(&pg_tbl, &ns, iceberg_tbl, &sql_day_a, extra),
        |cfg_a| {
            assert_ok(
                "15.3a run1: Day A sync (TIMESTAMPTZ partition)",
                &["sync", "--config", cfg_a, "--job", "bf_job"],
            );
            assert_eq!(
                scan_row_count_bf(&full),
                3,
                "15.3a run1: expected 3 rows after Day A sync"
            );

            let sql_day_b = format!(
                "SELECT id, label, value, event_ts \
             FROM {pg_tbl} \
             WHERE event_ts::date = '2024-05-11' \
             ORDER BY event_ts ASC, id ASC"
            );
            with_sync_config(
                &sync_cfg_yaml(&pg_tbl, &ns, iceberg_tbl, &sql_day_b, extra),
                |cfg_b| {
                    assert_ok(
                        "15.3a run1b: Day B sync",
                        &["sync", "--config", cfg_b, "--job", "bf_job"],
                    );
                    assert_eq!(
                        scan_row_count_bf(&full),
                        5,
                        "15.3a after both days: expected 5 rows"
                    );

                    pg_exec(&format!(
                        "DELETE FROM {pg_tbl} WHERE event_ts::date = '2024-05-10'"
                    ));
                    pg_exec(&format!(
                        "INSERT INTO {pg_tbl} (label, value, event_ts) VALUES \
                 ('day_a_new', 9999, TIMESTAMPTZ '2024-05-10 12:00:00+00')"
                    ));

                    assert_ok(
                        "15.3a run2: overwrite Day A",
                        &["sync", "--config", cfg_a, "--job", "bf_job"],
                    );
                    assert_eq!(
                        scan_row_count_bf(&full),
                        3,
                        "15.3a run2: Day A replaced (1) + Day B untouched (2) = 3"
                    );

                    assert_output(
                        "15.3a run2: Day B row still present",
                        "day_b_row1",
                        &["scan", "--table", &full, "--limit", "99999"],
                    );
                    assert_output(
                        "15.3a run2: new Day A row present",
                        "day_a_new",
                        &["scan", "--table", &full, "--limit", "99999"],
                    );
                },
            );
        },
    );

    pg_exec(&format!("DROP TABLE IF EXISTS {pg_tbl}"));
}

// =============================================================================
// § 15.3b — TIMESTAMPTZ partition column: hour transform
// =============================================================================
#[test]
fn t15_3b_timestamptz_partition_hour_transform() {
    if !pg_reachable() {
        eprintln!("SKIP t15_3b: postgres not reachable");
        return;
    }
    preflight();

    let id = test_id("bf15_3b");
    let pg_tbl = format!("bf_tstz_hr_{id}");
    let ns = format!("bf_{id}");
    let iceberg_tbl = "bf_tstz_hour";
    let full = format!("{ns}.{iceberg_tbl}");

    cli(&["create-namespace", "--namespace", &ns]);

    pg_exec(&format!(
        "CREATE TABLE {pg_tbl} ( \
           id       BIGSERIAL   PRIMARY KEY, \
           label    TEXT        NOT NULL, \
           event_ts TIMESTAMPTZ NOT NULL \
         )"
    ));

    for i in 1i64..=4 {
        pg_exec(&format!(
            "INSERT INTO {pg_tbl} (label, event_ts) VALUES \
             ('hr_row{i}', TIMESTAMPTZ '2024-06-15 09:00:00+00' + INTERVAL '{i} minutes')"
        ));
    }

    let sql = format!("SELECT id, label, event_ts FROM {pg_tbl} ORDER BY event_ts ASC, id ASC");
    let extra = "watermark_column: ~\n    batch_size: 100\n    mode: full\n    write_mode: append\n    iceberg_partition:\n      column: event_ts\n      transform: hour";

    with_sync_config(
        &sync_cfg_yaml(&pg_tbl, &ns, iceberg_tbl, &sql, extra),
        |cfg| {
            assert_ok(
                "15.3b hour-transform TIMESTAMPTZ sync",
                &["sync", "--config", cfg, "--job", "bf_job"],
            );
            assert_eq!(
                scan_row_count_bf(&full),
                4,
                "15.3b: expected 4 rows after hour-partition sync"
            );
        },
    );

    pg_exec(&format!("DROP TABLE IF EXISTS {pg_tbl}"));
}

// =============================================================================
// § 15.3c — plain TIMESTAMP (no tz) column as iceberg_partition: month transform
// =============================================================================
#[test]
fn t15_3c_timestamp_no_tz_partition_month_transform() {
    if !pg_reachable() {
        eprintln!("SKIP t15_3c: postgres not reachable");
        return;
    }
    preflight();

    let id = test_id("bf15_3c");
    let pg_tbl = format!("bf_ts_mo_{id}");
    let ns = format!("bf_{id}");
    let iceberg_tbl = "bf_ts_month";
    let full = format!("{ns}.{iceberg_tbl}");

    cli(&["create-namespace", "--namespace", &ns]);

    pg_exec(&format!(
        "CREATE TABLE {pg_tbl} ( \
           id       BIGSERIAL  PRIMARY KEY, \
           label    TEXT       NOT NULL, \
           event_ts TIMESTAMP  NOT NULL \
         )"
    ));

    for i in 1i64..=3 {
        pg_exec(&format!(
            "INSERT INTO {pg_tbl} (label, event_ts) VALUES \
             ('jan_{i}', TIMESTAMP '2023-01-{i:02} 12:00:00')"
        ));
    }
    for i in 1i64..=2 {
        pg_exec(&format!(
            "INSERT INTO {pg_tbl} (label, event_ts) VALUES \
             ('feb_{i}', TIMESTAMP '2023-02-{i:02} 12:00:00')"
        ));
    }

    let sql = format!("SELECT id, label, event_ts FROM {pg_tbl} ORDER BY event_ts ASC, id ASC");
    let extra = "watermark_column: ~\n    batch_size: 100\n    mode: full\n    write_mode: append\n    iceberg_partition:\n      column: event_ts\n      transform: month";

    with_sync_config(
        &sync_cfg_yaml(&pg_tbl, &ns, iceberg_tbl, &sql, extra),
        |cfg| {
            assert_ok(
                "15.3c month-transform TIMESTAMP (no tz) sync",
                &["sync", "--config", cfg, "--job", "bf_job"],
            );
            assert_eq!(
                scan_row_count_bf(&full),
                5,
                "15.3c: expected 5 rows (3 Jan + 2 Feb)"
            );
        },
    );

    pg_exec(&format!("DROP TABLE IF EXISTS {pg_tbl}"));
}

// =============================================================================
// § 15.4 — Consistency: watermark_type + cursor_type used together
// =============================================================================
#[test]
fn t15_4_timestamp_watermark_and_text_cursor_combined() {
    if !pg_reachable() {
        eprintln!("SKIP t15_4: postgres not reachable");
        return;
    }
    preflight();

    let id = test_id("bf15_4");
    let pg_tbl = format!("bf_combined_{id}");
    let ns = format!("bf_{id}");
    let iceberg_tbl = "bf_combined";
    let full = format!("{ns}.{iceberg_tbl}");

    cli(&["create-namespace", "--namespace", &ns]);

    pg_exec(&format!(
        "CREATE TABLE {pg_tbl} ( \
           uuid_v7_id  TEXT        PRIMARY KEY, \
           payload     TEXT        NOT NULL, \
           update_time TIMESTAMP   NOT NULL \
         )"
    ));
    pg_exec(&format!(
        "CREATE INDEX ON {pg_tbl}(update_time, uuid_v7_id)"
    ));

    for i in 1i64..=7 {
        pg_exec(&format!(
            "INSERT INTO {pg_tbl} (uuid_v7_id, payload, update_time) VALUES \
             ('UUIDV7-{i:04}', 'data_{i}', \
              TIMESTAMP '2023-09-01 00:00:00' + INTERVAL '{i} minutes')"
        ));
    }

    let sql = format!(
        "SELECT uuid_v7_id, payload, update_time \
         FROM {pg_tbl} \
         WHERE update_time > :watermark \
         ORDER BY update_time, uuid_v7_id"
    );
    let extra = "watermark_column: update_time\n    watermark_type: timestamp\n    cursor_column: uuid_v7_id\n    cursor_type: text\n    batch_size: 3\n    mode: incremental\n    write_mode: append";

    with_sync_config(
        &sync_cfg_yaml(&pg_tbl, &ns, iceberg_tbl, &sql, extra),
        |cfg| {
            assert_ok(
                "15.4 first sync (combined fixes)",
                &["sync", "--config", cfg, "--job", "bf_job"],
            );
            assert_eq!(
                scan_row_count_bf(&full),
                7,
                "15.4 first sync: expected all 7 rows"
            );

            for i in 8i64..=9 {
                pg_exec(&format!(
                    "INSERT INTO {pg_tbl} (uuid_v7_id, payload, update_time) VALUES \
                 ('UUIDV7-{i:04}', 'data_{i}', \
                  TIMESTAMP '2023-10-01 00:00:00' + INTERVAL '{i} minutes')"
                ));
            }
            assert_ok(
                "15.4 incremental sync",
                &["sync", "--config", cfg, "--job", "bf_job"],
            );
            assert_eq!(
                scan_row_count_bf(&full),
                9,
                "15.4 incremental: expected 9 total rows (7+2)"
            );

            assert_ok(
                "15.4 noop sync",
                &["sync", "--config", cfg, "--job", "bf_job"],
            );
            assert_eq!(
                scan_row_count_bf(&full),
                9,
                "15.4 noop: expected still 9 rows"
            );
        },
    );

    pg_exec(&format!("DROP TABLE IF EXISTS {pg_tbl}"));
}

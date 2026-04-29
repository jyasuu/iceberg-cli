//! Integration tests §1–10: namespace, table management, basic write/scan,
//! projection, append semantics, edge cases, expected failures, data types,
//! and stress tests.
//!
//! Run with:
//!   cargo test --test test_basic -- --test-threads=1

mod common;
use common::*;

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
        cli(&["create-namespace", "--namespace", NS]);
        assert_output("1.2 list-namespaces shows ns", NS, &["list-namespaces"]);
    }

    #[test]
    fn t1_3_create_namespace_idempotent() {
        preflight();
        cli(&["create-namespace", "--namespace", NS]);
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

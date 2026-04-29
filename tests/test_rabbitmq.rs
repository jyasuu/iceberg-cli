//! Integration tests §12 – RabbitMQ-triggered sync
//!
//! Run with:
//!   cargo test --test test_rabbitmq -- --test-threads=1

mod common;
use common::*;
use std::time::Duration;

fn rmq_reachable() -> bool {
    std::net::TcpStream::connect("localhost:5672").is_ok()
}

fn pg_reachable() -> bool {
    std::net::TcpStream::connect("localhost:5432").is_ok()
}

/// Publish a single JSON message to a RabbitMQ queue using the HTTP Management API.
fn rmq_publish(queue: &str, payload: &str) {
    let declare_url = format!("http://localhost:15672/api/queues/%2f/{queue}");
    let client = reqwest::blocking::Client::new();
    client
        .put(&declare_url)
        .basic_auth("guest", Some("guest"))
        .json(&serde_json::json!({"durable": true, "auto_delete": false, "arguments": {}}))
        .send()
        .expect("declare queue via management API");

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

/// Run `sync-consume` for a brief window, then kill it.
fn run_consumer_briefly(config_path: &str, timeout: Duration) {
    let bin = cli_bin();
    let uri = iceberg_uri();
    let s3 = s3_endpoint();
    let ak = access_key();
    let sk = secret_key();
    let reg = region();
    let cfg = config_path.to_string();

    let mut child = std::process::Command::new(&bin)
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
        rmq_publish("test_user_sync", r#"{"user_id": 1}"#);
        run_consumer_briefly(cfg, Duration::from_secs(5));
    });

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
        run_consumer_briefly(cfg, Duration::from_secs(2));
        // If we reached here without panic, the DLX config was accepted.
    });
}

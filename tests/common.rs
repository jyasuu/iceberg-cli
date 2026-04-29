//! Shared test helpers for iceberg-cli integration tests.
//!
//! Import with:
//!   mod common;
//!   use common::*;
#![allow(dead_code)]

use std::process::{Command, Output};
use std::time::Duration;

// ── Environment helpers ───────────────────────────────────────────────────────

pub fn cli_bin() -> String {
    let manifest = std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".into());
    format!("{manifest}/target/release/iceberg-cli")
}

pub fn iceberg_uri() -> String {
    std::env::var("ICEBERG_URI").unwrap_or_else(|_| "http://localhost:8181".into())
}
pub fn s3_endpoint() -> String {
    std::env::var("S3_ENDPOINT").unwrap_or_else(|_| "http://localhost:9000".into())
}
pub fn access_key() -> String {
    std::env::var("AWS_ACCESS_KEY_ID").unwrap_or_else(|_| "admin".into())
}
pub fn secret_key() -> String {
    std::env::var("AWS_SECRET_ACCESS_KEY").unwrap_or_else(|_| "password".into())
}
pub fn region() -> String {
    std::env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".into())
}
pub fn pg_dsn() -> String {
    std::env::var("PG_DSN").unwrap_or_else(|_| {
        "host=localhost port=5432 dbname=shop user=app password=secret sslmode=disable".into()
    })
}
pub fn rabbitmq_uri() -> String {
    std::env::var("RABBITMQ_URI").unwrap_or_else(|_| "amqp://guest:guest@localhost:5672/%2f".into())
}

// ── CLI runner ────────────────────────────────────────────────────────────────

pub fn cli(args: &[&str]) -> Output {
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

// ── Assertion helpers ─────────────────────────────────────────────────────────

#[track_caller]
pub fn assert_ok(label: &str, args: &[&str]) -> String {
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

#[track_caller]
pub fn assert_output(label: &str, needle: &str, args: &[&str]) {
    let stdout = assert_ok(label, args);
    assert!(
        stdout.contains(needle),
        "[{label}] expected '{needle}' in output\nstdout: {stdout}"
    );
}

#[track_caller]
pub fn assert_fail(label: &str, args: &[&str]) {
    let out = cli(args);
    assert!(
        !out.status.success(),
        "[{label}] expected failure but command succeeded\nstdout: {}",
        String::from_utf8_lossy(&out.stdout)
    );
}

// ── Infrastructure helpers ────────────────────────────────────────────────────

pub fn wait_for_http(url: &str, timeout: Duration) {
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

pub fn with_sync_config<F: FnOnce(&str)>(yaml: &str, f: F) {
    let mut file = tempfile::NamedTempFile::new().expect("tempfile");
    use std::io::Write;
    write!(file, "{yaml}").unwrap();
    f(file.path().to_str().unwrap());
}

pub fn preflight() {
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

/// Parse "N rows shown" from the tail of a `scan` stdout.
pub fn scan_row_count(full_table: &str) -> usize {
    let out = assert_ok(
        &format!("scan {full_table}"),
        &["scan", "--table", full_table, "--limit", "99999"],
    );
    out.lines()
        .rev()
        .find_map(|line| {
            let trimmed = line.trim().trim_start_matches('(');
            trimmed
                .split_once(" rows shown")
                .and_then(|(n, _)| n.trim().parse::<usize>().ok())
        })
        .unwrap_or(0)
}

// ── Postgres helper ───────────────────────────────────────────────────────────

pub fn pg_exec(sql: &str) {
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

// ── Fixed namespace ───────────────────────────────────────────────────────────

pub const NS: &str = "itest";

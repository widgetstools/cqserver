//! D4/P0.4 — backup/restore round-trip smoke test.
//!
//! Proves `scripts/backup-cqserver.sh` and `scripts/restore-cqserver.sh`
//! actually work end-to-end against a real server, not just that they
//! parse: start a server, seed N rows on a persistent topic, back it up
//! quiesce-free (force-rotate + copy + TxLogReader verify), stop the
//! server, restore into a FRESH data dir, start a verification server on
//! it (via `restore-cqserver.sh --start-server`), and assert the restored
//! `rowCount` for the seeded topic exactly matches the pre-backup count.
//!
//! This is intentionally a real subprocess test (spawns `bash`, which
//! itself spawns a second `cqserver`), not a unit test against the
//! scripts' internals — the whole point of D4 is proving the *procedure*
//! works end to end: admin API force-rotate, filesystem copy, TxLogReader
//! replay-verify, and a cold restart on the restored directory. Runs in
//! well under 30s locally, so it lives in the default lane (`cargo test`)
//! rather than being gated behind `#[ignore]` / nightly-tests.sh.

use cq_client::Client;
use cq_e2e_tests::{start_server, TopicSpec};
use serde_json::{json, Value};
use std::path::PathBuf;
use std::process::Command;

const ROWS_TO_SEED: usize = 500;

/// Path to this workspace's root (parent of `crates/`), used to locate
/// `scripts/*.sh` and the release binary the scripts shell out to.
fn workspace_root() -> PathBuf {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest_dir
        .parent() // crates/
        .and_then(|p| p.parent()) // workspace root
        .expect("workspace root")
        .to_path_buf()
}

fn run_script(script: &str, args: &[&str]) -> std::process::Output {
    let root = workspace_root();
    let script_path = root.join("scripts").join(script);
    assert!(
        script_path.exists(),
        "missing script: {}",
        script_path.display()
    );
    Command::new("bash")
        .arg(&script_path)
        .args(args)
        .output()
        .unwrap_or_else(|e| panic!("failed to spawn {script}: {e}"))
}

fn assert_success(label: &str, output: &std::process::Output) {
    if !output.status.success() {
        panic!(
            "{label} exited with {:?}\n--- stdout ---\n{}\n--- stderr ---\n{}",
            output.status.code(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
    }
}

/// Parse the `RESTORE-ROWS` column for `topic` out of
/// restore-cqserver.sh's printed verification table (stderr), e.g.:
///   /backuptest                          500            500 OK
fn parse_restored_row_count(stderr: &str, topic: &str) -> Option<u64> {
    for line in stderr.lines() {
        let trimmed = line.trim_start();
        if trimmed.starts_with(topic) {
            let cols: Vec<&str> = trimmed.split_whitespace().collect();
            // cols[0] = topic, cols[1] = restored rows, cols[2] = backed-up entries, cols[3..] = status
            if cols.len() >= 2 {
                return cols[1].parse::<u64>().ok();
            }
        }
    }
    None
}

/// Rewrite `tcp_addr` / `websocket_addr` / `admin_addr` top-level keys in
/// a generated config TOML so a verification server spawned at a
/// specific admin port doesn't collide with the original (already
/// stopped, but be defensive) server or other concurrently running
/// tests.
fn rewrite_ports(toml: &str, admin_port: u16) -> String {
    let tcp_port = admin_port - 2;
    let ws_port = admin_port - 1;
    let mut out = String::new();
    for line in toml.lines() {
        if line.starts_with("tcp_addr") {
            out.push_str(&format!(r#"tcp_addr = "127.0.0.1:{tcp_port}""#));
        } else if line.starts_with("websocket_addr") {
            out.push_str(&format!(r#"websocket_addr = "127.0.0.1:{ws_port}""#));
        } else if line.starts_with("admin_addr") {
            out.push_str(&format!(r#"admin_addr = "127.0.0.1:{admin_port}""#));
        } else {
            out.push_str(line);
        }
        out.push('\n');
    }
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn backup_restore_round_trip_preserves_row_count() {
    let root = workspace_root();
    let release_binary = root.join("target/release/cqserver");
    assert!(
        release_binary.exists(),
        "release binary not found at {} — run `cargo build --release -p cq-server` first",
        release_binary.display()
    );

    // --- 1. Start a server with one persistent topic and seed rows ----
    let topic = TopicSpec::new("/backuptest", "k")
        .with_inline_columns([("k", "string"), ("v", "long")])
        .with_persist();
    let topic = TopicSpec {
        initial_capacity: ROWS_TO_SEED * 2,
        ..topic
    };
    let mut server = start_server(vec![topic]).await;

    let client = Client::connect(&server.tcp_url())
        .await
        .expect("connect to seed server");
    for i in 0..ROWS_TO_SEED {
        client
            .publish(
                "/backuptest",
                json!({ "k": format!("k{i:05}"), "v": i as i64 }),
            )
            .await
            .unwrap_or_else(|e| panic!("publish row {i}: {e}"));
    }

    let topics_before: Value = reqwest::Client::new()
        .get(format!("{}/topics", server.admin_url()))
        .send()
        .await
        .expect("GET /topics before backup")
        .json()
        .await
        .expect("parse /topics JSON");
    let row_count_before = topics_before[0]["rowCount"]
        .as_u64()
        .expect("rowCount before backup");
    assert_eq!(
        row_count_before, ROWS_TO_SEED as u64,
        "sanity: seeded row count should match what we published"
    );

    // config_dir is `<root>/config`; the harness always lays out
    // `<root>/txlog` as the txlog directory (see `start_server_with` in
    // cq-e2e-tests/src/lib.rs) — mirror that here since ServerHandle
    // doesn't expose the harness's `root()` directly (that's only on
    // `KeptDir`).
    let server_root = server
        .config_dir
        .parent()
        .expect("config_dir has a parent")
        .to_path_buf();
    let data_dir = server_root.join("txlog");
    assert!(data_dir.is_dir(), "expected txlog dir at {data_dir:?}");
    let config_template = server.config_dir.join("cqserver.toml");
    assert!(
        config_template.exists(),
        "expected config template at {config_template:?}"
    );
    let template_contents =
        std::fs::read_to_string(&config_template).expect("read config template");

    let backup_dir = tempfile::tempdir().expect("backup tempdir");
    let backup_archive = backup_dir.path().join("backup.tar.gz");

    // --- 2. Back up the live server (quiesce-free) ---------------------
    let backup_out = run_script(
        "backup-cqserver.sh",
        &[
            "--admin-url",
            &server.admin_url(),
            "--data-dir",
            data_dir.to_str().unwrap(),
            "--output",
            backup_archive.to_str().unwrap(),
        ],
    );
    assert_success("backup-cqserver.sh", &backup_out);
    assert!(
        backup_archive.exists(),
        "backup archive was not created at {backup_archive:?}"
    );
    let backup_stderr = String::from_utf8_lossy(&backup_out.stderr);
    assert!(
        backup_stderr.contains("backup-cqserver.sh: OK"),
        "backup script did not report OK:\n{backup_stderr}"
    );
    assert!(
        backup_stderr.contains("MAXSEQ"),
        "expected manifest table (with MAXSEQ column) in backup output:\n{backup_stderr}"
    );

    // --- 3. Stop the server (graceful — fsyncs) -------------------------
    drop(client);
    let status = server
        .send_sigterm_and_wait(std::time::Duration::from_secs(15))
        .expect("server exited after SIGTERM");
    assert!(status.success(), "server exited uncleanly: {status:?}");

    // --- 4. Restore into a FRESH directory + start a verification ------
    //     server pointed at it via --start-server. Ports are rewritten
    //     in a scratch copy of the config template so the verification
    //     server doesn't collide with anything.
    let restore_dir = tempfile::tempdir().expect("restore tempdir");
    let restored_data_dir = restore_dir.path().join("txlog-restored");

    let verify_admin_port = 29500u16 + ((std::process::id() as u16) % 4000);
    let verify_admin_url = format!("http://127.0.0.1:{verify_admin_port}");
    let port_rewritten = rewrite_ports(&template_contents, verify_admin_port);
    let port_rewritten_path = restore_dir.path().join("verify-config.toml");
    std::fs::write(&port_rewritten_path, port_rewritten).expect("write port-rewritten config");

    let restore_out = run_script(
        "restore-cqserver.sh",
        &[
            "--backup",
            backup_archive.to_str().unwrap(),
            "--data-dir",
            restored_data_dir.to_str().unwrap(),
            "--start-server",
            "--binary",
            release_binary.to_str().unwrap(),
            "--config-template",
            port_rewritten_path.to_str().unwrap(),
            "--admin-url",
            &verify_admin_url,
        ],
    );
    assert_success("restore-cqserver.sh", &restore_out);
    let restore_stderr = String::from_utf8_lossy(&restore_out.stderr);
    assert!(
        restore_stderr.contains("restore-cqserver.sh: OK"),
        "restore script did not report OK:\n{restore_stderr}"
    );

    let row_count_after = parse_restored_row_count(&restore_stderr, "/backuptest")
        .unwrap_or_else(|| panic!("could not parse restored row count from:\n{restore_stderr}"));

    assert_eq!(
        row_count_after, row_count_before,
        "restored row count ({row_count_after}) does not match backed-up row count \
         ({row_count_before}) — backup/restore round trip lost or duplicated rows.\n\
         restore-cqserver.sh output:\n{restore_stderr}"
    );

    eprintln!(
        "backup/restore round trip OK: {row_count_before} rows backed up, \
         {row_count_after} rows restored and verified (topic /backuptest)"
    );
}

//! Smoke harness for the Q2 / Q11 loadgen scenarios. Marked
//! `#[ignore]` because they need a running server and produce
//! timing-sensitive output; run on demand with
//!
//!   cargo test --release -p cq-e2e-tests --test loadgen_scenarios -- --ignored --nocapture
//!
//! The point of these tests is to keep the loadgen wiring honest:
//! if a scenario stops compiling or its API drifts away from
//! `cq-loadgen`'s, this test catches it.

use cq_e2e_tests::{start_server, TopicSpec};

#[tokio::test]
#[ignore]
async fn scenario_h_publish_batch_vs_sequential_smoke() {
    let topic = TopicSpec::new("/loadgen_h", "k")
        .with_inline_columns([("k", "string"), ("v", "long")]);
    let server = start_server(vec![topic]).await;

    let cfg = cq_loadgen::ScenarioConfig {
        server_url: server.tcp_url(),
        topic: "/loadgen_h".into(),
        publish_rate: 5_000.0, // total rows
        warmup: std::time::Duration::from_secs(50), // batch size
        duration: std::time::Duration::ZERO,
        subscribers: 0,
        admin_url: format!("http://127.0.0.1:{}", server.admin_port),
        payload_bytes: 0,
        wide_rows: 0,
        wide_cols: 0,
    };
    let report = cq_loadgen::scenarios::publish_batch_vs_sequential(&cfg)
        .await
        .expect("scenario H");
    report.print();
    assert!(report.total_rows == 5_000);
    assert!(report.batch_size == 50);
    assert!(report.speedup > 0.0);
}

#[tokio::test]
#[ignore]
async fn scenario_i_schema_evolution_under_load_smoke() {
    let topic = TopicSpec::new("/loadgen_i", "k")
        .with_inline_columns([("k", "string"), ("v", "long")]);
    let server = start_server(vec![topic]).await;

    let cfg = cq_loadgen::ScenarioConfig {
        server_url: server.tcp_url(),
        topic: "/loadgen_i".into(),
        publish_rate: 500.0,
        warmup: std::time::Duration::ZERO,
        duration: std::time::Duration::from_secs(12), // 2 add-column events
        subscribers: 0,
        admin_url: format!("http://127.0.0.1:{}", server.admin_port),
        payload_bytes: 0,
        wide_rows: 0,
        wide_cols: 0,
    };
    let report = cq_loadgen::scenarios::schema_evolution_under_load(&cfg)
        .await
        .expect("scenario I");
    report.print();
    // At least 1 column added during the 12s window (every 5s).
    assert!(report.columns_added >= 1, "expected ≥1 column added");
    // Publishes mostly succeed (we tolerate transient blips during
    // the schema swap; require ≥ 95 %).
    let succ = report.publishes_acked as f64;
    let issued = report.publishes_issued.max(1) as f64;
    assert!(
        succ / issued >= 0.95,
        "ack ratio {:.2} below 95% threshold",
        succ / issued
    );
}

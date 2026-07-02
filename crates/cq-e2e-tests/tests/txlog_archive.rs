//! e2e: sealed txlog segments move to the configured archive dir;
//! recovery on restart reads both live + archive so SOW state is
//! identical to a pre-restart query.

use cq_client::Client;
use cq_e2e_tests::{
    restart_kept, start_server_with, stop_keeping_dir, ServerOpts, TopicSpec,
    TxLogArchiveOpts,
};
use serde_json::json;
use std::time::Duration;

#[tokio::test]
async fn archived_segments_replay_on_restart() {
    let topic = TopicSpec::new("/arch-trades", "k")
        .with_inline_columns([("k", "string"), ("v", "double")])
        .with_persist();
    let server = start_server_with(
        vec![topic],
        ServerOpts {
            outbound_queue_capacity: 1024,
            slow_consumer: None,
            tls: None,
            queues: Vec::new(),
            auth: None,
            // Tiny segment so a handful of publishes cause rotation.
            txlog_archive: Some(TxLogArchiveOpts::new(256)),
            views: Vec::new(),
            spillover: None,
            logging_sinks: Vec::new(),
            replication: None,
            hard_max_sow_result_rows: None,
            admin_token: None,
            admin_tls: None,
        },
    )
    .await;

    let client = Client::connect(&server.tcp_url()).await.expect("conn");
    // Each publish is ~50-80 bytes encoded; 30 publishes blows past
    // the 256-byte segment several times.
    let n = 30;
    for i in 0..n {
        client
            .publish(
                "/arch-trades",
                json!({ "k": format!("k{i:03}"), "v": i as f64 }),
            )
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(150)).await;

    let pre_rows = client.sow("/arch-trades", None).await.expect("pre-sow");
    assert_eq!(pre_rows.len(), n);
    drop(client);

    let kept = stop_keeping_dir(server).await;
    let server2 = restart_kept(kept).await;
    let client2 = Client::connect(&server2.tcp_url()).await.unwrap();
    let post_rows = client2.sow("/arch-trades", None).await.expect("post-sow");
    assert_eq!(
        post_rows.len(),
        n,
        "recovery dropped rows: pre={n}, post={} (segments may have failed to archive correctly)",
        post_rows.len()
    );
}

// ───── Diversification ────────────────────────────────────────────

/// Restart with NO archived segments yet (publishes never crossed
/// the rotation boundary) — recovery still reads live log.
#[tokio::test]
async fn restart_with_no_archived_segments_recovers_from_live_log() {
    let topic = TopicSpec::new("/arch-tiny", "k")
        .with_inline_columns([("k", "string"), ("v", "long")])
        .with_persist();
    let server = start_server_with(
        vec![topic],
        ServerOpts {
            outbound_queue_capacity: 1024,
            slow_consumer: None,
            tls: None,
            queues: Vec::new(),
            auth: None,
            // Very large segment so no rotation happens.
            txlog_archive: Some(TxLogArchiveOpts::new(10 * 1024 * 1024)),
            views: Vec::new(),
            spillover: None,
            logging_sinks: Vec::new(),
            replication: None,
            hard_max_sow_result_rows: None,
            admin_token: None,
            admin_tls: None,
        },
    )
    .await;

    let client = Client::connect(&server.tcp_url()).await.unwrap();
    for i in 0..5 {
        client
            .publish("/arch-tiny", json!({ "k": format!("k{i}"), "v": i }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    drop(client);

    let kept = stop_keeping_dir(server).await;
    let server2 = restart_kept(kept).await;
    let client2 = Client::connect(&server2.tcp_url()).await.unwrap();
    let rows = client2.sow("/arch-tiny", None).await.unwrap();
    assert_eq!(rows.len(), 5);
}

/// Restart with a mix of archived + live segments — recovery reads
/// both and produces complete state.
#[tokio::test]
async fn restart_mixes_archived_and_live_segments() {
    let topic = TopicSpec::new("/arch-mix", "k")
        .with_inline_columns([("k", "string"), ("v", "long")])
        .with_persist();
    let server = start_server_with(
        vec![topic],
        ServerOpts {
            outbound_queue_capacity: 1024,
            slow_consumer: None,
            tls: None,
            queues: Vec::new(),
            auth: None,
            // Mid-size segment — first ~10 rows fit in live, then rotate.
            txlog_archive: Some(TxLogArchiveOpts::new(512)),
            views: Vec::new(),
            spillover: None,
            logging_sinks: Vec::new(),
            replication: None,
            hard_max_sow_result_rows: None,
            admin_token: None,
            admin_tls: None,
        },
    )
    .await;

    let client = Client::connect(&server.tcp_url()).await.unwrap();
    for i in 0..20 {
        client
            .publish("/arch-mix", json!({ "k": format!("k{i:03}"), "v": i }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(150)).await;
    drop(client);

    let kept = stop_keeping_dir(server).await;
    let server2 = restart_kept(kept).await;
    let client2 = Client::connect(&server2.tcp_url()).await.unwrap();
    let rows = client2.sow("/arch-mix", None).await.unwrap();
    assert_eq!(rows.len(), 20, "mix of archived + live: {} rows", rows.len());

    // Verify no row was duplicated.
    let mut ks: Vec<String> = rows
        .iter()
        .map(|r| r.get("k").unwrap().as_str().unwrap().to_string())
        .collect();
    ks.sort();
    ks.dedup();
    assert_eq!(ks.len(), 20);
}

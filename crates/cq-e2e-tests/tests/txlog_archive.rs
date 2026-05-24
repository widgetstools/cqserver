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

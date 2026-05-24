//! e2e: zstd-compressed archived segments survive a restart.

use cq_client::Client;
use cq_e2e_tests::{
    restart_kept, start_server_with, stop_keeping_dir, ServerOpts, TopicSpec,
    TxLogArchiveOpts,
};
use serde_json::json;
use std::time::Duration;

#[tokio::test]
async fn compressed_archive_segments_replay_on_restart() {
    let topic = TopicSpec::new("/zarch", "k")
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
            txlog_archive: Some(TxLogArchiveOpts::new(256).with_compression()),
            views: Vec::new(),
            spillover: None,
            logging_sinks: Vec::new(),
        },
    )
    .await;

    let client = Client::connect(&server.tcp_url()).await.expect("conn");
    let n = 40;
    for i in 0..n {
        client
            .publish("/zarch", json!({ "k": format!("k{i:04}"), "v": i as f64 }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(150)).await;
    drop(client);

    let kept = stop_keeping_dir(server).await;
    let server2 = restart_kept(kept).await;
    let client2 = Client::connect(&server2.tcp_url()).await.unwrap();
    let rows = client2.sow("/zarch", None).await.expect("sow");
    assert_eq!(
        rows.len(),
        n,
        "recovery from compressed archive lost rows: pre={n}, post={}",
        rows.len()
    );
}

//! P8 e2e — STDDEV / STDDEV_SAMP / VARIANCE / VAR_SAMP aggregates.

use cq_client::Client;
use cq_e2e_tests::{start_server, TopicSpec};
use serde_json::json;
use std::time::Duration;

#[tokio::test]
async fn stddev_variance_match_known_values() {
    let topic = TopicSpec::new("/stddev-data", "k").with_inline_columns([
        ("k", "string"),
        ("v", "double"),
    ]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    // Wikipedia stddev example: {2,4,4,4,5,5,7,9} → pop stddev = 2.
    let values = [2.0_f64, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0];
    for (i, v) in values.iter().enumerate() {
        client
            .publish("/stddev-data", json!({ "k": format!("r{i}"), "v": v }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(150)).await;

    let row = client
        .sow_sql(
            "/stddev-data",
            "SELECT STDDEV(v) AS sd, STDDEV_SAMP(v) AS ss, \
                    VARIANCE(v) AS vp, VAR_SAMP(v) AS vs FROM t",
        )
        .await
        .expect("stddev sow")
        .pop()
        .expect("one row");
    let sd = row.get("sd").and_then(|x| x.as_f64()).unwrap();
    let ss = row.get("ss").and_then(|x| x.as_f64()).unwrap();
    let vp = row.get("vp").and_then(|x| x.as_f64()).unwrap();
    let vs = row.get("vs").and_then(|x| x.as_f64()).unwrap();
    assert!((sd - 2.0).abs() < 1e-9, "pop stddev: got {sd}");
    assert!((ss - (32.0_f64 / 7.0).sqrt()).abs() < 1e-9, "samp stddev: got {ss}");
    assert!((vp - 4.0).abs() < 1e-9, "pop variance: got {vp}");
    assert!((vs - (32.0_f64 / 7.0)).abs() < 1e-9, "samp variance: got {vs}");
}

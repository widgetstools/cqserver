//! P9 e2e — PERCENTILE_CONT(col, q) + MEDIAN(col).

use cq_client::Client;
use cq_e2e_tests::{start_server, TopicSpec};
use serde_json::json;
use std::time::Duration;

#[tokio::test]
async fn percentile_cont_and_median_match_known_values() {
    let topic = TopicSpec::new("/pct-data", "k").with_inline_columns([
        ("k", "string"),
        ("v", "double"),
    ]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    // {2,4,4,4,5,5,7,9} — median is midpoint of 4 and 5 = 4.5.
    for (i, v) in [2.0_f64, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0]
        .iter()
        .enumerate()
    {
        client
            .publish("/pct-data", json!({ "k": format!("r{i}"), "v": v }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(150)).await;

    let row = client
        .sow_sql(
            "/pct-data",
            "SELECT MEDIAN(v) AS m, \
                    PERCENTILE_CONT(v, 0.5) AS p50, \
                    PERCENTILE_CONT(v, 0.95) AS p95 FROM t",
        )
        .await
        .expect("percentile sow")
        .pop()
        .expect("one row");
    let m = row.get("m").and_then(|x| x.as_f64()).unwrap();
    let p50 = row.get("p50").and_then(|x| x.as_f64()).unwrap();
    let p95 = row.get("p95").and_then(|x| x.as_f64()).unwrap();
    assert!((m - 4.5).abs() < 1e-9, "median expected 4.5, got {m}");
    assert!((p50 - 4.5).abs() < 1e-9);
    // p95 = interp at rank 6.65 between values[6]=7 and values[7]=9
    //     = 7 + 0.65 * (9 - 7) = 8.3
    assert!((p95 - 8.3).abs() < 1e-9, "p95 expected 8.3, got {p95}");
}

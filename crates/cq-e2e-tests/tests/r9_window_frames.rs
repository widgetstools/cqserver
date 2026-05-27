//! R9 — aggregate-OVER-window with `ROWS BETWEEN k PRECEDING AND
//! CURRENT ROW` frames. The canonical AMPS pattern for rolling
//! N-trade slippage, rolling fee average, cumulative VWAP, etc.

use cq_client::Client;
use cq_e2e_tests::{start_server, TopicSpec};
use serde_json::json;
use std::time::Duration;

#[tokio::test]
async fn rolling_avg_over_n_preceding_rows() {
    // 5 rows; rolling AVG over 2 PRECEDING + current row (3-row window).
    let topic = TopicSpec::new("/r9_avg", "k")
        .with_inline_columns([("k", "string"), ("seq", "long"), ("v", "double")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");
    for (k, seq, v) in [
        ("a", 1_i64, 10.0_f64),
        ("b", 2, 20.0),
        ("c", 3, 30.0),
        ("d", 4, 40.0),
        ("e", 5, 50.0),
    ] {
        client
            .publish("/r9_avg", json!({ "k": k, "seq": seq, "v": v }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    let rows = client
        .sow_sql(
            "/r9_avg",
            "SELECT seq, AVG(v) OVER (ORDER BY seq ASC \
                                       ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS roll \
             FROM t ORDER BY seq ASC",
        )
        .await
        .expect("AVG OVER (ROWS BETWEEN N PRECEDING) must compile");
    assert_eq!(rows.len(), 5);
    let rolls: Vec<f64> = rows
        .iter()
        .map(|r| r.get("roll").unwrap().as_f64().unwrap())
        .collect();
    // pos 0: [10]                → 10
    // pos 1: [10, 20]            → 15
    // pos 2: [10, 20, 30]        → 20
    // pos 3: [20, 30, 40]        → 30
    // pos 4: [30, 40, 50]        → 40
    assert_eq!(rolls, vec![10.0, 15.0, 20.0, 30.0, 40.0]);
}

#[tokio::test]
async fn rolling_sum_with_partition() {
    let topic = TopicSpec::new("/r9_sum_part", "k")
        .with_inline_columns([("k", "string"), ("desk", "string"), ("seq", "long"), ("v", "double")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    for (k, desk, seq, v) in [
        ("a1", "FX", 1_i64, 1.0_f64),
        ("a2", "FX", 2, 2.0),
        ("a3", "FX", 3, 3.0),
        ("b1", "RATES", 1, 100.0),
        ("b2", "RATES", 2, 200.0),
    ] {
        client
            .publish("/r9_sum_part", json!({ "k": k, "desk": desk, "seq": seq, "v": v }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    let rows = client
        .sow_sql(
            "/r9_sum_part",
            "SELECT desk, seq, SUM(v) OVER (PARTITION BY desk ORDER BY seq ASC \
                                             ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS sum2 \
             FROM t",
        )
        .await
        .unwrap();
    let by_key: std::collections::HashMap<(String, i64), f64> = rows
        .iter()
        .map(|r| {
            (
                (
                    r.get("desk").unwrap().as_str().unwrap().to_string(),
                    r.get("seq").unwrap().as_i64().unwrap(),
                ),
                r.get("sum2").unwrap().as_f64().unwrap(),
            )
        })
        .collect();
    // FX 1: [1]      → 1
    // FX 2: [1,2]    → 3
    // FX 3: [2,3]    → 5
    // RATES 1: [100]    → 100
    // RATES 2: [100,200]→ 300
    assert_eq!(by_key[&("FX".into(), 1)], 1.0);
    assert_eq!(by_key[&("FX".into(), 2)], 3.0);
    assert_eq!(by_key[&("FX".into(), 3)], 5.0);
    assert_eq!(by_key[&("RATES".into(), 1)], 100.0);
    assert_eq!(by_key[&("RATES".into(), 2)], 300.0);
}

#[tokio::test]
async fn rolling_min_and_max() {
    let topic = TopicSpec::new("/r9_minmax", "k")
        .with_inline_columns([("k", "string"), ("seq", "long"), ("v", "double")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");
    for (k, seq, v) in [
        ("a", 1_i64, 5.0_f64),
        ("b", 2, 3.0),
        ("c", 3, 8.0),
        ("d", 4, 1.0),
        ("e", 5, 7.0),
    ] {
        client
            .publish("/r9_minmax", json!({ "k": k, "seq": seq, "v": v }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    let rows = client
        .sow_sql(
            "/r9_minmax",
            "SELECT seq, \
                    MIN(v) OVER (ORDER BY seq ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS lo, \
                    MAX(v) OVER (ORDER BY seq ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS hi \
             FROM t ORDER BY seq ASC",
        )
        .await
        .unwrap();
    let los: Vec<f64> = rows.iter().map(|r| r.get("lo").unwrap().as_f64().unwrap()).collect();
    let his: Vec<f64> = rows.iter().map(|r| r.get("hi").unwrap().as_f64().unwrap()).collect();
    // Window of last 3 rows:
    // pos 0: [5]        → min 5, max 5
    // pos 1: [5,3]      → min 3, max 5
    // pos 2: [5,3,8]    → min 3, max 8
    // pos 3: [3,8,1]    → min 1, max 8
    // pos 4: [8,1,7]    → min 1, max 8
    assert_eq!(los, vec![5.0, 3.0, 3.0, 1.0, 1.0]);
    assert_eq!(his, vec![5.0, 5.0, 8.0, 8.0, 8.0]);
}

#[tokio::test]
async fn cumulative_sum_via_unbounded_preceding() {
    // No frame clause defaults to `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`
    // → running cumulative sum.
    let topic = TopicSpec::new("/r9_cum", "k")
        .with_inline_columns([("k", "string"), ("seq", "long"), ("v", "long")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");
    for (k, seq, v) in [
        ("a", 1_i64, 10_i64),
        ("b", 2, 20),
        ("c", 3, 30),
    ] {
        client
            .publish("/r9_cum", json!({ "k": k, "seq": seq, "v": v }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    let rows = client
        .sow_sql(
            "/r9_cum",
            "SELECT seq, \
                    SUM(v) OVER (ORDER BY seq ASC \
                                  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running \
             FROM t ORDER BY seq ASC",
        )
        .await
        .unwrap();
    let runs: Vec<f64> = rows.iter().map(|r| r.get("running").unwrap().as_f64().unwrap()).collect();
    assert_eq!(runs, vec![10.0, 30.0, 60.0]);
}

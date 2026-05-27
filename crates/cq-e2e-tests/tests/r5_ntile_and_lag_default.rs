//! R5 — `NTILE(n)` window function + `LAG/LEAD(col, n, default)`
//! 3-arg form. AMPS uses NTILE for slippage-quartile / fee-quintile
//! reports; LAG/LEAD-with-default is the AMPS-style "previous price,
//! zero if none" tape pattern.

use cq_client::Client;
use cq_e2e_tests::{start_server, TopicSpec};
use serde_json::json;
use std::time::Duration;

#[tokio::test]
async fn ntile_distributes_rows_into_n_buckets() {
    // 10 rows, 4 buckets → first 2 get 3 rows each, last 2 get 2 rows each
    // (10 / 4 = 2 base, 2 extras → buckets 1+2 get 3, buckets 3+4 get 2).
    let topic = TopicSpec::new("/r5-ntile", "k")
        .with_inline_columns([("k", "string"), ("v", "long")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");
    for i in 1..=10_i64 {
        client
            .publish("/r5-ntile", json!({ "k": format!("k{i:02}"), "v": i }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    let rows = client
        .sow_sql(
            "/r5-ntile",
            "SELECT k, v, NTILE(4) OVER (ORDER BY v ASC) AS bucket FROM t ORDER BY v ASC",
        )
        .await
        .expect("NTILE must compile");
    assert_eq!(rows.len(), 10);
    let buckets: Vec<u64> = rows
        .iter()
        .map(|r| r.get("bucket").unwrap().as_u64().unwrap())
        .collect();
    // Sorted by v ASC → buckets are: 1,1,1,2,2,2,3,3,4,4
    assert_eq!(buckets, vec![1, 1, 1, 2, 2, 2, 3, 3, 4, 4]);
}

#[tokio::test]
async fn ntile_with_partition() {
    // 2 desks × 4 rows each, NTILE(2) per desk → each desk: 2 rows in 1, 2 in 2.
    let topic = TopicSpec::new("/r5-ntile-part", "k")
        .with_inline_columns([("k", "string"), ("desk", "string"), ("v", "long")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    for (k, desk, v) in [
        ("r1", "FX", 10_i64), ("r2", "FX", 20), ("r3", "FX", 30), ("r4", "FX", 40),
        ("r5", "RATES", 100), ("r6", "RATES", 200), ("r7", "RATES", 300), ("r8", "RATES", 400),
    ] {
        client
            .publish("/r5-ntile-part", json!({ "k": k, "desk": desk, "v": v }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    let rows = client
        .sow_sql(
            "/r5-ntile-part",
            "SELECT desk, v, NTILE(2) OVER (PARTITION BY desk ORDER BY v ASC) AS bucket FROM t",
        )
        .await
        .unwrap();
    // Per partition: first 2 → bucket 1, last 2 → bucket 2.
    let by_v: std::collections::HashMap<i64, u64> = rows
        .iter()
        .map(|r| {
            (
                r.get("v").unwrap().as_i64().unwrap(),
                r.get("bucket").unwrap().as_u64().unwrap(),
            )
        })
        .collect();
    assert_eq!(by_v[&10], 1);
    assert_eq!(by_v[&20], 1);
    assert_eq!(by_v[&30], 2);
    assert_eq!(by_v[&40], 2);
    assert_eq!(by_v[&100], 1);
    assert_eq!(by_v[&200], 1);
    assert_eq!(by_v[&300], 2);
    assert_eq!(by_v[&400], 2);
}

#[tokio::test]
async fn lag_with_default_zero() {
    // `LAG(v, 1, 0)` — first row of each partition gets 0 instead of null.
    let topic = TopicSpec::new("/r5-lag-zero", "k")
        .with_inline_columns([("k", "string"), ("v", "long")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");
    for i in 1..=3_i64 {
        client
            .publish("/r5-lag-zero", json!({ "k": format!("k{i}"), "v": i * 10 }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    let rows = client
        .sow_sql(
            "/r5-lag-zero",
            "SELECT v, LAG(v, 1, 0) OVER (ORDER BY v ASC) AS prev FROM t ORDER BY v ASC",
        )
        .await
        .expect("LAG with default must compile");
    assert_eq!(rows.len(), 3);
    let prevs: Vec<i64> = rows
        .iter()
        .map(|r| r.get("prev").unwrap().as_i64().unwrap())
        .collect();
    // First row's LAG falls off the leading edge → default = 0.
    assert_eq!(prevs, vec![0, 10, 20]);
}

#[tokio::test]
async fn lag_with_string_default() {
    let topic = TopicSpec::new("/r5-lag-str", "k")
        .with_inline_columns([("k", "string"), ("side", "string")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");
    for (k, s) in [("r1", "BUY"), ("r2", "SELL"), ("r3", "BUY")] {
        client
            .publish("/r5-lag-str", json!({ "k": k, "side": s }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    let rows = client
        .sow_sql(
            "/r5-lag-str",
            "SELECT k, side, LAG(side, 1, 'NONE') OVER (ORDER BY k ASC) AS prev FROM t ORDER BY k ASC",
        )
        .await
        .unwrap();
    let prev_str: Vec<String> = rows
        .iter()
        .map(|r| r.get("prev").unwrap().as_str().unwrap().to_string())
        .collect();
    assert_eq!(prev_str, vec!["NONE", "BUY", "SELL"]);
}

#[tokio::test]
async fn lead_with_default_zero() {
    let topic = TopicSpec::new("/r5-lead-zero", "k")
        .with_inline_columns([("k", "string"), ("v", "long")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");
    for i in 1..=3_i64 {
        client
            .publish("/r5-lead-zero", json!({ "k": format!("k{i}"), "v": i * 10 }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    let rows = client
        .sow_sql(
            "/r5-lead-zero",
            "SELECT v, LEAD(v, 1, 0) OVER (ORDER BY v ASC) AS next FROM t ORDER BY v ASC",
        )
        .await
        .unwrap();
    let nexts: Vec<i64> = rows
        .iter()
        .map(|r| r.get("next").unwrap().as_i64().unwrap())
        .collect();
    // Last row's LEAD falls off the trailing edge → default = 0.
    assert_eq!(nexts, vec![20, 30, 0]);
}

#[tokio::test]
async fn ntile_invalid_buckets_rejected() {
    use cq_client::ClientError;
    let topic = TopicSpec::new("/r5-ntile-bad", "k")
        .with_inline_columns([("k", "string"), ("v", "long")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");
    client
        .publish("/r5-ntile-bad", json!({ "k": "x", "v": 1 }))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(80)).await;

    // NTILE(0) must error cleanly.
    let r = client
        .sow_sql(
            "/r5-ntile-bad",
            "SELECT NTILE(0) OVER (ORDER BY v) AS b FROM t",
        )
        .await;
    assert!(matches!(r, Err(ClientError::Server(_))), "got {r:?}");
}

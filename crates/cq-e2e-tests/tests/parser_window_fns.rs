//! Q7 e2e — window functions over the wire.

use cq_client::Client;
use cq_e2e_tests::{start_server, TopicSpec};
use serde_json::json;
use std::time::Duration;

#[tokio::test]
async fn row_number_lag_lead_over_wire() {
    let topic = TopicSpec::new("/q7", "k").with_inline_columns([
        ("k", "string"),
        ("sym", "string"),
        ("px", "double"),
    ]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    // AAPL: 100, 150, 200 (sorted ASC)
    // MSFT: 50, 300
    for (k, sym, px) in [
        ("a1", "AAPL", 150.0_f64),
        ("a2", "AAPL", 100.0),
        ("a3", "AAPL", 200.0),
        ("m1", "MSFT", 300.0),
        ("m2", "MSFT", 50.0),
    ] {
        client
            .publish("/q7", json!({ "k": k, "sym": sym, "px": px }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(150)).await;

    let rows = client
        .sow_sql(
            "/q7",
            "SELECT sym, px, \
                    ROW_NUMBER() OVER (PARTITION BY sym ORDER BY px ASC) AS rn, \
                    LAG(px, 1)   OVER (PARTITION BY sym ORDER BY px ASC) AS prev, \
                    LEAD(px, 1)  OVER (PARTITION BY sym ORDER BY px ASC) AS next \
             FROM t",
        )
        .await
        .expect("window sow");
    assert_eq!(rows.len(), 5);
    // Build (sym, px) → (rn, prev, next).
    let mut by_key = std::collections::HashMap::new();
    for row in &rows {
        let sym = row.get("sym").unwrap().as_str().unwrap().to_string();
        let px = row.get("px").unwrap().as_f64().unwrap() as i64;
        let rn = row.get("rn").unwrap().as_u64().unwrap();
        let prev = row.get("prev").cloned();
        let next = row.get("next").cloned();
        by_key.insert((sym, px), (rn, prev, next));
    }
    // AAPL @ 100 → rn=1, prev=null, next=150
    let (rn, prev, next) = &by_key[&("AAPL".to_string(), 100)];
    assert_eq!(*rn, 1);
    assert!(prev.as_ref().unwrap().is_null());
    assert_eq!(next.as_ref().unwrap().as_f64().unwrap(), 150.0);
    // AAPL @ 150 → rn=2, prev=100, next=200
    let (rn, prev, next) = &by_key[&("AAPL".to_string(), 150)];
    assert_eq!(*rn, 2);
    assert_eq!(prev.as_ref().unwrap().as_f64().unwrap(), 100.0);
    assert_eq!(next.as_ref().unwrap().as_f64().unwrap(), 200.0);
    // AAPL @ 200 → rn=3, prev=150, next=null
    let (rn, prev, next) = &by_key[&("AAPL".to_string(), 200)];
    assert_eq!(*rn, 3);
    assert_eq!(prev.as_ref().unwrap().as_f64().unwrap(), 150.0);
    assert!(next.as_ref().unwrap().is_null());
    // MSFT @ 50 → rn=1
    let (rn, _, _) = &by_key[&("MSFT".to_string(), 50)];
    assert_eq!(*rn, 1);
    let (rn, _, _) = &by_key[&("MSFT".to_string(), 300)];
    assert_eq!(*rn, 2);
}

// ───── Diversification ────────────────────────────────────────────

/// RANK + DENSE_RANK with explicit ties — wire path must agree with
/// the proptest-verified semantics.
#[tokio::test]
async fn rank_dense_rank_distinguish_tie_handling() {
    let topic = TopicSpec::new("/q7_rank", "k")
        .with_inline_columns([("k", "string"), ("v", "long")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    // {100,100,200,300} — RANK: 1,1,3,4; DENSE_RANK: 1,1,2,3.
    for (k, v) in [("a", 100), ("b", 100), ("c", 200), ("d", 300)] {
        client
            .publish("/q7_rank", json!({ "k": k, "v": v }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    let rows = client
        .sow_sql(
            "/q7_rank",
            "SELECT k, v, \
                    RANK()       OVER (ORDER BY v ASC) AS r, \
                    DENSE_RANK() OVER (ORDER BY v ASC) AS d \
             FROM t",
        )
        .await
        .unwrap();
    let by_k: std::collections::HashMap<String, (u64, u64)> = rows
        .iter()
        .map(|r| {
            (
                r.get("k").unwrap().as_str().unwrap().to_string(),
                (
                    r.get("r").unwrap().as_u64().unwrap(),
                    r.get("d").unwrap().as_u64().unwrap(),
                ),
            )
        })
        .collect();
    // a and b tied at v=100.
    assert_eq!(by_k["a"], (1, 1));
    assert_eq!(by_k["b"], (1, 1));
    assert_eq!(by_k["c"], (3, 2), "RANK skips to 3 after the tie; DENSE goes 2");
    assert_eq!(by_k["d"], (4, 3));
}

/// Window function over an empty topic returns no rows (not 1 row of NULLs).
#[tokio::test]
async fn window_over_empty_topic_returns_no_rows() {
    let topic = TopicSpec::new("/q7_empty", "k")
        .with_inline_columns([("k", "string"), ("v", "long")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    let rows = client
        .sow_sql(
            "/q7_empty",
            "SELECT k, ROW_NUMBER() OVER (ORDER BY v) AS rn FROM t",
        )
        .await
        .unwrap();
    assert!(rows.is_empty());
}

/// LAG with explicit offset > 1 — offset=2 must skip one row.
#[tokio::test]
async fn lag_with_explicit_offset_two() {
    let topic = TopicSpec::new("/q7_lag2", "k")
        .with_inline_columns([("k", "string"), ("seq", "long")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    for i in 1..=5_i64 {
        client
            .publish("/q7_lag2", json!({ "k": format!("k{i}"), "seq": i }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    let rows = client
        .sow_sql(
            "/q7_lag2",
            "SELECT seq, LAG(seq, 2) OVER (ORDER BY seq ASC) AS lag2 FROM t",
        )
        .await
        .unwrap();
    let by_seq: std::collections::HashMap<i64, Option<i64>> = rows
        .iter()
        .map(|r| {
            (
                r.get("seq").unwrap().as_i64().unwrap(),
                r.get("lag2").and_then(|v| v.as_i64()),
            )
        })
        .collect();
    assert_eq!(by_seq[&1], None, "row 1 has no lag2");
    assert_eq!(by_seq[&2], None, "row 2 has no lag2");
    assert_eq!(by_seq[&3], Some(1));
    assert_eq!(by_seq[&4], Some(2));
    assert_eq!(by_seq[&5], Some(3));
}

//! Q1 e2e — RIGHT OUTER + FULL OUTER JOIN.

use cq_client::Client;
use cq_e2e_tests::{start_server, TopicSpec};
use serde_json::{json, Value};
use std::time::Duration;

#[tokio::test]
async fn right_and_full_outer_join_keep_unmatched_sides() {
    let positions = TopicSpec::new("/pos_q1", "positionKey").with_inline_columns([
        ("positionKey", "string"),
        ("cusip", "string"),
        ("marketValue", "double"),
    ]);
    let securities = TopicSpec::new("/sec_q1", "cusip").with_inline_columns([
        ("cusip", "string"),
        ("sector", "string"),
    ]);
    let server = start_server(vec![positions, securities]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    // Positions: AAPL, MSFT. Securities: AAPL, GOOG.
    // INNER → AAPL only. LEFT → AAPL+MSFT. RIGHT → AAPL+GOOG.
    // FULL → AAPL+MSFT+GOOG.
    for (c, s) in [("AAPL", "Tech"), ("GOOG", "Tech")] {
        client
            .publish("/sec_q1", json!({ "cusip": c, "sector": s }))
            .await
            .unwrap();
    }
    for (k, c, mv) in [("p1", "AAPL", 10_000.0_f64), ("p2", "MSFT", 20_000.0)] {
        client
            .publish(
                "/pos_q1",
                json!({ "positionKey": k, "cusip": c, "marketValue": mv }),
            )
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(200)).await;

    let inner = client
        .sow_sql(
            "/pos_q1",
            "SELECT cusip, marketValue, sector FROM pos_q1 JOIN sec_q1 USING (cusip)",
        )
        .await
        .expect("inner sow");
    assert_eq!(inner.len(), 1, "INNER must keep only AAPL");

    let right = client
        .sow_sql(
            "/pos_q1",
            "SELECT cusip, marketValue, sector FROM pos_q1 RIGHT JOIN sec_q1 USING (cusip)",
        )
        .await
        .expect("right sow");
    let right_by_cusip: std::collections::HashMap<String, Value> = right
        .into_iter()
        .map(|r| {
            (
                r.get("cusip").unwrap().as_str().unwrap().to_string(),
                r.get("marketValue").cloned().unwrap_or(Value::Null),
            )
        })
        .collect();
    assert_eq!(right_by_cusip.len(), 2, "RIGHT must keep AAPL + GOOG");
    assert!(right_by_cusip.contains_key("AAPL"));
    assert!(right_by_cusip.contains_key("GOOG"));
    assert!(
        right_by_cusip["GOOG"].is_null(),
        "GOOG marketValue must be null (right-only)"
    );

    let full = client
        .sow_sql(
            "/pos_q1",
            "SELECT cusip, marketValue, sector FROM pos_q1 \
             FULL OUTER JOIN sec_q1 USING (cusip)",
        )
        .await
        .expect("full sow");
    assert_eq!(full.len(), 3, "FULL must keep AAPL + MSFT + GOOG");
    let cusips: std::collections::HashSet<String> = full
        .iter()
        .map(|r| r.get("cusip").unwrap().as_str().unwrap().to_string())
        .collect();
    assert!(cusips.contains("AAPL"));
    assert!(cusips.contains("MSFT"));
    assert!(cusips.contains("GOOG"));
}

// ───── Diversification ────────────────────────────────────────────

/// RIGHT JOIN with empty left side keeps every right row.
#[tokio::test]
async fn right_outer_with_empty_left_returns_all_right_rows() {
    let l = TopicSpec::new("/q1_rle", "k")
        .with_inline_columns([("k", "string"), ("c", "string")]);
    let r = TopicSpec::new("/q1_rrf", "c")
        .with_inline_columns([("c", "string"), ("tag", "string")]);
    let server = start_server(vec![l, r]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    for c in ["A", "B", "C"] {
        client
            .publish("/q1_rrf", json!({ "c": c, "tag": c }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    let rows = client
        .sow_sql(
            "/q1_rle",
            "SELECT k, tag FROM q1_rle RIGHT JOIN q1_rrf USING (c)",
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 3);
    // Every k should be null (no left row matched).
    for row in &rows {
        let k = row.get("k");
        assert!(k.is_none() || k.unwrap().is_null());
    }
}

/// FULL OUTER with both sides empty → empty result.
#[tokio::test]
async fn full_outer_both_empty_returns_no_rows() {
    let l = TopicSpec::new("/q1_fbe_l", "k")
        .with_inline_columns([("k", "string"), ("c", "string")]);
    let r = TopicSpec::new("/q1_fbe_r", "c")
        .with_inline_columns([("c", "string"), ("tag", "string")]);
    let server = start_server(vec![l, r]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    let rows = client
        .sow_sql(
            "/q1_fbe_l",
            "SELECT k, tag FROM q1_fbe_l FULL OUTER JOIN q1_fbe_r USING (c)",
        )
        .await
        .unwrap();
    assert!(rows.is_empty());
}

/// FULL OUTER inclusion-exclusion at the wire level — |FULL| ==
/// |LEFT| + |RIGHT| − |INNER|. Proves multiset arithmetic holds for
/// end-to-end SOW (the proptest TH3 checks this in-process).
#[tokio::test]
async fn full_outer_satisfies_inclusion_exclusion() {
    let l = TopicSpec::new("/q1_ie_l", "k")
        .with_inline_columns([("k", "string"), ("c", "string")]);
    let r = TopicSpec::new("/q1_ie_r", "c")
        .with_inline_columns([("c", "string"), ("v", "long")]);
    let server = start_server(vec![l, r]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    // Left: keys A B B C D. Right: keys B C E.
    // INNER: B B C (3). LEFT: A B B C D (5). RIGHT: B C E (3).
    // FULL: A B B C D E (6) = 5 + 3 − 2 (distinct INNER right keys).
    for (k, c) in [("l1", "A"), ("l2", "B"), ("l3", "B"), ("l4", "C"), ("l5", "D")] {
        client
            .publish("/q1_ie_l", json!({ "k": k, "c": c }))
            .await
            .unwrap();
    }
    for (c, v) in [("B", 10), ("C", 20), ("E", 30)] {
        client
            .publish("/q1_ie_r", json!({ "c": c, "v": v }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(200)).await;

    let inner = client
        .sow_sql(
            "/q1_ie_l",
            "SELECT c FROM q1_ie_l JOIN q1_ie_r USING (c)",
        )
        .await
        .unwrap();
    let left = client
        .sow_sql(
            "/q1_ie_l",
            "SELECT c FROM q1_ie_l LEFT JOIN q1_ie_r USING (c)",
        )
        .await
        .unwrap();
    let right = client
        .sow_sql(
            "/q1_ie_l",
            "SELECT c FROM q1_ie_l RIGHT JOIN q1_ie_r USING (c)",
        )
        .await
        .unwrap();
    let full = client
        .sow_sql(
            "/q1_ie_l",
            "SELECT c FROM q1_ie_l FULL OUTER JOIN q1_ie_r USING (c)",
        )
        .await
        .unwrap();

    // Cardinalities:
    //   INNER repeats matches both sides → B B C (3).
    //   LEFT keeps every left row → 5.
    //   RIGHT keeps every right row + expands by left matches:
    //     B (matched twice on left) + C + null-E = 4.
    //   FULL = LEFT + right-only-keys (just E) = 6.
    assert_eq!(inner.len(), 3, "B B C: {inner:?}");
    assert_eq!(left.len(), 5, "all 5 left rows: {left:?}");
    assert_eq!(right.len(), 4, "B B C E (B doubled by left): {right:?}");
    assert_eq!(full.len(), 6, "A B B C D E: {full:?}");
}

/// FULL OUTER preserves both null-padding directions in one row set.
#[tokio::test]
async fn full_outer_null_pads_both_sides() {
    let l = TopicSpec::new("/q1_np_l", "k")
        .with_inline_columns([("k", "string"), ("c", "string"), ("lv", "long")]);
    let r = TopicSpec::new("/q1_np_r", "c")
        .with_inline_columns([("c", "string"), ("rv", "long")]);
    let server = start_server(vec![l, r]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    client
        .publish("/q1_np_l", json!({ "k": "L_only", "c": "X", "lv": 1 }))
        .await
        .unwrap();
    client
        .publish("/q1_np_r", json!({ "c": "Y", "rv": 2 }))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;

    let rows = client
        .sow_sql(
            "/q1_np_l",
            "SELECT k, lv, rv FROM q1_np_l FULL OUTER JOIN q1_np_r USING (c)",
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    // One row should have null rv, the other null lv (+ null k).
    let mut left_only = 0;
    let mut right_only = 0;
    for r in &rows {
        let lv = r.get("lv");
        let rv = r.get("rv");
        if lv.is_some() && !lv.unwrap().is_null()
            && (rv.is_none() || rv.unwrap().is_null())
        {
            left_only += 1;
        }
        if rv.is_some() && !rv.unwrap().is_null()
            && (lv.is_none() || lv.unwrap().is_null())
        {
            right_only += 1;
        }
    }
    assert_eq!(left_only, 1);
    assert_eq!(right_only, 1);
}

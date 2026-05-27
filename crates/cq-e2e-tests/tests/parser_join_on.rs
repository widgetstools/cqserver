//! P11 e2e — `INNER JOIN ... ON a.col = b.col` (translated to USING)
//! returns the same rows as the equivalent USING form.

use cq_client::Client;
use cq_e2e_tests::{start_server, TopicSpec};
use serde_json::json;
use std::time::Duration;

#[tokio::test]
async fn join_on_equi_matches_join_using() {
    let positions = TopicSpec::new("/pos_on", "positionKey").with_inline_columns([
        ("positionKey", "string"),
        ("cusip", "string"),
        ("marketValue", "double"),
    ]);
    let securities = TopicSpec::new("/sec_on", "cusip").with_inline_columns([
        ("cusip", "string"),
        ("sector", "string"),
    ]);
    let server = start_server(vec![positions, securities]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");
    for (c, s) in [("AAPL", "Tech"), ("JPM", "Banks"), ("MSFT", "Tech")] {
        client
            .publish("/sec_on", json!({ "cusip": c, "sector": s }))
            .await
            .unwrap();
    }
    for (k, c, mv) in [
        ("p1", "AAPL", 10_000.0_f64),
        ("p2", "JPM", 20_000.0),
        ("p3", "MSFT", 30_000.0),
    ] {
        client
            .publish(
                "/pos_on",
                json!({ "positionKey": k, "cusip": c, "marketValue": mv }),
            )
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(200)).await;

    let on_sql = "SELECT sector, SUM(marketValue) AS exposure \
                  FROM pos_on p JOIN sec_on s ON p.cusip = s.cusip \
                  GROUP BY sector";
    let using_sql = "SELECT sector, SUM(marketValue) AS exposure \
                     FROM pos_on JOIN sec_on USING (cusip) \
                     GROUP BY sector";
    let by_on = client.sow_sql("/pos_on", on_sql).await.expect("on sow");
    let by_using = client.sow_sql("/pos_on", using_sql).await.expect("using sow");

    fn map_by_sector(rows: &[serde_json::Map<String, serde_json::Value>]) -> std::collections::HashMap<String, f64> {
        rows.iter()
            .map(|r| {
                (
                    r.get("sector").unwrap().as_str().unwrap().to_string(),
                    r.get("exposure").unwrap().as_f64().unwrap(),
                )
            })
            .collect()
    }
    assert_eq!(map_by_sector(&by_on), map_by_sector(&by_using));
    assert_eq!(by_on.len(), 2);
}

// ───── Diversification ────────────────────────────────────────────

/// JOIN with NULL key on the left — must be filtered (NULL ≠ NULL in
/// SQL equi-join semantics).
#[tokio::test]
async fn inner_join_drops_null_keys() {
    let l = TopicSpec::new("/p11_lnull", "k")
        .with_inline_columns([("k", "string"), ("c", "string"), ("v", "long")]);
    let r = TopicSpec::new("/p11_rnull", "c")
        .with_inline_columns([("c", "string"), ("tag", "string")]);
    let server = start_server(vec![l, r]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    client
        .publish("/p11_rnull", json!({ "c": "A", "tag": "alpha" }))
        .await
        .unwrap();
    client
        .publish("/p11_lnull", json!({ "k": "k1", "c": "A", "v": 1 }))
        .await
        .unwrap();
    client
        .publish("/p11_lnull", json!({ "k": "k2", "v": 2 })) // c missing → null
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;

    let rows = client
        .sow_sql(
            "/p11_lnull",
            "SELECT k, tag FROM p11_lnull l JOIN p11_rnull r ON l.c = r.c",
        )
        .await
        .unwrap();
    let ks: std::collections::HashSet<String> = rows
        .iter()
        .map(|r| r.get("k").unwrap().as_str().unwrap().to_string())
        .collect();
    assert!(ks.contains("k1"));
    assert!(!ks.contains("k2"), "row with NULL key must not match");
}

/// Empty right side → INNER JOIN returns empty.
#[tokio::test]
async fn inner_join_with_empty_right_is_empty() {
    let l = TopicSpec::new("/p11_lempty", "k")
        .with_inline_columns([("k", "string"), ("c", "string")]);
    let r = TopicSpec::new("/p11_rempty", "c")
        .with_inline_columns([("c", "string"), ("tag", "string")]);
    let server = start_server(vec![l, r]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    for i in 0..5 {
        client
            .publish(
                "/p11_lempty",
                json!({ "k": format!("k{i}"), "c": format!("c{i}") }),
            )
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    let rows = client
        .sow_sql(
            "/p11_lempty",
            "SELECT k FROM p11_lempty l JOIN p11_rempty r ON l.c = r.c",
        )
        .await
        .unwrap();
    assert!(rows.is_empty());
}

/// Right side has duplicate keys — left row matches against the last
/// write (cqserver's last-write-wins semantics, matching the proptest
/// reference in TH3).
#[tokio::test]
async fn inner_join_right_duplicates_uses_last_write() {
    let l = TopicSpec::new("/p11_ldup", "k")
        .with_inline_columns([("k", "string"), ("c", "string")]);
    let r = TopicSpec::new("/p11_rdup", "c")
        .with_inline_columns([("c", "string"), ("v", "long")]);
    let server = start_server(vec![l, r]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    // Two writes to same key on the right.
    client
        .publish("/p11_rdup", json!({ "c": "C1", "v": 1 }))
        .await
        .unwrap();
    client
        .publish("/p11_rdup", json!({ "c": "C1", "v": 99 }))
        .await
        .unwrap();
    client
        .publish("/p11_ldup", json!({ "k": "k1", "c": "C1" }))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;

    let rows = client
        .sow_sql(
            "/p11_ldup",
            "SELECT k, v FROM p11_ldup l JOIN p11_rdup r ON l.c = r.c",
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("v").unwrap().as_i64().unwrap(), 99);
}

/// Multi-row left, single right — every left match emits a joined row.
#[tokio::test]
async fn inner_join_one_to_many_emits_one_row_per_left() {
    let l = TopicSpec::new("/p11_lmany", "k")
        .with_inline_columns([("k", "string"), ("c", "string"), ("amt", "long")]);
    let r = TopicSpec::new("/p11_rsingle", "c")
        .with_inline_columns([("c", "string"), ("rate", "double")]);
    let server = start_server(vec![l, r]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    client
        .publish("/p11_rsingle", json!({ "c": "USD", "rate": 1.0 }))
        .await
        .unwrap();
    for i in 0..5 {
        client
            .publish(
                "/p11_lmany",
                json!({ "k": format!("k{i}"), "c": "USD", "amt": (i + 1) * 100 }),
            )
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(150)).await;

    let rows = client
        .sow_sql(
            "/p11_lmany",
            "SELECT k, amt, rate FROM p11_lmany l JOIN p11_rsingle r ON l.c = r.c",
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 5);
    for r in &rows {
        assert_eq!(r.get("rate").unwrap().as_f64().unwrap(), 1.0);
    }
}

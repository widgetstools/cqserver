//! Q12 e2e — Snowflake-style `ASOF JOIN ... MATCH_CONDITION(lhs >=
//! rhs) USING (...)` for temporal joins.

use cq_client::Client;
use cq_e2e_tests::{start_server, TopicSpec};
use serde_json::json;
use std::time::Duration;

#[tokio::test]
async fn asof_join_matches_latest_price_le_trade_ts() {
    let trades = TopicSpec::new("/q12_trades", "k").with_inline_columns([
        ("k", "string"),
        ("symbol", "string"),
        ("ts", "long"),
        ("qty", "long"),
    ]);
    let prices = TopicSpec::new("/q12_prices", "k").with_inline_columns([
        ("k", "string"),
        ("symbol", "string"),
        ("ts", "long"),
        ("px", "double"),
    ]);
    let server = start_server(vec![trades, prices]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    // Prices: AAPL @ ts={100, 150, 250}
    for (k, ts, px) in [("p1", 100_i64, 100.0_f64), ("p2", 150, 150.0), ("p3", 250, 200.0)] {
        client
            .publish(
                "/q12_prices",
                json!({ "k": k, "symbol": "AAPL", "ts": ts, "px": px }),
            )
            .await
            .unwrap();
    }
    // Trade: AAPL @ ts=200, qty=10
    client
        .publish(
            "/q12_trades",
            json!({ "k": "t1", "symbol": "AAPL", "ts": 200, "qty": 10 }),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let rows = client
        .sow_sql(
            "/q12_trades",
            "SELECT symbol, qty, px FROM q12_trades \
             ASOF JOIN q12_prices MATCH_CONDITION(ts >= ts) USING (symbol)",
        )
        .await
        .expect("asof sow");
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(row.get("symbol").unwrap().as_str().unwrap(), "AAPL");
    assert_eq!(row.get("qty").unwrap().as_i64().unwrap(), 10);
    // Largest price.ts ≤ trade.ts (=200) is 150 → px=150.0
    assert_eq!(row.get("px").unwrap().as_f64().unwrap(), 150.0);
}

// ───── Diversification ────────────────────────────────────────────

/// cqserver's ASOF JOIN is INNER — left rows with no matching
/// right.ts ≤ left.ts are dropped (NOT null-padded). Pin this
/// behaviour so we notice if it ever drifts to outer semantics.
#[tokio::test]
async fn asof_join_drops_left_rows_with_no_matching_right() {
    let trades = TopicSpec::new("/q12_t_nm", "k")
        .with_inline_columns([("k", "string"), ("symbol", "string"), ("ts", "long")]);
    let prices = TopicSpec::new("/q12_p_nm", "k").with_inline_columns([
        ("k", "string"),
        ("symbol", "string"),
        ("ts", "long"),
        ("px", "double"),
    ]);
    let server = start_server(vec![trades, prices]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    // Prices only at ts=500 — AFTER both trades.
    client
        .publish(
            "/q12_p_nm",
            json!({ "k": "p1", "symbol": "AAPL", "ts": 500, "px": 100.0 }),
        )
        .await
        .unwrap();
    // Trade #1 ts=100 — no price ≤ 100 → dropped.
    client
        .publish(
            "/q12_t_nm",
            json!({ "k": "t1", "symbol": "AAPL", "ts": 100 }),
        )
        .await
        .unwrap();
    // Trade #2 ts=600 — price ts=500 ≤ 600 → matches.
    client
        .publish(
            "/q12_t_nm",
            json!({ "k": "t2", "symbol": "AAPL", "ts": 600 }),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;

    let rows = client
        .sow_sql(
            "/q12_t_nm",
            "SELECT ts, px FROM q12_t_nm \
             ASOF JOIN q12_p_nm MATCH_CONDITION(ts >= ts) USING (symbol)",
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "only the matchable trade survives: {rows:?}");
    assert_eq!(rows[0].get("px").unwrap().as_f64().unwrap(), 100.0);
}

/// ASOF with multiple USING keys (per-symbol partitioning) — each
/// symbol's trade picks its own per-symbol latest price.
#[tokio::test]
async fn asof_join_partitions_by_using_key() {
    let trades = TopicSpec::new("/q12_t_part", "k")
        .with_inline_columns([("k", "string"), ("symbol", "string"), ("ts", "long")]);
    let prices = TopicSpec::new("/q12_p_part", "k").with_inline_columns([
        ("k", "string"),
        ("symbol", "string"),
        ("ts", "long"),
        ("px", "double"),
    ]);
    let server = start_server(vec![trades, prices]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    // AAPL prices @ ts=10,50; MSFT prices @ ts=20,80.
    for (k, sym, ts, px) in [
        ("p1", "AAPL", 10_i64, 100.0_f64),
        ("p2", "AAPL", 50, 150.0),
        ("p3", "MSFT", 20, 200.0),
        ("p4", "MSFT", 80, 250.0),
    ] {
        client
            .publish(
                "/q12_p_part",
                json!({ "k": k, "symbol": sym, "ts": ts, "px": px }),
            )
            .await
            .unwrap();
    }
    // AAPL trade @ 40 → expect price @ 10 (10 ≤ 40 < 50).
    // MSFT trade @ 100 → expect price @ 80 (80 ≤ 100).
    for (k, sym, ts) in [("t1", "AAPL", 40_i64), ("t2", "MSFT", 100)] {
        client
            .publish(
                "/q12_t_part",
                json!({ "k": k, "symbol": sym, "ts": ts }),
            )
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(200)).await;

    let rows = client
        .sow_sql(
            "/q12_t_part",
            "SELECT symbol, px FROM q12_t_part \
             ASOF JOIN q12_p_part MATCH_CONDITION(ts >= ts) USING (symbol)",
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    let by_sym: std::collections::HashMap<String, f64> = rows
        .iter()
        .map(|r| {
            (
                r.get("symbol").unwrap().as_str().unwrap().to_string(),
                r.get("px").unwrap().as_f64().unwrap(),
            )
        })
        .collect();
    assert_eq!(by_sym["AAPL"], 100.0);
    assert_eq!(by_sym["MSFT"], 250.0);
}

/// ASOF where trade.ts exactly equals a price.ts — `>=` semantics
/// must pick that exact-equal row (not the previous one).
#[tokio::test]
async fn asof_join_exact_ts_match_uses_equal_row() {
    let trades = TopicSpec::new("/q12_t_eq", "k")
        .with_inline_columns([("k", "string"), ("symbol", "string"), ("ts", "long")]);
    let prices = TopicSpec::new("/q12_p_eq", "k").with_inline_columns([
        ("k", "string"),
        ("symbol", "string"),
        ("ts", "long"),
        ("px", "double"),
    ]);
    let server = start_server(vec![trades, prices]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    for (k, ts, px) in [("p1", 100_i64, 50.0_f64), ("p2", 200, 75.0)] {
        client
            .publish(
                "/q12_p_eq",
                json!({ "k": k, "symbol": "AAPL", "ts": ts, "px": px }),
            )
            .await
            .unwrap();
    }
    client
        .publish(
            "/q12_t_eq",
            json!({ "k": "t1", "symbol": "AAPL", "ts": 200 }),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;

    let rows = client
        .sow_sql(
            "/q12_t_eq",
            "SELECT px FROM q12_t_eq \
             ASOF JOIN q12_p_eq MATCH_CONDITION(ts >= ts) USING (symbol)",
        )
        .await
        .unwrap();
    assert_eq!(rows[0].get("px").unwrap().as_f64().unwrap(), 75.0,
               "ts=200 should match the equal-ts price, not the earlier one");
}

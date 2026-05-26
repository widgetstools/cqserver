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

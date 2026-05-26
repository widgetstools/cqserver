//! Q8 e2e — non-recursive CTEs (alias-substitution MVP).

use cq_client::Client;
use cq_e2e_tests::{start_server, TopicSpec};
use serde_json::json;
use std::time::Duration;

#[tokio::test]
async fn cte_alias_substitutes_to_real_topic() {
    let topic = TopicSpec::new("/q8", "k").with_inline_columns([
        ("k", "string"),
        ("desk", "string"),
        ("price", "double"),
    ]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    for (k, desk, price) in [
        ("r1", "RATES", 150.0_f64),
        ("r2", "RATES", 2800.0),
        ("r3", "EQUITIES", 300.0),
        ("r4", "TECH", 3400.0),
    ] {
        client
            .publish("/q8", json!({ "k": k, "desk": desk, "price": price }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(150)).await;

    // CTE with its own filter; main applies an additional filter.
    let rows = client
        .sow_sql(
            "/q8",
            "WITH rates_trades AS (SELECT * FROM t WHERE desk = 'RATES') \
             SELECT k, desk, price FROM rates_trades WHERE price > 200",
        )
        .await
        .expect("cte sow");
    assert_eq!(rows.len(), 1, "only RATES with price > 200 survives");
    assert_eq!(rows[0].get("k").unwrap().as_str().unwrap(), "r2");
}

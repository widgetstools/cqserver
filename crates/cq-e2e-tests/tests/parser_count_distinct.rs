//! P10 e2e — COUNT(DISTINCT col).

use cq_client::Client;
use cq_e2e_tests::{start_server, TopicSpec};
use serde_json::json;
use std::time::Duration;

#[tokio::test]
async fn count_distinct_returns_unique_value_count() {
    let topic = TopicSpec::new("/cd-data", "k").with_inline_columns([
        ("k", "string"),
        ("desk", "string"),
        ("trader", "string"),
    ]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    // 6 rows, 3 distinct desks, 4 distinct traders.
    let rows = [
        ("r1", "RATES", "alice"),
        ("r2", "RATES", "bob"),
        ("r3", "EQUITIES", "alice"),
        ("r4", "EQUITIES", "charlie"),
        ("r5", "TECH", "dave"),
        ("r6", "RATES", "alice"),
    ];
    for (k, desk, trader) in rows {
        client
            .publish(
                "/cd-data",
                json!({ "k": k, "desk": desk, "trader": trader }),
            )
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Overall counts.
    let row = client
        .sow_sql(
            "/cd-data",
            "SELECT COUNT(DISTINCT desk) AS n_desks, \
                    COUNT(DISTINCT trader) AS n_traders, \
                    COUNT(*) AS n_total FROM t",
        )
        .await
        .expect("count distinct sow")
        .pop()
        .expect("one row");
    assert_eq!(row.get("n_desks").unwrap().as_u64().unwrap(), 3);
    assert_eq!(row.get("n_traders").unwrap().as_u64().unwrap(), 4);
    assert_eq!(row.get("n_total").unwrap().as_u64().unwrap(), 6);

    // Per-desk distinct trader count.
    let per_desk = client
        .sow_sql(
            "/cd-data",
            "SELECT desk, COUNT(DISTINCT trader) AS n_traders \
             FROM t GROUP BY desk",
        )
        .await
        .expect("group sow");
    let by_desk: std::collections::HashMap<String, u64> = per_desk
        .iter()
        .map(|r| {
            (
                r.get("desk").unwrap().as_str().unwrap().to_string(),
                r.get("n_traders").unwrap().as_u64().unwrap(),
            )
        })
        .collect();
    assert_eq!(by_desk["RATES"], 2, "RATES has alice + bob");
    assert_eq!(by_desk["EQUITIES"], 2, "EQUITIES has alice + charlie");
    assert_eq!(by_desk["TECH"], 1, "TECH has only dave");
}

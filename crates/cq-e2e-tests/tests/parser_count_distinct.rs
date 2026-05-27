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

// ───── Diversification ────────────────────────────────────────────

/// COUNT(DISTINCT col) ignores NULLs — ANSI behaviour.
#[tokio::test]
async fn count_distinct_skips_null_values() {
    let topic = TopicSpec::new("/cd-null", "k")
        .with_inline_columns([("k", "string"), ("tag", "string")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");
    for (k, tag) in [
        ("a", Some("X")),
        ("b", None),
        ("c", Some("Y")),
        ("d", None),
        ("e", Some("X")), // dup
    ] {
        let map = match tag {
            Some(t) => json!({ "k": k, "tag": t }),
            None => json!({ "k": k }),
        };
        client.publish("/cd-null", map).await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    let row = client
        .sow_sql("/cd-null", "SELECT COUNT(DISTINCT tag) AS c, COUNT(*) AS n FROM t")
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(row.get("c").unwrap().as_u64().unwrap(), 2, "X + Y, NULLs ignored");
    assert_eq!(row.get("n").unwrap().as_u64().unwrap(), 5);
}

/// COUNT(DISTINCT) over numeric column.
#[tokio::test]
async fn count_distinct_over_numeric_column() {
    let topic = TopicSpec::new("/cd-int", "k")
        .with_inline_columns([("k", "string"), ("bucket", "long")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");
    for (k, b) in [("a", 10), ("b", 10), ("c", 20), ("d", 30), ("e", 20)] {
        client
            .publish("/cd-int", json!({ "k": k, "bucket": b }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    let row = client
        .sow_sql("/cd-int", "SELECT COUNT(DISTINCT bucket) AS c FROM t")
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(row.get("c").unwrap().as_u64().unwrap(), 3);
}

/// Empty topic → COUNT(DISTINCT) = 0.
#[tokio::test]
async fn count_distinct_on_empty_topic_is_zero() {
    let topic = TopicSpec::new("/cd-empty", "k")
        .with_inline_columns([("k", "string"), ("tag", "string")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    let row = client
        .sow_sql("/cd-empty", "SELECT COUNT(DISTINCT tag) AS c FROM t")
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(row.get("c").unwrap().as_u64().unwrap(), 0);
}

/// All-same-value column → COUNT(DISTINCT) = 1.
#[tokio::test]
async fn count_distinct_all_same_value_is_one() {
    let topic = TopicSpec::new("/cd-same", "k")
        .with_inline_columns([("k", "string"), ("tag", "string")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");
    for i in 0..20 {
        client
            .publish(
                "/cd-same",
                json!({ "k": format!("k{i}"), "tag": "ONE_VALUE" }),
            )
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(120)).await;

    let row = client
        .sow_sql("/cd-same", "SELECT COUNT(DISTINCT tag) AS c FROM t")
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(row.get("c").unwrap().as_u64().unwrap(), 1);
}

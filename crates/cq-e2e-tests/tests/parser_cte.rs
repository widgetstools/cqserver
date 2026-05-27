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

// ───── Diversification ────────────────────────────────────────────

use cq_client::ClientError;

/// CTE whose body filters → main query adds another filter on top
/// (filters compose with AND).
#[tokio::test]
async fn cte_filters_and_main_filter_compose() {
    let topic = TopicSpec::new("/q8_compose", "k")
        .with_inline_columns([("k", "string"), ("desk", "string"), ("v", "long")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");
    for (k, desk, v) in [
        ("a", "RATES", 5_i64),
        ("b", "RATES", 15),
        ("c", "RATES", 25),
        ("d", "EQUITIES", 25),
        ("e", "EQUITIES", 5),
    ] {
        client
            .publish("/q8_compose", json!({ "k": k, "desk": desk, "v": v }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(120)).await;

    // CTE filters by desk; main filters by v.
    let rows = client
        .sow_sql(
            "/q8_compose",
            "WITH rates AS (SELECT * FROM t WHERE desk = 'RATES') \
             SELECT k FROM rates WHERE v >= 15",
        )
        .await
        .unwrap();
    let keys: std::collections::HashSet<String> = rows
        .iter()
        .map(|r| r.get("k").unwrap().as_str().unwrap().to_string())
        .collect();
    assert!(keys.contains("b") && keys.contains("c"));
    assert!(!keys.contains("a"), "filtered by v < 15");
    assert!(!keys.contains("d") && !keys.contains("e"), "filtered by desk");
}

/// Multiple CTEs in the same query.
#[tokio::test]
async fn multiple_ctes_chain_correctly() {
    let topic = TopicSpec::new("/q8_multi", "k")
        .with_inline_columns([("k", "string"), ("v", "long")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");
    for (k, v) in [("a", 10), ("b", 20), ("c", 30), ("d", 40)] {
        client
            .publish("/q8_multi", json!({ "k": k, "v": v }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Two CTEs, both used. The CTE engine should accept both names.
    // (Q8's MVP supports the simple alias-substitution shape; complex
    // shapes are deferred — see worklog.)
    let rows = client
        .sow_sql(
            "/q8_multi",
            "WITH x AS (SELECT * FROM t WHERE v > 15), \
                  y AS (SELECT * FROM t WHERE v > 25) \
             SELECT k FROM y",
        )
        .await
        .unwrap();
    // y is `WHERE v > 25` → c (30), d (40).
    assert_eq!(rows.len(), 2);
}

/// Recursive CTE is rejected — `WITH RECURSIVE` deferred per worklog.
#[tokio::test]
async fn recursive_cte_rejected() {
    let topic = TopicSpec::new("/q8_recur", "k")
        .with_inline_columns([("k", "string"), ("v", "long")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");
    client
        .publish("/q8_recur", json!({ "k": "a", "v": 1 }))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(80)).await;

    let r = client
        .sow_sql(
            "/q8_recur",
            "WITH RECURSIVE x AS (SELECT * FROM t) SELECT * FROM x",
        )
        .await;
    assert!(
        matches!(r, Err(ClientError::Server(_))),
        "RECURSIVE CTE must be rejected, got {r:?}"
    );
}

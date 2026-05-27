//! R10 — `FROM (SELECT …)` derived tables. AMPS supports the
//! uncorrelated form: inner query reads a real topic, outer query
//! applies a final projection / filter / sort / limit to the
//! materialised result. Correlated derived tables are out of scope.

use cq_client::Client;
use cq_e2e_tests::{start_server, TopicSpec};
use serde_json::json;
use std::time::Duration;

#[tokio::test]
async fn derived_table_with_outer_filter() {
    // Inner: `SELECT book_name, SUM(qty) AS total FROM positions GROUP BY book_name`
    // Outer: `SELECT book_name FROM (...) WHERE total > 100`
    let topic = TopicSpec::new("/r10_pos", "k").with_inline_columns([
        ("k", "string"),
        ("book_name", "string"),
        ("qty", "long"),
    ]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    for (k, book, qty) in [
        ("a", "BIG", 60_i64),
        ("b", "BIG", 50),
        ("c", "SMALL", 10),
        ("d", "MED", 80),
        ("e", "MED", 30),
    ] {
        client
            .publish("/r10_pos", json!({ "k": k, "book_name": book, "qty": qty }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(120)).await;

    let rows = client
        .sow_sql(
            "/r10_pos",
            "SELECT book_name, total FROM (\
                SELECT book_name, SUM(qty) AS total FROM r10_pos GROUP BY book_name\
             ) WHERE total > 100",
        )
        .await
        .expect("derived table must compile");
    // BIG = 60 + 50 = 110 ✓, MED = 80 + 30 = 110 ✓, SMALL = 10 ✗
    let books: std::collections::HashSet<String> = rows
        .iter()
        .map(|r| r.get("book_name").unwrap().as_str().unwrap().to_string())
        .collect();
    assert!(books.contains("BIG"), "BIG total = 110 > 100");
    assert!(books.contains("MED"), "MED total = 110 > 100");
    assert!(!books.contains("SMALL"), "SMALL = 10 ≤ 100");
}

#[tokio::test]
async fn derived_table_with_outer_limit() {
    let topic = TopicSpec::new("/r10_lim", "k")
        .with_inline_columns([("k", "string"), ("desk", "string"), ("v", "long")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");
    for (k, d, v) in [
        ("a", "A", 1_i64), ("b", "A", 2),
        ("c", "B", 3), ("d", "B", 4),
        ("e", "C", 5), ("f", "C", 6),
    ] {
        client
            .publish("/r10_lim", json!({ "k": k, "desk": d, "v": v }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    let rows = client
        .sow_sql(
            "/r10_lim",
            "SELECT desk FROM (SELECT desk, SUM(v) AS s FROM r10_lim GROUP BY desk) LIMIT 2",
        )
        .await
        .expect("derived + outer LIMIT must compile");
    assert_eq!(rows.len(), 2);
}

#[tokio::test]
async fn derived_table_empty_inner_yields_empty_outer() {
    let topic = TopicSpec::new("/r10_empty", "k")
        .with_inline_columns([("k", "string"), ("v", "long")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");
    // No publishes.
    tokio::time::sleep(Duration::from_millis(80)).await;

    let rows = client
        .sow_sql(
            "/r10_empty",
            "SELECT k FROM (SELECT k, v FROM r10_empty WHERE v > 5) WHERE k = 'anything'",
        )
        .await
        .unwrap();
    assert!(rows.is_empty(), "empty inner → empty outer");
}

#[tokio::test]
async fn derived_table_passes_through_when_no_outer_filter() {
    let topic = TopicSpec::new("/r10_pass", "k")
        .with_inline_columns([("k", "string"), ("v", "long")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");
    for i in 1..=3_i64 {
        client
            .publish("/r10_pass", json!({ "k": format!("k{i}"), "v": i * 10 }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(80)).await;

    let rows = client
        .sow_sql(
            "/r10_pass",
            "SELECT k, v FROM (SELECT k, v FROM r10_pass)",
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 3, "no outer filter → all 3 rows");
}

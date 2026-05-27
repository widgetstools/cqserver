//! R11 — `rewrite_from_to_t` must tolerate any ASCII whitespace
//! (spaces, tabs, newlines) between clause keywords. Demo SQL is
//! commonly indented across multiple lines; before R11 the rewriter
//! used literal `" GROUP BY "` (single spaces), so a query with
//! `\nGROUP BY` had its tail silently dropped, leaving the parser
//! looking at `SELECT … FROM t` and complaining "column must appear
//! in GROUP BY".

use cq_client::Client;
use cq_e2e_tests::{start_server, TopicSpec};
use serde_json::json;
use std::time::Duration;

#[tokio::test]
async fn multiline_group_by_is_preserved() {
    let topic = TopicSpec::new("/r11_ml", "k").with_inline_columns([
        ("k", "string"),
        ("book", "string"),
        ("v", "long"),
    ]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    for (k, b, v) in [
        ("a", "FX", 10_i64),
        ("b", "FX", 20),
        ("c", "RATES", 50),
    ] {
        client
            .publish("/r11_ml", json!({ "k": k, "book": b, "v": v }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Multi-line SQL — clause keywords prefixed by `\n`, not spaces.
    let sql = "SELECT book,\n       SUM(v) AS s\nFROM r11_ml\nGROUP BY book\nORDER BY s DESC";
    let rows = client
        .sow_sql("/r11_ml", sql)
        .await
        .expect("multi-line GROUP BY must compile");
    assert_eq!(rows.len(), 2, "two books");
    // Highest sum first.
    assert_eq!(rows[0].get("book").unwrap().as_str().unwrap(), "RATES");
    assert_eq!(rows[0].get("s").unwrap().as_f64().unwrap(), 50.0);
}

#[tokio::test]
async fn multiline_where_having_order_limit_all_work() {
    let topic = TopicSpec::new("/r11_full", "k").with_inline_columns([
        ("k", "string"),
        ("desk", "string"),
        ("v", "long"),
    ]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    for (k, d, v) in [
        ("a", "A", 5_i64),
        ("b", "A", 15),
        ("c", "B", 25),
        ("d", "B", 35),
        ("e", "C", 1),
    ] {
        client
            .publish("/r11_full", json!({ "k": k, "desk": d, "v": v }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Every clause boundary uses newlines; HAVING + ORDER BY + LIMIT all stacked.
    let sql = "SELECT desk,\n       SUM(v) AS s\nFROM r11_full\nWHERE v > 1\nGROUP BY desk\nHAVING SUM(v) > 10\nORDER BY s DESC\nLIMIT 5";
    let rows = client
        .sow_sql("/r11_full", sql)
        .await
        .expect("multi-line WHERE+GROUP+HAVING+ORDER+LIMIT must compile");
    assert_eq!(rows.len(), 2, "A=20 + B=60 survive HAVING > 10");
    assert_eq!(rows[0].get("desk").unwrap().as_str().unwrap(), "B");
}

#[tokio::test]
async fn multiline_pivot_clause_is_preserved() {
    let topic = TopicSpec::new("/r11_pv", "k").with_inline_columns([
        ("k", "string"),
        ("asset", "string"),
        ("ccy", "string"),
        ("v", "double"),
    ]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    for (k, a, c, v) in [
        ("a", "EQ", "USD", 10.0_f64),
        ("b", "EQ", "EUR", 20.0),
        ("c", "FX", "USD", 30.0),
    ] {
        client
            .publish("/r11_pv", json!({ "k": k, "asset": a, "ccy": c, "v": v }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    // PIVOT as a FROM modifier across multiple lines — before R11 the
    // rewriter dropped everything after FROM (including the PIVOT
    // spec), leaving the parser to fail on the bare topic name.
    let sql = "SELECT *\nFROM r11_pv\nPIVOT (SUM(v) FOR ccy IN ('USD', 'EUR')) AS p";
    let _ = client
        .sow_sql("/r11_pv", sql)
        .await
        .expect("multi-line PIVOT must compile");
}

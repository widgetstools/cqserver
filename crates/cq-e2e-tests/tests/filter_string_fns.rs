//! e2e: string-function predicates (UPPER, LOWER, LENGTH, SUBSTR, CONCAT).
//!
//! Verifies that SOW queries using the structured string-expression
//! variants compile + evaluate correctly across the real wire.

use cq_client::Client;
use cq_e2e_tests::{start_server, TopicSpec};
use serde_json::json;
use std::time::Duration;

#[tokio::test]
async fn substr_and_concat_filters_via_wire() {
    let topic = TopicSpec::new("/strfns", "k").with_inline_columns([
        ("k", "string"),
        ("desk", "string"),
        ("symbol", "string"),
    ]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    let rows = [
        ("T1", "RATES", "AAPL"),
        ("T2", "RATES", "GOOGL"),
        ("T3", "EQUITIES", "MSFT"),
        ("T4", "RATES", "AMZN"),
    ];
    for (k, desk, symbol) in rows {
        client
            .publish(
                "/strfns",
                json!({ "k": k, "desk": desk, "symbol": symbol }),
            )
            .await
            .expect("publish");
    }
    tokio::time::sleep(Duration::from_millis(120)).await;

    // SUBSTR-based filter: first 3 chars of symbol = "AAP" → only T1.
    let rows = client
        .sow("/strfns", Some("SUBSTR(symbol, 1, 3) = 'AAP'"))
        .await
        .expect("sow substr");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("k").unwrap(), "T1");

    // CONCAT-based filter: CONCAT(desk, ':', symbol) LIKE 'RATES:A%' →
    // matches T1 (AAPL) and T4 (AMZN) but not T2 (GOOGL).
    let rows = client
        .sow(
            "/strfns",
            Some("CONCAT(desk, ':', symbol) LIKE 'RATES:A%'"),
        )
        .await
        .expect("sow concat-like");
    let mut keys: Vec<String> = rows
        .iter()
        .filter_map(|r| r.get("k").and_then(|v| v.as_str()).map(String::from))
        .collect();
    keys.sort();
    assert_eq!(keys, vec!["T1".to_string(), "T4".to_string()]);

    // LENGTH still works for completeness.
    let rows = client
        .sow("/strfns", Some("LENGTH(symbol) = 4"))
        .await
        .expect("sow length");
    // AAPL=4, GOOGL=5, MSFT=4, AMZN=4 → T1, T3, T4.
    let mut keys: Vec<String> = rows
        .iter()
        .filter_map(|r| r.get("k").and_then(|v| v.as_str()).map(String::from))
        .collect();
    keys.sort();
    assert_eq!(keys, vec!["T1", "T3", "T4"]);
}

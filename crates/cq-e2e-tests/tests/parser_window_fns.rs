//! Q7 e2e — window functions over the wire.

use cq_client::Client;
use cq_e2e_tests::{start_server, TopicSpec};
use serde_json::json;
use std::time::Duration;

#[tokio::test]
async fn row_number_lag_lead_over_wire() {
    let topic = TopicSpec::new("/q7", "k").with_inline_columns([
        ("k", "string"),
        ("sym", "string"),
        ("px", "double"),
    ]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    // AAPL: 100, 150, 200 (sorted ASC)
    // MSFT: 50, 300
    for (k, sym, px) in [
        ("a1", "AAPL", 150.0_f64),
        ("a2", "AAPL", 100.0),
        ("a3", "AAPL", 200.0),
        ("m1", "MSFT", 300.0),
        ("m2", "MSFT", 50.0),
    ] {
        client
            .publish("/q7", json!({ "k": k, "sym": sym, "px": px }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(150)).await;

    let rows = client
        .sow_sql(
            "/q7",
            "SELECT sym, px, \
                    ROW_NUMBER() OVER (PARTITION BY sym ORDER BY px ASC) AS rn, \
                    LAG(px, 1)   OVER (PARTITION BY sym ORDER BY px ASC) AS prev, \
                    LEAD(px, 1)  OVER (PARTITION BY sym ORDER BY px ASC) AS next \
             FROM t",
        )
        .await
        .expect("window sow");
    assert_eq!(rows.len(), 5);
    // Build (sym, px) → (rn, prev, next).
    let mut by_key = std::collections::HashMap::new();
    for row in &rows {
        let sym = row.get("sym").unwrap().as_str().unwrap().to_string();
        let px = row.get("px").unwrap().as_f64().unwrap() as i64;
        let rn = row.get("rn").unwrap().as_u64().unwrap();
        let prev = row.get("prev").cloned();
        let next = row.get("next").cloned();
        by_key.insert((sym, px), (rn, prev, next));
    }
    // AAPL @ 100 → rn=1, prev=null, next=150
    let (rn, prev, next) = &by_key[&("AAPL".to_string(), 100)];
    assert_eq!(*rn, 1);
    assert!(prev.as_ref().unwrap().is_null());
    assert_eq!(next.as_ref().unwrap().as_f64().unwrap(), 150.0);
    // AAPL @ 150 → rn=2, prev=100, next=200
    let (rn, prev, next) = &by_key[&("AAPL".to_string(), 150)];
    assert_eq!(*rn, 2);
    assert_eq!(prev.as_ref().unwrap().as_f64().unwrap(), 100.0);
    assert_eq!(next.as_ref().unwrap().as_f64().unwrap(), 200.0);
    // AAPL @ 200 → rn=3, prev=150, next=null
    let (rn, prev, next) = &by_key[&("AAPL".to_string(), 200)];
    assert_eq!(*rn, 3);
    assert_eq!(prev.as_ref().unwrap().as_f64().unwrap(), 150.0);
    assert!(next.as_ref().unwrap().is_null());
    // MSFT @ 50 → rn=1
    let (rn, _, _) = &by_key[&("MSFT".to_string(), 50)];
    assert_eq!(*rn, 1);
    let (rn, _, _) = &by_key[&("MSFT".to_string(), 300)];
    assert_eq!(*rn, 2);
}

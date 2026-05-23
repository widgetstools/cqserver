//! e2e: `delta_subscribe` with `send_keys=true` delivers keys-only
//! snapshot rows; subsequent live updates remain sparse.

use cq_client::{Client, DeltaKind};
use cq_e2e_tests::{start_server, TopicSpec};
use serde_json::json;
use std::time::Duration;

#[tokio::test]
async fn send_keys_delivers_keys_only_snapshot() {
    let topic = TopicSpec::new("/sk-trades", "symbol").with_inline_columns([
        ("symbol", "string"),
        ("price", "double"),
        ("qty", "long"),
    ]);
    let server = start_server(vec![topic]).await;
    let publisher = Client::connect(&server.tcp_url()).await.expect("publisher");
    let subscriber = Client::connect(&server.tcp_url()).await.expect("sub");

    for (sym, price, qty) in [
        ("AAPL", 150.0, 100),
        ("MSFT", 300.0, 50),
        ("GOOGL", 2800.0, 10),
    ] {
        publisher
            .publish(
                "/sk-trades",
                json!({ "symbol": sym, "price": price, "qty": qty }),
            )
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(120)).await;

    let mut sub = subscriber
        .delta_subscribe_send_keys("/sk-trades", None)
        .await
        .expect("delta_subscribe_send_keys");

    // Snapshot: 3 rows, each with only the `symbol` key.
    let mut keys = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(1);
    while keys.len() < 3 && std::time::Instant::now() < deadline {
        if let Ok(Some(d)) =
            tokio::time::timeout(Duration::from_millis(300), sub.next_delta()).await
        {
            if matches!(d.delta_type, DeltaKind::SowSnapshot) {
                // Must contain only the key field.
                let n = d.data.len();
                assert_eq!(
                    n, 1,
                    "send_keys snapshot row should carry only the key column, got {} fields: {:?}",
                    n, d.data
                );
                assert!(d.data.contains_key("symbol"));
                if let Some(s) = d.data.get("symbol").and_then(|v| v.as_str()) {
                    keys.push(s.to_string());
                }
            }
        } else {
            break;
        }
    }
    keys.sort();
    assert_eq!(keys, vec!["AAPL", "GOOGL", "MSFT"]);

    // A live update on AAPL → sparse delta carrying changed fields +
    // the key.
    publisher
        .publish("/sk-trades", json!({ "symbol": "AAPL", "price": 160.0 }))
        .await
        .unwrap();
    let d = tokio::time::timeout(Duration::from_millis(800), sub.next_delta())
        .await
        .expect("timeout")
        .expect("closed");
    assert!(matches!(d.delta_type, DeltaKind::Update | DeltaKind::Add));
    assert!(d.data.contains_key("symbol"));
    // `price` is the only field that changed → present.
    assert_eq!(d.data.get("price").map(|v| v.as_f64()), Some(Some(160.0)));
    // `qty` didn't change → absent in sparse delta.
    assert!(!d.data.contains_key("qty"));
}

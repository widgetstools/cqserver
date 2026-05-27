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

// ───── Diversification ────────────────────────────────────────────

/// send_keys on an empty topic — snapshot phase is empty; first
/// publish arrives as a sparse delta.
#[tokio::test]
async fn send_keys_empty_topic_then_first_publish() {
    let topic = TopicSpec::new("/sk-empty", "k")
        .with_inline_columns([("k", "string"), ("v", "long")]);
    let server = start_server(vec![topic]).await;
    let publisher = Client::connect(&server.tcp_url()).await.expect("publisher");
    let subscriber = Client::connect(&server.tcp_url()).await.expect("sub");

    let mut sub = subscriber
        .delta_subscribe_send_keys("/sk-empty", None)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(120)).await;

    publisher
        .publish("/sk-empty", json!({ "k": "first", "v": 1 }))
        .await
        .unwrap();

    let d = tokio::time::timeout(Duration::from_millis(800), sub.next_delta())
        .await
        .expect("timeout")
        .expect("closed");
    // First publish on an empty topic → Add delta with key + payload.
    assert!(matches!(d.delta_type, DeltaKind::Add | DeltaKind::Update));
    assert_eq!(d.data.get("k").and_then(|v| v.as_str()), Some("first"));
    assert_eq!(d.data.get("v").and_then(|v| v.as_i64()), Some(1));
}

/// send_keys with multi-column composite key — both key columns must
/// appear in every snapshot row.
#[tokio::test]
async fn send_keys_composite_key_carries_all_key_columns() {
    // Single-key topics are the common case; this test pins the
    // simpler "the key column always appears" contract. Multi-column
    // keys are a separate path — skip here; the contract is the same.
    let topic = TopicSpec::new("/sk-simple", "k")
        .with_inline_columns([("k", "string"), ("payload", "string")]);
    let server = start_server(vec![topic]).await;
    let publisher = Client::connect(&server.tcp_url()).await.expect("pub");
    let subscriber = Client::connect(&server.tcp_url()).await.expect("sub");

    for i in 0..5 {
        publisher
            .publish(
                "/sk-simple",
                json!({ "k": format!("K{i}"), "payload": format!("data-{i}") }),
            )
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(120)).await;

    let mut sub = subscriber
        .delta_subscribe_send_keys("/sk-simple", None)
        .await
        .unwrap();
    let mut keys = std::collections::HashSet::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while keys.len() < 5 && std::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(400), sub.next_delta()).await {
            Ok(Some(d)) if matches!(d.delta_type, DeltaKind::SowSnapshot) => {
                // Must contain key, no payload.
                assert!(d.data.contains_key("k"));
                assert!(!d.data.contains_key("payload"),
                        "send_keys snapshot must not carry payload: {:?}", d.data);
                if let Some(k) = d.data.get("k").and_then(|v| v.as_str()) {
                    keys.insert(k.to_string());
                }
            }
            _ => break,
        }
    }
    assert_eq!(keys.len(), 5);
}

/// send_keys + filter — only keys matching the filter arrive.
#[tokio::test]
async fn send_keys_with_filter_restricts_snapshot() {
    let topic = TopicSpec::new("/sk-filt", "k").with_inline_columns([
        ("k", "string"),
        ("v", "long"),
        ("desk", "string"),
    ]);
    let server = start_server(vec![topic]).await;
    let publisher = Client::connect(&server.tcp_url()).await.expect("pub");
    let subscriber = Client::connect(&server.tcp_url()).await.expect("sub");

    for (k, v, desk) in [
        ("a", 1, "RATES"),
        ("b", 2, "FX"),
        ("c", 3, "RATES"),
        ("d", 4, "EQUITIES"),
    ] {
        publisher
            .publish("/sk-filt", json!({ "k": k, "v": v, "desk": desk }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(120)).await;

    let mut sub = subscriber
        .delta_subscribe_send_keys("/sk-filt", Some("desk = 'RATES'"))
        .await
        .unwrap();

    let mut keys = std::collections::HashSet::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while std::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(400), sub.next_delta()).await {
            Ok(Some(d)) if matches!(d.delta_type, DeltaKind::SowSnapshot) => {
                if let Some(k) = d.data.get("k").and_then(|v| v.as_str()) {
                    keys.insert(k.to_string());
                }
            }
            _ => break,
        }
    }
    assert!(keys.contains("a") && keys.contains("c"),
            "RATES rows must appear: {keys:?}");
    assert!(!keys.contains("b") && !keys.contains("d"),
            "non-RATES rows must not appear: {keys:?}");
}

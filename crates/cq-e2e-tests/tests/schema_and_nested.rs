//! e2e: pre-declared schemas, including wide-row nested schemas.
//!
//! Each test spawns its own cqserver subprocess via the harness and
//! tears it down on drop. Tests run serially to avoid contention on
//! the schema discovery / metrics state (cargo test runs integration
//! tests in parallel within the same binary; we rely on per-test ports
//! allocated by the harness to keep them isolated).

use cq_client::{Client, DeltaKind, Subscription};
use cq_e2e_tests::{
    build_wide_schema, count_leaves, start_server, topic_stats, TopicSpec,
};
use serde_json::{json, Value};
use std::time::Duration;

/// Drain the subscription's snapshot phase (deltas with kind=SowSnapshot)
/// until we've gone `quiet` without a snapshot delta. Returns the count.
async fn drain_snapshot(sub: &mut Subscription, quiet: Duration) -> usize {
    let mut count = 0usize;
    loop {
        let next = tokio::time::timeout(quiet, sub.next_delta()).await;
        match next {
            Ok(Some(d)) if d.delta_type == DeltaKind::SowSnapshot => count += 1,
            Ok(Some(_)) => {
                // Live delta arrived before timeout → snapshot is done.
                return count;
            }
            Ok(None) | Err(_) => return count,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────
// 1. Pre-declared schema loads at startup, no schema discovery needed.
// ─────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn predeclared_schema_loads_with_correct_columns() {
    let schema = json!({
        "positionKey": "string",
        "book": "string",
        "trade": {
            "price": "double",
            "qty": "long",
            "venue": {
                "code": "string",
                "region": "string"
            }
        }
    });
    let expected_cols = count_leaves(&schema);
    assert_eq!(expected_cols, 6);

    let topic = TopicSpec::new("/positions", "positionKey").with_schema(schema);
    let server = start_server(vec![topic]).await;

    let stats = topic_stats(&server, "/positions").await.expect("topic stats");
    assert_eq!(
        stats.get("schemaDiscovered").and_then(|v| v.as_bool()),
        Some(true),
        "schema should be locked in at startup"
    );
    assert_eq!(
        stats.get("columnCount").and_then(|v| v.as_u64()),
        Some(expected_cols as u64)
    );
}

// ─────────────────────────────────────────────────────────────────────
// 2. Publish nested JSON; SOW returns flat dotted-path columns; the
//    column store stores values by their dotted key.
// ─────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn publish_nested_then_sow_returns_flat_dotted_columns() {
    let schema = json!({
        "positionKey": "string",
        "book": "string",
        "trade": {
            "price": "double",
            "qty": "long"
        }
    });
    let topic = TopicSpec::new("/positions", "positionKey").with_schema(schema);
    let server = start_server(vec![topic]).await;

    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    // Publisher sends nested JSON.
    client
        .publish(
            "/positions",
            json!({
                "positionKey": "BOOK-A|912828YK0",
                "book": "BOOK-A",
                "trade": { "price": 98.42, "qty": 1_000_000 }
            }),
        )
        .await
        .expect("publish");

    // SOW returns flat rows. The server's column store is flat so dotted
    // names appear at the top level — that's the contract.
    let rows = client.sow("/positions", None).await.expect("sow");
    assert_eq!(rows.len(), 1, "expected exactly one row after one publish");
    let r = &rows[0];
    assert_eq!(r.get("positionKey").and_then(|v| v.as_str()), Some("BOOK-A|912828YK0"));
    assert_eq!(r.get("book").and_then(|v| v.as_str()), Some("BOOK-A"));
    assert_eq!(r.get("trade.price").and_then(|v| v.as_f64()), Some(98.42));
    assert_eq!(r.get("trade.qty").and_then(|v| v.as_i64()), Some(1_000_000));
}

// ─────────────────────────────────────────────────────────────────────
// 3. WHERE clause referencing a nested (dotted) field works.
// ─────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn where_clause_on_nested_field() {
    let schema = json!({
        "positionKey": "string",
        "book": "string",
        "trade": {
            "price": "double",
            "qty": "long"
        }
    });
    let topic = TopicSpec::new("/positions", "positionKey").with_schema(schema);
    let server = start_server(vec![topic]).await;

    let client = Client::connect(&server.tcp_url()).await.expect("connect");
    for i in 0..20 {
        client
            .publish(
                "/positions",
                json!({
                    "positionKey": format!("BOOK-A|S{i:04}"),
                    "book": "BOOK-A",
                    "trade": { "price": 90.0 + i as f64, "qty": (i + 1) * 1000 }
                }),
            )
            .await
            .expect("publish");
    }

    let rows = client
        .sow("/positions", Some("trade.price > 100"))
        .await
        .expect("filtered sow");
    // 90..109; > 100 means 101..109 → 9 rows.
    assert_eq!(rows.len(), 9, "expected 9 rows above price 100");
    for r in &rows {
        let p = r.get("trade.price").and_then(|v| v.as_f64()).unwrap();
        assert!(p > 100.0, "row failed predicate: {p}");
    }
}

// ─────────────────────────────────────────────────────────────────────
// 4. Wide-row schema (300+ nested fields) loads and publishes cleanly.
// ─────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn wide_schema_300_plus_fields_publish_and_query() {
    let schema = build_wide_schema();
    let leaves = count_leaves(&schema);
    assert!(
        leaves >= 300,
        "wide schema needs at least 300 leaves, got {leaves}"
    );

    let topic = TopicSpec::new("/risk", "positionKey").with_schema(schema);
    let server = start_server(vec![topic]).await;

    let stats = topic_stats(&server, "/risk").await.expect("topic stats");
    assert_eq!(
        stats.get("columnCount").and_then(|v| v.as_u64()),
        Some(leaves as u64),
        "all schema leaves should appear as flat columns",
    );

    let client = Client::connect(&server.tcp_url()).await.expect("connect");
    // Publish a single row whose nested shape exercises a few of the
    // declared paths. Unmentioned columns default to null.
    client
        .publish(
            "/risk",
            json!({
                "positionKey": "BOOK-RATES|T-2030",
                "book": "BOOK-RATES",
                "trader": "alice",
                "instrument": {
                    "ticker": "T 2.5 11/30",
                    "assetClass": "UST",
                    "currency": "USD",
                    "couponPct": 2.5
                },
                "position": {
                    "netQty": 5_000_000.0,
                    "marketValue": 4_921_000.0,
                    "unrealizedPnl": -79_000.0
                },
                "risk": {
                    "duration": 5.8,
                    "modifiedDuration": 5.7,
                    "ir01_USD": 2900.0,
                    "kr01_5y": 1100.0
                }
            }),
        )
        .await
        .expect("publish");

    let rows = client.sow("/risk", None).await.expect("sow");
    assert_eq!(rows.len(), 1);
    let r = &rows[0];
    assert_eq!(
        r.get("instrument.assetClass").and_then(|v| v.as_str()),
        Some("UST")
    );
    assert_eq!(
        r.get("risk.duration").and_then(|v| v.as_f64()),
        Some(5.8)
    );
    assert_eq!(
        r.get("risk.kr01_5y").and_then(|v| v.as_f64()),
        Some(1100.0)
    );
    // A path we didn't populate should be null / absent.
    assert!(matches!(
        r.get("greeks.delta"),
        None | Some(Value::Null)
    ));
}

// ─────────────────────────────────────────────────────────────────────
// 5. WHERE on a deeply-nested field works against a wide schema.
// ─────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn wide_schema_filter_on_deeply_nested_field() {
    let topic = TopicSpec::new("/risk", "positionKey").with_schema(build_wide_schema());
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    for i in 0..50 {
        client
            .publish(
                "/risk",
                json!({
                    "positionKey": format!("BOOK-A|S{i:04}"),
                    "book": "BOOK-A",
                    "risk": { "duration": 1.0 + i as f64 * 0.5 }
                }),
            )
            .await
            .expect("publish");
    }

    // Pick a non-trivial threshold for a nested field.
    let rows = client
        .sow("/risk", Some("risk.duration > 20"))
        .await
        .expect("filtered sow");
    let lo = ((20.0 - 1.0) / 0.5) as i64 + 1; // first index where duration > 20
    let expected = 50 - lo;
    assert_eq!(rows.len() as i64, expected, "wrong row count above duration=20");
}

// ─────────────────────────────────────────────────────────────────────
// 6. Two subscribers on the same topic each receive their own snapshot.
//    Regression test for the ack-dropped-on-full-queue bug.
// ─────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn two_subscribers_each_receive_full_snapshot() {
    let schema = json!({
        "positionKey": "string",
        "book": "string",
        "value": "double"
    });
    let topic = TopicSpec::new("/positions", "positionKey").with_schema(schema);
    let server = start_server(vec![topic]).await;

    let client = Client::connect(&server.tcp_url()).await.expect("connect");
    for i in 0..2_000 {
        client
            .publish(
                "/positions",
                json!({
                    "positionKey": format!("k{i}"),
                    "book": "BOOK-A",
                    "value": i as f64
                }),
            )
            .await
            .expect("publish");
    }

    // Two clients subscribing — different sessions, but both should
    // each receive a complete snapshot.
    let a = Client::connect(&server.tcp_url()).await.expect("connect a");
    let b = Client::connect(&server.tcp_url()).await.expect("connect b");

    let mut sub_a = a
        .sow_and_subscribe("/positions", None, None)
        .await
        .expect("sub a");
    let mut sub_b = b
        .sow_and_subscribe("/positions", None, None)
        .await
        .expect("sub b");

    let snapshot_quiet = Duration::from_millis(800);
    let count_a = drain_snapshot(&mut sub_a, snapshot_quiet).await;
    let count_b = drain_snapshot(&mut sub_b, snapshot_quiet).await;
    assert_eq!(count_a, 2_000, "client A snapshot incomplete (got {count_a})");
    assert_eq!(count_b, 2_000, "client B snapshot incomplete (got {count_b})");
}

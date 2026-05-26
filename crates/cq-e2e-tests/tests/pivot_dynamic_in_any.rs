//! Q3 e2e — `PIVOT (...) FOR col IN (ANY)` discovers the value list
//! from the source SOW at SOW time and pivots across it.

use cq_client::Client;
use cq_e2e_tests::{start_server, TopicSpec};
use serde_json::json;
use std::time::Duration;

#[tokio::test]
async fn pivot_in_any_discovers_values_from_source() {
    // Each row's unique key is `rk` (row key). The pivot's auto-
    // anchor logic uses every non-pivot/non-aggregate column, so
    // anchors become (rk, trader). Output: one row per (rk, trader)
    // combo — 4 rows total, each with the relevant pivot cell
    // populated and the rest null.
    let topic = TopicSpec::new("/pivot_any", "rk").with_inline_columns([
        ("rk", "string"),
        ("trader", "string"),
        ("desk", "string"),
        ("qty", "long"),
    ]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    for (rk, trader, desk, qty) in [
        ("r1", "alice", "RATES", 100_i64),
        ("r2", "alice", "FX", 200),
        ("r3", "alice", "EQUITIES", 50),
        ("r4", "bob", "RATES", 25),
    ] {
        client
            .publish(
                "/pivot_any",
                json!({ "rk": rk, "trader": trader, "desk": desk, "qty": qty }),
            )
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(200)).await;

    let snap = client
        .sow_sql(
            "/pivot_any",
            "SELECT * FROM t PIVOT (SUM(qty) FOR desk IN (ANY))",
        )
        .await
        .expect("pivot any sow");
    assert_eq!(snap.len(), 4, "expected 4 (rk,trader) anchor rows, got {snap:?}");
    // Every row carries the 3 dynamically-discovered desk columns
    // (RATES + FX + EQUITIES), with the matching one populated.
    let mut seen_desks: std::collections::HashSet<&str> = std::collections::HashSet::new();
    for row in &snap {
        for col in &["RATES", "FX", "EQUITIES"] {
            assert!(
                row.contains_key(*col),
                "row missing dynamically-discovered pivot col `{col}`: {row:?}"
            );
            if row.get(*col).map(|v| !v.is_null()).unwrap_or(false) {
                seen_desks.insert(*col);
            }
        }
    }
    assert_eq!(seen_desks.len(), 3, "expected all 3 desks to surface");
}

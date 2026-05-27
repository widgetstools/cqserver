//! P3 e2e — `HAVING` clause filters aggregate rows post-finalise.
//!
//! AMPS supports HAVING on the aggregate row, e.g.
//! `GROUP BY desk HAVING SUM(qty) > 100`. P3 wires the compile +
//! evaluate path so cqserver can drop groups whose aggregate doesn't
//! satisfy the predicate.

use cq_client::Client;
use cq_e2e_tests::{start_server, TopicSpec};
use serde_json::json;
use std::time::Duration;

#[tokio::test]
async fn having_drops_groups_under_threshold() {
    let topic = TopicSpec::new("/having-trades", "k").with_inline_columns([
        ("k", "string"),
        ("desk", "string"),
        ("qty", "long"),
    ]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    // RATES: 60+50 = 110; EQUITIES: 25+75 = 100; TECH: 10. 3 desks.
    let rows = [
        ("T1", "RATES", 60_i64),
        ("T2", "RATES", 50),
        ("T3", "EQUITIES", 25),
        ("T4", "EQUITIES", 75),
        ("T5", "TECH", 10),
    ];
    for (k, desk, qty) in rows {
        client
            .publish(
                "/having-trades",
                json!({ "k": k, "desk": desk, "qty": qty }),
            )
            .await
            .expect("publish");
    }
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Baseline: every desk surfaces.
    let baseline = client
        .sow_sql(
            "/having-trades",
            "SELECT desk, SUM(qty) AS total FROM t GROUP BY desk",
        )
        .await
        .expect("baseline aggregate sow");
    assert_eq!(baseline.len(), 3, "baseline must have all 3 desks");

    // HAVING > 50: RATES (110) + EQUITIES (100) pass; TECH (10) drops.
    let gt50 = client
        .sow_sql(
            "/having-trades",
            "SELECT desk, SUM(qty) AS total FROM t \
             GROUP BY desk HAVING SUM(qty) > 50",
        )
        .await
        .expect("HAVING > 50 sow");
    assert_eq!(gt50.len(), 2, "HAVING > 50 must drop TECH (rows={gt50:?})");
    let names: std::collections::HashSet<String> = gt50
        .iter()
        .map(|r| r.get("desk").unwrap().as_str().unwrap().to_string())
        .collect();
    assert!(names.contains("RATES"));
    assert!(names.contains("EQUITIES"));
    assert!(!names.contains("TECH"));

    // HAVING on alias — equivalent to HAVING SUM(qty) > 50.
    let by_alias = client
        .sow_sql(
            "/having-trades",
            "SELECT desk, SUM(qty) AS total FROM t \
             GROUP BY desk HAVING total > 50",
        )
        .await
        .expect("HAVING alias sow");
    assert_eq!(by_alias.len(), gt50.len());

    // Combined AND: HAVING SUM(qty) > 50 AND desk <> 'EQUITIES'.
    // Only RATES survives.
    let combined = client
        .sow_sql(
            "/having-trades",
            "SELECT desk, SUM(qty) AS total FROM t \
             GROUP BY desk HAVING SUM(qty) > 50 AND desk <> 'EQUITIES'",
        )
        .await
        .expect("HAVING AND sow");
    assert_eq!(combined.len(), 1);
    assert_eq!(combined[0].get("desk").unwrap().as_str().unwrap(), "RATES");
}

// ───── Diversification ────────────────────────────────────────────

/// HAVING COUNT(*) — group-cardinality filter.
#[tokio::test]
async fn having_count_star_filters_by_group_size() {
    let topic = TopicSpec::new("/having-count", "k")
        .with_inline_columns([("k", "string"), ("desk", "string")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    for (k, desk) in [
        ("h1", "RATES"),
        ("h2", "RATES"),
        ("h3", "EQUITIES"),
        ("h4", "FX"),
        ("h5", "FX"),
        ("h6", "FX"),
    ] {
        client
            .publish("/having-count", json!({ "k": k, "desk": desk }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    let rows = client
        .sow_sql(
            "/having-count",
            "SELECT desk, COUNT(*) AS c FROM t GROUP BY desk HAVING COUNT(*) >= 2",
        )
        .await
        .expect("HAVING COUNT(*) sow");
    let names: std::collections::HashSet<String> = rows
        .iter()
        .map(|r| r.get("desk").unwrap().as_str().unwrap().to_string())
        .collect();
    assert!(names.contains("RATES"), "RATES has 2 rows");
    assert!(names.contains("FX"), "FX has 3 rows");
    assert!(!names.contains("EQUITIES"), "EQUITIES has 1 row");
}

/// HAVING combined with WHERE — pre-filter rows, then aggregate, then
/// post-filter groups.
#[tokio::test]
async fn having_after_where_aggregates_only_filtered_rows() {
    let topic = TopicSpec::new("/having-where", "k").with_inline_columns([
        ("k", "string"),
        ("desk", "string"),
        ("price", "double"),
    ]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    for (k, desk, price) in [
        ("w1", "RATES", 100.0),
        ("w2", "RATES", 200.0),
        ("w3", "RATES", 5.0),     // below cutoff
        ("w4", "EQUITIES", 150.0),
        ("w5", "EQUITIES", 3.0),  // below cutoff
        ("w6", "FX", 1.0),        // below cutoff (single row drops too)
    ] {
        client
            .publish("/having-where", json!({ "k": k, "desk": desk, "price": price }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(120)).await;

    let rows = client
        .sow_sql(
            "/having-where",
            "SELECT desk, AVG(price) AS a FROM t WHERE price > 10 \
             GROUP BY desk HAVING AVG(price) > 100",
        )
        .await
        .expect("WHERE+HAVING sow");
    // RATES: w1+w2 = avg 150 → keep; EQUITIES: only w4 = 150 → keep; FX: filtered by WHERE → no group.
    let names: std::collections::HashSet<String> = rows
        .iter()
        .map(|r| r.get("desk").unwrap().as_str().unwrap().to_string())
        .collect();
    assert!(names.contains("RATES"));
    assert!(names.contains("EQUITIES"));
    assert!(!names.contains("FX"));
}

/// HAVING that filters out every group → empty result, not error.
#[tokio::test]
async fn having_that_matches_no_groups_returns_empty() {
    let topic = TopicSpec::new("/having-empty", "k")
        .with_inline_columns([("k", "string"), ("d", "string"), ("v", "long")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    for (k, d, v) in [("a", "A", 1), ("b", "B", 2), ("c", "C", 3)] {
        client
            .publish("/having-empty", json!({ "k": k, "d": d, "v": v }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(80)).await;

    let rows = client
        .sow_sql(
            "/having-empty",
            "SELECT d, SUM(v) AS s FROM t GROUP BY d HAVING SUM(v) > 1000",
        )
        .await
        .expect("HAVING-no-match sow");
    assert!(rows.is_empty(), "no group should pass, got {rows:?}");
}

/// R1 — `ORDER BY <select-alias>` of an aggregate output. Before R1
/// cqserver rejected this as "Unknown column"; the AMPS-style PnL
/// ladder pattern (`SELECT desk, SUM(qty) AS total ORDER BY total
/// DESC`) now sorts correctly.
#[tokio::test]
async fn order_by_select_alias_of_aggregate_sorts_correctly() {
    let topic = TopicSpec::new("/r1-order-alias", "k")
        .with_inline_columns([("k", "string"), ("desk", "string"), ("qty", "long")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    for (k, desk, qty) in [
        ("a", "FX", 10),
        ("b", "FX", 20),
        ("c", "RATES", 200),
        ("d", "RATES", 100),
        ("e", "EQUITIES", 50),
    ] {
        client
            .publish("/r1-order-alias", json!({ "k": k, "desk": desk, "qty": qty }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(120)).await;

    // DESC by aggregate alias.
    let rows = client
        .sow_sql(
            "/r1-order-alias",
            "SELECT desk, SUM(qty) AS total FROM t GROUP BY desk ORDER BY total DESC",
        )
        .await
        .expect("ORDER BY <agg-alias> sow");
    assert_eq!(rows.len(), 3);
    let totals: Vec<i64> = rows
        .iter()
        .map(|r| r.get("total").unwrap().as_i64().unwrap())
        .collect();
    assert_eq!(totals, vec![300, 50, 30], "DESC order: RATES(300) > EQUITIES(50) > FX(30)");

    // ASC by aggregate alias.
    let rows = client
        .sow_sql(
            "/r1-order-alias",
            "SELECT desk, SUM(qty) AS total FROM t GROUP BY desk ORDER BY total ASC",
        )
        .await
        .unwrap();
    let totals: Vec<i64> = rows
        .iter()
        .map(|r| r.get("total").unwrap().as_i64().unwrap())
        .collect();
    assert_eq!(totals, vec![30, 50, 300]);
}

/// HAVING + LIMIT chained — verify the limit kicks in *after* the
/// HAVING filter (not the raw groups).
#[tokio::test]
async fn having_with_limit_caps_after_filtering() {
    let topic = TopicSpec::new("/having-chain", "k")
        .with_inline_columns([("k", "string"), ("desk", "string"), ("qty", "long")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    for (k, desk, qty) in [
        ("c1", "A", 10), ("c2", "A", 20),
        ("c3", "B", 100), ("c4", "B", 100),
        ("c5", "C", 50), ("c6", "C", 50),
        ("c7", "D", 1),  // group sum = 1 → HAVING filters out
    ] {
        client
            .publish("/having-chain", json!({ "k": k, "desk": desk, "qty": qty }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(120)).await;

    // After HAVING > 5: A(30), B(200), C(100). LIMIT 2 picks 2 of them.
    let rows = client
        .sow_sql(
            "/having-chain",
            "SELECT desk, SUM(qty) AS s FROM t GROUP BY desk \
             HAVING SUM(qty) > 5 LIMIT 2",
        )
        .await
        .expect("HAVING+LIMIT sow");
    assert_eq!(rows.len(), 2);
    // D must never appear (filtered by HAVING).
    for r in &rows {
        let desk = r.get("desk").unwrap().as_str().unwrap();
        assert_ne!(desk, "D");
    }
}

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

//! P1 e2e — `FROM t alias` + qualified column refs `alias.col`.
//!
//! AMPS supports SQL-92 table aliases everywhere. cqserver's parser
//! rejected them until P1 (see `AMPS_PARITY_WORKLOG.md`). The Atlas
//! demo's Query Builder used to strip aliases client-side; this test
//! pins the server-side behaviour so we can drop that workaround.

use cq_client::Client;
use cq_e2e_tests::{start_server, TopicSpec};
use serde_json::json;
use std::time::Duration;

#[tokio::test]
async fn aliased_sow_matches_unqualified_sow() {
    let topic = TopicSpec::new("/alias-trades", "k").with_inline_columns([
        ("k", "string"),
        ("desk", "string"),
        ("qty", "long"),
        ("price", "double"),
    ]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    let rows = [
        ("T1", "RATES", 100, 100.0),
        ("T2", "RATES", 50, 200.0),
        ("T3", "EQUITIES", 25, 300.0),
        ("T4", "EQUITIES", 75, 400.0),
        ("T5", "EQUITIES", 200, 500.0),
        ("T6", "TECH", 10, 1000.0),
    ];
    for (k, desk, qty, price) in rows {
        client
            .publish(
                "/alias-trades",
                json!({ "k": k, "desk": desk, "qty": qty, "price": price }),
            )
            .await
            .expect("publish");
    }
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Sanity: pure unaliased SOW works against this fixture.
    let baseline = client
        .sow_sql(
            "/alias-trades",
            "SELECT k, desk, price FROM t WHERE price > 250",
        )
        .await
        .expect("baseline sow");
    assert!(!baseline.is_empty(), "baseline must return rows");

    // 1) Filter with alias.col on the LHS of a WHERE comparison.
    let aliased = client
        .sow_sql(
            "/alias-trades",
            "SELECT t.k, t.desk, t.price FROM t WHERE t.price > 250",
        )
        .await
        .expect("aliased filter sow");
    let plain = client
        .sow_sql(
            "/alias-trades",
            "SELECT k, desk, price FROM t WHERE price > 250",
        )
        .await
        .expect("plain filter sow");
    assert_eq!(
        sort_by_k(&aliased),
        sort_by_k(&plain),
        "aliased and plain rows must match"
    );

    // 2) GROUP BY t.desk with aggregate on t.qty.
    let aliased_agg = client
        .sow_sql(
            "/alias-trades",
            "SELECT t.desk, SUM(t.qty) AS total FROM t GROUP BY t.desk",
        )
        .await
        .expect("aliased aggregate sow");
    let plain_agg = client
        .sow_sql(
            "/alias-trades",
            "SELECT desk, SUM(qty) AS total FROM t GROUP BY desk",
        )
        .await
        .expect("plain aggregate sow");
    assert_eq!(
        sort_by_desk(&aliased_agg),
        sort_by_desk(&plain_agg),
        "aliased and plain aggregate rows must match"
    );

    // 3) ORDER BY t.price DESC LIMIT 3 — top movers.
    let top = client
        .sow_sql(
            "/alias-trades",
            "SELECT t.k, t.price FROM t ORDER BY t.price DESC LIMIT 3",
        )
        .await
        .expect("aliased ORDER BY sow");
    assert_eq!(top.len(), 3);
    let top_keys: Vec<String> = top
        .iter()
        .map(|r| r.get("k").unwrap().as_str().unwrap().to_string())
        .collect();
    assert_eq!(top_keys, vec!["T6", "T5", "T4"]);
}

type RowMap = serde_json::Map<String, serde_json::Value>;

fn sort_by_k(rows: &[RowMap]) -> Vec<RowMap> {
    let mut v = rows.to_vec();
    v.sort_by_key(|r| r.get("k").and_then(|x| x.as_str()).unwrap_or("").to_string());
    v
}

fn sort_by_desk(rows: &[RowMap]) -> Vec<RowMap> {
    let mut v = rows.to_vec();
    v.sort_by_key(|r| {
        r.get("desk")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .to_string()
    });
    v
}

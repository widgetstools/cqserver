//! P9 e2e — PERCENTILE_CONT(col, q) + MEDIAN(col).

use cq_client::Client;
use cq_e2e_tests::{start_server, TopicSpec};
use serde_json::json;
use std::time::Duration;

#[tokio::test]
async fn percentile_cont_and_median_match_known_values() {
    let topic = TopicSpec::new("/pct-data", "k").with_inline_columns([
        ("k", "string"),
        ("v", "double"),
    ]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    // {2,4,4,4,5,5,7,9} — median is midpoint of 4 and 5 = 4.5.
    for (i, v) in [2.0_f64, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0]
        .iter()
        .enumerate()
    {
        client
            .publish("/pct-data", json!({ "k": format!("r{i}"), "v": v }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(150)).await;

    let row = client
        .sow_sql(
            "/pct-data",
            "SELECT MEDIAN(v) AS m, \
                    PERCENTILE_CONT(v, 0.5) AS p50, \
                    PERCENTILE_CONT(v, 0.95) AS p95 FROM t",
        )
        .await
        .expect("percentile sow")
        .pop()
        .expect("one row");
    let m = row.get("m").and_then(|x| x.as_f64()).unwrap();
    let p50 = row.get("p50").and_then(|x| x.as_f64()).unwrap();
    let p95 = row.get("p95").and_then(|x| x.as_f64()).unwrap();
    assert!((m - 4.5).abs() < 1e-9, "median expected 4.5, got {m}");
    assert!((p50 - 4.5).abs() < 1e-9);
    // p95 = interp at rank 6.65 between values[6]=7 and values[7]=9
    //     = 7 + 0.65 * (9 - 7) = 8.3
    assert!((p95 - 8.3).abs() < 1e-9, "p95 expected 8.3, got {p95}");
}

// ───── Diversification ────────────────────────────────────────────

/// q = 0 returns the min, q = 1 returns the max.
#[tokio::test]
async fn percentile_cont_at_extremes_returns_min_and_max() {
    let topic = TopicSpec::new("/pct-ext", "k")
        .with_inline_columns([("k", "string"), ("v", "double")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    for (i, v) in [3.0_f64, 1.0, 4.0, 1.0, 5.0, 9.0, 2.0, 6.0].iter().enumerate() {
        client
            .publish("/pct-ext", json!({ "k": format!("r{i}"), "v": v }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(120)).await;

    let row = client
        .sow_sql(
            "/pct-ext",
            "SELECT PERCENTILE_CONT(v, 0) AS lo, PERCENTILE_CONT(v, 1) AS hi FROM t",
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(row.get("lo").and_then(|x| x.as_f64()).unwrap(), 1.0);
    assert_eq!(row.get("hi").and_then(|x| x.as_f64()).unwrap(), 9.0);
}

/// PERCENTILE_CONT with GROUP BY computes per-group quantile.
#[tokio::test]
async fn percentile_cont_group_by_per_partition() {
    let topic = TopicSpec::new("/pct-group", "k").with_inline_columns([
        ("k", "string"),
        ("g", "string"),
        ("v", "double"),
    ]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");
    for (k, g, v) in [
        ("a1", "A", 10.0), ("a2", "A", 20.0), ("a3", "A", 30.0),
        ("b1", "B", 100.0), ("b2", "B", 200.0), ("b3", "B", 300.0),
    ] {
        client
            .publish("/pct-group", json!({ "k": k, "g": g, "v": v }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    let rows = client
        .sow_sql(
            "/pct-group",
            "SELECT g, MEDIAN(v) AS m FROM t GROUP BY g",
        )
        .await
        .unwrap();
    let by_g: std::collections::HashMap<String, f64> = rows
        .iter()
        .map(|r| {
            (
                r.get("g").unwrap().as_str().unwrap().to_string(),
                r.get("m").unwrap().as_f64().unwrap(),
            )
        })
        .collect();
    assert_eq!(by_g["A"], 20.0);
    assert_eq!(by_g["B"], 200.0);
}

/// Invalid q (outside [0, 1]) → clean server error.
#[tokio::test]
async fn percentile_cont_invalid_q_rejected() {
    use cq_client::ClientError;
    let topic = TopicSpec::new("/pct-bad-q", "k")
        .with_inline_columns([("k", "string"), ("v", "double")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");
    client
        .publish("/pct-bad-q", json!({ "k": "r1", "v": 1.0 }))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(80)).await;

    let r = client
        .sow_sql("/pct-bad-q", "SELECT PERCENTILE_CONT(v, 1.5) AS p FROM t")
        .await;
    assert!(
        matches!(r, Err(ClientError::Server(_))),
        "q outside [0,1] must error, got {r:?}"
    );
    let r2 = client
        .sow_sql("/pct-bad-q", "SELECT PERCENTILE_CONT(v, -0.1) AS p FROM t")
        .await;
    assert!(matches!(r2, Err(ClientError::Server(_))));
}

/// Empty topic → MEDIAN and PERCENTILE_CONT return null (no rows to interpolate).
#[tokio::test]
async fn percentile_cont_on_empty_topic_is_null() {
    let topic = TopicSpec::new("/pct-empty", "k")
        .with_inline_columns([("k", "string"), ("v", "double")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    let rows = client
        .sow_sql(
            "/pct-empty",
            "SELECT MEDIAN(v) AS m, PERCENTILE_CONT(v, 0.5) AS p FROM t",
        )
        .await
        .unwrap();
    // Either no rows, or one row with both fields absent/null.
    if let Some(row) = rows.first() {
        let m = row.get("m");
        let p = row.get("p");
        assert!(m.is_none() || m.unwrap().is_null());
        assert!(p.is_none() || p.unwrap().is_null());
    }
}

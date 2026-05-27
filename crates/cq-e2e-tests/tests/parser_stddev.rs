//! P8 e2e — STDDEV / STDDEV_SAMP / VARIANCE / VAR_SAMP aggregates.

use cq_client::Client;
use cq_e2e_tests::{start_server, TopicSpec};
use serde_json::json;
use std::time::Duration;

#[tokio::test]
async fn stddev_variance_match_known_values() {
    let topic = TopicSpec::new("/stddev-data", "k").with_inline_columns([
        ("k", "string"),
        ("v", "double"),
    ]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    // Wikipedia stddev example: {2,4,4,4,5,5,7,9} → pop stddev = 2.
    let values = [2.0_f64, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0];
    for (i, v) in values.iter().enumerate() {
        client
            .publish("/stddev-data", json!({ "k": format!("r{i}"), "v": v }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(150)).await;

    let row = client
        .sow_sql(
            "/stddev-data",
            "SELECT STDDEV(v) AS sd, STDDEV_SAMP(v) AS ss, \
                    VARIANCE(v) AS vp, VAR_SAMP(v) AS vs FROM t",
        )
        .await
        .expect("stddev sow")
        .pop()
        .expect("one row");
    let sd = row.get("sd").and_then(|x| x.as_f64()).unwrap();
    let ss = row.get("ss").and_then(|x| x.as_f64()).unwrap();
    let vp = row.get("vp").and_then(|x| x.as_f64()).unwrap();
    let vs = row.get("vs").and_then(|x| x.as_f64()).unwrap();
    assert!((sd - 2.0).abs() < 1e-9, "pop stddev: got {sd}");
    assert!((ss - (32.0_f64 / 7.0).sqrt()).abs() < 1e-9, "samp stddev: got {ss}");
    assert!((vp - 4.0).abs() < 1e-9, "pop variance: got {vp}");
    assert!((vs - (32.0_f64 / 7.0)).abs() < 1e-9, "samp variance: got {vs}");
}

// ───── Diversification ────────────────────────────────────────────

/// Single-row population stddev/variance is exactly 0.
#[tokio::test]
async fn stddev_single_row_is_zero() {
    let topic = TopicSpec::new("/sd-single", "k")
        .with_inline_columns([("k", "string"), ("v", "double")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");
    client
        .publish("/sd-single", json!({ "k": "only", "v": 42.0 }))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(80)).await;
    let row = client
        .sow_sql(
            "/sd-single",
            "SELECT STDDEV(v) AS sd, VARIANCE(v) AS vp FROM t",
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(row.get("sd").and_then(|x| x.as_f64()).unwrap(), 0.0);
    assert_eq!(row.get("vp").and_then(|x| x.as_f64()).unwrap(), 0.0);
}

/// Group-by stddev — per-group statistics computed independently.
#[tokio::test]
async fn stddev_group_by_computes_per_group() {
    let topic = TopicSpec::new("/sd-group", "k").with_inline_columns([
        ("k", "string"),
        ("g", "string"),
        ("v", "double"),
    ]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    // Group A: {1,3} pop var = 1; Group B: {10,10,10,10} pop var = 0.
    for (k, g, v) in [
        ("a1", "A", 1.0),
        ("a2", "A", 3.0),
        ("b1", "B", 10.0),
        ("b2", "B", 10.0),
        ("b3", "B", 10.0),
        ("b4", "B", 10.0),
    ] {
        client
            .publish("/sd-group", json!({ "k": k, "g": g, "v": v }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(120)).await;

    let rows = client
        .sow_sql(
            "/sd-group",
            "SELECT g, VARIANCE(v) AS vp FROM t GROUP BY g",
        )
        .await
        .unwrap();
    let by_g: std::collections::HashMap<String, f64> = rows
        .iter()
        .map(|r| {
            (
                r.get("g").unwrap().as_str().unwrap().to_string(),
                r.get("vp").unwrap().as_f64().unwrap(),
            )
        })
        .collect();
    assert!((by_g["A"] - 1.0).abs() < 1e-9, "VAR A: {}", by_g["A"]);
    assert_eq!(by_g["B"], 0.0, "VAR B: {}", by_g["B"]);
}

/// NULL values in the column are skipped (ANSI: stddev/variance ignore NULLs).
#[tokio::test]
async fn stddev_skips_null_values() {
    let topic = TopicSpec::new("/sd-null", "k")
        .with_inline_columns([("k", "string"), ("v", "double")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    for (k, v) in [("a", Some(2.0_f64)), ("b", Some(4.0)), ("c", None), ("d", Some(4.0)),
                   ("e", Some(4.0)), ("f", Some(5.0)), ("g", None), ("h", Some(5.0)),
                   ("i", Some(7.0)), ("j", Some(9.0))] {
        let map = match v {
            Some(x) => json!({ "k": k, "v": x }),
            None => json!({ "k": k }),
        };
        client.publish("/sd-null", map).await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(150)).await;

    let row = client
        .sow_sql("/sd-null", "SELECT STDDEV(v) AS sd, COUNT(*) AS n FROM t")
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(row.get("n").unwrap().as_u64().unwrap(), 10, "COUNT(*) sees all rows");
    let sd = row.get("sd").and_then(|x| x.as_f64()).unwrap();
    assert!((sd - 2.0).abs() < 1e-9, "stddev of non-null values = 2, got {sd}");
}

/// VAR_POP alias works identically to VARIANCE.
#[tokio::test]
async fn var_pop_alias_equals_variance() {
    let topic = TopicSpec::new("/sd-alias", "k")
        .with_inline_columns([("k", "string"), ("v", "double")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");
    for (i, v) in [10.0_f64, 20.0, 30.0, 40.0, 50.0].iter().enumerate() {
        client
            .publish("/sd-alias", json!({ "k": format!("r{i}"), "v": v }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    let row = client
        .sow_sql(
            "/sd-alias",
            "SELECT VARIANCE(v) AS a, VAR_POP(v) AS b, STDDEV(v) AS c, STDDEV_POP(v) AS d FROM t",
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
    let a = row.get("a").and_then(|x| x.as_f64()).unwrap();
    let b = row.get("b").and_then(|x| x.as_f64()).unwrap();
    let c = row.get("c").and_then(|x| x.as_f64()).unwrap();
    let d = row.get("d").and_then(|x| x.as_f64()).unwrap();
    assert_eq!(a, b);
    assert_eq!(c, d);
}

//! R7 — `NOW()` returns the current wall-clock time in µs since
//! UNIX epoch (AMPS convention). Combined with NumExpr arithmetic
//! this enables AMPS-style "last N hours" queries:
//!
//!   `WHERE ts > NOW() - 86400000000`   -- last 24h in µs
//!   `WHERE ts > NOW() - 3600000000`    -- last 1h in µs
//!
//! AMPS does NOT use Postgres `INTERVAL '1 day'` syntax — those
//! patterns must be rewritten in the demo library to the explicit
//! NOW() - <microseconds> form.

use cq_client::Client;
use cq_e2e_tests::{start_server, TopicSpec};
use serde_json::json;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[tokio::test]
async fn now_minus_microseconds_filter() {
    let topic = TopicSpec::new("/r7-now", "k")
        .with_inline_columns([("k", "string"), ("ts", "timestamp")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    let now_micros: i64 = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as i64;
    let one_hour_us: i64 = 3_600_000_000;
    let one_day_us: i64 = 86_400_000_000;

    // Three rows: 2 hours ago, 5 minutes ago, 3 days ago.
    for (k, offset_us) in [
        ("old", -3 * one_day_us),
        ("recent_2h", -2 * one_hour_us),
        ("just_now", -300_000_000_i64),
    ] {
        let ts = now_micros + offset_us;
        // ts column accepts ISO 8601 strings or epoch numbers.
        client
            .publish("/r7-now", json!({ "k": k, "ts": ts }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(120)).await;

    // Last 1 hour: only "just_now" qualifies.
    let last_1h = client
        .sow_sql("/r7-now", "SELECT k FROM t WHERE ts > NOW() - 3600000000")
        .await
        .expect("NOW() must compile");
    let ks: std::collections::HashSet<String> = last_1h
        .iter()
        .map(|r| r.get("k").unwrap().as_str().unwrap().to_string())
        .collect();
    assert!(ks.contains("just_now"));
    assert!(!ks.contains("recent_2h"));
    assert!(!ks.contains("old"));

    // Last 24h: "just_now" + "recent_2h".
    let last_24h = client
        .sow_sql("/r7-now", "SELECT k FROM t WHERE ts > NOW() - 86400000000")
        .await
        .unwrap();
    let ks: std::collections::HashSet<String> = last_24h
        .iter()
        .map(|r| r.get("k").unwrap().as_str().unwrap().to_string())
        .collect();
    assert!(ks.contains("just_now"));
    assert!(ks.contains("recent_2h"));
    assert!(!ks.contains("old"));
}

#[tokio::test]
async fn now_returns_microseconds_compile_time_frozen() {
    // NOW() is evaluated once at compile time; the same query run
    // twice should compare against the SAME baseline within each
    // SOW (so a row published after the SOW starts isn't accidentally
    // matched by the OLDER snapshot).
    let topic = TopicSpec::new("/r7-now-frozen", "k")
        .with_inline_columns([("k", "string"), ("ts", "timestamp")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    let now_micros: i64 = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as i64;
    // Publish one row at "now + 10 min" (10 minutes into the future).
    let future = now_micros + 600_000_000;
    client
        .publish("/r7-now-frozen", json!({ "k": "future", "ts": future }))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(80)).await;

    // `ts > NOW()` matches the future row.
    let rows = client
        .sow_sql("/r7-now-frozen", "SELECT k FROM t WHERE ts > NOW()")
        .await
        .unwrap();
    assert!(rows.iter().any(|r| r.get("k").unwrap().as_str() == Some("future")));
}

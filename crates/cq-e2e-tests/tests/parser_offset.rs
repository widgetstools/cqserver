//! P4 e2e — `LIMIT n OFFSET m` paginates after ORDER BY.

use cq_client::Client;
use cq_e2e_tests::{start_server, TopicSpec};
use serde_json::json;
use std::time::Duration;

#[tokio::test]
async fn offset_paginates_after_order_by() {
    let topic = TopicSpec::new("/offset-trades", "k").with_inline_columns([
        ("k", "string"),
        ("seq", "long"),
    ]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    for i in 0..20_i64 {
        client
            .publish(
                "/offset-trades",
                json!({ "k": format!("k{i:02}"), "seq": i }),
            )
            .await
            .expect("publish");
    }
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Page 1 — first 5.
    let p1 = client
        .sow_sql(
            "/offset-trades",
            "SELECT k, seq FROM t ORDER BY seq ASC LIMIT 5 OFFSET 0",
        )
        .await
        .expect("page 1 sow");
    assert_eq!(p1.len(), 5);
    assert_eq!(p1[0].get("seq").unwrap().as_i64().unwrap(), 0);
    assert_eq!(p1[4].get("seq").unwrap().as_i64().unwrap(), 4);

    // Page 2 — seqs 5..9.
    let p2 = client
        .sow_sql(
            "/offset-trades",
            "SELECT k, seq FROM t ORDER BY seq ASC LIMIT 5 OFFSET 5",
        )
        .await
        .expect("page 2 sow");
    assert_eq!(p2.len(), 5);
    assert_eq!(p2[0].get("seq").unwrap().as_i64().unwrap(), 5);
    assert_eq!(p2[4].get("seq").unwrap().as_i64().unwrap(), 9);

    // Page beyond end — empty.
    let none = client
        .sow_sql(
            "/offset-trades",
            "SELECT k, seq FROM t ORDER BY seq ASC LIMIT 5 OFFSET 50",
        )
        .await
        .expect("beyond-end sow");
    assert_eq!(none.len(), 0);
}

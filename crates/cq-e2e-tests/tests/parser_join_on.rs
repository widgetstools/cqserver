//! P11 e2e — `INNER JOIN ... ON a.col = b.col` (translated to USING)
//! returns the same rows as the equivalent USING form.

use cq_client::Client;
use cq_e2e_tests::{start_server, TopicSpec};
use serde_json::json;
use std::time::Duration;

#[tokio::test]
async fn join_on_equi_matches_join_using() {
    let positions = TopicSpec::new("/pos_on", "positionKey").with_inline_columns([
        ("positionKey", "string"),
        ("cusip", "string"),
        ("marketValue", "double"),
    ]);
    let securities = TopicSpec::new("/sec_on", "cusip").with_inline_columns([
        ("cusip", "string"),
        ("sector", "string"),
    ]);
    let server = start_server(vec![positions, securities]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");
    for (c, s) in [("AAPL", "Tech"), ("JPM", "Banks"), ("MSFT", "Tech")] {
        client
            .publish("/sec_on", json!({ "cusip": c, "sector": s }))
            .await
            .unwrap();
    }
    for (k, c, mv) in [
        ("p1", "AAPL", 10_000.0_f64),
        ("p2", "JPM", 20_000.0),
        ("p3", "MSFT", 30_000.0),
    ] {
        client
            .publish(
                "/pos_on",
                json!({ "positionKey": k, "cusip": c, "marketValue": mv }),
            )
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(200)).await;

    let on_sql = "SELECT sector, SUM(marketValue) AS exposure \
                  FROM pos_on p JOIN sec_on s ON p.cusip = s.cusip \
                  GROUP BY sector";
    let using_sql = "SELECT sector, SUM(marketValue) AS exposure \
                     FROM pos_on JOIN sec_on USING (cusip) \
                     GROUP BY sector";
    let by_on = client.sow_sql("/pos_on", on_sql).await.expect("on sow");
    let by_using = client.sow_sql("/pos_on", using_sql).await.expect("using sow");

    fn map_by_sector(rows: &[serde_json::Map<String, serde_json::Value>]) -> std::collections::HashMap<String, f64> {
        rows.iter()
            .map(|r| {
                (
                    r.get("sector").unwrap().as_str().unwrap().to_string(),
                    r.get("exposure").unwrap().as_f64().unwrap(),
                )
            })
            .collect()
    }
    assert_eq!(map_by_sector(&by_on), map_by_sector(&by_using));
    assert_eq!(by_on.len(), 2);
}

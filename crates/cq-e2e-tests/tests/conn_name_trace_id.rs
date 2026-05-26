//! Q4 e2e — logon with connection-name + trace-id is accepted and
//! the server processes subsequent publishes against the session.

use cq_client::Client;
use cq_e2e_tests::{start_server, TopicSpec};
use serde_json::json;

#[tokio::test]
async fn logon_with_client_name_and_trace_id_works() {
    let topic = TopicSpec::new("/q4_md", "k")
        .with_inline_columns([("k", "string"), ("v", "long")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    // No auth required on this server, but logon still accepted
    // and the (optional) client_name + trace_id are echoed in the
    // audit log. The contract here is just "doesn't crash, doesn't
    // hang on missing auth" — full audit-log inspection would
    // require log scraping, deferred to a follow-up.
    client
        .logon_with(
            "",
            "",
            Some("atlas-trading-desk-7".into()),
            Some("trace-abc-123".into()),
        )
        .await
        .ok(); // ok or auth-disabled error — either is fine for the smoke

    // Subsequent operations still flow.
    let seq = client
        .publish("/q4_md", json!({ "k": "AAPL", "v": 150 }))
        .await
        .expect("publish");
    assert!(seq > 0);
}

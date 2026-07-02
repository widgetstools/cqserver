//! S16 — JWT auth end-to-end.
//!
//! Configure a server with `[auth.jwt]` (no static users), then drive
//! Logon via the SDK's new `logon_jwt` method. Verify:
//!   - A valid token is accepted and subsequent commands run.
//!   - An expired token is rejected.
//!   - A token signed with the wrong secret is rejected.

use cq_client::Client;
use cq_e2e_tests::{
    start_server_with, AuthOpts, JwtAuthOpts, ServerOpts, TopicSpec,
};
use serde_json::json;

fn issue_token(secret: &str, claims: serde_json::Value) -> String {
    let header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::HS256);
    jsonwebtoken::encode(
        &header,
        &claims,
        &jsonwebtoken::EncodingKey::from_secret(secret.as_bytes()),
    )
    .expect("encode jwt")
}

fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

#[tokio::test]
async fn valid_jwt_logon_succeeds() {
    let topic = TopicSpec::new("/jwt-e2e", "k").with_inline_columns([
        ("k", "string"),
        ("v", "long"),
    ]);
    let secret = "e2e-test-secret";
    let server = start_server_with(
        vec![topic],
        ServerOpts {
            auth: Some(AuthOpts {
                users: Vec::new(),
                jwt: Some(JwtAuthOpts {
                    secret: secret.into(),
                    ..JwtAuthOpts::default()
                }),
            }),
            ..ServerOpts::default()
        },
    )
    .await;

    let token = issue_token(
        secret,
        json!({
            "sub": "alice",
            "entitlements": ["*:*"],
            "exp": (unix_now() + 3600) as i64,
        }),
    );
    let client = Client::connect(&server.tcp_url()).await.expect("connect");
    client.logon_jwt(&token).await.expect("jwt logon");

    // After a successful JWT logon, normal commands should succeed.
    let seq = client
        .publish("/jwt-e2e", json!({ "k": "row1", "v": 42 }))
        .await
        .expect("publish");
    assert!(seq > 0);
}

#[tokio::test]
async fn expired_jwt_logon_rejected() {
    let topic = TopicSpec::new("/jwt-bad", "k").with_inline_columns([("k", "string")]);
    let secret = "e2e-test-secret";
    let server = start_server_with(
        vec![topic],
        ServerOpts {
            auth: Some(AuthOpts {
                users: Vec::new(),
                jwt: Some(JwtAuthOpts {
                    secret: secret.into(),
                    ..JwtAuthOpts::default()
                }),
            }),
            ..ServerOpts::default()
        },
    )
    .await;

    let expired = issue_token(
        secret,
        json!({
            "sub": "alice",
            "entitlements": [],
            "exp": (unix_now() - 7200) as i64,
        }),
    );
    let client = Client::connect(&server.tcp_url()).await.expect("connect");
    let err = client.logon_jwt(&expired).await;
    assert!(err.is_err(), "expected expired JWT logon to fail; got Ok");
}

/// D7/P0.7 — the JWT secret can be indirected through `env://VAR`
/// instead of living in plaintext TOML. Configure `[auth.jwt].secret
/// = "env://..."`, set the env var in *this* test process (the
/// server subprocess inherits it — `std::process::Command` doesn't
/// clear the parent environment by default), and confirm a token
/// signed with the env-var value authenticates end-to-end. This
/// proves the resolver actually runs at config-load time in the real
/// server binary, not just in the `cq-server` unit tests.
#[tokio::test]
async fn jwt_secret_resolved_from_env_var_authenticates() {
    let topic = TopicSpec::new("/jwt-env-e2e", "k").with_inline_columns([
        ("k", "string"),
        ("v", "long"),
    ]);
    let env_var = format!("CQ_E2E_JWT_SECRET_{}", std::process::id());
    let secret = "env-indirected-secret-value";
    std::env::set_var(&env_var, secret);

    let server = start_server_with(
        vec![topic],
        ServerOpts {
            auth: Some(AuthOpts {
                users: Vec::new(),
                jwt: Some(JwtAuthOpts {
                    secret: format!("env://{env_var}"),
                    ..JwtAuthOpts::default()
                }),
            }),
            ..ServerOpts::default()
        },
    )
    .await;

    let token = issue_token(
        secret,
        json!({
            "sub": "alice",
            "entitlements": ["*:*"],
            "exp": (unix_now() + 3600) as i64,
        }),
    );
    let client = Client::connect(&server.tcp_url()).await.expect("connect");
    client
        .logon_jwt(&token)
        .await
        .expect("jwt logon with env-resolved secret");

    let seq = client
        .publish("/jwt-env-e2e", json!({ "k": "row1", "v": 42 }))
        .await
        .expect("publish");
    assert!(seq > 0);

    std::env::remove_var(&env_var);
}

#[tokio::test]
async fn jwt_signed_with_wrong_secret_rejected() {
    let topic = TopicSpec::new("/jwt-wrong-sig", "k").with_inline_columns([("k", "string")]);
    let server_secret = "trusted-secret";
    let server = start_server_with(
        vec![topic],
        ServerOpts {
            auth: Some(AuthOpts {
                users: Vec::new(),
                jwt: Some(JwtAuthOpts {
                    secret: server_secret.into(),
                    ..JwtAuthOpts::default()
                }),
            }),
            ..ServerOpts::default()
        },
    )
    .await;

    let bogus = issue_token(
        "DIFFERENT-secret",
        json!({
            "sub": "alice",
            "entitlements": [],
            "exp": (unix_now() + 3600) as i64,
        }),
    );
    let client = Client::connect(&server.tcp_url()).await.expect("connect");
    let err = client.logon_jwt(&bogus).await;
    assert!(err.is_err(), "expected wrong-signed JWT logon to fail");
}

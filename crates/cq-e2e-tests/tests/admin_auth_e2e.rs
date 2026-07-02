//! Task 2.1 (D1 / P0.1) — empirical proof that `admin_token` actually
//! gates every admin route.
//!
//! `config.rs`'s doc-comment on `admin_token` claims it guards every
//! admin endpoint except `GET /healthz`. `PRODUCTION_READINESS.md`
//! (P0.1) claims the admin port has no authentication at all. Those
//! can't both be true — this suite spins up a real release `cqserver`
//! binary with `admin_token` set and hits every mutating (and
//! config-disclosing) admin route with (a) no token, (b) the wrong
//! token, (c) the correct token, asserting 401 / 401 / success, plus
//! confirms `/healthz` stays open with no token at all.

use cq_client::Client;
use cq_e2e_tests::{start_server_with, ServerOpts, TopicSpec};
use serde_json::json;
use std::time::Duration;

const TOKEN: &str = "s3cret-admin-token";

fn topic() -> TopicSpec {
    TopicSpec::new("/admin-auth", "k")
        .with_inline_columns([("k", "string"), ("v", "double")])
}

async fn server_with_token() -> cq_e2e_tests::ServerHandle {
    start_server_with(
        vec![topic()],
        ServerOpts {
            admin_token: Some(TOKEN.to_string()),
            ..ServerOpts::default()
        },
    )
    .await
}

fn no_auth(rc: &reqwest::Client, url: &str) -> reqwest::RequestBuilder {
    rc.get(url)
}

/// `/healthz` must stay open even with `admin_token` set — orchestrator
/// liveness probes can't be expected to carry a credential.
#[tokio::test]
async fn healthz_open_without_token_even_when_admin_token_set() {
    let server = server_with_token().await;
    let resp = reqwest::get(format!("{}/healthz", server.admin_url()))
        .await
        .expect("healthz request");
    assert_eq!(resp.status(), 200, "healthz must stay open");
}

/// GET routes: no token / wrong token → 401; correct token → 200.
/// Covers the read-only routes that leak topology/config, which the
/// brief calls out explicitly (`/admin/config` disclosing the whole
/// TOML, `/topics`, `/subscriptions`, `/stats`, `/metrics`, `/queues`,
/// `/admin/catalog`, `/admin/clients`, `/admin/replication`).
#[tokio::test]
async fn get_routes_require_token() {
    let server = server_with_token().await;
    let rc = reqwest::Client::new();
    let base = server.admin_url();

    let get_routes = [
        "/",
        "/stats",
        "/topics",
        "/subscriptions",
        "/metrics",
        "/admin/replication",
        "/queues",
        "/admin/views",
        "/admin/catalog",
        "/admin/config",
        "/admin/clients",
    ];

    for path in get_routes {
        let url = format!("{base}{path}");

        // (a) no token
        let resp = no_auth(&rc, &url).send().await.expect("no-token request");
        assert_eq!(
            resp.status(),
            401,
            "GET {path} without token should 401, got {:?}",
            resp.status()
        );

        // (b) wrong token
        let resp = rc
            .get(&url)
            .header("Authorization", "Bearer wrong-token")
            .send()
            .await
            .expect("wrong-token request");
        assert_eq!(
            resp.status(),
            401,
            "GET {path} with wrong token should 401, got {:?}",
            resp.status()
        );

        // (c) correct token
        let resp = rc
            .get(&url)
            .header("Authorization", format!("Bearer {TOKEN}"))
            .send()
            .await
            .expect("correct-token request");
        assert!(
            resp.status().is_success(),
            "GET {path} with correct token should succeed, got {:?}",
            resp.status()
        );
    }
}

/// `GET /admin/config` specifically: unauthenticated access must not
/// leak the rendered TOML (which contains topology, ports, and, if
/// configured, the token itself). Verifies both the 401 status AND
/// that the body isn't the config content.
#[tokio::test]
async fn admin_config_leak_is_gated() {
    let server = server_with_token().await;
    let rc = reqwest::Client::new();
    let url = format!("{}/admin/config", server.admin_url());

    let resp = rc.get(&url).send().await.expect("no-token request");
    assert_eq!(resp.status(), 401);
    let body = resp.text().await.unwrap_or_default();
    assert!(
        !body.contains("admin_addr") && !body.contains(TOKEN),
        "unauthenticated /admin/config response leaked config content: {body}"
    );

    let resp = rc
        .get(&url)
        .header("Authorization", format!("Bearer {TOKEN}"))
        .send()
        .await
        .expect("authed request");
    assert!(resp.status().is_success());
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("admin_addr"),
        "authenticated /admin/config should return the TOML"
    );
}

/// `DELETE /subscriptions/:id` — dangerous mutating route explicitly
/// called out in the brief. Uses a real subscription id for the
/// success case so we prove the token check happens (not just that a
/// bogus id 404s past the guard).
#[tokio::test]
async fn delete_subscription_requires_token() {
    let server = server_with_token().await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");
    let _sub = client
        .subscribe("/admin-auth", None)
        .await
        .expect("subscribe");
    tokio::time::sleep(Duration::from_millis(100)).await;

    let rc = reqwest::Client::new();
    let subs: serde_json::Value = rc
        .get(format!("{}/subscriptions", server.admin_url()))
        .header("Authorization", format!("Bearer {TOKEN}"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let sub_id = subs
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s.get("topic").and_then(|v| v.as_str()) == Some("/admin-auth"))
        .and_then(|s| s.get("subId").and_then(|v| v.as_str()))
        .expect("sub id present")
        .replace(':', "%3A");

    let url = format!("{}/subscriptions/{}", server.admin_url(), sub_id);

    let resp = rc.delete(&url).send().await.expect("no-token delete");
    assert_eq!(resp.status(), 401, "no-token DELETE should 401");

    let resp = rc
        .delete(&url)
        .header("Authorization", "Bearer wrong")
        .send()
        .await
        .expect("wrong-token delete");
    assert_eq!(resp.status(), 401, "wrong-token DELETE should 401");

    let resp = rc
        .delete(&url)
        .header("Authorization", format!("Bearer {TOKEN}"))
        .send()
        .await
        .expect("correct-token delete");
    assert!(
        resp.status().is_success(),
        "correct-token DELETE should succeed, got {:?}",
        resp.status()
    );
}

/// `POST /admin/rotate-journal/:topic` — dangerous mutating route
/// explicitly called out in the brief.
#[tokio::test]
async fn rotate_journal_requires_token() {
    let t = TopicSpec::new("/rot-auth", "k")
        .with_inline_columns([("k", "string")])
        .with_persist();
    let server = start_server_with(
        vec![t],
        ServerOpts {
            admin_token: Some(TOKEN.to_string()),
            ..ServerOpts::default()
        },
    )
    .await;
    let rc = reqwest::Client::new();
    let url = format!("{}/admin/rotate-journal/%2Frot-auth", server.admin_url());

    let resp = rc.post(&url).send().await.expect("no-token post");
    assert_eq!(resp.status(), 401);

    let resp = rc
        .post(&url)
        .header("Authorization", "Bearer wrong")
        .send()
        .await
        .expect("wrong-token post");
    assert_eq!(resp.status(), 401);

    let resp = rc
        .post(&url)
        .header("Authorization", format!("Bearer {TOKEN}"))
        .send()
        .await
        .expect("correct-token post");
    assert!(resp.status().is_success(), "got {:?}", resp.status());
}

/// `POST /admin/shrink-store-all` — dangerous mutating route
/// explicitly called out in the brief.
#[tokio::test]
async fn shrink_store_all_requires_token() {
    let server = server_with_token().await;
    let rc = reqwest::Client::new();
    let url = format!("{}/admin/shrink-store-all", server.admin_url());

    let resp = rc.post(&url).send().await.expect("no-token post");
    assert_eq!(resp.status(), 401);

    let resp = rc
        .post(&url)
        .header("Authorization", "Bearer wrong")
        .send()
        .await
        .expect("wrong-token post");
    assert_eq!(resp.status(), 401);

    let resp = rc
        .post(&url)
        .header("Authorization", format!("Bearer {TOKEN}"))
        .send()
        .await
        .expect("correct-token post");
    assert!(resp.status().is_success(), "got {:?}", resp.status());
}

/// `POST /admin/views` (create) and `DELETE /admin/views/:name`
/// (delete) — both explicitly called out in the brief.
#[tokio::test]
async fn view_create_and_delete_require_token() {
    let server = server_with_token().await;
    let rc = reqwest::Client::new();
    let create_url = format!("{}/admin/views", server.admin_url());
    let body = json!({
        "name": "/v_auth",
        "source": "/admin-auth",
        "sql": "SELECT k, COUNT(*) AS n FROM t GROUP BY k"
    });

    // (a) no token
    let resp = rc
        .post(&create_url)
        .json(&body)
        .send()
        .await
        .expect("no-token create");
    assert_eq!(resp.status(), 401, "create-view no-token should 401");

    // (b) wrong token
    let resp = rc
        .post(&create_url)
        .header("Authorization", "Bearer wrong")
        .json(&body)
        .send()
        .await
        .expect("wrong-token create");
    assert_eq!(resp.status(), 401, "create-view wrong-token should 401");

    // (c) correct token
    let resp = rc
        .post(&create_url)
        .header("Authorization", format!("Bearer {TOKEN}"))
        .json(&body)
        .send()
        .await
        .expect("correct-token create");
    assert!(
        resp.status().is_success(),
        "create-view correct-token should succeed, got {:?}",
        resp.status()
    );
    tokio::time::sleep(Duration::from_millis(100)).await;

    let delete_url = format!("{}/admin/views/%2Fv_auth", server.admin_url());

    let resp = rc.delete(&delete_url).send().await.expect("no-token delete");
    assert_eq!(resp.status(), 401, "delete-view no-token should 401");

    let resp = rc
        .delete(&delete_url)
        .header("Authorization", "Bearer wrong")
        .send()
        .await
        .expect("wrong-token delete");
    assert_eq!(resp.status(), 401, "delete-view wrong-token should 401");

    let resp = rc
        .delete(&delete_url)
        .header("Authorization", format!("Bearer {TOKEN}"))
        .send()
        .await
        .expect("correct-token delete");
    assert!(
        resp.status().is_success(),
        "delete-view correct-token should succeed, got {:?}",
        resp.status()
    );
}

/// `POST /admin/add-column/:topic` — mutating schema-evolution route
/// explicitly called out in the brief.
#[tokio::test]
async fn add_column_requires_token() {
    let server = server_with_token().await;
    let rc = reqwest::Client::new();
    let url = format!(
        "{}/admin/add-column/%2Fadmin-auth?name=desk&type=string",
        server.admin_url()
    );

    let resp = rc.post(&url).send().await.expect("no-token post");
    assert_eq!(resp.status(), 401);

    let resp = rc
        .post(&url)
        .header("Authorization", "Bearer wrong")
        .send()
        .await
        .expect("wrong-token post");
    assert_eq!(resp.status(), 401);

    let resp = rc
        .post(&url)
        .header("Authorization", format!("Bearer {TOKEN}"))
        .send()
        .await
        .expect("correct-token post");
    assert!(resp.status().is_success(), "got {:?}", resp.status());
}

/// Sanity check on the flip side: with `admin_token` unset (the
/// default), the admin API stays open — no regression for existing
/// deployments/tests that don't configure a token.
#[tokio::test]
async fn no_token_configured_leaves_api_open() {
    let server = cq_e2e_tests::start_server(vec![topic()]).await;
    let resp = reqwest::get(format!("{}/admin/config", server.admin_url()))
        .await
        .expect("request");
    assert!(
        resp.status().is_success(),
        "admin API should be open when no admin_token is configured, got {:?}",
        resp.status()
    );
}

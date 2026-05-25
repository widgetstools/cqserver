//! Admin HTTP API.
//!
//! Routes
//! ------
//! - `GET /`                            — Galvanometer-style admin UI (HTML).
//! - `GET /fi-demo`                     — fixed-income demo dashboard (HTML).
//! - `GET /stats`                       — aggregate server stats.
//! - `GET /topics`                      — per-topic stats array.
//! - `GET /subscriptions`               — per-subscription stats array.
//! - `DELETE /subscriptions/{sub_id}`   — admin-triggered unsubscribe of a slow consumer.
//! - `GET /metrics`                     — Prometheus exposition.
//! - `GET /healthz`                     — liveness probe.

use axum::{
    extract::{Path, State},
    http::{header, StatusCode},
    response::IntoResponse,
    routing::{delete, get, post},
    Json, Router,
};

// HTML pages baked into the binary so the admin port is self-contained
// (no separate static asset directory to ship/configure).
const ADMIN_HTML: &str = include_str!("../static/admin.html");
const FI_DEMO_HTML: &str = include_str!("../static/fi-demo.html");
use cq_core::topic::SharedTopic;
use cq_transport::queue::QueueRegistry;
use cq_transport::session::SessionRegistry;
use dashmap::DashMap;
use metrics_exporter_prometheus::PrometheusHandle;
use std::sync::Arc;
use tracing::info;

#[derive(Clone)]
pub struct AdminState {
    pub topics: Arc<DashMap<String, SharedTopic>>,
    pub registry: SessionRegistry,
    pub queues: QueueRegistry,
    pub prom: PrometheusHandle,
    /// H6: static shard table (topic-prefix → instance URL). Empty
    /// vec = single-node mode where this instance owns everything.
    pub shards: Arc<Vec<crate::config::ShardEntry>>,
    /// H6: URL of *this* instance, returned when no shard entry
    /// matches. Allows clients to confirm "yes this node owns it"
    /// without a separate request.
    pub self_url: Arc<String>,
    /// U5: view definitions from the config. Surfaced via
    /// `/admin/views` so the admin UI can show name/source/SQL.
    pub views: Arc<Vec<crate::config::ViewEntry>>,
    /// U5: replication role + peer/listen — captured at startup so
    /// `/admin/replication` can show the topology without re-reading
    /// the config file.
    pub replication: Arc<ReplicationView>,
    /// U5: rendered config TOML (post env-var substitution). Served
    /// verbatim by `/admin/config` for the Config screen.
    pub raw_config_toml: Arc<String>,
}

/// Captured replication topology for `/admin/replication`.
#[derive(Debug, Clone)]
pub struct ReplicationView {
    pub role: String, // "standalone" | "primary" | "standby"
    pub peer: Option<String>,
    pub listen: Option<String>,
}

pub async fn start_admin_server(
    addr: String,
    state: AdminState,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut app = Router::new()
        .route("/", get(admin_ui))
        .route("/fi-demo", get(fi_demo_ui))
        .route("/healthz", get(healthz))
        .route("/stats", get(get_stats))
        .route("/topics", get(get_topics))
        .route("/subscriptions", get(get_subscriptions))
        .route("/subscriptions/:sub_id", delete(delete_subscription))
        .route("/metrics", get(get_metrics))
        .route("/admin/rotate-journal/:topic", post(rotate_journal))
        .route("/admin/shrink-store/:topic", post(shrink_store))
        .route("/admin/shrink-store-all", post(shrink_store_all))
        .route("/admin/replication", get(replication_status))
        .route("/admin/shard-for/:topic", get(shard_for))
        .route("/admin/explain", post(explain_query))
        .route("/queues", get(get_queues))
        .route("/admin/views", get(get_views))
        .route("/admin/config", get(get_config_toml))
        .with_state(state);

    // U7: serve the admin-ui static bundle under /ui. Resolved
    // from `CQSERVER_ADMIN_UI_DIR` (override) or
    // `./clients/admin-ui/dist` relative to the process CWD
    // (the demo + standard dev layout). If the dir is missing,
    // log + skip — the JSON admin endpoints work either way.
    let ui_dir = std::env::var("CQSERVER_ADMIN_UI_DIR")
        .unwrap_or_else(|_| "clients/admin-ui/dist".to_string());
    if std::path::Path::new(&ui_dir).is_dir() {
        let index = std::path::Path::new(&ui_dir).join("index.html");
        // ServeDir with a fallback to index.html so client-side
        // routes (e.g. /ui/topics) hit the SPA shell on hard-reload.
        let serve = tower_http::services::ServeDir::new(&ui_dir)
            .fallback(tower_http::services::ServeFile::new(index));
        app = app.nest_service("/ui", serve);
        info!(dir = %ui_dir, "Admin UI mounted at /ui");
    } else {
        info!(
            dir = %ui_dir,
            "Admin UI dist not found; /ui will 404. Build with \
             `cd clients/admin-ui && npm run build` or set \
             CQSERVER_ADMIN_UI_DIR to mount."
        );
    }

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    info!(addr = %addr, "Admin HTTP server listening");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn admin_ui() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
        ADMIN_HTML,
    )
}

async fn fi_demo_ui() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
        FI_DEMO_HTML,
    )
}

async fn healthz() -> &'static str {
    "ok"
}

async fn get_stats(State(s): State<AdminState>) -> impl IntoResponse {
    let total_rows: u64 = s.topics.iter().map(|e| e.value().row_count() as u64).sum();
    let total_subs: usize = s
        .topics
        .iter()
        .map(|e| e.value().subscription_count())
        .sum();
    let (rss, virt) = memory_stats::memory_stats()
        .map(|m| (m.physical_mem as u64, m.virtual_mem as u64))
        .unwrap_or((0, 0));
    metrics::gauge!("cq_process_rss_bytes").set(rss as f64);
    metrics::gauge!("cq_process_virtual_bytes").set(virt as f64);
    Json(serde_json::json!({
        "topics": s.topics.len(),
        "totalRows": total_rows,
        "totalSubscriptions": total_subs,
        "activeRoutes": s.registry.len(),
        "processRssBytes": rss,
        "processVirtualBytes": virt,
    }))
}

async fn get_topics(State(s): State<AdminState>) -> impl IntoResponse {
    let topics: Vec<_> = s.topics.iter().map(|e| e.value().stats()).collect();
    Json(serde_json::Value::Array(topics))
}

async fn get_subscriptions(State(s): State<AdminState>) -> impl IntoResponse {
    let stats = cq_transport::session::collect_subscription_stats(&s.registry);
    let arr: Vec<serde_json::Value> = stats
        .into_iter()
        .map(|st| {
            serde_json::json!({
                "subId": st.sub_id,
                "topic": st.topic,
                "sessionId": st.session_id,
                "queueDepth": st.queue_depth,
                "queueCapacity": st.queue_capacity,
                "fillRatio": st.fill_ratio(),
                "dropped": st.dropped,
                "ageMs": st.age_ms,
                "conflated": st.conflated,
                "conflationIntervalMs": st.conflation_interval_ms,
            })
        })
        .collect();
    Json(serde_json::Value::Array(arr))
}

/// Admin-triggered unsubscribe. Drops the route from the session
/// registry (so future deltas stop flowing) and tells the topic to
/// release the subscription's evaluator state. The owning session
/// stays alive — it can still send other commands. Use this on a
/// slow consumer when you don't want to kill the whole TCP/WS
/// connection.
async fn delete_subscription(
    State(s): State<AdminState>,
    Path(sub_id): Path<String>,
) -> impl IntoResponse {
    let route = match s.registry.remove(&sub_id) {
        Some((_, r)) => r,
        None => return (StatusCode::NOT_FOUND, "no such subscription").into_response(),
    };
    if let Some(topic) = s.topics.get(&route.topic) {
        topic.unsubscribe(&sub_id);
    }
    metrics::counter!(
        "cq_subscription_admin_disconnect_total",
        "topic" => route.topic.clone()
    )
    .increment(1);
    tracing::warn!(
        sub = %sub_id,
        topic = %route.topic,
        session = %route.session_id,
        dropped = route.dropped_count(),
        age_ms = route.age_ms(),
        "Admin-triggered subscription disconnect"
    );
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "subId": sub_id,
            "topic": route.topic,
            "dropped": route.dropped_count(),
            "ageMs": route.age_ms(),
        })),
    )
        .into_response()
}

/// `POST /admin/rotate-journal/{topic}` — force the active txlog
/// segment to seal. The topic's writer opens a fresh segment for
/// subsequent appends; the sealed segment moves to archive if
/// configured. Useful before a backup or to bound replay latency.
async fn rotate_journal(
    State(s): State<AdminState>,
    Path(topic): Path<String>,
) -> impl IntoResponse {
    let topic_arc = match s.topics.get(&topic) {
        Some(t) => t.clone(),
        None => {
            return (StatusCode::NOT_FOUND, "no such topic").into_response();
        }
    };
    if !topic_arc.has_txlog() {
        return (
            StatusCode::BAD_REQUEST,
            "topic is not persistent (no txlog to rotate)",
        )
            .into_response();
    }
    match topic_arc.force_rotate_txlog() {
        Ok(()) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "topic": topic,
                "rotated": true,
            })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("rotate failed: {e}"),
        )
            .into_response(),
    }
}

/// `POST /admin/shrink-store/{topic}` — release unused tail capacity
/// from the column store. Useful after a large delete or to recover
/// the slack that `grow()` reserved (~25 % over the row count). Takes
/// the topic write lock briefly; readers are unaffected.
async fn shrink_store(
    State(s): State<AdminState>,
    Path(topic): Path<String>,
) -> impl IntoResponse {
    let Some(topic_arc) = s.topics.get(&topic).map(|e| e.clone()) else {
        return (StatusCode::NOT_FOUND, "no such topic").into_response();
    };
    let (old, new) = topic_arc.shrink_store();
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "topic": topic,
            "oldCapacity": old,
            "newCapacity": new,
            "reclaimedRows": old.saturating_sub(new),
        })),
    )
        .into_response()
}

/// `POST /admin/shrink-store-all` — shrink every topic. One-shot
/// convenience for "I want my memory back now" after a load test.
async fn shrink_store_all(State(s): State<AdminState>) -> impl IntoResponse {
    let results: Vec<serde_json::Value> = s
        .topics
        .iter()
        .map(|e| {
            let (old, new) = e.value().shrink_store();
            serde_json::json!({
                "topic": e.key(),
                "oldCapacity": old,
                "newCapacity": new,
                "reclaimedRows": old.saturating_sub(new),
            })
        })
        .collect();
    Json(serde_json::Value::Array(results))
}

/// `GET /admin/replication` — snapshot of replication state.
/// Reports the server's role (standalone / primary / standby) plus,
/// for every persistent topic, the current sequence high-water on
/// disk. Detailed per-destination lag will be added when the
/// shipper exposes more telemetry.
async fn replication_status(State(s): State<AdminState>) -> impl IntoResponse {
    let topics: Vec<serde_json::Value> = s
        .topics
        .iter()
        .filter(|e| e.value().has_txlog())
        .map(|e| {
            let t = e.value();
            serde_json::json!({
                "topic": t.name(),
                "current_sequence": t.current_sequence(),
            })
        })
        .collect();
    Json(serde_json::json!({
        "role": s.replication.role,
        "peer": s.replication.peer,
        "listen": s.replication.listen,
        "topics": topics,
    }))
}

/// U5: `GET /admin/views` — list every materialized view declared in
/// `[[views]]`, with its source topic + SQL body + capacity. The
/// admin UI's Views screen consumes this and shows the SQL on a
/// detail tab.
async fn get_views(State(s): State<AdminState>) -> impl IntoResponse {
    let arr: Vec<serde_json::Value> = s
        .views
        .iter()
        .map(|v| {
            serde_json::json!({
                "name": v.name,
                "source": v.source,
                "sql": v.sql,
                "initial_capacity": v.initial_capacity,
                "tap_capacity": v.tap_capacity,
            })
        })
        .collect();
    Json(serde_json::Value::Array(arr))
}

/// U5: `GET /admin/config` — the running config's TOML text (with
/// any `${VAR:-default}` substitutions already applied), so the admin
/// UI's Config screen mirrors what the process actually loaded
/// rather than the on-disk file.
async fn get_config_toml(State(s): State<AdminState>) -> impl IntoResponse {
    (
        StatusCode::OK,
        [("content-type", "text/plain; charset=utf-8")],
        s.raw_config_toml.as_str().to_owned(),
    )
}

/// H6: `GET /admin/shard-for/{topic}` — answer "which instance
/// owns this topic?" via the static shard table. Returns
/// `{ "topic": "...", "instance_url": "...", "self": true|false }`.
/// `self: true` means this instance owns it (caller can use the
/// current connection); `false` means redirect to `instance_url`.
///
/// This is the minimum viable shard primitive — the directory
/// service (H6.2) layered on a real client SDK call. Real
/// cross-instance replication (H6.3) and client smart-connect
/// (H6.4) remain separate worklog items.
async fn shard_for(
    State(s): State<AdminState>,
    Path(topic): Path<String>,
) -> impl IntoResponse {
    // Re-resolve via the same longest-prefix rule used in
    // ServerConfig::resolve_shard, but on the Arc<Vec<ShardEntry>>
    // we hold here.
    let matched = s
        .shards
        .iter()
        .filter(|e| topic.starts_with(&e.topic_prefix))
        .max_by_key(|e| e.topic_prefix.len());

    match matched {
        Some(entry) => Json(serde_json::json!({
            "topic": topic,
            "instance_url": entry.instance_url,
            "matched_prefix": entry.topic_prefix,
            "self": entry.instance_url == s.self_url.as_str(),
        })),
        None => Json(serde_json::json!({
            "topic": topic,
            "instance_url": s.self_url.as_str(),
            "matched_prefix": serde_json::Value::Null,
            "self": true,
        })),
    }
}

/// `GET /queues` — array of queue topic snapshots. Each entry has
/// `name`, `kind: "queue"`, `buffered` (in-memory queue depth),
/// `consumers` (registered subscriber count), `sequence` (next
/// to-be-assigned sequence). Admin UI U4 consumes this for the
/// Queues screen.
async fn get_queues(State(s): State<AdminState>) -> impl IntoResponse {
    let arr: Vec<serde_json::Value> = s
        .queues
        .iter()
        .map(|e| e.value().stats())
        .collect();
    Json(serde_json::Value::Array(arr))
}

async fn get_metrics(State(s): State<AdminState>) -> impl IntoResponse {
    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4")],
        s.prom.render(),
    )
}

#[derive(serde::Deserialize)]
struct ExplainRequest {
    /// Topic against which the query will be parsed and executed.
    /// Schema is read from this topic; the query may still reference
    /// other topics via JOIN, but JOIN cost estimation is deferred.
    topic: String,
    /// SQL to estimate. Same dialect the subscribe path uses.
    sql: String,
}

/// `POST /admin/explain` — estimate the cost of a query before
/// subscribing. Returns the `QueryCostEstimate` from cq-core as JSON.
/// Operator-facing tool for the admin UI's Query Explain screen
/// (admin-ui U6) and for ad-hoc debugging.
async fn explain_query(
    State(s): State<AdminState>,
    Json(req): Json<ExplainRequest>,
) -> impl IntoResponse {
    let topic = match s.topics.get(&req.topic) {
        Some(t) => t.clone(),
        None => {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({
                    "error": format!("topic not found: {}", req.topic),
                })),
            )
                .into_response()
        }
    };

    match topic.estimate_cost(&req.sql) {
        Ok(est) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "estimated_source_rows": est.estimated_source_rows,
                "estimated_result_rows": est.estimated_result_rows,
                "estimated_result_bytes": est.estimated_result_bytes,
                "estimated_join_fanout_avg": est.estimated_join_fanout_avg,
                "used_indexes": est.used_indexes,
                "assumptions": est.assumptions,
                "confidence": est.confidence.as_str(),
            })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": format!("{}", e),
            })),
        )
            .into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use cq_core::schema::{ColumnType, Schema};
    use cq_core::topic::{Topic, TopicConfig};
    use cq_transport::session::new_registry;
    use http_body_util::BodyExt;
    use metrics_exporter_prometheus::PrometheusBuilder;
    use std::sync::Arc;
    use tower::ServiceExt;

    fn router_with_one_topic() -> Router {
        let topics: Arc<DashMap<String, SharedTopic>> = Arc::new(DashMap::new());
        let schema = Arc::new(Schema::from_strs(
            &["symbol"],
            &[ColumnType::String],
        ));
        let topic = Topic::new(
            TopicConfig {
                name: "/t".into(),
                key_fields: vec!["symbol".into()],
                persist: false,
                conflation_ms: None,
                index_columns: vec![],
            expire_seconds: None,
            },
            schema,
            10,
        );
        topics.insert("/t".into(), Arc::new(topic));
        let prom = PrometheusBuilder::new()
            .build_recorder()
            .handle();
        let state = AdminState {
            topics,
            registry: new_registry(),
            queues: cq_transport::queue::new_queue_registry(),
            prom,
            shards: Arc::new(Vec::new()),
            self_url: Arc::new("ws://127.0.0.1:9000/cqp".to_string()),
            views: Arc::new(Vec::new()),
            replication: Arc::new(ReplicationView {
                role: "standalone".into(),
                peer: None,
                listen: None,
            }),
            raw_config_toml: Arc::new(String::new()),
        };
        Router::new()
            .route("/healthz", get(healthz))
            .route("/stats", get(get_stats))
            .route("/topics", get(get_topics))
            .route("/metrics", get(get_metrics))
            .with_state(state)
    }

    #[tokio::test]
    async fn healthz_returns_ok() {
        let app = router_with_one_topic();
        let res = app
            .oneshot(Request::get("/healthz").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let body = res.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(&body[..], b"ok");
    }

    #[tokio::test]
    async fn stats_lists_topics() {
        let app = router_with_one_topic();
        let res = app
            .oneshot(Request::get("/stats").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let body = res.into_body().collect().await.unwrap().to_bytes();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v.get("topics").unwrap(), 1);
        assert_eq!(v.get("totalRows").unwrap(), 0);
    }

    #[tokio::test]
    async fn topics_returns_array() {
        let app = router_with_one_topic();
        let res = app
            .oneshot(Request::get("/topics").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let body = res.into_body().collect().await.unwrap().to_bytes();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let arr = v.as_array().unwrap();
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0].get("name").unwrap(), "/t");
    }

    fn router_with_shards(shards: Vec<crate::config::ShardEntry>, self_url: &str) -> Router {
        let topics: Arc<DashMap<String, SharedTopic>> = Arc::new(DashMap::new());
        let prom = PrometheusBuilder::new().build_recorder().handle();
        let state = AdminState {
            topics,
            registry: new_registry(),
            queues: cq_transport::queue::new_queue_registry(),
            prom,
            shards: Arc::new(shards),
            self_url: Arc::new(self_url.to_string()),
            views: Arc::new(Vec::new()),
            replication: Arc::new(ReplicationView {
                role: "standalone".into(),
                peer: None,
                listen: None,
            }),
            raw_config_toml: Arc::new(String::new()),
        };
        Router::new()
            .route("/admin/shard-for/:topic", get(shard_for))
            .with_state(state)
    }

    #[tokio::test]
    async fn shard_for_longest_prefix_wins() {
        let shards = vec![
            crate::config::ShardEntry {
                topic_prefix: "/orders".into(),
                instance_url: "ws://node-a:9000/cqp".into(),
            },
            crate::config::ShardEntry {
                topic_prefix: "/orders/usd".into(),
                instance_url: "ws://node-b:9000/cqp".into(),
            },
        ];
        let app = router_with_shards(shards, "ws://self:9000/cqp");
        let res = app
            .oneshot(
                Request::get("/admin/shard-for/%2Forders%2Fusd%2Faapl")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let body = res.into_body().collect().await.unwrap().to_bytes();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v.get("instance_url").unwrap(), "ws://node-b:9000/cqp");
        assert_eq!(v.get("matched_prefix").unwrap(), "/orders/usd");
        assert_eq!(v.get("self").unwrap(), false);
    }

    #[tokio::test]
    async fn shard_for_no_match_returns_self() {
        let app = router_with_shards(Vec::new(), "ws://self:9000/cqp");
        let res = app
            .oneshot(
                Request::get("/admin/shard-for/%2Fanything")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let body = res.into_body().collect().await.unwrap().to_bytes();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v.get("instance_url").unwrap(), "ws://self:9000/cqp");
        assert!(v.get("matched_prefix").unwrap().is_null());
        assert_eq!(v.get("self").unwrap(), true);
    }

    #[tokio::test]
    async fn metrics_renders_prometheus_format() {
        let app = router_with_one_topic();
        let res = app
            .oneshot(Request::get("/metrics").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        // Render is empty until something is recorded — that's fine.
        // We're just verifying the endpoint exists and serves text.
        let ct = res.headers().get("content-type").unwrap();
        assert!(ct.to_str().unwrap().starts_with("text/plain"));
    }
}

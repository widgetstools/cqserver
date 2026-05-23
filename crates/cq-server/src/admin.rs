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
use cq_transport::session::SessionRegistry;
use dashmap::DashMap;
use metrics_exporter_prometheus::PrometheusHandle;
use std::sync::Arc;
use tracing::info;

#[derive(Clone)]
pub struct AdminState {
    pub topics: Arc<DashMap<String, SharedTopic>>,
    pub registry: SessionRegistry,
    pub prom: PrometheusHandle,
}

pub async fn start_admin_server(
    addr: String,
    state: AdminState,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let app = Router::new()
        .route("/", get(admin_ui))
        .route("/fi-demo", get(fi_demo_ui))
        .route("/healthz", get(healthz))
        .route("/stats", get(get_stats))
        .route("/topics", get(get_topics))
        .route("/subscriptions", get(get_subscriptions))
        .route("/subscriptions/:sub_id", delete(delete_subscription))
        .route("/metrics", get(get_metrics))
        .route("/admin/rotate-journal/:topic", post(rotate_journal))
        .route("/admin/replication", get(replication_status))
        .with_state(state);

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
    Json(serde_json::json!({
        "topics": s.topics.len(),
        "totalRows": total_rows,
        "totalSubscriptions": total_subs,
        "activeRoutes": s.registry.len(),
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
        "topics": topics,
    }))
}

async fn get_metrics(State(s): State<AdminState>) -> impl IntoResponse {
    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4")],
        s.prom.render(),
    )
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
            prom,
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

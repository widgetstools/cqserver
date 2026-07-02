//! CQServer — Continuous Query Server
//!
//! A high-performance, content-aware messaging server written in Rust.
//! Drop-in replacement for AMPS (60East Technologies).

// Replace the system allocator with jemalloc on non-MSVC targets.
// Tuned for bursty workloads where peak heap usage (SOW snapshot
// delivery to slow WebSocket clients) is much higher than steady
// state:
//   - background_threads (feature)  → jemalloc purges on its own
//     thread, no application calls needed.
//   - dirty_decay_ms:1000           → return dirty pages to the OS
//                                     1 s after they're freed.
//   - muzzy_decay_ms:1000           → same for "muzzy" (uncommitted)
//                                     pages — keeps RSS close to the
//                                     working set.
#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[allow(non_upper_case_globals)]
#[export_name = "malloc_conf"]
pub static MALLOC_CONF: &[u8] = b"background_thread:true,dirty_decay_ms:1000,muzzy_decay_ms:1000\0";

mod admin;
mod config;
mod logging;
mod watch;

use admin::AdminState;
use config::{ColumnTypeSpec, ReplicationRole, TopicEntry, TxLogFsyncConfig, ViewEntry};
use cq_core::schema::{ColumnType, Schema};
use cq_core::topic::{SharedTopic, Topic, TopicConfig};
use cq_core::query::peek_join;
use cq_core::view::{spawn_view_runner, spawn_view_runner_joined, View};
use cq_replication::{receiver as repl_recv, shipper as repl_ship};
use cq_transport::auth::{AuthStore, User};
use cq_transport::delivery::spawn_evaluators;
use cq_transport::heartbeat::HeartbeatConfig;
use cq_transport::queue::{new_queue_registry, Queue};
use cq_transport::session::new_registry;
use cq_transport::tcp::{self, TcpConfig};
use cq_transport::websocket::{self, WsConfig};
use cq_txlog::reader::TxLogReader;
use cq_txlog::writer::TxLogWriter;
use cq_txlog::FsyncPolicy;
use dashmap::DashMap;
use metrics_exporter_prometheus::PrometheusBuilder;
use parking_lot::Mutex;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{info, warn};

/// Minimal CLI: just `--config <path>` (or `-c <path>`). Anything
/// else is ignored — we deliberately don't pull in a full clap
/// dependency here to keep cqserver's binary lean.
fn parse_config_arg() -> Option<std::path::PathBuf> {
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        if a == "--config" || a == "-c" {
            if let Some(p) = args.next() {
                return Some(std::path::PathBuf::from(p));
            }
        } else if let Some(rest) = a.strip_prefix("--config=") {
            return Some(std::path::PathBuf::from(rest));
        }
    }
    None
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Load the config FIRST so the logging section can drive tracing
    // sink installation. Bootstrapping issue: before logging is up,
    // we can't surface errors via tracing — `load_config` returns
    // them straight to stderr via the `?` operator instead.
    // Honor `--config <path>` so operators can run cqserver from any
    // working directory pointing at any config file. Without an arg,
    // falls back to the historical `./config/cqserver.toml` lookup so
    // existing start-demo.sh / Makefiles keep working.
    let config_override = parse_config_arg();
    let (mut server_config, config_dir, raw_config_toml) = match config_override {
        Some(path) => config::load_config_from_with_raw(&path)?,
        None => config::load_config_with_raw()?,
    };

    // Resolve the runtime-views file (admin-created views) and merge
    // it into the declared views BEFORE validation + the init_view
    // loop, so persisted views are recreated on restart exactly like
    // hand-authored ones.
    let runtime_views_path: std::path::PathBuf = server_config
        .runtime_views_path
        .clone()
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| config_dir.join("runtime_views.toml"));
    match config::load_runtime_views(&runtime_views_path) {
        Ok(rt_views) => {
            if !rt_views.is_empty() {
                server_config.views.extend(rt_views);
            }
        }
        // `load_runtime_views` returns Ok(empty) for an absent or blank
        // file, so reaching this arm means the file EXISTS but failed to
        // read/parse — i.e. it's corrupt. Continuing would silently drop
        // every admin-created view on restart (data loss), so fail hard
        // and let the operator fix or remove the file.
        Err(e) => {
            return Err(format!(
                "runtime-views file {} is present but could not be parsed: {e}; \
                 refusing to start to avoid silently losing admin-created views",
                runtime_views_path.display()
            )
            .into());
        }
    }

    // Install tracing sinks from `[logging]`. Errors opening
    // individual sink files are returned so we can warn about them
    // post-install (other sinks may still have come up); failure to
    // install ANY layer falls back to a stderr default inside
    // `logging::install`.
    let sink_errors = logging::install(&server_config.logging, server_config.audit.as_ref());
    for e in &sink_errors {
        // Fail-open by design (see cq_server::logging module docs): the
        // audit sink failing to build must never stop the server from
        // starting. But an operator silently losing their audit trail
        // is a big deal, so give it a distinct, unmistakable warning
        // rather than blending into routine sink-install noise.
        if let Some(detail) = e.strip_prefix(logging::AUDIT_SINK_ERROR_PREFIX) {
            warn!(
                error = %detail,
                "AUDIT SINK FAILED TO INITIALIZE — server running WITHOUT audit trail"
            );
        } else {
            warn!(error = %e, "Logging sink failed to install");
        }
    }

    info!("CQServer starting...");

    // Install the Prometheus recorder before any metrics calls happen.
    let prom_handle = PrometheusBuilder::new()
        .install_recorder()
        .map_err(|e| format!("Prometheus install: {}", e))?;

    let topics: Arc<DashMap<String, SharedTopic>> = Arc::new(DashMap::new());
    let registry = new_registry();
    let queues = new_queue_registry();

    // Queues are independent of topics — populate from config.
    for q in &server_config.queues {
        let mut queue = Queue::with_lease(q.name.clone(), q.lease_ms);
        if let Some(n) = q.max_delivery_count {
            queue = queue.with_max_delivery_count(n);
        }
        if let Some(dlq) = &q.dlq {
            queue = queue.with_dlq(dlq.clone());
        }
        if let Some(cap) = q.max_buffer {
            queue = queue.with_max_buffer(cap);
        }
        let queue_arc = Arc::new(queue);
        if q.lease_ms.is_some() {
            cq_transport::queue::spawn_lease_sweeper(
                queue_arc.clone(),
                registry.clone(),
                Some(queues.clone()),
            );
        }
        queues.insert(q.name.clone(), queue_arc);
        info!(
            queue = %q.name,
            lease_ms = q.lease_ms,
            dlq = ?q.dlq,
            "Queue initialized"
        );
    }

    let txlog_dir = PathBuf::from(&server_config.txlog.directory);
    let fsync = match server_config.txlog.fsync {
        TxLogFsyncConfig::None => FsyncPolicy::None,
        TxLogFsyncConfig::EveryWrite => FsyncPolicy::EveryWrite,
        TxLogFsyncConfig::Interval => FsyncPolicy::Interval(
            std::time::Duration::from_millis(server_config.txlog.fsync_interval_ms),
        ),
    };
    let txlog_preallocate = server_config.txlog.preallocate;

    // Resolve the optional archive directory once per server.
    let archive_dir_root: Option<PathBuf> = server_config
        .txlog
        .archive_directory
        .as_ref()
        .map(PathBuf::from);
    let segment_size_override = server_config.txlog.segment_size;
    let archive_compress = server_config.txlog.archive_compress;

    let default_eval_lanes = server_config.resolved_default_eval_lanes();
    info!(default_eval_lanes, "Evaluator lane default resolved");

    // S20 — resolve this node's AMPS-style instance id once. Stamped on
    // every local txlog write so peers can dedup convergently. An HA node
    // (primary/standby) with no configured name gets no convergence
    // protection — warn loudly.
    let instance_name = server_config.replication.resolve_instance_name();
    if instance_name.is_empty() {
        if server_config.replication.role != ReplicationRole::Standalone {
            warn!(
                "replication.instance_name is unset for a non-standalone node — \
                 origin tagging and split-brain convergence dedup are DISABLED"
            );
        }
    } else {
        info!(instance = %instance_name, "Replication instance identity resolved");
    }

    let mut evaluator_handles = Vec::new();
    for topic_cfg in &server_config.topics {
        let topic_archive_dir = archive_dir_root.as_ref().map(|root| {
            let slug: String = topic_cfg
                .name
                .chars()
                .map(|c| {
                    if c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.' {
                        c
                    } else {
                        '_'
                    }
                })
                .collect();
            root.join(slug.trim_start_matches('_'))
        });
        let shared = init_topic(
            topic_cfg,
            &txlog_dir,
            fsync,
            &config_dir,
            topic_archive_dir.clone(),
            segment_size_override,
            archive_compress,
            txlog_preallocate,
            default_eval_lanes,
            &instance_name,
        )?;

        // Recover from log BEFORE spawning the evaluator. Any mutation
        // events emitted during replay are drained — subscribers don't
        // exist yet so deltas would be meaningless anyway.
        let rx = shared
            .take_mutation_rx()
            .expect("freshly-created topic must have an rx");
        if topic_cfg.persist {
            let log_path = log_path_for(&txlog_dir, &topic_cfg.name);
            // SOW snapshot fast-restart: if a checkpoint exists, rebuild the
            // bulk of the store from it in one pass and then replay only the
            // txlog tail (segments after the snapshot). Falls back to a full
            // replay when no/invalid snapshot is present.
            let mut resume_segment = 0u64;
            match cq_txlog::snapshot::Snapshot::load(&log_path) {
                Ok(Some(snap)) => {
                    let rows = snap.rows.len();
                    let seq = snap.sequence;
                    resume_segment = snap.segment_id;
                    shared.apply_snapshot(&snap);
                    info!(
                        topic = %topic_cfg.name,
                        rows,
                        sequence = seq,
                        resume_segment,
                        "Loaded SOW snapshot"
                    );
                }
                Ok(None) => {}
                Err(e) => warn!(
                    topic = %topic_cfg.name,
                    error = %e,
                    "Snapshot load failed; falling back to full txlog replay"
                ),
            }
            if log_path.exists() {
                let count = recover_topic_with_archive(
                    &shared,
                    &log_path,
                    topic_archive_dir.as_deref(),
                    resume_segment,
                )?;
                if count > 0 {
                    info!(
                        topic = %topic_cfg.name,
                        entries = count,
                        "Recovered SOW from txlog"
                    );
                }
                // Drain any MutationEvents queued during replay.
                while rx.try_recv().is_ok() {}
            }
        }

        evaluator_handles.extend(spawn_evaluators(shared.clone(), rx, registry.clone())?);

        // TTL sweeper task — only spawned when `expire_seconds` is
        // configured. Ticks every second by default.
        if let Some(ttl) = shared.expire_seconds() {
            let topic_for_ttl = shared.clone();
            // Tick at the smaller of 1s or ttl/4 (with a 200ms floor).
            // Bounds the worst-case retention drift to one tick.
            let candidate_ms =
                ((ttl as u128 * 1000) / 4).max(200).min(1000) as u64;
            let tick = std::time::Duration::from_millis(candidate_ms);
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(tick).await;
                    if let Err(e) = topic_for_ttl.sweep_expired() {
                        tracing::warn!(
                            topic = %topic_for_ttl.name(),
                            error = %e,
                            "TTL sweep failed"
                        );
                    }
                }
            });
        }

        // P14 — register under the canonical slash-prefixed name so
        // every lookup (router SOW, SDK publish, view source resolver)
        // can use one form instead of dual-checking.
        let canonical = cq_core::topic::canonicalize_topic(&topic_cfg.name);
        topics.insert(canonical.clone(), shared);
        info!(topic = %canonical, "Topic ready");
    }

    info!(topics = topics.len(), "Topic registry initialized");

    // Query Guardrails G1: validate the view graph (chain depth,
    // passthrough bodies, cycles) before we register any view. A
    // failure here is a config-time error and prevents startup.
    {
        let query_limits = server_config.query_limits.to_core();
        let mut view_sources: std::collections::HashMap<String, String> =
            std::collections::HashMap::new();
        let mut view_bodies: std::collections::HashMap<String, String> =
            std::collections::HashMap::new();
        for v in &server_config.views {
            view_sources.insert(v.name.clone(), v.source.clone());
            view_bodies.insert(v.name.clone(), v.sql.clone());
        }
        cq_core::query::validate_view_graph(&view_sources, &view_bodies, &query_limits)
            .map_err(|e| format!("query_limits: {}", e))?;
    }

    // S20 materialized views. Each view declares an aggregate query
    // over an existing source topic; the server creates the view
    // topic, registers it in the same `topics` map (so subscribers
    // find it via the regular subscribe path), spawns the view
    // runner thread, and spawns the view topic's own evaluator
    // thread so its subscribers see row-level deltas as the view's
    // SOW changes.
    let view_teardown: Arc<DashMap<String, ViewTeardown>> = Arc::new(DashMap::new());
    for view_cfg in &server_config.views {
        match init_view(view_cfg, &topics, registry.clone()) {
            Ok((handle, teardown)) => {
                evaluator_handles.push(handle);
                view_teardown
                    .insert(cq_core::topic::canonicalize_topic(&view_cfg.name), teardown);
                info!(
                    view = %view_cfg.name,
                    source = %view_cfg.source,
                    "Materialized view ready"
                );
            }
            Err(e) => {
                return Err(format!(
                    "view '{}': {}",
                    view_cfg.name, e
                )
                .into());
            }
        }
    }

    info!(
        topics = topics.len(),
        views = server_config.views.len(),
        "Topic + view registry ready"
    );

    let ws_topics = topics.clone();
    let tcp_topics = topics.clone();
    let ws_registry = registry.clone();
    let tcp_registry = registry.clone();

    // H6: derive a stable self-URL for shard-routing responses. We
    // prefer the WS address (most clients) and fall back to TCP. The
    // value is opaque to the server — clients use it verbatim.
    let self_url = if !server_config.websocket_addr.is_empty() {
        format!(
            "ws://{}{}",
            server_config.websocket_addr, server_config.websocket_path
        )
    } else {
        format!("tcp://{}", server_config.tcp_addr)
    };
    let view_names: Arc<dashmap::DashSet<String>> = Arc::new(dashmap::DashSet::new());
    for v in &server_config.views {
        view_names.insert(cq_core::topic::canonicalize_topic(&v.name));
    }
    let admin_state = AdminState {
        topics: topics.clone(),
        registry: registry.clone(),
        queues: queues.clone(),
        prom: prom_handle,
        shards: Arc::new(server_config.shards.clone()),
        self_url: Arc::new(self_url),
        views: Arc::new(server_config.views.clone()),
        replication: Arc::new(admin::ReplicationView {
            role: match server_config.replication.role {
                ReplicationRole::Standalone => "standalone".into(),
                ReplicationRole::Primary => "primary".into(),
                ReplicationRole::Standby => "standby".into(),
                ReplicationRole::ActiveActive => "active_active".into(),
            },
            peer: server_config.replication.peer.clone(),
            peers: server_config.replication.peers.clone(),
            listen: server_config.replication.listen.clone(),
        }),
        raw_config_toml: Arc::new(raw_config_toml),
        view_names: view_names.clone(),
        runtime_views_path: Arc::new(runtime_views_path.clone()),
        view_create_lock: Arc::new(tokio::sync::Mutex::new(())),
        view_teardown: view_teardown.clone(),
        admin_token: Arc::new(server_config.admin_token.clone()),
    };
    let admin_addr = server_config.admin_addr.clone();

    // Admin TLS (D2/P0.2). Built up front (like the transport TLS
    // acceptor below) so a misconfigured cert/key fails the process at
    // startup rather than surfacing as a mysterious per-connection
    // error later, on the dedicated admin thread where it'd be easy to
    // miss.
    let admin_tls_acceptor = match &server_config.admin_tls {
        Some(tls) => {
            let acceptor = cq_transport::tls::build_tls_acceptor(&tls.cert_file, &tls.key_file)
                .map_err(|e| format!("admin TLS config: {}", e))?;
            info!(
                cert = %tls.cert_file,
                key = %tls.key_file,
                "TLS enabled on admin HTTP server"
            );
            Some(acceptor)
        }
        None => None,
    };

    let outbound_capacity = server_config.transport.outbound_queue_capacity;
    let sow_batch_size = server_config.transport.sow_batch_size;

    // Build the TLS acceptor up front so a misconfigured cert/key
    // produces an immediate startup error rather than a per-connection
    // failure later. The acceptor is cheap to clone (Arc<ServerConfig>).
    let tls_acceptor = match &server_config.transport.tls {
        Some(tls) => {
            let acceptor = cq_transport::tls::build_tls_acceptor(&tls.cert_file, &tls.key_file)
                .map_err(|e| format!("TLS config: {}", e))?;
            info!(
                cert = %tls.cert_file,
                key = %tls.key_file,
                "TLS enabled on TCP transport"
            );
            Some(acceptor)
        }
        None => None,
    };

    info!(
        outbound_queue_capacity = outbound_capacity,
        sow_batch_size,
        tls = tls_acceptor.is_some(),
        "Transport configured"
    );
    // One bookmark store per server process, shared across both
    // transports — a client that reconnects over either TCP or
    // WebSocket finds its previous high-water on the same map.
    let bookmark_store = cq_transport::router::new_bookmark_store();
    // S21 spillover, when configured: every subscription gets a
    // per-route overflow file under `[transport.spillover.directory]`.
    let spillover_ctx = server_config.transport.spillover.as_ref().map(|sp| {
        let dir = PathBuf::from(&sp.directory);
        if let Err(e) = std::fs::create_dir_all(&dir) {
            warn!(
                directory = %dir.display(),
                error = %e,
                "Failed to create spillover directory; disabling spillover"
            );
        }
        info!(
            directory = %dir.display(),
            max_bytes_per_sub = sp.max_bytes_per_sub,
            "Spillover enabled"
        );
        cq_transport::router::SpilloverContext {
            directory: dir,
            max_bytes_per_sub: sp.max_bytes_per_sub,
        }
    });
    // Replica-reads S1: a `standby` server still listens for client
    // connections (followers serve subscribes), but publish + delta_publish
    // are rejected with a clear error so a misdirected publisher learns
    // immediately instead of silently writing to a follower.
    let read_only = server_config.replication.role == ReplicationRole::Standby;
    if read_only {
        info!("Replication role = standby — transports running in read-only mode (publishes will be rejected)");
    }
    let query_limits = server_config.query_limits.to_core();
    // S11 — a `replicated` ack can only be satisfied when this node
    // actually ships to a standby (primary role with ≥1 peer). On any
    // other topology the publish path rejects `ack_type = "replicated"`
    // rather than hanging until timeout.
    let sync_replication = cq_transport::router::SyncReplication {
        active: matches!(
            server_config.replication.role,
            ReplicationRole::Primary | ReplicationRole::ActiveActive
        ) && !server_config.replication.resolve_peers().is_empty(),
        ack_timeout: std::time::Duration::from_millis(
            server_config.replication.sync_ack_timeout_ms,
        ),
    };

    // D3/P0.3 — one `ConnectionLimits` instance for the whole process,
    // shared (via Arc) between the TCP and WebSocket accept loops so a
    // client can't dodge `max_connections` by switching transport.
    let connection_limits = cq_transport::limits::ConnectionLimits::new(
        server_config.transport.limits.to_transport(),
    );
    info!(
        max_connections = server_config.transport.limits.max_connections,
        max_connections_per_ip = server_config.transport.limits.max_connections_per_ip,
        accept_rate_per_sec = server_config.transport.limits.accept_rate_per_sec,
        max_sessions_per_user = server_config.transport.limits.max_sessions_per_user,
        "Connection/rate limits configured"
    );

    let ws_config = WsConfig {
        listen_addr: server_config.websocket_addr.clone(),
        path: server_config.websocket_path.clone(),
        outbound_queue_capacity: outbound_capacity,
        sow_batch_size,
        bookmark_store: Some(bookmark_store.clone()),
        spillover: spillover_ctx.clone(),
        read_only,
        query_limits,
        max_message_bytes: server_config
            .ws_max_message_bytes
            .unwrap_or(cq_protocol::codec::MAX_FRAME_SIZE),
        sync_replication,
        connection_limits: Some(connection_limits.clone()),
    };
    let tcp_config = TcpConfig {
        listen_addr: server_config.tcp_addr.clone(),
        outbound_queue_capacity: outbound_capacity,
        sow_batch_size,
        tls_acceptor,
        bookmark_store: Some(bookmark_store),
        spillover: spillover_ctx,
        read_only,
        query_limits,
        sync_replication,
        connection_limits: Some(connection_limits),
    };

    let heartbeat_cfg = HeartbeatConfig {
        interval: std::time::Duration::from_secs(server_config.heartbeat_interval_s),
        idle_timeout: std::time::Duration::from_secs(server_config.heartbeat_idle_timeout_s),
    };

    info!(
        interval_s = server_config.heartbeat_interval_s,
        idle_timeout_s = server_config.heartbeat_idle_timeout_s,
        "Heartbeat enabled"
    );

    // Build auth store from config.
    let mut auth_users = Vec::with_capacity(server_config.auth.users.len());
    for u in &server_config.auth.users {
        let budget = u.query_budget.map(|b| b.to_runtime());
        match User::from_parts_full(
            u.username.clone(),
            u.password_hash.clone(),
            &u.entitlements,
            u.row_filter.clone(),
            budget,
        ) {
            Some(user) => auth_users.push(user),
            None => {
                return Err(format!(
                    "Invalid entitlement in config for user '{}'",
                    u.username
                )
                .into());
            }
        }
    }
    let mut auth_store = AuthStore::new(server_config.auth.required, auth_users);
    if let Some(jwt_cfg) = &server_config.auth.jwt {
        use config::JwtAlgorithm;
        let validator = match jwt_cfg.algorithm {
            JwtAlgorithm::Hs256 => {
                let secret = jwt_cfg.secret.as_deref().ok_or_else(|| {
                    "auth.jwt.secret is required when algorithm = \"HS256\"".to_string()
                })?;
                cq_transport::auth::JwtValidator::new_hs256(
                    secret.as_bytes(),
                    jwt_cfg.issuer.as_deref(),
                    jwt_cfg.audience.as_deref(),
                    &jwt_cfg.username_claim,
                    &jwt_cfg.entitlements_claim,
                )
            }
            JwtAlgorithm::Rs256 => {
                let pem = match (&jwt_cfg.public_key, &jwt_cfg.public_key_path) {
                    (Some(_), Some(_)) => {
                        return Err("auth.jwt: set only one of public_key / public_key_path".into());
                    }
                    (Some(inline), None) => inline.clone().into_bytes(),
                    (None, Some(path)) => std::fs::read(path).map_err(|e| {
                        format!("auth.jwt.public_key_path '{path}' could not be read: {e}")
                    })?,
                    (None, None) => {
                        return Err("auth.jwt: public_key or public_key_path is required when algorithm = \"RS256\"".into());
                    }
                };
                cq_transport::auth::JwtValidator::new_rs256(
                    &pem,
                    jwt_cfg.issuer.as_deref(),
                    jwt_cfg.audience.as_deref(),
                    &jwt_cfg.username_claim,
                    &jwt_cfg.entitlements_claim,
                )
                .map_err(|e| format!("auth.jwt: invalid RS256 public key: {e}"))?
            }
        };
        auth_store = auth_store.with_jwt(validator);
        info!(
            algorithm = ?jwt_cfg.algorithm,
            issuer = ?jwt_cfg.issuer,
            audience = ?jwt_cfg.audience,
            username_claim = %jwt_cfg.username_claim,
            entitlements_claim = %jwt_cfg.entitlements_claim,
            "JWT validator configured"
        );
    }
    let auth = Arc::new(auth_store);
    info!(
        required = auth.required,
        users = auth.user_count(),
        jwt = auth.has_jwt(),
        "Auth configured"
    );

    // Replication.
    let repl_role = server_config.replication.role;
    let repl_topics_for_ship: Vec<(String, PathBuf)> = server_config
        .topics
        .iter()
        .filter(|t| t.persist)
        .map(|t| (t.name.clone(), log_path_for(&txlog_dir, &t.name)))
        .collect();
    // S11 — hand the per-topic SharedTopic refs to the shipper so its
    // Ack reader can bump `last_replicated_sequence` on every Ack.
    let repl_topic_refs: std::collections::HashMap<String, SharedTopic> = topics
        .iter()
        .map(|e| (e.key().clone(), e.value().clone()))
        .collect();
    info!(role = ?repl_role, "Replication role");

    // Slow-consumer watcher — diffs per-sub drop counters every N
    // seconds, emits warnings + optional auto-disconnect. Runs for
    // the process lifetime; spawned before transports start serving so
    // it captures the first scan as a baseline.
    watch::spawn(
        registry.clone(),
        topics.clone(),
        watch::SlowConsumerConfig {
            scan_interval: std::time::Duration::from_secs(
                server_config.transport.slow_consumer.scan_interval_s,
            ),
            drops_per_sec_threshold: server_config
                .transport
                .slow_consumer
                .drops_per_sec_threshold,
            auto_disconnect: server_config.transport.slow_consumer.auto_disconnect,
            adaptive_conflation: server_config.transport.slow_consumer.adaptive_conflation,
            adaptive_max_interval_ms: server_config
                .transport
                .slow_consumer
                .adaptive_max_interval_ms,
            adaptive_fill_threshold: server_config
                .transport
                .slow_consumer
                .adaptive_fill_threshold,
        },
    );

    info!("Starting transports + admin + replication...");

    // Spawn the admin HTTP server on its OWN tokio runtime, on a
    // dedicated OS thread. The original wiring had admin in the same
    // `tokio::select!` as the WS/TCP transports — under heavy WS
    // load (e.g. concurrent SOW-snapshot encoders), the shared
    // runtime starved the admin handler and `/stats` / `/healthz`
    // would time out, exactly when you most need them to debug what's
    // happening. A dedicated 2-worker runtime guarantees the admin
    // endpoint stays responsive regardless of what the data path is
    // doing.
    {
        let admin_addr = admin_addr.clone();
        let admin_state = admin_state.clone();
        let admin_tls_acceptor = admin_tls_acceptor.clone();
        std::thread::Builder::new()
            .name("cq-admin-rt".into())
            .spawn(move || {
                let rt = match tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .enable_all()
                    .thread_name("cq-admin-worker")
                    .build()
                {
                    Ok(rt) => rt,
                    Err(e) => {
                        tracing::error!(error = %e, "Failed to build admin runtime");
                        return;
                    }
                };
                if let Err(e) = rt.block_on(admin::start_admin_server_with_tls(
                    admin_addr,
                    admin_state,
                    admin_tls_acceptor,
                )) {
                    tracing::error!(error = %e, "Admin server exited with error");
                }
            })
            .map_err(|e| format!("spawn admin thread: {}", e))?;
    }

    let shutdown_topics = topics.clone();
    let snapshot_on_shutdown = server_config.txlog.snapshot_on_shutdown;
    let snapshot_reclaim = server_config.txlog.snapshot_reclaim;
    tokio::select! {
        result = websocket::start_ws_server(
            ws_config, ws_topics, ws_registry, queues.clone(), heartbeat_cfg, auth.clone(),
        ) => {
            if let Err(e) = result {
                tracing::error!(error = %e, "WebSocket server failed");
            }
        }
        result = tcp::start_tcp_server(
            tcp_config, tcp_topics, tcp_registry, queues.clone(), heartbeat_cfg, auth.clone(),
        ) => {
            if let Err(e) = result {
                tracing::error!(error = %e, "TCP server failed");
            }
        }
        result = run_replication(
            repl_role,
            server_config.replication.resolve_peers(),
            server_config.replication.listen.clone(),
            repl_topics_for_ship,
            topics.clone(),
            server_config.replication.filter.clone(),
            server_config.replication.transform.clone(),
            repl_topic_refs,
            server_config.replication.auth_token.clone(),
            instance_name.clone(),
        ) => {
            if let Err(e) = result {
                tracing::error!(error = %e, "Replication failed");
            }
        }
        signal = wait_for_shutdown_signal() => {
            info!(signal = %signal, "Shutdown signal received");
        }
    }

    shutdown(&shutdown_topics, snapshot_on_shutdown, snapshot_reclaim).await;
    Ok(())
}

/// Block until SIGINT or (on Unix) SIGTERM arrives. Returns the
/// signal name as a static string so the caller can log it.
async fn wait_for_shutdown_signal() -> &'static str {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut term = match signal(SignalKind::terminate()) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(error = %e, "Failed to install SIGTERM handler; SIGINT only");
                let _ = tokio::signal::ctrl_c().await;
                return "SIGINT";
            }
        };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => "SIGINT",
            _ = term.recv() => "SIGTERM",
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
        "SIGINT"
    }
}

/// Graceful shutdown sequence. Fsyncs every persistent topic's
/// txlog so the on-disk state matches the in-memory store at the
/// moment of exit — regardless of the configured fsync policy on
/// the hot write path. Bounded by a wall-clock budget so a stuck
/// disk can't hang the process forever.
async fn shutdown(
    topics: &Arc<DashMap<String, SharedTopic>>,
    snapshot_on_shutdown: bool,
    snapshot_reclaim: bool,
) {
    metrics::counter!("cq_shutdown_initiated_total").increment(1);
    let started = std::time::Instant::now();
    // Generous wall-clock cap: an fsync alone is milliseconds, but when
    // snapshot_on_shutdown is set we also serialize each persistent
    // topic's live SOW to a checkpoint file, which for a multi-GB topic
    // (e.g. a wide /positions universe) is seconds of work. The budget
    // exists only to stop a *stuck disk* from hanging the process
    // forever — it must be comfortably above the legitimate snapshot
    // time, or we'd time out and exit before the checkpoint lands,
    // forcing the next start back into a full txlog replay.
    let budget = std::time::Duration::from_secs(120);

    // Per-topic fsync is synchronous (file::sync_all) but typically
    // milliseconds. Run them on the runtime's blocking pool with an
    // overall timeout so we never block shutdown indefinitely.
    let names: Vec<String> = topics.iter().map(|e| e.key().clone()).collect();
    let flushed = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let failed = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let mut tasks = Vec::new();
    for name in names {
        let Some(topic) = topics.get(&name).map(|e| e.value().clone()) else {
            continue;
        };
        let flushed = flushed.clone();
        let failed = failed.clone();
        tasks.push(tokio::task::spawn_blocking(move || {
            match topic.flush_txlog() {
                Ok(()) => {
                    flushed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                Err(e) => {
                    failed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    tracing::warn!(topic = %name, error = %e, "txlog flush failed");
                    // Skip the snapshot if we couldn't even make the log
                    // durable — the snapshot would claim a sequence the log
                    // can't back up.
                    return;
                }
            }
            // Checkpoint the SOW after the log is durable, so the next start
            // loads the snapshot and replays only the (now-empty) tail
            // instead of the whole log. Best-effort: a snapshot failure
            // degrades to a slower full replay, never to data loss.
            if !snapshot_on_shutdown {
                return;
            }
            let wrote = match topic.write_snapshot() {
                Ok(true) => {
                    tracing::info!(topic = %name, "SOW snapshot written");
                    true
                }
                Ok(false) => false,
                Err(e) => {
                    tracing::warn!(topic = %name, error = %e, "snapshot write failed");
                    false
                }
            };
            // Reclaim sealed segments only after a durable snapshot — the
            // snapshot now backs every row, so the older segments are
            // redundant for local restart. Opt-in (snapshot_reclaim) since a
            // far-behind replication standby would need those segments to
            // catch up without a base-SOW resync.
            if wrote && snapshot_reclaim {
                match topic.reclaim_sealed_segments() {
                    Ok(n) if n > 0 => {
                        tracing::info!(topic = %name, reclaimed = n, "reclaimed sealed txlog segments")
                    }
                    Ok(_) => {}
                    Err(e) => {
                        tracing::warn!(topic = %name, error = %e, "segment reclaim failed")
                    }
                }
            }
        }));
    }
    let drain = async {
        for t in tasks {
            let _ = t.await;
        }
    };
    match tokio::time::timeout(budget, drain).await {
        Ok(()) => {}
        Err(_) => tracing::warn!(
            elapsed_ms = started.elapsed().as_millis() as u64,
            "Shutdown txlog flush exceeded budget — exiting anyway"
        ),
    }
    let flushed_n = flushed.load(std::sync::atomic::Ordering::Relaxed);
    metrics::counter!("cq_topics_flushed_total").increment(flushed_n);
    info!(
        flushed = flushed_n,
        failed = failed.load(std::sync::atomic::Ordering::Relaxed),
        elapsed_ms = started.elapsed().as_millis() as u64,
        "CQServer shutdown complete"
    );
}

async fn run_replication(
    role: ReplicationRole,
    peers: Vec<String>,
    listen: Option<String>,
    ship_topics: Vec<(String, PathBuf)>,
    standby_topics: Arc<DashMap<String, SharedTopic>>,
    filter: Option<cq_replication::filter::FilterSpec>,
    transform: Option<cq_replication::filter::TransformSpec>,
    topic_refs: std::collections::HashMap<String, SharedTopic>,
    auth_token: Option<String>,
    instance_name: String,
) -> Result<(), Box<dyn std::error::Error>> {
    match role {
        ReplicationRole::Standalone => {
            // Idle forever — `tokio::select!` will pick another branch.
            std::future::pending::<()>().await;
            Ok(())
        }
        ReplicationRole::Primary => {
            if peers.is_empty() {
                return Err(
                    "replication.peer or replication.peers required for primary role".into(),
                );
            }
            run_shippers(
                peers,
                ship_topics,
                filter,
                transform,
                topic_refs,
                auth_token,
                instance_name,
            )
            .await;
            Ok(())
        }
        ReplicationRole::Standby => {
            let listen = listen.ok_or("replication.listen required for standby role")?;
            let cfg = repl_recv::ReceiverConfig {
                listen_addr: listen,
                token: auth_token,
                instance_name,
                concurrent: false,
            };
            repl_recv::run(cfg, standby_topics)
                .await
                .map_err(|e| format!("receiver: {}", e).into())
        }
        ReplicationRole::ActiveActive => {
            // S21 full mesh: every node both ships its local writes to all
            // peers AND receives their writes concurrently. Run the shipper
            // fanout and a concurrent receiver together; either completing
            // (only on fatal error) ends the replication future.
            if peers.is_empty() {
                return Err(
                    "replication.peers required for active_active role".into(),
                );
            }
            let listen = listen
                .ok_or("replication.listen required for active_active role")?;
            let recv_cfg = repl_recv::ReceiverConfig {
                listen_addr: listen,
                token: auth_token.clone(),
                instance_name: instance_name.clone(),
                concurrent: true,
            };
            let ship = run_shippers(
                peers,
                ship_topics,
                filter,
                transform,
                topic_refs,
                auth_token,
                instance_name,
            );
            let recv = repl_recv::run(recv_cfg, standby_topics);
            tokio::select! {
                _ = ship => {
                    tracing::warn!("active-active shippers all exited");
                    Ok(())
                }
                r = recv => r.map_err(|e| format!("receiver: {}", e).into()),
            }
        }
    }
}

/// Spawn one shipper task per peer and wait on all of them. Each task
/// tails the shared per-topic txlog directories (read-only) and ships
/// its local writes; the shipper's `run()` loops internally on
/// reconnect, so a task only exits on a fatal error (logged). Shared by
/// the `primary` and `active_active` roles.
async fn run_shippers(
    peers: Vec<String>,
    ship_topics: Vec<(String, PathBuf)>,
    filter: Option<cq_replication::filter::FilterSpec>,
    transform: Option<cq_replication::filter::TransformSpec>,
    topic_refs: std::collections::HashMap<String, SharedTopic>,
    auth_token: Option<String>,
    instance_name: String,
) {
    let mut handles = Vec::with_capacity(peers.len());
    for peer in peers {
        let cfg = repl_ship::ShipperConfig {
            peer: peer.clone(),
            topics: ship_topics.clone(),
            filter: filter.clone(),
            transform: transform.clone(),
            topic_refs: topic_refs.clone(),
            token: auth_token.clone(),
            instance_name: instance_name.clone(),
            ..Default::default()
        };
        handles.push(tokio::spawn(async move {
            if let Err(e) = repl_ship::run(cfg).await {
                tracing::warn!(peer = %peer, error = %e, "Shipper task exited");
            }
        }));
    }
    for h in handles {
        let _ = h.await;
    }
}

/// Directory path for a topic's segmented txlog. Topic names are
/// slugified (any non-alphanum/_-./ replaced with `_`) to be
/// filesystem-safe; segments live as `{8-digit id}.log` files inside.
fn log_path_for(dir: &PathBuf, topic: &str) -> PathBuf {
    let slug: String = topic
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.' {
                c
            } else {
                '_'
            }
        })
        .collect();
    dir.join(slug.trim_start_matches('_'))
}

fn init_topic(
    cfg: &TopicEntry,
    txlog_dir: &PathBuf,
    fsync: FsyncPolicy,
    config_dir: &PathBuf,
    archive_dir: Option<PathBuf>,
    segment_size_override: Option<u64>,
    archive_compress: bool,
    preallocate: bool,
    default_eval_lanes: usize,
    instance_name: &str,
) -> Result<SharedTopic, Box<dyn std::error::Error>> {
    let schema = build_topic_schema(cfg, config_dir)?;
    let columns = schema.column_count();
    // Per-topic override wins over the server-wide resolved default.
    let lanes = cfg.evaluator_lanes.unwrap_or(default_eval_lanes).max(1);
    let mut topic = Topic::new(
        TopicConfig {
            name: cfg.name.clone(),
            key_fields: cfg.key.clone(),
            persist: cfg.persist,
            conflation_ms: cfg.conflation_ms,
            index_columns: cfg.index_columns.clone(),
            expire_seconds: cfg.expire_seconds,
        },
        Arc::new(schema),
        cfg.initial_capacity,
    )
    .with_eval_lanes(lanes)
    .with_sparse_delta_txlog(cfg.sparse_delta_txlog)
    .with_origin_id(instance_name);

    if cfg.persist {
        let path = log_path_for(txlog_dir, &cfg.name);
        let segment_size =
            segment_size_override.unwrap_or(cq_txlog::DEFAULT_SEGMENT_SIZE);
        let writer = if archive_compress {
            if let Some(ad) = archive_dir.as_deref() {
                TxLogWriter::open_with_archive_compressed(&path, fsync, segment_size, ad)?
            } else {
                // Compression configured but no archive_directory — fall
                // back to plain (un-archived) write path with a warning.
                tracing::warn!(
                    topic = %cfg.name,
                    "archive_compress set but no archive_directory; ignoring"
                );
                TxLogWriter::open_with_segment_size(&path, fsync, segment_size)?
            }
        } else {
            TxLogWriter::open_with_archive(
                &path,
                fsync,
                segment_size,
                archive_dir.as_deref(),
            )?
        };
        let writer = writer.with_preallocate(preallocate);
        topic.attach_txlog(Arc::new(Mutex::new(writer)));
        info!(topic = %cfg.name, path = %path.display(), "TxLog attached");
    }

    info!(
        topic = %cfg.name,
        columns,
        key = ?cfg.key,
        "Topic initialised"
    );
    Ok(Arc::new(topic))
}

/// Stand up one materialized view declared in config. Resolves the
/// source topic, parses + compiles the view SQL, builds the view
/// topic with a derived schema, registers it in the global `topics`
/// map alongside regular topics, attaches a tap on the source for
/// re-aggregation wake-ups, spawns the view runner thread, and
/// spawns the view topic's own evaluator so subscribers see deltas.
/// What soft-delete needs to stop a view's runner: drop the source
/// view-tap(s) by id. (The evaluator is intentionally NOT torn down.)
#[derive(Clone)]
pub(crate) struct ViewTeardown {
    pub source: SharedTopic,
    pub left_tap_id: u64,
    pub right: Option<(SharedTopic, u64)>,
}

/// Typed error from `init_view` so callers can distinguish a name
/// collision (HTTP 409) from other failures (HTTP 400) without
/// string-matching the message.
#[derive(Debug)]
pub(crate) enum InitViewError {
    NameCollision(String),
    SourceNotFound(String),
    Build(String),
}

impl std::fmt::Display for InitViewError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InitViewError::NameCollision(m)
            | InitViewError::SourceNotFound(m)
            | InitViewError::Build(m) => write!(f, "{m}"),
        }
    }
}

pub(crate) fn init_view(
    cfg: &ViewEntry,
    topics: &Arc<DashMap<String, SharedTopic>>,
    registry: cq_transport::session::SessionRegistry,
) -> Result<(std::thread::JoinHandle<()>, ViewTeardown), InitViewError> {
    // P14 — view name and source name are both canonicalised so the
    // collision check and source lookup hit the same key the topic
    // registration used.
    let canonical_view = cq_core::topic::canonicalize_topic(&cfg.name);
    let canonical_source = cq_core::topic::canonicalize_topic(&cfg.source);
    if topics.contains_key(&canonical_view) {
        return Err(InitViewError::NameCollision(format!(
            "view name `{}` collides with an existing topic",
            canonical_view
        )));
    }
    let source = topics
        .get(&canonical_source)
        .map(|e| e.value().clone())
        .ok_or_else(|| {
            InitViewError::SourceNotFound(format!(
                "source topic `{}` not found",
                canonical_source
            ))
        })?;

    // S20 — detect a JOIN clause on the view SQL. JOIN views go
    // through `build_view_topic_joined` (parses against the
    // combined left∪right schema) and the runner fans both source
    // taps into a single refresh signal; un-joined views take the
    // single-source path as before.
    let join_topics = peek_join(&cfg.sql)
        .map_err(|e| InitViewError::Build(format!("parse view sql: {}", e)))?;

    let (view_topic, query, group_by_names, right_topic_opt): (
        cq_core::topic::Topic,
        cq_core::query::ParsedQuery,
        Vec<String>,
        Option<SharedTopic>,
    ) = if join_topics.is_some() {
        let topics_for_resolver = topics.clone();
        let (vt, q, g, rt) = View::build_view_topic_joined(
            &source,
            move |name| {
                // P14 — registry is canonical (slash-prefixed). The
                // SQL may carry either form; canonicalise and look
                // up once.
                let key = cq_core::topic::canonicalize_topic(name);
                topics_for_resolver
                    .get(&key)
                    .map(|e| e.value().clone())
            },
            &cfg.sql,
            canonical_view.clone(),
            cfg.initial_capacity,
        )
        .map_err(|e| InitViewError::Build(format!("build joined view topic: {}", e)))?;
        (vt, q, g, Some(rt))
    } else {
        let (vt, q, g) = View::build_view_topic(
            &source,
            &cfg.sql,
            canonical_view.clone(),
            cfg.initial_capacity,
        )
        .map_err(|e| InitViewError::Build(format!("build view topic: {}", e)))?;
        (vt, q, g, None)
    };
    let view_topic_arc = Arc::new(view_topic);

    // Take the view topic's own mutation receiver before anyone else
    // can race for it; this is the receiver the view-topic evaluator
    // will read so view subscribers get row-level deltas as the view
    // SOW changes.
    let view_topic_rx = view_topic_arc
        .take_mutation_rx()
        .expect("freshly-created view topic must have an rx");

    let (left_tap_id, left_tap) = source.register_view_tap(cfg.tap_capacity);
    let right_tap = right_topic_opt
        .as_ref()
        .map(|r| r.register_view_tap(cfg.tap_capacity));
    let (right_tap_id, right_tap_rx): (Option<u64>, Option<_>) = match right_tap {
        Some((id, rx)) => (Some(id), Some(rx)),
        None => (None, None),
    };

    // Clone the source(s) for the teardown record BEFORE they're moved
    // into `View::new` below (SharedTopic is an Arc, cheap to clone).
    let source_for_teardown = source.clone();
    let right_for_teardown = right_topic_opt.clone();

    let view = View::new(
        source,
        view_topic_arc.clone(),
        query,
        group_by_names,
        right_topic_opt,
    )
    .map_err(|e| InitViewError::Build(format!("instantiate view: {}", e)))?;

    // The view's runner thread keeps the view SOW in sync with the
    // source(s); the view-topic evaluator dispatches deltas to view
    // subscribers. Both run for the process lifetime.
    let _runner = match right_tap_rx {
        Some(rt) => spawn_view_runner_joined(view, left_tap, rt),
        None => spawn_view_runner(view, left_tap),
    }
    .map_err(|e| InitViewError::Build(format!("view runner thread spawn failed: {e}")))?;
    let evaluator_handle = cq_transport::delivery::spawn_evaluator(
        view_topic_arc.clone(),
        view_topic_rx,
        registry,
    )
    .map_err(|e| InitViewError::Build(format!("view evaluator thread spawn failed: {e}")))?;

    topics.insert(canonical_view, view_topic_arc);
    let teardown = ViewTeardown {
        source: source_for_teardown,
        left_tap_id,
        right: match (right_for_teardown, right_tap_id) {
            (Some(rt), Some(rid)) => Some((rt, rid)),
            _ => None,
        },
    };
    Ok((evaluator_handle, teardown))
}

/// Build a topic's `Schema` from its `TopicEntry`. Precedence:
///   1. `schema_file` (external JSON, nested or flat list)
///   2. inline `columns = [...]`
///   3. fall back to placeholder schema (schema-on-first-publish)
fn build_topic_schema(
    cfg: &TopicEntry,
    config_dir: &PathBuf,
) -> Result<Schema, Box<dyn std::error::Error>> {
    if let Some(file) = &cfg.schema_file {
        let path = config_dir.join(file);
        let content = std::fs::read_to_string(&path)
            .map_err(|e| format!("reading schema_file {}: {}", path.display(), e))?;
        let json: serde_json::Value = serde_json::from_str(&content)
            .map_err(|e| format!("parsing schema_file {}: {}", path.display(), e))?;
        let columns = parse_schema_json(&json)?;
        return Ok(schema_from_columns(&columns));
    }
    if !cfg.columns.is_empty() {
        let columns: Vec<(String, ColumnType)> = cfg
            .columns
            .iter()
            .map(|c| (c.name.clone(), column_type_from_spec(c.col_type)))
            .collect();
        return Ok(schema_from_columns(&columns));
    }
    Ok(create_default_schema())
}

fn column_type_from_spec(t: ColumnTypeSpec) -> ColumnType {
    match t {
        ColumnTypeSpec::String => ColumnType::String,
        ColumnTypeSpec::Int => ColumnType::Int,
        ColumnTypeSpec::Long | ColumnTypeSpec::Integer => ColumnType::Long,
        ColumnTypeSpec::Double | ColumnTypeSpec::Float => ColumnType::Double,
        ColumnTypeSpec::Bool => ColumnType::Bool,
        ColumnTypeSpec::Timestamp => ColumnType::Timestamp,
    }
}

fn schema_from_columns(columns: &[(String, ColumnType)]) -> Schema {
    let names: Vec<&str> = columns.iter().map(|(n, _)| n.as_str()).collect();
    let types: Vec<ColumnType> = columns.iter().map(|(_, t)| *t).collect();
    Schema::from_strs(&names, &types)
}

/// Parse a schema JSON document into a flat ordered list of
/// `(dotted-path, ColumnType)`. Two shapes are accepted:
///
///   1. Array form: `[{"name":"a.b","type":"string"}, ...]`
///   2. Object form: nested object whose leaves are type strings, e.g.
///      `{"trade":{"price":"double","qty":"long"},"book":"string"}`
///
/// In the object form the keys are joined with `.` to produce the
/// internal column name. The column store remains flat — nesting is a
/// pure documentation/wire-format convenience.
fn parse_schema_json(
    json: &serde_json::Value,
) -> Result<Vec<(String, ColumnType)>, Box<dyn std::error::Error>> {
    match json {
        serde_json::Value::Array(items) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                let obj = item.as_object().ok_or("schema array entry must be object")?;
                let name = obj
                    .get("name")
                    .and_then(|v| v.as_str())
                    .ok_or("schema entry missing `name`")?;
                let ty = obj
                    .get("type")
                    .and_then(|v| v.as_str())
                    .ok_or("schema entry missing `type`")?;
                out.push((name.to_string(), parse_type_str(ty)?));
            }
            Ok(out)
        }
        serde_json::Value::Object(_) => {
            let mut out = Vec::new();
            flatten_schema_object(json, String::new(), &mut out)?;
            Ok(out)
        }
        _ => Err("schema file must be a JSON object or array".into()),
    }
}

fn flatten_schema_object(
    node: &serde_json::Value,
    prefix: String,
    out: &mut Vec<(String, ColumnType)>,
) -> Result<(), Box<dyn std::error::Error>> {
    let obj = node.as_object().ok_or("expected object in schema")?;
    for (key, value) in obj {
        let path = if prefix.is_empty() {
            key.clone()
        } else {
            format!("{prefix}.{key}")
        };
        match value {
            serde_json::Value::String(s) => out.push((path, parse_type_str(s)?)),
            serde_json::Value::Object(_) => flatten_schema_object(value, path, out)?,
            other => {
                return Err(
                    format!("unexpected schema value at {path}: {other}").into(),
                );
            }
        }
    }
    Ok(())
}

fn parse_type_str(s: &str) -> Result<ColumnType, Box<dyn std::error::Error>> {
    match s.to_ascii_lowercase().as_str() {
        "string" | "str" => Ok(ColumnType::String),
        "int" | "i32" => Ok(ColumnType::Int),
        "long" | "i64" | "integer" => Ok(ColumnType::Long),
        "double" | "f64" | "float" => Ok(ColumnType::Double),
        "bool" | "boolean" => Ok(ColumnType::Bool),
        "timestamp" | "datetime" | "ts" => Ok(ColumnType::Timestamp),
        other => Err(format!("unknown column type: {other}").into()),
    }
}

/// Replay every entry in `log_path` into `topic`. Returns the number of
/// entries applied.
/// Compatibility wrapper used by existing call sites that don't yet
/// pass an archive directory.
#[allow(dead_code)]
#[allow(dead_code)]
fn recover_topic(
    topic: &SharedTopic,
    log_path: &PathBuf,
) -> Result<usize, Box<dyn std::error::Error>> {
    recover_topic_with_archive(topic, log_path, None, 0)
}

/// Replay the txlog (tail) into `topic`. `resume_segment` fast-forwards the
/// reader past every segment with a lower id — set to the snapshot's
/// `segment_id` after `apply_snapshot` so fully-covered segments are skipped
/// entirely. Any entries in the resume segment that predate the snapshot are
/// harmlessly deduped by the snapshot baseline. `0` replays from the start.
fn recover_topic_with_archive(
    topic: &SharedTopic,
    log_path: &PathBuf,
    archive_dir: Option<&std::path::Path>,
    resume_segment: u64,
) -> Result<usize, Box<dyn std::error::Error>> {
    let mut reader = TxLogReader::open_with_archive(log_path, archive_dir)?;
    if resume_segment > 0 {
        reader.skip_to_segment(resume_segment);
    }
    let mut count = 0;
    loop {
        let entry = match reader.read_next() {
            Ok(Some(e)) => e,
            Ok(None) => break,
            Err(e) => {
                // Mid-stream corruption: bail out and let the operator
                // investigate rather than silently truncating data.
                warn!(
                    error = %e,
                    path = %log_path.display(),
                    "Aborting recovery on corruption"
                );
                return Err(Box::new(e));
            }
        };

        if entry.is_tombstone() {
            topic.replay_delete_origin(&entry.origin, entry.sequence, &entry.key);
        } else {
            match serde_json::from_slice::<serde_json::Value>(&entry.payload) {
                Ok(serde_json::Value::Object(map)) => {
                    if entry.payload_format == cq_txlog::PayloadFormat::JsonDelta {
                        // Sparse delta as published — merge into the row's
                        // prior state instead of replacing it.
                        topic.replay_delta_map_origin(&entry.origin, entry.sequence, &map);
                    } else {
                        topic.replay_upsert_map_origin(&entry.origin, entry.sequence, &map);
                    }
                }
                Ok(_) => {
                    warn!("Skipping non-object payload during recovery");
                }
                Err(e) => {
                    warn!(error = %e, "Skipping un-decodable payload during recovery");
                }
            }
        }
        count += 1;
    }
    Ok(count)
}

fn create_default_schema() -> Schema {
    Schema::from_strs(&["_key"], &[ColumnType::String])
}

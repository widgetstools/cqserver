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

use admin::{start_admin_server, AdminState};
use config::{ColumnTypeSpec, ReplicationRole, TopicEntry, TxLogFsyncConfig, ViewEntry};
use cq_core::schema::{ColumnType, Schema};
use cq_core::topic::{SharedTopic, Topic, TopicConfig};
use cq_core::query::peek_join;
use cq_core::view::{spawn_view_runner, spawn_view_runner_joined, View};
use cq_replication::{receiver as repl_recv, shipper as repl_ship};
use cq_transport::auth::{AuthStore, User};
use cq_transport::delivery::spawn_evaluator;
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Load the config FIRST so the logging section can drive tracing
    // sink installation. Bootstrapping issue: before logging is up,
    // we can't surface errors via tracing — `load_config` returns
    // them straight to stderr via the `?` operator instead.
    let (server_config, config_dir) = config::load_config()?;

    // Install tracing sinks from `[logging]`. Errors opening
    // individual sink files are returned so we can warn about them
    // post-install (other sinks may still have come up); failure to
    // install ANY layer falls back to a stderr default inside
    // `logging::install`.
    let sink_errors = logging::install(&server_config.logging);
    for e in &sink_errors {
        warn!(error = %e, "Logging sink failed to install");
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
    };

    // Resolve the optional archive directory once per server.
    let archive_dir_root: Option<PathBuf> = server_config
        .txlog
        .archive_directory
        .as_ref()
        .map(PathBuf::from);
    let segment_size_override = server_config.txlog.segment_size;
    let archive_compress = server_config.txlog.archive_compress;

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
        )?;

        // Recover from log BEFORE spawning the evaluator. Any mutation
        // events emitted during replay are drained — subscribers don't
        // exist yet so deltas would be meaningless anyway.
        let rx = shared
            .take_mutation_rx()
            .expect("freshly-created topic must have an rx");
        if topic_cfg.persist {
            let log_path = log_path_for(&txlog_dir, &topic_cfg.name);
            if log_path.exists() {
                let count = recover_topic_with_archive(
                    &shared,
                    &log_path,
                    topic_archive_dir.as_deref(),
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

        let handle = spawn_evaluator(shared.clone(), rx, registry.clone());
        evaluator_handles.push(handle);

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

        topics.insert(topic_cfg.name.clone(), shared);
        info!(topic = %topic_cfg.name, "Topic ready");
    }

    info!(topics = topics.len(), "Topic registry initialized");

    // S20 materialized views. Each view declares an aggregate query
    // over an existing source topic; the server creates the view
    // topic, registers it in the same `topics` map (so subscribers
    // find it via the regular subscribe path), spawns the view
    // runner thread, and spawns the view topic's own evaluator
    // thread so its subscribers see row-level deltas as the view's
    // SOW changes.
    for view_cfg in &server_config.views {
        match init_view(view_cfg, &topics, registry.clone()) {
            Ok(handle) => {
                evaluator_handles.push(handle);
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
    let admin_state = AdminState {
        topics: topics.clone(),
        registry: registry.clone(),
        prom: prom_handle,
        shards: Arc::new(server_config.shards.clone()),
        self_url: Arc::new(self_url),
    };
    let admin_addr = server_config.admin_addr.clone();

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
    let ws_config = WsConfig {
        listen_addr: server_config.websocket_addr.clone(),
        path: server_config.websocket_path.clone(),
        outbound_queue_capacity: outbound_capacity,
        sow_batch_size,
        bookmark_store: Some(bookmark_store.clone()),
        spillover: spillover_ctx.clone(),
        read_only,
    };
    let tcp_config = TcpConfig {
        listen_addr: server_config.tcp_addr.clone(),
        outbound_queue_capacity: outbound_capacity,
        sow_batch_size,
        tls_acceptor,
        bookmark_store: Some(bookmark_store),
        spillover: spillover_ctx,
        read_only,
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
        match User::from_parts_with_row_filter(
            u.username.clone(),
            u.password_hash.clone(),
            &u.entitlements,
            u.row_filter.clone(),
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
        let validator = cq_transport::auth::JwtValidator::new_hs256(
            jwt_cfg.secret.as_bytes(),
            jwt_cfg.issuer.as_deref(),
            jwt_cfg.audience.as_deref(),
            &jwt_cfg.username_claim,
            &jwt_cfg.entitlements_claim,
        );
        auth_store = auth_store.with_jwt(validator);
        info!(
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
                if let Err(e) = rt.block_on(start_admin_server(admin_addr, admin_state)) {
                    tracing::error!(error = %e, "Admin server exited with error");
                }
            })
            .map_err(|e| format!("spawn admin thread: {}", e))?;
    }

    let shutdown_topics = topics.clone();
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
            server_config.replication.peer.clone(),
            server_config.replication.listen.clone(),
            repl_topics_for_ship,
            topics.clone(),
            server_config.replication.filter.clone(),
            server_config.replication.transform.clone(),
            repl_topic_refs,
        ) => {
            if let Err(e) = result {
                tracing::error!(error = %e, "Replication failed");
            }
        }
        signal = wait_for_shutdown_signal() => {
            info!(signal = %signal, "Shutdown signal received");
        }
    }

    shutdown(&shutdown_topics).await;
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
async fn shutdown(topics: &Arc<DashMap<String, SharedTopic>>) {
    metrics::counter!("cq_shutdown_initiated_total").increment(1);
    let started = std::time::Instant::now();
    let budget = std::time::Duration::from_secs(10);

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
    peer: Option<String>,
    listen: Option<String>,
    ship_topics: Vec<(String, PathBuf)>,
    standby_topics: Arc<DashMap<String, SharedTopic>>,
    filter: Option<cq_replication::filter::FilterSpec>,
    transform: Option<cq_replication::filter::TransformSpec>,
    topic_refs: std::collections::HashMap<String, SharedTopic>,
) -> Result<(), Box<dyn std::error::Error>> {
    match role {
        ReplicationRole::Standalone => {
            // Idle forever — `tokio::select!` will pick another branch.
            std::future::pending::<()>().await;
            Ok(())
        }
        ReplicationRole::Primary => {
            let peer = peer.ok_or("replication.peer required for primary role")?;
            let cfg = repl_ship::ShipperConfig {
                peer,
                topics: ship_topics,
                filter,
                transform,
                topic_refs,
                ..Default::default()
            };
            repl_ship::run(cfg)
                .await
                .map_err(|e| format!("shipper: {}", e).into())
        }
        ReplicationRole::Standby => {
            let listen = listen.ok_or("replication.listen required for standby role")?;
            let cfg = repl_recv::ReceiverConfig {
                listen_addr: listen,
            };
            repl_recv::run(cfg, standby_topics)
                .await
                .map_err(|e| format!("receiver: {}", e).into())
        }
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
) -> Result<SharedTopic, Box<dyn std::error::Error>> {
    let schema = build_topic_schema(cfg, config_dir)?;
    let columns = schema.column_count();
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
    );

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
fn init_view(
    cfg: &ViewEntry,
    topics: &Arc<DashMap<String, SharedTopic>>,
    registry: cq_transport::session::SessionRegistry,
) -> Result<std::thread::JoinHandle<()>, String> {
    if topics.contains_key(&cfg.name) {
        return Err(format!(
            "view name `{}` collides with an existing topic",
            cfg.name
        ));
    }
    let source = topics
        .get(&cfg.source)
        .map(|e| e.value().clone())
        .ok_or_else(|| format!("source topic `{}` not found", cfg.source))?;

    // S20 — detect a JOIN clause on the view SQL. JOIN views go
    // through `build_view_topic_joined` (parses against the
    // combined left∪right schema) and the runner fans both source
    // taps into a single refresh signal; un-joined views take the
    // single-source path as before.
    let join_topics = peek_join(&cfg.sql)
        .map_err(|e| format!("parse view sql: {}", e))?;

    let (view_topic, query, group_by_names, right_topic_opt): (
        cq_core::topic::Topic,
        cq_core::query::ParsedQuery,
        Vec<String>,
        Option<SharedTopic>,
    ) = if join_topics.is_some() {
        let topics_for_resolver = topics.clone();
        let (vt, q, g, rt) = View::build_view_topic_joined(
            &source,
            move |name| topics_for_resolver.get(name).map(|e| e.value().clone()),
            &cfg.sql,
            cfg.name.clone(),
            cfg.initial_capacity,
        )
        .map_err(|e| format!("build joined view topic: {}", e))?;
        (vt, q, g, Some(rt))
    } else {
        let (vt, q, g) = View::build_view_topic(
            &source,
            &cfg.sql,
            cfg.name.clone(),
            cfg.initial_capacity,
        )
        .map_err(|e| format!("build view topic: {}", e))?;
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

    let left_tap = source.register_view_tap(cfg.tap_capacity);
    let right_tap = right_topic_opt
        .as_ref()
        .map(|r| r.register_view_tap(cfg.tap_capacity));

    let view = View::new(
        source,
        view_topic_arc.clone(),
        query,
        group_by_names,
        right_topic_opt,
    )
    .map_err(|e| format!("instantiate view: {}", e))?;

    // The view's runner thread keeps the view SOW in sync with the
    // source(s); the view-topic evaluator dispatches deltas to view
    // subscribers. Both run for the process lifetime.
    let _runner = match right_tap {
        Some(rt) => spawn_view_runner_joined(view, left_tap, rt),
        None => spawn_view_runner(view, left_tap),
    };
    let evaluator_handle = cq_transport::delivery::spawn_evaluator(
        view_topic_arc.clone(),
        view_topic_rx,
        registry,
    );

    topics.insert(cfg.name.clone(), view_topic_arc);
    Ok(evaluator_handle)
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
        other => Err(format!("unknown column type: {other}").into()),
    }
}

/// Replay every entry in `log_path` into `topic`. Returns the number of
/// entries applied.
/// Compatibility wrapper used by existing call sites that don't yet
/// pass an archive directory.
#[allow(dead_code)]
fn recover_topic(
    topic: &SharedTopic,
    log_path: &PathBuf,
) -> Result<usize, Box<dyn std::error::Error>> {
    recover_topic_with_archive(topic, log_path, None)
}

fn recover_topic_with_archive(
    topic: &SharedTopic,
    log_path: &PathBuf,
    archive_dir: Option<&std::path::Path>,
) -> Result<usize, Box<dyn std::error::Error>> {
    let mut reader = TxLogReader::open_with_archive(log_path, archive_dir)?;
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
            topic.replay_delete(entry.sequence, &entry.key);
        } else {
            match serde_json::from_slice::<serde_json::Value>(&entry.payload) {
                Ok(serde_json::Value::Object(map)) => {
                    topic.replay_upsert_map(entry.sequence, &map);
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

//! Server configuration loading.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    pub tcp_addr: String,
    pub websocket_addr: String,
    pub websocket_path: String,
    /// Maximum inbound WebSocket message/frame size in bytes. Caps both
    /// tungstenite's `max_message_size` and `max_frame_size` so a single
    /// client cannot force unbounded buffering on the read path. `None`
    /// (the default) uses the transport default of 16 MiB, which matches
    /// the TCP codec's frame cap.
    #[serde(default)]
    pub ws_max_message_bytes: Option<usize>,
    #[serde(default = "default_admin_addr")]
    pub admin_addr: String,
    /// Optional bearer token guarding the admin API. When set, every
    /// admin endpoint except the unauthenticated `GET /healthz` liveness
    /// probe requires an `Authorization: Bearer <token>` header that
    /// matches. `None` (the default) leaves the API open — acceptable
    /// only because the port now binds to loopback by default. Set this
    /// before exposing the admin port beyond localhost.
    #[serde(default)]
    pub admin_token: Option<String>,
    /// Optional TLS for the admin HTTP server. Mirrors
    /// `[transport.tls]`'s shape exactly (same `TlsConfig`, same
    /// PEM-file expectations). A separate top-level `[admin_tls]`
    /// table (rather than nesting under a new `[admin]` table) keeps
    /// the existing `admin_addr`/`admin_token` top-level keys
    /// untouched. `None` (the default) serves the admin API over
    /// plain HTTP, matching all existing deployments/tests.
    #[serde(default)]
    pub admin_tls: Option<TlsConfig>,
    #[serde(default = "default_heartbeat_interval_s")]
    pub heartbeat_interval_s: u64,
    #[serde(default = "default_heartbeat_idle_timeout_s")]
    pub heartbeat_idle_timeout_s: u64,
    #[serde(default)]
    pub topics: Vec<TopicEntry>,
    /// S20 materialized views. Each `ViewEntry` declares a derived
    /// topic populated by a continuous SELECT-GROUP-BY against an
    /// underlying source topic. View topics are themselves
    /// subscribable; the server spawns one per-view runner thread that
    /// re-aggregates on every source mutation and applies the diff to
    /// the view's SOW.
    #[serde(default)]
    pub views: Vec<ViewEntry>,
    /// Path to the server-owned runtime-views file (admin-created
    /// views). Defaults to `<config_dir>/runtime_views.toml` when
    /// unset. Loaded + merged into `views` at boot so admin-created
    /// views survive restart.
    #[serde(default)]
    pub runtime_views_path: Option<String>,
    #[serde(default)]
    pub queues: Vec<QueueEntry>,
    #[serde(default)]
    pub txlog: TxLogConfig,
    #[serde(default)]
    pub auth: AuthConfig,
    #[serde(default)]
    pub replication: ReplicationConfig,
    #[serde(default)]
    pub transport: TransportConfig,
    /// S25 — per-target tracing sinks. When empty (or absent), the
    /// server installs the historical single-stderr layer driven by
    /// `RUST_LOG`. When populated, each entry becomes a layer in the
    /// tracing-subscriber Registry; events are routed by `target`.
    #[serde(default)]
    pub logging: crate::logging::LoggingConfig,
    /// D5/P0.5 — dedicated audit-log sink. Sugar over the S25
    /// per-target sink machinery above: when set, every
    /// `target = "cq_audit"` event (logon success/fail, admin
    /// mutations, entitlement denials) is additionally routed to this
    /// destination, independent of whatever `[logging].sinks` does.
    /// `None` (the default) leaves audit events to fall through to
    /// whatever `[logging].sinks` (or the stderr fallback) already
    /// routes them to.
    #[serde(default)]
    pub audit: Option<crate::logging::AuditConfig>,
    /// H6 (minimum viable shard primitive): static topic-prefix →
    /// instance-URL map. When non-empty, the admin endpoint
    /// `/admin/shard-for/{topic}` answers with the owning instance's
    /// URL, letting clients route subscribes to the right node. This
    /// is the smallest deployable shape of horizontal scale-out and
    /// does NOT include cross-instance replication or topology
    /// rebalancing — those are H6.2 / H6.3 / H6.4 in the worklog.
    /// Empty means "this instance owns everything" (single-node
    /// default).
    #[serde(default)]
    pub shards: Vec<ShardEntry>,
    /// Query Guardrails G1: structural limits enforced at parse / view
    /// registration time. See `cq_core::query::QueryLimits`.
    #[serde(default)]
    pub query_limits: QueryLimitsConfig,
    /// Number of parallel evaluator lanes per topic (S?? multi-threaded
    /// evaluator sharding). Each lane owns a disjoint subset of a topic's
    /// subscriptions and runs on its own thread, so delta evaluation
    /// (predicate matching + delta build) fans across cores while
    /// per-subscription ordering is preserved. `None` (the default)
    /// means **1 lane** — sharding is opt-in. It only helps topics whose
    /// bottleneck is per-event matching across many subscriptions; it
    /// *costs* delivery efficiency, because each lane owns its own
    /// encode-once-fanout body cache, so a row delivered to subscribers
    /// spread across L active lanes is serialized L times instead of
    /// once. Set this (globally or per-topic) only for genuinely
    /// matching-bound hot topics. A per-topic `evaluator_lanes` override
    /// takes precedence. Distinct from the `shards` table above, which
    /// routes topics across *instances*.
    #[serde(default)]
    pub evaluator_lanes: Option<usize>,
}

impl ServerConfig {
    /// Resolve the global default evaluator lane count: the explicit
    /// `evaluator_lanes` if set (clamped `>= 1`), else `1`. Sharding is
    /// opt-in: the single-lane path preserves global encode-once-fanout
    /// (one body serialize per delta event, reused across all subscribers
    /// on the topic) at zero thread overhead. Multi-lane sharding trades
    /// that for parallel matching and is only worth it on matching-bound
    /// hot topics, so operators enable it deliberately.
    pub fn resolved_default_eval_lanes(&self) -> usize {
        self.evaluator_lanes.unwrap_or(1).max(1)
    }
}

/// TOML representation of `cq_core::query::QueryLimits`. Defined here
/// (not in cq-core) so the deserialization shape lives next to the
/// rest of the server config; converted to the cq-core type via
/// `to_core()`.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(default)]
pub struct QueryLimitsConfig {
    pub max_pivot_in_list_size: usize,
    pub max_view_chain_depth: usize,
    pub reject_degenerate_groupby: bool,
    pub reject_passthrough_views: bool,
    pub max_sow_estimated_rows: u64,
    pub max_sow_estimated_bytes: u64,
    pub max_join_estimated_fanout: u64,
    pub max_group_estimated_cardinality: u64,
    pub warn_sow_rows_threshold: u64,
    pub warn_sow_bytes_threshold: u64,
    pub hard_max_sow_result_rows: u64,
    pub hard_max_sow_result_bytes: u64,
}

impl Default for QueryLimitsConfig {
    fn default() -> Self {
        // Mirror cq_core::query::QueryLimits::default() so behavior
        // stays consistent if a user omits the [query_limits] block.
        let core = cq_core::query::QueryLimits::default();
        Self {
            max_pivot_in_list_size: core.max_pivot_in_list_size,
            max_view_chain_depth: core.max_view_chain_depth,
            reject_degenerate_groupby: core.reject_degenerate_groupby,
            reject_passthrough_views: core.reject_passthrough_views,
            max_sow_estimated_rows: core.max_sow_estimated_rows,
            max_sow_estimated_bytes: core.max_sow_estimated_bytes,
            max_join_estimated_fanout: core.max_join_estimated_fanout,
            max_group_estimated_cardinality: core.max_group_estimated_cardinality,
            warn_sow_rows_threshold: core.warn_sow_rows_threshold,
            warn_sow_bytes_threshold: core.warn_sow_bytes_threshold,
            hard_max_sow_result_rows: core.hard_max_sow_result_rows,
            hard_max_sow_result_bytes: core.hard_max_sow_result_bytes,
        }
    }
}

impl QueryLimitsConfig {
    pub fn to_core(self) -> cq_core::query::QueryLimits {
        cq_core::query::QueryLimits {
            max_pivot_in_list_size: self.max_pivot_in_list_size,
            max_view_chain_depth: self.max_view_chain_depth,
            reject_degenerate_groupby: self.reject_degenerate_groupby,
            reject_passthrough_views: self.reject_passthrough_views,
            max_sow_estimated_rows: self.max_sow_estimated_rows,
            max_sow_estimated_bytes: self.max_sow_estimated_bytes,
            max_join_estimated_fanout: self.max_join_estimated_fanout,
            max_group_estimated_cardinality: self.max_group_estimated_cardinality,
            warn_sow_rows_threshold: self.warn_sow_rows_threshold,
            warn_sow_bytes_threshold: self.warn_sow_bytes_threshold,
            hard_max_sow_result_rows: self.hard_max_sow_result_rows,
            hard_max_sow_result_bytes: self.hard_max_sow_result_bytes,
        }
    }
}

/// One row of the static shard table. The topic-prefix uses
/// longest-match semantics: a topic named `/trades/us` matches
/// prefix `/trades/` over `/`.
#[derive(Debug, Clone, Deserialize)]
pub struct ShardEntry {
    /// Topic prefix to match. Matched against the start of the
    /// topic name; the longest matching prefix wins.
    pub topic_prefix: String,
    /// URL of the instance that owns topics matching this prefix.
    /// Format: `tcp://host:port` or `ws://host:port/path`.
    pub instance_url: String,
}

impl ServerConfig {
    /// Resolve a topic to its owning instance URL using the configured
    /// shard table. Returns `None` if no entry matches — the caller
    /// should treat that as "this instance owns it." Longest-prefix
    /// match wins; ties broken by config-file order.
    ///
    /// Currently invoked only by tests and reserved for future
    /// in-process replication routing; the admin endpoint inlines
    /// the same rule against `AdminState.shards`.
    #[allow(dead_code)]
    pub fn resolve_shard(&self, topic: &str) -> Option<&str> {
        self.shards
            .iter()
            .filter(|e| topic.starts_with(&e.topic_prefix))
            .max_by_key(|e| e.topic_prefix.len())
            .map(|e| e.instance_url.as_str())
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct TransportConfig {
    /// Per-session outbound queue depth (frames). Sized to absorb a SOW
    /// snapshot burst before the writer drains it. Frames are dropped
    /// (with a metric) when the queue is full on the live-delta path;
    /// the SOW path uses backpressure (await on send) and won't drop.
    #[serde(default = "default_outbound_queue_capacity")]
    pub outbound_queue_capacity: usize,
    /// Rows packed per outbound `sow_batch` frame on the streaming SOW
    /// path. Larger values reduce per-frame overhead and serde calls;
    /// smaller values reduce peak buffer memory and time-to-first-row
    /// observed by the subscriber. 200 is a reasonable default for
    /// 100-column rows over a local-network WebSocket.
    #[serde(default = "default_sow_batch_size")]
    pub sow_batch_size: usize,

    /// Slow-consumer policy.
    #[serde(default)]
    pub slow_consumer: SlowConsumerConfig,

    /// Optional TLS for the TCP transport. When present, the listener
    /// performs a TLS handshake on every accepted connection before
    /// handing it to the framing handler. Clients connect with rustls.
    #[serde(default)]
    pub tls: Option<TlsConfig>,

    /// S21 slow-consumer disk spillover. When set, every subscription
    /// gets a per-route overflow file under `directory`; queue-full
    /// events spill to disk instead of dropping, and a background
    /// drain task replays the backlog as the consumer catches up.
    /// `None` keeps the legacy "drop on full" behaviour.
    #[serde(default)]
    pub spillover: Option<SpilloverConfig>,

    /// D3/P0.3 — connection & rate limits, enforced in the TCP/WebSocket
    /// accept loops (and, for `max_sessions_per_user`, at Logon time).
    /// Omitting `[transport.limits]` entirely uses the defaults, which
    /// preserve current behavior except for `max_connections` (capped
    /// at 10000 instead of unbounded).
    #[serde(default)]
    pub limits: TransportLimitsConfig,
}

/// S21 spillover configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct SpilloverConfig {
    /// Directory under which per-subscription overflow files live.
    /// The server creates it on startup if missing.
    pub directory: String,
    /// Maximum bytes per subscription's overflow file. Writes past
    /// this are dropped (and counted via `cq_deltas_dropped_total{reason="spillover_over_cap"}`).
    #[serde(default = "default_spillover_max_bytes")]
    pub max_bytes_per_sub: u64,
}

/// D3/P0.3 — `[transport.limits]`. All fields default to "off" except
/// `max_connections`, which defaults to `10000` — still generous, but a
/// real ceiling instead of the previous unbounded accept loop. Every
/// other knob's `0` means disabled, matching pre-D3 behavior when the
/// whole `[transport.limits]` table is omitted from config.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(default)]
pub struct TransportLimitsConfig {
    /// Global cap on live connections (summed across TCP + WebSocket).
    /// Enforced at accept time via a shared semaphore.
    pub max_connections: usize,
    /// Cap on live connections from a single peer IP. `0` disables the
    /// check (unlimited connections per IP).
    pub max_connections_per_ip: usize,
    /// Cap on new accepted connections per rolling one-second window
    /// (summed across TCP + WebSocket). `0` disables the check.
    pub accept_rate_per_sec: u32,
    /// Cap on concurrently logged-on sessions for one authenticated
    /// username. Enforced at Logon time (identity isn't known at
    /// accept time). `0` disables the check.
    pub max_sessions_per_user: usize,
}

impl Default for TransportLimitsConfig {
    fn default() -> Self {
        Self {
            max_connections: cq_transport::limits::DEFAULT_MAX_CONNECTIONS,
            max_connections_per_ip: 0,
            accept_rate_per_sec: 0,
            max_sessions_per_user: 0,
        }
    }
}

impl TransportLimitsConfig {
    pub fn to_transport(self) -> cq_transport::limits::LimitsConfig {
        cq_transport::limits::LimitsConfig {
            max_connections: self.max_connections,
            max_connections_per_ip: self.max_connections_per_ip,
            accept_rate_per_sec: self.accept_rate_per_sec,
            max_sessions_per_user: self.max_sessions_per_user,
        }
    }
}

fn default_spillover_max_bytes() -> u64 {
    // 64 MiB per subscription by default. Generous enough to absorb
    // multi-minute hiccups on a typical wire rate; tunable per
    // deployment.
    64 * 1024 * 1024
}

/// TLS settings for the TCP transport.
#[derive(Debug, Clone, Deserialize)]
pub struct TlsConfig {
    /// PEM-encoded certificate chain (server cert first).
    pub cert_file: String,
    /// PEM-encoded private key (PKCS#8, PKCS#1 RSA, or SEC1 EC).
    pub key_file: String,
}

/// Slow-consumer detection settings. The watcher runs in a background
/// task on the admin runtime; every `scan_interval_s` seconds it diffs
/// drop counters per subscription and emits a structured warning when
/// a sub's drop rate exceeds `drops_per_sec_threshold` for at least
/// one window.
///
/// Two remediation policies, picked independently:
///   - `auto_disconnect = true` → evict the route + admin metric (hard).
///   - `adaptive_conflation = true` → widen the per-sub conflator
///     flush interval up to `adaptive_max_interval_ms` to shed load
///     (soft).  When pressure clears, decays back toward the route's
///     baseline conflation interval.
#[derive(Debug, Clone, Deserialize)]
pub struct SlowConsumerConfig {
    #[serde(default = "default_slow_scan_interval_s")]
    pub scan_interval_s: u64,
    #[serde(default = "default_slow_drops_per_sec_threshold")]
    pub drops_per_sec_threshold: u64,
    #[serde(default)]
    pub auto_disconnect: bool,

    /// Soft remediation: widen per-sub conflator flush interval under
    /// back-pressure instead of (or before) disconnecting. Effective
    /// only for routes that have conflation enabled at all.
    #[serde(default)]
    pub adaptive_conflation: bool,
    /// Maximum interval (ms) the watcher will widen to. Cap exists so a
    /// chronically-bad sub doesn't end up effectively muted forever.
    #[serde(default = "default_adaptive_max_interval_ms")]
    pub adaptive_max_interval_ms: u64,
    /// Fill ratio threshold above which the watcher starts widening
    /// the interval. 0.5 means "start backing off at 50% full".
    #[serde(default = "default_adaptive_fill_threshold")]
    pub adaptive_fill_threshold: f32,
}

impl Default for SlowConsumerConfig {
    fn default() -> Self {
        Self {
            scan_interval_s: default_slow_scan_interval_s(),
            drops_per_sec_threshold: default_slow_drops_per_sec_threshold(),
            auto_disconnect: false,
            adaptive_conflation: false,
            adaptive_max_interval_ms: default_adaptive_max_interval_ms(),
            adaptive_fill_threshold: default_adaptive_fill_threshold(),
        }
    }
}

fn default_slow_scan_interval_s() -> u64 {
    5
}
fn default_slow_drops_per_sec_threshold() -> u64 {
    500
}
fn default_adaptive_max_interval_ms() -> u64 {
    5_000
}
fn default_adaptive_fill_threshold() -> f32 {
    0.5
}

impl Default for TransportConfig {
    fn default() -> Self {
        Self {
            outbound_queue_capacity: default_outbound_queue_capacity(),
            sow_batch_size: default_sow_batch_size(),
            slow_consumer: SlowConsumerConfig::default(),
            tls: None,
            spillover: None,
            limits: TransportLimitsConfig::default(),
        }
    }
}

fn default_outbound_queue_capacity() -> usize {
    // H1: dropped from 8192 to 2048. See the matching constant in
    // `crates/cq-transport/src/session.rs::DEFAULT_OUTBOUND_QUEUE_CAPACITY`
    // for rationale — short version is that 8K per-session was the
    // dominant per-sub memory at high N.
    2048
}

fn default_sow_batch_size() -> usize {
    200
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct ReplicationConfig {
    #[serde(default)]
    pub role: ReplicationRole,
    /// Address of the peer (primary→standby) when `role = primary`.
    /// **Deprecated** in favor of `peers`; kept for backward
    /// compatibility. If both are set the union is used.
    #[serde(default)]
    pub peer: Option<String>,
    /// Multi-peer replication targets. When non-empty (or `peer` is
    /// set), `role = primary` spawns one shipper task per peer; each
    /// ships the same txlog stream to its own follower. Lets a
    /// single leader fan out to N followers from one process,
    /// which is the replica-reads deployment shape — see
    /// `docs/deploy/replica-reads.md` and
    /// `CLOUD_REPLICATION_TEST_WORKLOG.md`.
    #[serde(default)]
    pub peers: Vec<String>,
    /// Listen address for the standby's replication acceptor when
    /// `role = standby`.
    #[serde(default)]
    pub listen: Option<String>,
    /// S12 — optional per-destination filter. Only applies when
    /// `role = primary`. Drops entries whose JSON payload doesn't
    /// match `column = value`. Tombstones always ship.
    #[serde(default)]
    pub filter: Option<cq_replication::filter::FilterSpec>,
    /// S12 — optional per-destination transform. Strips the listed
    /// JSON fields from outbound entries.
    #[serde(default)]
    pub transform: Option<cq_replication::filter::TransformSpec>,
    /// Shared secret authenticating the replication channel. When set,
    /// a `primary` presents it on connect and a `standby` requires a
    /// matching token before accepting any entries. Both ends must agree.
    /// `None` (default) leaves the channel unauthenticated — only safe
    /// when the replication port is confined to a trusted network.
    #[serde(default)]
    pub auth_token: Option<String>,
    /// S11 — upper bound (ms) a publish requesting `ack_type =
    /// "replicated"` waits for a standby to confirm the write before the
    /// publish is failed back to the client. Only meaningful when `role
    /// = primary` with at least one peer; on a standalone node a
    /// `replicated` ack is rejected outright. Defaults to 5000 ms.
    #[serde(default = "default_repl_sync_ack_timeout_ms")]
    pub sync_ack_timeout_ms: u64,
    /// S20 — stable AMPS-style instance identity for this node. Stamped as
    /// the `origin` on every local txlog write and reported to peers in the
    /// replication handshake, so re-delivery of the same `(origin, sequence)`
    /// over any path is deduplicated idempotently (the anti-split-brain
    /// convergence guarantee). Must be unique per node and stable across
    /// restarts. `None` (default) disables origin tagging — only safe for a
    /// standalone node; an HA deployment that leaves this unset gets a
    /// startup warning and no convergence protection.
    #[serde(default)]
    pub instance_name: Option<String>,
}

fn default_repl_sync_ack_timeout_ms() -> u64 {
    5000
}

impl ReplicationConfig {
    /// Resolve the effective set of peer addresses for the shipper.
    /// Concatenates `peer` (if set) with `peers` and dedups. Returns
    /// empty if neither field is populated — the caller (main.rs)
    /// converts that to a startup error when `role = primary`.
    pub fn resolve_peers(&self) -> Vec<String> {
        let mut out: Vec<String> = Vec::new();
        if let Some(p) = &self.peer {
            if !p.is_empty() && !out.contains(p) {
                out.push(p.clone());
            }
        }
        for p in &self.peers {
            if !p.is_empty() && !out.contains(p) {
                out.push(p.clone());
            }
        }
        out
    }

    /// Resolve this node's instance id for origin tagging. Returns the
    /// configured `instance_name`, or an empty string when unset (which
    /// disables origin tagging / convergence dedup).
    pub fn resolve_instance_name(&self) -> String {
        self.instance_name.clone().unwrap_or_default()
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReplicationRole {
    #[default]
    Standalone,
    Primary,
    Standby,
    /// S21 — full-mesh active-active. Every node both ships its local
    /// writes to all `peers` AND listens on `listen` for inbound entries
    /// from those same peers. All nodes are writable; convergence is the
    /// existing per-`(origin, sequence)` dedup (replicated entries are
    /// applied via `replay_*_origin`, which never re-enters the local
    /// txlog, so each node ships only its own local writes — no echo
    /// loops, no transitive forwarding). Failover is implicit: any node
    /// can serve writes, and a recovering node re-syncs each origin
    /// stream past its high-water on reconnect.
    ActiveActive,
}

#[derive(Debug, Clone, Deserialize)]
pub struct QueueEntry {
    pub name: String,
    /// Per-message lease window in ms. `None` (default) = at-most-
    /// once fire-and-forget (legacy). `Some(ms)` = at-least-once
    /// with redelivery after `ms` of no ack.
    #[serde(default)]
    pub lease_ms: Option<u64>,
    /// Redelivery cap. Default 8. Once exceeded the message is
    /// routed to the configured `dlq` (or dropped if none).
    #[serde(default)]
    pub max_delivery_count: Option<u32>,
    /// Name of the dead-letter queue to route exhausted messages to.
    /// Must reference another queue declared in the same config.
    #[serde(default)]
    pub dlq: Option<String>,
    /// Cap on undelivered (buffered) messages. When exceeded, the oldest
    /// buffered message is evicted (ring-buffer) so a producerless queue
    /// can't exhaust memory. `None` uses the engine default (1M).
    #[serde(default)]
    pub max_buffer: Option<usize>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct AuthConfig {
    #[serde(default)]
    pub required: bool,
    #[serde(default)]
    pub users: Vec<UserConfig>,
    /// S16 JWT validator. When `Some`, the server treats the
    /// `Logon` frame's `data.token` field as a JWT and authenticates
    /// based on its claims. Static `[[auth.users]]` are also
    /// honoured; this is purely additive — a JWT logon path is
    /// available in addition to the password path. Per-deployment
    /// operators typically pick one mode and don't populate both.
    #[serde(default)]
    pub jwt: Option<JwtConfig>,
}

impl AuthConfig {
    /// D7/P0.7 — resolve `env://`/`file://` indirection on every
    /// secret field this config carries: each user's `password_hash`
    /// and the JWT validator's `secret`. Called once at config-load
    /// time so the rest of the server only ever sees the final,
    /// resolved plaintext value; callers never need to know whether
    /// an operator kept the secret in TOML, an env var, or a file.
    pub fn resolve_secrets(&mut self) -> Result<(), ConfigError> {
        for user in &mut self.users {
            user.password_hash = resolve_secret(&user.password_hash)?;
        }
        if let Some(jwt) = &mut self.jwt {
            if let Some(secret) = &jwt.secret {
                jwt.secret = Some(resolve_secret(secret)?);
            }
        }
        Ok(())
    }
}

/// Error resolving a secret-indirection reference (`env://` /
/// `file://`) found in a config secret field.
#[derive(Debug)]
pub enum ConfigError {
    /// An `env://VARNAME` reference pointed at an unset (or empty)
    /// environment variable.
    EnvVarNotSet(String),
    /// A `file://` reference pointed at a file that couldn't be read.
    FileNotReadable {
        path: String,
        source: std::io::Error,
    },
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigError::EnvVarNotSet(var) => {
                write!(f, "env var {var} not set")
            }
            ConfigError::FileNotReadable { path, source } => {
                write!(f, "secret file {path} not readable: {source}")
            }
        }
    }
}

impl std::error::Error for ConfigError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ConfigError::EnvVarNotSet(_) => None,
            ConfigError::FileNotReadable { source, .. } => Some(source),
        }
    }
}

/// D7/P0.7 — resolve a config secret field's raw TOML value into its
/// final plaintext form. Supports two indirection schemes so secrets
/// (JWT signing keys, bcrypt password hashes) don't have to live in
/// plaintext TOML:
///
///   - `env://VARNAME` — read the named environment variable. Errors
///     (naming the var) if it's unset *or* set to an empty string; an
///     empty secret is almost certainly a misconfiguration, not an
///     intentional value.
///   - `file:///abs/path` (or `file://relative/path`) — read the file
///     at the given path and trim trailing whitespace/newlines (many
///     secrets managers/`kubectl` mounts append a trailing newline).
///     Errors (naming the path) if the file can't be read.
///   - Anything else — returned verbatim. This keeps existing
///     deployments with plaintext hashes/secrets in TOML working
///     unchanged; there is no required migration.
pub fn resolve_secret(raw: &str) -> Result<String, ConfigError> {
    if let Some(var) = raw.strip_prefix("env://") {
        return match std::env::var(var) {
            Ok(val) if !val.is_empty() => Ok(val),
            Ok(_) => Err(ConfigError::EnvVarNotSet(var.to_string())),
            Err(_) => Err(ConfigError::EnvVarNotSet(var.to_string())),
        };
    }
    if let Some(path) = raw.strip_prefix("file://") {
        return std::fs::read_to_string(path)
            .map(|s| s.trim_end().to_string())
            .map_err(|source| ConfigError::FileNotReadable {
                path: path.to_string(),
                source,
            });
    }
    Ok(raw.to_string())
}

/// JWT signing algorithm the validator should enforce. Tokens signed
/// with any other algorithm are rejected (the `alg` header is pinned),
/// which closes the classic "alg confusion" downgrade attack.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub enum JwtAlgorithm {
    /// Symmetric HMAC-SHA256 — validated with a shared `secret`.
    #[serde(rename = "HS256")]
    Hs256,
    /// Asymmetric RSA-SHA256 — validated with a PEM RSA public key.
    /// The matching private key stays with the token issuer.
    #[serde(rename = "RS256")]
    Rs256,
}

fn default_jwt_algorithm() -> JwtAlgorithm {
    JwtAlgorithm::Hs256
}

/// S16 JWT validator config.
#[derive(Debug, Clone, Deserialize)]
pub struct JwtConfig {
    /// Signing algorithm to enforce. Defaults to `HS256` for backward
    /// compatibility; set to `RS256` to validate asymmetric tokens via
    /// `public_key` / `public_key_path`.
    #[serde(default = "default_jwt_algorithm")]
    pub algorithm: JwtAlgorithm,
    /// Shared secret used to validate HS256 tokens. Required when
    /// `algorithm = "HS256"`; ignored for RS256.
    ///
    /// D7/P0.7: keep this out of plaintext TOML in production by
    /// writing `env://VARNAME` (read from the environment at
    /// startup) or `file:///abs/path` (read from a file, trailing
    /// newline trimmed) instead of the literal secret. Both forms are
    /// resolved once by `AuthConfig::resolve_secrets` at config-load
    /// time; a plain value is still accepted verbatim for backward
    /// compatibility. See `resolve_secret` in this module.
    #[serde(default)]
    pub secret: Option<String>,
    /// PEM-encoded RSA public key (inline) used to validate RS256
    /// tokens. Mutually exclusive with `public_key_path`.
    #[serde(default)]
    pub public_key: Option<String>,
    /// Path to a PEM-encoded RSA public key file used to validate
    /// RS256 tokens. Read once at startup.
    #[serde(default)]
    pub public_key_path: Option<String>,
    /// Optional issuer claim ("iss") the token must carry. When set,
    /// tokens with a missing or non-matching `iss` are rejected.
    #[serde(default)]
    pub issuer: Option<String>,
    /// Optional audience claim ("aud") the token must carry.
    #[serde(default)]
    pub audience: Option<String>,
    /// Claim name to read the user's entitlements from. Defaults to
    /// `"entitlements"` (a JSON string array). Same shape as the
    /// static `entitlements = [...]` field in `[[auth.users]]`.
    #[serde(default = "default_jwt_entitlements_claim")]
    pub entitlements_claim: String,
    /// Claim name to read the username from. Defaults to `"sub"`
    /// (the standard JWT subject claim).
    #[serde(default = "default_jwt_username_claim")]
    pub username_claim: String,
}

fn default_jwt_entitlements_claim() -> String {
    "entitlements".into()
}

fn default_jwt_username_claim() -> String {
    "sub".into()
}

#[derive(Debug, Clone, Deserialize)]
pub struct UserConfig {
    pub username: String,
    /// Bcrypt password hash. D7/P0.7: accepts `env://VARNAME` or
    /// `file:///abs/path` indirection in place of the literal hash —
    /// see the note on `JwtConfig::secret` and `resolve_secret` in
    /// this module for the full scheme. A plain bcrypt hash is still
    /// accepted verbatim (backward compatible).
    pub password_hash: String,
    #[serde(default)]
    pub entitlements: Vec<String>,
    /// Optional row-level entitlement: a SQL WHERE-fragment that the
    /// server AND's into every subscribe/sow this user issues.
    /// Example: `row_filter = "desk = 'RATES'"`.
    #[serde(default)]
    pub row_filter: Option<String>,
    /// G5: optional per-user query budget. Each field tightens the
    /// corresponding `[query_limits]` server default for this user
    /// only. A user can only be more restricted than the global
    /// setting; the merge picks the smaller (tighter) of server +
    /// user when both are non-zero.
    #[serde(default)]
    pub query_budget: Option<QueryBudgetConfig>,
}

/// TOML shape for a per-user query budget. All fields optional so an
/// admin can tighten one dimension without restating the rest. Maps
/// to `cq_transport::auth::QueryBudget` via `to_runtime()`.
#[derive(Debug, Clone, Copy, Default, Deserialize)]
pub struct QueryBudgetConfig {
    #[serde(default)]
    pub max_sow_estimated_rows: Option<u64>,
    #[serde(default)]
    pub max_sow_estimated_bytes: Option<u64>,
    #[serde(default)]
    pub max_join_estimated_fanout: Option<u64>,
    #[serde(default)]
    pub max_group_estimated_cardinality: Option<u64>,
    #[serde(default)]
    pub hard_max_sow_result_rows: Option<u64>,
    #[serde(default)]
    pub hard_max_sow_result_bytes: Option<u64>,
}

impl QueryBudgetConfig {
    pub fn to_runtime(self) -> cq_transport::auth::QueryBudget {
        cq_transport::auth::QueryBudget {
            max_sow_estimated_rows: self.max_sow_estimated_rows,
            max_sow_estimated_bytes: self.max_sow_estimated_bytes,
            max_join_estimated_fanout: self.max_join_estimated_fanout,
            max_group_estimated_cardinality: self.max_group_estimated_cardinality,
            hard_max_sow_result_rows: self.hard_max_sow_result_rows,
            hard_max_sow_result_bytes: self.hard_max_sow_result_bytes,
        }
    }
}

fn default_admin_addr() -> String {
    // Loopback by default: the admin port exposes mutating endpoints
    // (unsubscribe, journal rotation, store shrink, view create/delete,
    // schema mutation) and unauthenticated config disclosure. Binding it
    // to all interfaces out of the box would expose those to the network.
    // Operators who front it with auth / a reverse proxy can override.
    "127.0.0.1:8085".into()
}

fn default_heartbeat_interval_s() -> u64 {
    30
}

fn default_heartbeat_idle_timeout_s() -> u64 {
    65
}

#[derive(Debug, Clone, Deserialize)]
pub struct TxLogConfig {
    #[serde(default = "default_txlog_dir")]
    pub directory: String,
    #[serde(default)]
    pub fsync: TxLogFsyncConfig,
    /// Optional archive directory. When set, sealed segments are
    /// moved here on rotation. The active segment continues to be
    /// written to `directory`; recovery reads both.
    #[serde(default)]
    pub archive_directory: Option<String>,
    /// Segment size in bytes for rotation. Defaults to the txlog
    /// crate's `DEFAULT_SEGMENT_SIZE` (256 MB). Exposed primarily so
    /// e2e tests can force rapid rotation with a small value.
    #[serde(default)]
    pub segment_size: Option<u64>,
    /// When `true` *and* `archive_directory` is set, sealed segments
    /// are zstd-compressed on archive (saving 5-10× on log-shaped
    /// payloads). The reader transparently decompresses on replay.
    #[serde(default)]
    pub archive_compress: bool,
    /// Interval (milliseconds) between fsyncs when `fsync = "interval"`.
    /// Ignored for the other modes. Bounds data-loss-on-power-failure to
    /// roughly this much wall-clock time of writes. Defaults to 200 ms.
    #[serde(default = "default_fsync_interval_ms")]
    pub fsync_interval_ms: u64,
    /// When `true`, each segment's disk blocks are reserved up front
    /// (`fallocate`/`F_PREALLOCATE`) so steady-state appends avoid
    /// per-write extent allocation. Best-effort: unsupported filesystems
    /// fall back to on-demand allocation. Off by default — enable for
    /// hot persistent topics on filesystems that benefit (ext4/xfs/APFS).
    #[serde(default)]
    pub preallocate: bool,
    /// When `true` (default), each persistent topic writes a SOW snapshot
    /// (a point-in-time checkpoint of its live store + sequence watermark)
    /// on graceful shutdown. On the next start the server loads the
    /// snapshot and replays only the txlog tail instead of the whole log —
    /// turning a multi-minute restart on a large log into a near-instant
    /// one. Cheap and safe (adds one file); disable only if you have a
    /// specific reason to.
    #[serde(default = "default_true")]
    pub snapshot_on_shutdown: bool,
    /// When `true`, after a *durable* shutdown snapshot is written, the
    /// fully-covered sealed segments (every segment older than the active
    /// one) are deleted to reclaim disk. Off by default because it is
    /// irreversible and unsafe for a replication **source** whose standbys
    /// may still need those segments to catch up — enable it on standalone
    /// nodes (or where standbys are known to be caught up) that want the
    /// log's disk footprint bounded to roughly one segment after a clean
    /// shutdown. Fast restart does **not** require this: the snapshot +
    /// tail-scan already make startup O(one segment) regardless.
    #[serde(default)]
    pub snapshot_reclaim: bool,
    /// Interval (seconds) between periodic in-process checkpoints. `0`
    /// (default) disables the checkpointer — checkpoints then only happen
    /// on graceful shutdown. When set > 0, a background sweeper runs every
    /// `checkpoint_interval_secs` and, for each persistent topic, performs
    /// the same durable sequence as shutdown: fsync the log → write a
    /// crash-durable `snapshot.bin` → reclaim the now-redundant sealed
    /// segments. This bounds a long-running server's on-disk txlog WITHOUT
    /// a restart (the AMPS `sow-compact-action` equivalent) and is the
    /// prerequisite for multi-day soaks.
    ///
    /// Safety: reclaim only ever deletes sealed segments that the freshly
    /// fsync'd snapshot fully covers, so a crash at any point recovers the
    /// full acked state (snapshot + surviving tail replay). The snapshot
    /// build holds only a topic **read** lock for the store copy, so
    /// publishers are not blocked for the whole checkpoint.
    ///
    /// Like `snapshot_reclaim`, the reclaim step is unsafe for a
    /// replication **source** whose standbys may still need old segments;
    /// the periodic reclaim here is therefore gated on `snapshot_reclaim`
    /// being enabled as well.
    #[serde(default)]
    pub checkpoint_interval_secs: u64,
}

impl Default for TxLogConfig {
    fn default() -> Self {
        TxLogConfig {
            directory: default_txlog_dir(),
            fsync: TxLogFsyncConfig::default(),
            archive_directory: None,
            segment_size: None,
            archive_compress: false,
            fsync_interval_ms: default_fsync_interval_ms(),
            preallocate: false,
            snapshot_on_shutdown: true,
            snapshot_reclaim: false,
            checkpoint_interval_secs: 0,
        }
    }
}

fn default_true() -> bool {
    true
}

fn default_fsync_interval_ms() -> u64 {
    200
}

#[derive(Debug, Clone, Copy, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum TxLogFsyncConfig {
    #[default]
    None,
    EveryWrite,
    /// Group commit — fsync at most once per `fsync_interval_ms`.
    Interval,
}

fn default_txlog_dir() -> String {
    "./data/txlog".into()
}

#[derive(Debug, Clone, Deserialize)]
pub struct TopicEntry {
    pub name: String,
    pub key: Vec<String>,
    #[serde(default)]
    pub persist: bool,
    pub conflation_ms: Option<u64>,
    #[serde(default = "default_capacity")]
    pub initial_capacity: usize,
    /// Inline column list. Either this or `schema_file` declares the
    /// topic's schema up front; absence of both falls back to
    /// schema-on-first-publish (the old behaviour).
    #[serde(default)]
    pub columns: Vec<ColumnSpec>,
    /// External JSON file holding the topic's schema. Useful when the
    /// schema is large (hundreds of fields) or shared between topics.
    /// File path is resolved relative to the TOML file's directory.
    #[serde(default)]
    pub schema_file: Option<String>,
    /// Schema column names to maintain a secondary equality index for.
    /// The SOW query planner uses these to short-circuit a full scan
    /// when the WHERE clause contains an equality predicate on one of
    /// these columns. Dotted-path column names (e.g. `instrument.cusip`)
    /// are supported just like the rest of the schema model.
    #[serde(default)]
    pub index_columns: Vec<String>,
    /// Per-row TTL in seconds. When set, a background sweeper task
    /// deletes rows whose `last touched` timestamp is older than
    /// this. `None` (default) disables TTL.
    #[serde(default)]
    pub expire_seconds: Option<u64>,
    /// Per-topic override for the number of parallel evaluator lanes
    /// (see `ServerConfig::evaluator_lanes`). `None` (default) inherits
    /// the server-wide resolved default. Set this on hot topics with many
    /// subscriptions that need the extra fanout, leaving low-traffic
    /// topics on the global default.
    #[serde(default)]
    pub evaluator_lanes: Option<usize>,
    /// When `true`, `delta_publish` payloads are journaled sparsely
    /// (`{key + changed fields}` as published, merge-on-replay) instead of
    /// as the fully-merged row. Set this on wide-row persistent topics
    /// whose publishers tick a handful of columns at a high rate — it cuts
    /// journal write volume roughly by the row-width / changed-fields
    /// ratio (a 16-field tick on a 200-column row journals ~12× less).
    /// Off by default: merged-row entries are self-contained, which keeps
    /// bookmark-replay consumers independent of prior history.
    #[serde(default)]
    pub sparse_delta_txlog: bool,
}

/// Column declaration in a TOML topic spec. `name` may be a dotted
/// path (e.g. `trade.price`) — internally the column store is flat and
/// dotted paths are used as literal column keys.
#[derive(Debug, Clone, Deserialize)]
pub struct ColumnSpec {
    pub name: String,
    #[serde(rename = "type")]
    pub col_type: ColumnTypeSpec,
}

/// Wire-level column type as declared in config. Mirrors the
/// `cq_core::schema::ColumnType` enum but lives here so config doesn't
/// pull cq-core into its public surface.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ColumnTypeSpec {
    String,
    Int,
    Long,
    Double,
    /// Alias for `double` — common in user-facing configs.
    #[serde(alias = "float")]
    Float,
    /// Alias for `long` — for users who think of integers as 64-bit by default.
    #[serde(alias = "integer")]
    Integer,
    /// Native boolean column — mirrors AMPS's `Bool` type lattice entry.
    /// JSON `true` / `false` round-trips; canonical string forms
    /// (`"true"`, `"false"`, `"1"`, `"0"`) coerce to bool on ingest.
    #[serde(alias = "boolean")]
    Bool,
    /// i64 μs-since-epoch column. JSON in: RFC 3339 / ISO 8601 strings
    /// (`2026-05-25T22:48:43Z`) or epoch numbers (seconds / ms / μs /
    /// ns by magnitude). JSON out: RFC 3339 micros-precision strings.
    #[serde(alias = "datetime")]
    #[serde(alias = "ts")]
    Timestamp,
}

fn default_capacity() -> usize {
    100_000
}

/// S20 materialized-view declaration. A view's `source` must reference
/// another topic in the same config. The `sql` is parsed against the
/// source topic's schema and MUST be an aggregate query
/// (`SELECT ... GROUP BY ...`). The view's schema is derived from the
/// SELECT clause; its key fields default to the `GROUP BY` columns.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ViewEntry {
    /// Name of the view topic (must not collide with any `topics` entry).
    pub name: String,
    /// Name of the source topic the view aggregates over.
    pub source: String,
    /// SELECT-GROUP-BY query (the `FROM` clause is interpreted as the
    /// source topic; the parser tolerates an arbitrary identifier).
    pub sql: String,
    /// Initial capacity hint for the view topic's SOW. Defaults to a
    /// modest 10K — views usually have far fewer rows than their
    /// underlying source.
    #[serde(default = "default_view_capacity")]
    pub initial_capacity: usize,
    /// Bounded depth of the view's tap channel on the source topic.
    /// A backlogged view runner can't apply backpressure to publishers;
    /// instead old events are dropped (with a metric) and the next
    /// refresh catches up by reading current source state. Defaults
    /// to a comfortable 1024.
    #[serde(default = "default_view_tap_capacity")]
    pub tap_capacity: usize,
}

fn default_view_capacity() -> usize {
    10_000
}

fn default_view_tap_capacity() -> usize {
    1024
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            tcp_addr: "0.0.0.0:9007".into(),
            websocket_addr: "0.0.0.0:9008".into(),
            websocket_path: "/cq/json".into(),
            ws_max_message_bytes: None,
            admin_addr: default_admin_addr(),
            admin_token: None,
            admin_tls: None,
            heartbeat_interval_s: default_heartbeat_interval_s(),
            heartbeat_idle_timeout_s: default_heartbeat_idle_timeout_s(),
            topics: vec![
                TopicEntry {
                    name: "/market-data".into(),
                    key: vec!["symbol".into()],
                    persist: false,
                    conflation_ms: Some(100),
                    initial_capacity: 100_000,
                    columns: Vec::new(),
                    schema_file: None,
                    index_columns: Vec::new(),
                    expire_seconds: None,
                    evaluator_lanes: None,
                    sparse_delta_txlog: false,
                },
                TopicEntry {
                    name: "/orders".into(),
                    key: vec!["orderId".into()],
                    persist: true,
                    conflation_ms: None,
                    initial_capacity: 100_000,
                    columns: Vec::new(),
                    schema_file: None,
                    index_columns: Vec::new(),
                    expire_seconds: None,
                    evaluator_lanes: None,
                    sparse_delta_txlog: false,
                },
            ],
            views: Vec::new(),
            runtime_views_path: None,
            queues: Vec::new(),
            txlog: TxLogConfig::default(),
            auth: AuthConfig::default(),
            replication: ReplicationConfig::default(),
            transport: TransportConfig::default(),
            logging: crate::logging::LoggingConfig::default(),
            audit: None,
            shards: Vec::new(),
            query_limits: QueryLimitsConfig::default(),
            evaluator_lanes: None,
        }
    }
}

/// Load configuration from file or use defaults. The returned PathBuf
/// is the directory the TOML file lives in (or the current dir when
/// using defaults), so per-topic `schema_file` paths can be resolved
/// relative to the config.
#[allow(dead_code)]
pub fn load_config() -> Result<(ServerConfig, std::path::PathBuf), Box<dyn std::error::Error>> {
    let (cfg, dir, _raw) = load_config_with_raw()?;
    Ok((cfg, dir))
}

/// Like `load_config` but also returns the *expanded* TOML text (after
/// env-var substitution). The admin UI's Config screen renders this
/// verbatim so operators can see exactly what the running process
/// believes its config to be — which can differ from the on-disk file
/// if any `${VAR:-default}` substitutions happened.
pub fn load_config_with_raw() -> Result<
    (ServerConfig, std::path::PathBuf, String),
    Box<dyn std::error::Error>,
> {
    let config_path = std::path::Path::new("config/cqserver.toml");
    if config_path.exists() {
        load_config_from_with_raw(config_path)
    } else {
        tracing::info!("No config file found, using defaults");
        Ok((
            ServerConfig::default(),
            std::path::PathBuf::from("."),
            "# no config/cqserver.toml found; running with defaults\n".to_string(),
        ))
    }
}

/// Like `load_config_with_raw` but takes an explicit config-file
/// path. Used by `--config <path>` to let operators run cqserver
/// from any CWD. The returned `PathBuf` is the directory the file
/// lives in (used to resolve relative `schema_file` paths in
/// `[[topics]]`).
pub fn load_config_from_with_raw(
    config_path: &std::path::Path,
) -> Result<(ServerConfig, std::path::PathBuf, String), Box<dyn std::error::Error>> {
    if !config_path.exists() {
        return Err(format!(
            "config file not found at {}",
            config_path.display()
        )
        .into());
    }
    let raw = std::fs::read_to_string(config_path)?;
    let content = substitute_env_vars(&raw)?;
    let mut config: ServerConfig = toml::from_str(&content)?;
    // D7/P0.7 — resolve `env://`/`file://` indirection on secret
    // fields (JWT secret, user password hashes, admin API token,
    // replication shared secret) after parsing. Deliberately applied
    // to the parsed `ServerConfig`, not the `content` string returned
    // to the caller, so the admin UI's "expanded config" view keeps
    // showing the operator's literal `env://`/`file://` reference
    // rather than the resolved secret.
    config.auth.resolve_secrets()?;
    if let Some(admin_token) = &config.admin_token {
        config.admin_token = Some(resolve_secret(admin_token)?);
    }
    if let Some(auth_token) = &config.replication.auth_token {
        config.replication.auth_token = Some(resolve_secret(auth_token)?);
    }
    let dir = config_path
        .parent()
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| std::path::PathBuf::from("."));
    Ok((config, dir, content))
}

/// Apply `${VAR}` and `${VAR:-default}` substitution to a TOML
/// source. Used by `load_config` so operators can parameterize ports,
/// directory paths, etc. via environment variables.
///
/// Syntax:
///   - `${VAR}` — substitutes the value of the `VAR` env var; errors
///     if `VAR` is unset.
///   - `${VAR:-default}` — substitutes `VAR` if set, else the literal
///     `default` (which itself is not further-expanded).
///
/// Anything else (e.g. `$VAR` without braces, `$$`) is left alone so
/// SQL-fragment values in `row_filter` don't get mangled.
pub fn substitute_env_vars(s: &str) -> Result<String, Box<dyn std::error::Error>> {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '$' && chars.peek() == Some(&'{') {
            chars.next(); // consume '{'
            let mut name = String::new();
            let mut default: Option<String> = None;
            let mut in_default = false;
            let mut closed = false;
            while let Some(&nc) = chars.peek() {
                chars.next();
                if nc == '}' {
                    closed = true;
                    break;
                }
                if !in_default && nc == ':' && chars.peek() == Some(&'-') {
                    chars.next(); // consume '-'
                    in_default = true;
                    default = Some(String::new());
                    continue;
                }
                if in_default {
                    default.as_mut().unwrap().push(nc);
                } else {
                    name.push(nc);
                }
            }
            if !closed {
                return Err(format!("unterminated env-var reference: ${{{name}").into());
            }
            match std::env::var(&name) {
                Ok(v) => out.push_str(&v),
                Err(_) => match default {
                    Some(d) => out.push_str(&d),
                    None => {
                        return Err(format!(
                            "env var `{name}` is not set (no default provided)"
                        )
                        .into());
                    }
                },
            }
        } else {
            out.push(c);
        }
    }
    Ok(out)
}

#[cfg(test)]
mod env_var_tests {
    use super::substitute_env_vars;

    #[test]
    fn substitutes_when_var_is_set() {
        std::env::set_var("CQ_TEST_X", "hello");
        let out = substitute_env_vars("a=${CQ_TEST_X}b").unwrap();
        assert_eq!(out, "a=hellob");
    }

    #[test]
    fn uses_default_when_var_unset() {
        std::env::remove_var("CQ_TEST_UNSET_1");
        let out = substitute_env_vars("a=${CQ_TEST_UNSET_1:-fallback}b").unwrap();
        assert_eq!(out, "a=fallbackb");
    }

    #[test]
    fn errors_when_var_unset_and_no_default() {
        std::env::remove_var("CQ_TEST_UNSET_2");
        let r = substitute_env_vars("a=${CQ_TEST_UNSET_2}b");
        assert!(r.is_err());
    }

    #[test]
    fn leaves_dollar_without_brace_intact() {
        // `$VAR` (no braces) is NOT substituted, so SQL fragments
        // like `row_filter = "desk = $1"` survive unchanged.
        std::env::remove_var("CQ_TEST_LITERAL");
        let out = substitute_env_vars("hello $WORLD ${CQ_TEST_LITERAL:-x}").unwrap();
        assert_eq!(out, "hello $WORLD x");
    }

    #[test]
    fn resolve_shard_longest_prefix_wins() {
        let cfg = super::ServerConfig {
            shards: vec![
                super::ShardEntry {
                    topic_prefix: "/orders".into(),
                    instance_url: "ws://a:9000/cqp".into(),
                },
                super::ShardEntry {
                    topic_prefix: "/orders/usd".into(),
                    instance_url: "ws://b:9000/cqp".into(),
                },
            ],
            ..super::ServerConfig::default()
        };
        assert_eq!(cfg.resolve_shard("/orders/usd/aapl"), Some("ws://b:9000/cqp"));
        assert_eq!(cfg.resolve_shard("/orders/eur/aapl"), Some("ws://a:9000/cqp"));
        assert_eq!(cfg.resolve_shard("/positions/x"), None);
    }

    #[test]
    fn full_toml_round_trip_with_env_vars() {
        // Demonstrate the production code path: read a TOML with
        // ${VAR} references, substitute, then parse into the real
        // ServerConfig struct.
        std::env::set_var("CQ_TEST_TCP_PORT", "12345");
        std::env::remove_var("CQ_TEST_OPTIONAL");
        let raw = r#"
tcp_addr = "127.0.0.1:${CQ_TEST_TCP_PORT}"
websocket_addr = "127.0.0.1:${CQ_TEST_OPTIONAL:-9008}"
websocket_path = "/cq/json"
"#;
        let expanded = substitute_env_vars(raw).unwrap();
        let cfg: super::ServerConfig = toml::from_str(&expanded).unwrap();
        assert_eq!(cfg.tcp_addr, "127.0.0.1:12345");
        assert_eq!(cfg.websocket_addr, "127.0.0.1:9008");
    }
}

/// D7/P0.7 — `resolve_secret` (env://VAR / file:///path indirection
/// for secret config fields) unit tests. Every env-var test uses a
/// unique `CQ_TEST_SECRET_*` name (env is process-global and tests
/// run in parallel within the same binary) so no test can observe
/// another's `set_var`/`remove_var`.
#[cfg(test)]
mod resolve_secret_tests {
    use super::{resolve_secret, AuthConfig, ConfigError, JwtAlgorithm, JwtConfig, UserConfig};

    #[test]
    fn plaintext_passthrough_unchanged() {
        let out = resolve_secret("$2b$12$abcdefghijklmnopqrstuv").unwrap();
        assert_eq!(out, "$2b$12$abcdefghijklmnopqrstuv");
    }

    #[test]
    fn plaintext_with_no_recognized_scheme_passthrough() {
        // Something that merely contains "://" but isn't env:// or
        // file:// should still pass through verbatim.
        let out = resolve_secret("s3://bucket/secret").unwrap();
        assert_eq!(out, "s3://bucket/secret");
    }

    #[test]
    fn env_scheme_resolves_when_var_present() {
        std::env::set_var("CQ_TEST_SECRET_ENV_PRESENT", "super-secret-value");
        let out = resolve_secret("env://CQ_TEST_SECRET_ENV_PRESENT").unwrap();
        assert_eq!(out, "super-secret-value");
        std::env::remove_var("CQ_TEST_SECRET_ENV_PRESENT");
    }

    #[test]
    fn env_scheme_errors_with_var_name_when_missing() {
        std::env::remove_var("CQ_TEST_SECRET_ENV_MISSING");
        let err = resolve_secret("env://CQ_TEST_SECRET_ENV_MISSING").unwrap_err();
        match &err {
            ConfigError::EnvVarNotSet(var) => {
                assert_eq!(var, "CQ_TEST_SECRET_ENV_MISSING");
            }
            other => panic!("expected EnvVarNotSet, got {other:?}"),
        }
        let msg = err_to_string(&err);
        assert!(
            msg.contains("CQ_TEST_SECRET_ENV_MISSING"),
            "error message should name the missing var: {msg}"
        );
    }

    #[test]
    fn env_scheme_errors_when_var_is_empty() {
        std::env::set_var("CQ_TEST_SECRET_ENV_EMPTY", "");
        let err = resolve_secret("env://CQ_TEST_SECRET_ENV_EMPTY").unwrap_err();
        assert!(matches!(err, ConfigError::EnvVarNotSet(_)));
        std::env::remove_var("CQ_TEST_SECRET_ENV_EMPTY");
    }

    #[test]
    fn file_scheme_resolves_and_trims_trailing_newline() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!(
            "cq_test_secret_{}_{}.txt",
            std::process::id(),
            "file_present"
        ));
        std::fs::write(&path, "file-secret-value\n").expect("write temp secret file");
        let uri = format!("file://{}", path.display());
        let out = resolve_secret(&uri).unwrap();
        assert_eq!(out, "file-secret-value");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn file_scheme_trims_trailing_whitespace_too() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!(
            "cq_test_secret_{}_{}.txt",
            std::process::id(),
            "file_whitespace"
        ));
        std::fs::write(&path, "  file-secret-value  \n\n").expect("write temp secret file");
        let uri = format!("file://{}", path.display());
        let out = resolve_secret(&uri).unwrap();
        // Only trailing whitespace is trimmed, not leading — matches
        // the documented behavior (secrets managers append trailing
        // newlines; leading whitespace in a secret would be unusual
        // enough that silently stripping it is more surprising than
        // helpful).
        assert_eq!(out, "  file-secret-value");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn file_scheme_errors_with_path_when_missing() {
        let path = std::env::temp_dir().join(format!(
            "cq_test_secret_{}_does_not_exist.txt",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        let uri = format!("file://{}", path.display());
        let err = resolve_secret(&uri).unwrap_err();
        match &err {
            ConfigError::FileNotReadable { path: p, .. } => {
                assert_eq!(p, &path.display().to_string());
            }
            other => panic!("expected FileNotReadable, got {other:?}"),
        }
        let msg = err_to_string(&err);
        assert!(
            msg.contains(&path.display().to_string()),
            "error message should name the missing file path: {msg}"
        );
    }

    #[test]
    fn auth_config_resolve_secrets_resolves_jwt_and_password_hashes() {
        std::env::set_var("CQ_TEST_SECRET_AUTHCFG_JWT", "jwt-signing-secret");
        std::env::set_var("CQ_TEST_SECRET_AUTHCFG_PW", "bcrypt-hash-value");

        let mut cfg = AuthConfig {
            required: true,
            users: vec![UserConfig {
                username: "alice".into(),
                password_hash: "env://CQ_TEST_SECRET_AUTHCFG_PW".into(),
                entitlements: Vec::new(),
                row_filter: None,
                query_budget: None,
            }],
            jwt: Some(JwtConfig {
                algorithm: JwtAlgorithm::Hs256,
                secret: Some("env://CQ_TEST_SECRET_AUTHCFG_JWT".into()),
                public_key: None,
                public_key_path: None,
                issuer: None,
                audience: None,
                entitlements_claim: "entitlements".into(),
                username_claim: "sub".into(),
            }),
        };

        cfg.resolve_secrets().expect("resolve secrets");

        assert_eq!(cfg.users[0].password_hash, "bcrypt-hash-value");
        assert_eq!(cfg.jwt.unwrap().secret.unwrap(), "jwt-signing-secret");

        std::env::remove_var("CQ_TEST_SECRET_AUTHCFG_JWT");
        std::env::remove_var("CQ_TEST_SECRET_AUTHCFG_PW");
    }

    #[test]
    fn auth_config_resolve_secrets_surfaces_missing_var_error() {
        std::env::remove_var("CQ_TEST_SECRET_AUTHCFG_MISSING");
        let mut cfg = AuthConfig {
            required: true,
            users: vec![UserConfig {
                username: "bob".into(),
                password_hash: "env://CQ_TEST_SECRET_AUTHCFG_MISSING".into(),
                entitlements: Vec::new(),
                row_filter: None,
                query_budget: None,
            }],
            jwt: None,
        };
        let err = cfg.resolve_secrets().unwrap_err();
        assert!(matches!(err, ConfigError::EnvVarNotSet(_)));
    }

    fn err_to_string(err: &ConfigError) -> String {
        format!("{err}")
    }

    /// D7/P0.7 follow-up — `admin_token` (top-level `ServerConfig`)
    /// and `replication.auth_token` are two more plaintext-secret
    /// fields; without wiring them through `resolve_secret` at
    /// load time, `env://`/`file://` would silently do nothing for
    /// them (the literal string would be used as the token). These
    /// tests exercise the real load path — `load_config_from_with_raw`
    /// against a temp TOML file — rather than calling `resolve_secret`
    /// directly, so they catch a regression if the load-site wiring
    /// is ever removed.
    fn minimal_config_toml(extra: &str) -> String {
        format!(
            "tcp_addr = \"127.0.0.1:0\"\nwebsocket_addr = \"127.0.0.1:0\"\nwebsocket_path = \"/\"\n{extra}\n"
        )
    }

    fn write_temp_config(name: &str, contents: &str) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!(
            "cq_test_config_{}_{}.toml",
            std::process::id(),
            name
        ));
        std::fs::write(&path, contents).expect("write temp config file");
        path
    }

    #[test]
    fn admin_token_env_scheme_resolves_at_load_time() {
        std::env::set_var("CQ_TEST_ADMINTOK_PRESENT", "admin-secret-value");
        let toml = minimal_config_toml("admin_token = \"env://CQ_TEST_ADMINTOK_PRESENT\"\n");
        let path = write_temp_config("admintok_present", &toml);

        let (config, _dir, _raw) =
            super::load_config_from_with_raw(&path).expect("load config with resolved secret");

        assert_eq!(config.admin_token.as_deref(), Some("admin-secret-value"));

        let _ = std::fs::remove_file(&path);
        std::env::remove_var("CQ_TEST_ADMINTOK_PRESENT");
    }

    #[test]
    fn admin_token_env_scheme_missing_var_errors_loudly() {
        std::env::remove_var("CQ_TEST_ADMINTOK_MISSING");
        let toml = minimal_config_toml("admin_token = \"env://CQ_TEST_ADMINTOK_MISSING\"\n");
        let path = write_temp_config("admintok_missing", &toml);

        let err =
            super::load_config_from_with_raw(&path).expect_err("missing env var must error");
        let msg = err.to_string();
        assert!(
            msg.contains("CQ_TEST_ADMINTOK_MISSING"),
            "error should name the missing var: {msg}"
        );

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn admin_token_plaintext_passes_through() {
        let toml = minimal_config_toml("admin_token = \"plain-admin-token\"\n");
        let path = write_temp_config("admintok_plaintext", &toml);

        let (config, _dir, _raw) =
            super::load_config_from_with_raw(&path).expect("load config with plaintext token");

        assert_eq!(config.admin_token.as_deref(), Some("plain-admin-token"));

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn replication_auth_token_env_scheme_resolves_at_load_time() {
        std::env::set_var("CQ_TEST_REPLTOK_PRESENT", "replication-secret-value");
        let toml = minimal_config_toml(
            "[replication]\nauth_token = \"env://CQ_TEST_REPLTOK_PRESENT\"\n",
        );
        let path = write_temp_config("repltok_present", &toml);

        let (config, _dir, _raw) =
            super::load_config_from_with_raw(&path).expect("load config with resolved secret");

        assert_eq!(
            config.replication.auth_token.as_deref(),
            Some("replication-secret-value")
        );

        let _ = std::fs::remove_file(&path);
        std::env::remove_var("CQ_TEST_REPLTOK_PRESENT");
    }

    #[test]
    fn replication_auth_token_plaintext_passes_through() {
        let toml = minimal_config_toml("[replication]\nauth_token = \"plain-repl-token\"\n");
        let path = write_temp_config("repltok_plaintext", &toml);

        let (config, _dir, _raw) =
            super::load_config_from_with_raw(&path).expect("load config with plaintext token");

        assert_eq!(
            config.replication.auth_token.as_deref(),
            Some("plain-repl-token")
        );

        let _ = std::fs::remove_file(&path);
    }
}

/// On-disk shape of the runtime-views file. A plain `[[views]]` array
/// of `ViewEntry`, identical to the `views` section of cqserver.toml,
/// but written/owned by the server (admin-created views) rather than
/// hand-authored. Kept separate so programmatic rewrites never disturb
/// the operator's cqserver.toml.
#[derive(Debug, Default, Deserialize, Serialize)]
pub struct RuntimeViewsFile {
    #[serde(default)]
    pub views: Vec<ViewEntry>,
}

/// Read the runtime-views file. Returns an empty vec if the file is
/// absent or blank (the common first-run case).
pub fn load_runtime_views(
    path: &std::path::Path,
) -> Result<Vec<ViewEntry>, Box<dyn std::error::Error>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let raw = std::fs::read_to_string(path)?;
    if raw.trim().is_empty() {
        return Ok(Vec::new());
    }
    let parsed: RuntimeViewsFile = toml::from_str(&raw)?;
    Ok(parsed.views)
}

/// Append a view to the runtime-views file. Read-modify-write the whole
/// file, then rename a temp file over the target so a crash mid-write
/// never leaves a half-written config.
pub fn persist_runtime_view(
    path: &std::path::Path,
    entry: &ViewEntry,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut existing = load_runtime_views(path)?;
    existing.push(entry.clone());
    let file = RuntimeViewsFile { views: existing };
    let toml_text = toml::to_string_pretty(&file)?;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension("toml.tmp");
    std::fs::write(&tmp, toml_text)?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

/// Remove a view (matched by canonical name) from the runtime-views
/// file. Returns true if an entry was removed; false if the file is
/// absent or the name wasn't present.
pub fn remove_runtime_view(
    path: &std::path::Path,
    name: &str,
) -> Result<bool, Box<dyn std::error::Error>> {
    let mut views = load_runtime_views(path)?;
    let want = cq_core::topic::canonicalize_topic(name);
    let before = views.len();
    views.retain(|v| cq_core::topic::canonicalize_topic(&v.name) != want);
    if views.len() == before {
        return Ok(false);
    }
    let file = RuntimeViewsFile { views };
    let toml_text = toml::to_string_pretty(&file)?;
    let tmp = path.with_extension("toml.tmp");
    std::fs::write(&tmp, toml_text)?;
    std::fs::rename(&tmp, path)?;
    Ok(true)
}

#[cfg(test)]
mod runtime_views_tests {
    use super::*;

    fn temp_path(tag: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "cq_rt_views_{}_{}.toml",
            std::process::id(),
            tag
        ))
    }

    fn sample(name: &str) -> ViewEntry {
        ViewEntry {
            name: name.into(),
            source: "/positions".into(),
            sql: "SELECT sector, COUNT(*) AS n FROM t GROUP BY sector".into(),
            initial_capacity: 10_000,
            tap_capacity: 1024,
        }
    }

    #[test]
    fn load_missing_file_returns_empty() {
        let p = temp_path("missing");
        let _ = std::fs::remove_file(&p);
        let views = load_runtime_views(&p).expect("load");
        assert!(views.is_empty());
    }

    #[test]
    fn persist_then_load_roundtrips() {
        let p = temp_path("roundtrip");
        let _ = std::fs::remove_file(&p);
        persist_runtime_view(&p, &sample("/v_a")).expect("persist a");
        persist_runtime_view(&p, &sample("/v_b")).expect("persist b");
        let views = load_runtime_views(&p).expect("load");
        assert_eq!(views.len(), 2);
        assert_eq!(views[0].name, "/v_a");
        assert_eq!(views[1].name, "/v_b");
        assert_eq!(views[0].source, "/positions");
        let _ = std::fs::remove_file(&p);
    }
}

//! Server configuration loading.

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    pub tcp_addr: String,
    pub websocket_addr: String,
    pub websocket_path: String,
    #[serde(default = "default_admin_addr")]
    pub admin_addr: String,
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
    #[serde(default)]
    pub peer: Option<String>,
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
}

#[derive(Debug, Clone, Copy, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReplicationRole {
    #[default]
    Standalone,
    Primary,
    Standby,
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

/// S16 JWT validator config.
#[derive(Debug, Clone, Deserialize)]
pub struct JwtConfig {
    /// Shared secret used to validate HS256 tokens. Future revisions
    /// may add asymmetric-key support (RS256 etc.); for now we only
    /// support HS256 because it keeps the deployment story to a
    /// single secret value.
    pub secret: String,
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
    "0.0.0.0:8085".into()
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
}

impl Default for TxLogConfig {
    fn default() -> Self {
        TxLogConfig {
            directory: default_txlog_dir(),
            fsync: TxLogFsyncConfig::default(),
            archive_directory: None,
            segment_size: None,
            archive_compress: false,
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum TxLogFsyncConfig {
    #[default]
    None,
    EveryWrite,
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
}

fn default_capacity() -> usize {
    100_000
}

/// S20 materialized-view declaration. A view's `source` must reference
/// another topic in the same config. The `sql` is parsed against the
/// source topic's schema and MUST be an aggregate query
/// (`SELECT ... GROUP BY ...`). The view's schema is derived from the
/// SELECT clause; its key fields default to the `GROUP BY` columns.
#[derive(Debug, Clone, Deserialize)]
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
            admin_addr: default_admin_addr(),
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
                },
            ],
            views: Vec::new(),
            queues: Vec::new(),
            txlog: TxLogConfig::default(),
            auth: AuthConfig::default(),
            replication: ReplicationConfig::default(),
            transport: TransportConfig::default(),
            logging: crate::logging::LoggingConfig::default(),
            shards: Vec::new(),
            query_limits: QueryLimitsConfig::default(),
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
        let raw = std::fs::read_to_string(config_path)?;
        let content = substitute_env_vars(&raw)?;
        let config: ServerConfig = toml::from_str(&content)?;
        let dir = config_path
            .parent()
            .map(|p| p.to_path_buf())
            .unwrap_or_else(|| std::path::PathBuf::from("."));
        Ok((config, dir, content))
    } else {
        tracing::info!("No config file found, using defaults");
        Ok((
            ServerConfig::default(),
            std::path::PathBuf::from("."),
            "# no config/cqserver.toml found; running with defaults\n".to_string(),
        ))
    }
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

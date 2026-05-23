//! End-to-end test harness for cqserver.
//!
//! Spins up a cqserver subprocess against a temp directory + generated
//! config, exposes the URLs back to the test, and shuts the server down
//! when the harness is dropped. Tests use the Rust client SDK to publish,
//! sow, subscribe and assert on real wire-level behaviour.

use serde_json::Value;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::{Duration, Instant};

/// Pick non-overlapping ports per harness instance. We start well above
/// the dev ports (9007 / 9008 / 8085) and bump three at a time so
/// concurrent tests don't collide.
static NEXT_PORT_BASE: AtomicU16 = AtomicU16::new(19000);

/// A running cqserver instance with its ports, config dir, and child.
/// Drop will SIGKILL the child and remove the temp dir.
pub struct ServerHandle {
    pub tcp_port: u16,
    pub ws_port: u16,
    pub admin_port: u16,
    pub config_dir: PathBuf,
    // Optional so `stop_keeping_dir` can move them out and into a
    // `KeptDir` for a later restart on the same on-disk state.
    child: Option<Child>,
    tempdir: Option<tempfile::TempDir>,
}

impl ServerHandle {
    pub fn admin_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.admin_port)
    }
    pub fn tcp_url(&self) -> String {
        format!("tcp://127.0.0.1:{}", self.tcp_port)
    }
    pub fn ws_url(&self) -> String {
        format!("ws://127.0.0.1:{}/cq/json", self.ws_port)
    }

    /// On Unix, deliver SIGTERM to the child and wait for it to exit
    /// within `timeout`. Unlike `Child::kill` (SIGKILL) this triggers
    /// the cqserver graceful-shutdown path that fsyncs txlogs before
    /// exiting. On non-Unix platforms (where SIGTERM has no analog)
    /// falls back to `kill`.
    ///
    /// Returns the child's exit status on a clean shutdown, or
    /// `None` if the timeout elapsed first.
    #[cfg(unix)]
    pub fn send_sigterm_and_wait(&mut self, timeout: Duration) -> Option<std::process::ExitStatus> {
        let child = self.child.as_mut()?;
        let pid = child.id() as libc::pid_t;
        // SAFETY: pid comes from a child we spawned that hasn't been
        // reaped (we still hold `Child`). SIGTERM is a real, safe signal.
        unsafe {
            libc::kill(pid, libc::SIGTERM);
        }
        let started = Instant::now();
        loop {
            match child.try_wait() {
                Ok(Some(status)) => return Some(status),
                Ok(None) => {}
                Err(_) => return None,
            }
            if started.elapsed() > timeout {
                return None;
            }
            std::thread::sleep(Duration::from_millis(40));
        }
    }
    #[cfg(not(unix))]
    pub fn send_sigterm_and_wait(&mut self, _timeout: Duration) -> Option<std::process::ExitStatus> {
        let child = self.child.as_mut()?;
        let _ = child.kill();
        child.wait().ok()
    }
}

impl Drop for ServerHandle {
    fn drop(&mut self) {
        if let Some(child) = self.child.as_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

/// Build a `[transport]` section + `[[topics]]` entries from this spec,
/// drop the schema JSON files onto disk, and spawn the server.
pub struct TopicSpec {
    pub name: String,
    pub key: Vec<String>,
    pub persist: bool,
    pub conflation_ms: Option<u64>,
    pub initial_capacity: usize,
    /// External schema file contents. Will be written to
    /// `{config_dir}/schemas/{topic_name}.json` and referenced from the
    /// topic's `schema_file` setting.
    pub schema_json: Option<Value>,
    /// Inline columns — alternative to `schema_json`. List of
    /// `(name, type)` pairs where type is one of
    /// "string" | "int" | "long" | "double" | "float" | "integer".
    pub inline_columns: Vec<(String, String)>,
    /// Columns to maintain a secondary equality index on. Tests
    /// that exercise the index acceleration path pass this; tests
    /// that don't leave it empty.
    pub index_columns: Vec<String>,
    /// Per-row TTL in seconds. `None` = no TTL.
    pub expire_seconds: Option<u64>,
}

impl TopicSpec {
    pub fn new(name: impl Into<String>, key: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            key: vec![key.into()],
            persist: false,
            conflation_ms: None,
            initial_capacity: 10_000,
            schema_json: None,
            inline_columns: Vec::new(),
            index_columns: Vec::new(),
            expire_seconds: None,
        }
    }

    /// TTL in seconds for SOW rows.
    pub fn with_expire_seconds(mut self, s: u64) -> Self {
        self.expire_seconds = Some(s);
        self
    }

    /// Maintain a secondary equality index on the given column names.
    pub fn with_index_columns<I, S>(mut self, columns: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.index_columns = columns.into_iter().map(Into::into).collect();
        self
    }
    pub fn with_schema(mut self, schema: Value) -> Self {
        self.schema_json = Some(schema);
        self
    }
    pub fn with_inline_columns<I, S, T>(mut self, columns: I) -> Self
    where
        I: IntoIterator<Item = (S, T)>,
        S: Into<String>,
        T: Into<String>,
    {
        self.inline_columns = columns.into_iter().map(|(n, t)| (n.into(), t.into())).collect();
        self
    }
    pub fn with_conflation(mut self, ms: u64) -> Self {
        self.conflation_ms = Some(ms);
        self
    }
    pub fn with_persist(mut self) -> Self {
        self.persist = true;
        self
    }
}

/// Tunable parameters for a server instance. Defaults match the demo
/// config.
#[derive(Clone)]
pub struct ServerOpts {
    pub outbound_queue_capacity: usize,
    /// When `Some`, enables the slow-consumer watcher with this config.
    pub slow_consumer: Option<SlowConsumerOpts>,
    /// When `Some`, the TCP transport is served over TLS. The harness
    /// generates a self-signed cert+key for the SAN in `TlsOpts.san`.
    pub tls: Option<TlsOpts>,
    /// Queue declarations to add to the config (in addition to
    /// `topics`). Empty by default — most tests don't need queues.
    pub queues: Vec<QueueSpec>,
    /// Auth + entitlements. When `Some(_)`, `auth.required = true`
    /// is set in the config and every command (except logon /
    /// heartbeat) must be authenticated.
    pub auth: Option<AuthOpts>,
    /// When `Some(_)`, sealed txlog segments are moved here under
    /// per-topic subdirectories. The harness creates the root
    /// directory inside the test's tempdir.
    pub txlog_archive: Option<TxLogArchiveOpts>,
}

#[derive(Clone)]
pub struct TxLogArchiveOpts {
    /// Segment size in bytes. Tests use a tiny value so rotation
    /// fires after only a few publishes.
    pub segment_size: u64,
    /// Compress sealed segments with zstd. Only meaningful with the
    /// archive path enabled (the harness always sets one).
    pub compress: bool,
}

impl TxLogArchiveOpts {
    pub fn new(segment_size: u64) -> Self {
        Self {
            segment_size,
            compress: false,
        }
    }
    pub fn with_compression(mut self) -> Self {
        self.compress = true;
        self
    }
}

#[derive(Clone)]
pub struct AuthOpts {
    pub users: Vec<UserSpec>,
}

#[derive(Clone)]
pub struct UserSpec {
    pub username: String,
    pub password_hash: String,
    pub entitlements: Vec<String>,
    pub row_filter: Option<String>,
}

/// Lightweight queue spec used by the e2e harness.
#[derive(Clone)]
pub struct QueueSpec {
    pub name: String,
    pub lease_ms: Option<u64>,
    pub max_delivery_count: Option<u32>,
    pub dlq: Option<String>,
}

impl QueueSpec {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            lease_ms: None,
            max_delivery_count: None,
            dlq: None,
        }
    }
    pub fn with_lease(mut self, ms: u64) -> Self {
        self.lease_ms = Some(ms);
        self
    }
    pub fn with_max_delivery(mut self, n: u32) -> Self {
        self.max_delivery_count = Some(n);
        self
    }
    pub fn with_dlq(mut self, dlq_name: impl Into<String>) -> Self {
        self.dlq = Some(dlq_name.into());
        self
    }
}

#[derive(Clone, Copy)]
pub struct SlowConsumerOpts {
    pub scan_interval_s: u64,
    pub drops_per_sec_threshold: u64,
    pub auto_disconnect: bool,
    pub adaptive_conflation: bool,
    pub adaptive_max_interval_ms: u64,
    pub adaptive_fill_threshold: f32,
}

impl Default for SlowConsumerOpts {
    fn default() -> Self {
        Self {
            scan_interval_s: 5,
            drops_per_sec_threshold: 500,
            auto_disconnect: false,
            adaptive_conflation: false,
            adaptive_max_interval_ms: 5_000,
            adaptive_fill_threshold: 0.5,
        }
    }
}

/// TLS config for the test server. When set, the harness generates a
/// self-signed cert+key for `127.0.0.1` at test time and points the
/// server at them. Tests use [`tls_client_config_dangerous_no_verify`]
/// to connect.
#[derive(Clone)]
pub struct TlsOpts {
    /// Subject Alternative Name to issue the cert for. Defaults to
    /// `127.0.0.1` so localhost-port URLs validate by SAN, not by
    /// hostname mismatch tolerance.
    pub san: String,
}

impl Default for TlsOpts {
    fn default() -> Self {
        Self {
            san: "127.0.0.1".into(),
        }
    }
}

impl Default for ServerOpts {
    fn default() -> Self {
        Self {
            outbound_queue_capacity: 16384,
            slow_consumer: None,
            tls: None,
            queues: Vec::new(),
            auth: None,
            txlog_archive: None,
        }
    }
}

/// Build + spawn a fresh server with default ServerOpts.
pub async fn start_server(topics: Vec<TopicSpec>) -> ServerHandle {
    start_server_with(topics, ServerOpts::default()).await
}

/// Build + spawn a fresh server. Awaits `/healthz` before returning.
pub async fn start_server_with(
    topics: Vec<TopicSpec>,
    opts: ServerOpts,
) -> ServerHandle {
    let port_base = NEXT_PORT_BASE.fetch_add(3, Ordering::SeqCst);
    let tcp_port = port_base;
    let ws_port = port_base + 1;
    let admin_port = port_base + 2;

    let tempdir = tempfile::tempdir().expect("tempdir");
    let root = tempdir.path().to_path_buf();
    let config_dir = root.join("config");
    let schemas_dir = config_dir.join("schemas");
    std::fs::create_dir_all(&schemas_dir).expect("mkdir schemas");
    let txlog_dir = root.join("txlog");
    std::fs::create_dir_all(&txlog_dir).expect("mkdir txlog");

    // If TLS is requested, generate a self-signed cert+key for the SAN
    // and stash the paths in the config dir so `build_toml` can embed
    // them under [transport.tls].
    let tls_paths = opts.tls.as_ref().map(|t| {
        let cert = config_dir.join("test-tls.crt");
        let key = config_dir.join("test-tls.key");
        let mut params = rcgen::CertificateParams::new(vec![t.san.clone()]).expect("rcgen params");
        params.distinguished_name.push(
            rcgen::DnType::CommonName,
            t.san.clone(),
        );
        let key_pair = rcgen::KeyPair::generate().expect("keypair");
        let cert_obj = params.self_signed(&key_pair).expect("self-signed cert");
        std::fs::write(&cert, cert_obj.pem()).expect("write cert");
        std::fs::write(&key, key_pair.serialize_pem()).expect("write key");
        (cert, key)
    });

    let toml = build_toml(
        tcp_port,
        ws_port,
        admin_port,
        &txlog_dir,
        &topics,
        &schemas_dir,
        &opts,
        tls_paths.as_ref(),
    );
    let toml_path = config_dir.join("cqserver.toml");
    std::fs::write(&toml_path, toml).expect("write toml");

    spawn_server(root, config_dir, tempdir, tcp_port, ws_port, admin_port).await
}

/// Stop the server but keep its config dir and txlog. Useful for
/// recovery / restart tests.
pub async fn stop_keeping_dir(mut handle: ServerHandle) -> KeptDir {
    if let Some(mut child) = handle.child.take() {
        let _ = child.kill();
        let _ = child.wait();
    }
    let tempdir = handle.tempdir.take().expect("tempdir already taken");
    let root = handle.config_dir.parent().unwrap().to_path_buf();
    KeptDir {
        tempdir,
        config_dir: handle.config_dir.clone(),
        root,
        tcp_port: handle.tcp_port,
        ws_port: handle.ws_port,
        admin_port: handle.admin_port,
    }
}

/// Restart a server against a previously-kept config / txlog. Returns
/// a fresh ServerHandle on the same ports.
pub async fn restart_kept(kept: KeptDir) -> ServerHandle {
    let KeptDir { tempdir, config_dir, root, tcp_port, ws_port, admin_port } = kept;
    // Give the OS a beat to free the listening socket.
    tokio::time::sleep(Duration::from_millis(120)).await;
    spawn_server(root, config_dir, tempdir, tcp_port, ws_port, admin_port).await
}

async fn spawn_server(
    root: PathBuf,
    config_dir: PathBuf,
    tempdir: tempfile::TempDir,
    tcp_port: u16,
    ws_port: u16,
    admin_port: u16,
) -> ServerHandle {
    let binary = locate_server_binary();
    let mut cmd = Command::new(&binary);
    let log_level = std::env::var("CQ_E2E_LOG").unwrap_or_else(|_| "warn".into());
    let inherit_stderr = std::env::var("CQ_E2E_STDERR").is_ok();
    cmd.current_dir(&root)
        .env("RUST_LOG", log_level)
        .stdout(Stdio::null());
    if inherit_stderr {
        cmd.stderr(Stdio::inherit());
    } else {
        cmd.stderr(Stdio::piped());
    }
    let child = cmd.spawn().expect("spawn cqserver");

    let handle = ServerHandle {
        tcp_port,
        ws_port,
        admin_port,
        config_dir,
        child: Some(child),
        tempdir: Some(tempdir),
    };

    wait_for_healthz(&handle).await;
    handle
}

/// Server stopped but the on-disk state is preserved for a subsequent
/// `restart_kept` call. The temp dir is moved here so it isn't deleted
/// when the original ServerHandle is dropped.
pub struct KeptDir {
    tempdir: tempfile::TempDir,
    config_dir: PathBuf,
    root: PathBuf,
    tcp_port: u16,
    ws_port: u16,
    admin_port: u16,
}

fn build_toml(
    tcp_port: u16,
    ws_port: u16,
    admin_port: u16,
    txlog_dir: &Path,
    topics: &[TopicSpec],
    schemas_dir: &Path,
    opts: &ServerOpts,
    tls_paths: Option<&(PathBuf, PathBuf)>,
) -> String {
    let mut out = String::new();
    use std::fmt::Write as _;
    writeln!(out, r#"tcp_addr = "127.0.0.1:{tcp_port}""#).unwrap();
    writeln!(out, r#"websocket_addr = "127.0.0.1:{ws_port}""#).unwrap();
    writeln!(out, r#"websocket_path = "/cq/json""#).unwrap();
    writeln!(out, r#"admin_addr = "127.0.0.1:{admin_port}""#).unwrap();
    writeln!(out, "heartbeat_interval_s = 60").unwrap();
    writeln!(out, "heartbeat_idle_timeout_s = 600").unwrap();
    writeln!(out).unwrap();
    writeln!(out, "[transport]").unwrap();
    writeln!(out, "outbound_queue_capacity = {}", opts.outbound_queue_capacity).unwrap();
    if let Some(sc) = opts.slow_consumer {
        writeln!(out, "[transport.slow_consumer]").unwrap();
        writeln!(out, "scan_interval_s = {}", sc.scan_interval_s).unwrap();
        writeln!(
            out,
            "drops_per_sec_threshold = {}",
            sc.drops_per_sec_threshold
        )
        .unwrap();
        writeln!(out, "auto_disconnect = {}", sc.auto_disconnect).unwrap();
        writeln!(out, "adaptive_conflation = {}", sc.adaptive_conflation).unwrap();
        writeln!(
            out,
            "adaptive_max_interval_ms = {}",
            sc.adaptive_max_interval_ms
        )
        .unwrap();
        writeln!(
            out,
            "adaptive_fill_threshold = {}",
            sc.adaptive_fill_threshold
        )
        .unwrap();
    }
    if let Some((cert_path, key_path)) = tls_paths {
        writeln!(out, "[transport.tls]").unwrap();
        writeln!(
            out,
            r#"cert_file = "{}""#,
            cert_path.display().to_string().replace('\\', "/")
        )
        .unwrap();
        writeln!(
            out,
            r#"key_file = "{}""#,
            key_path.display().to_string().replace('\\', "/")
        )
        .unwrap();
    }
    writeln!(out).unwrap();
    writeln!(out, "[txlog]").unwrap();
    writeln!(
        out,
        r#"directory = "{}""#,
        txlog_dir.display().to_string().replace('\\', "/")
    )
    .unwrap();
    if let Some(archive) = &opts.txlog_archive {
        // The archive dir lives in the same root as the live dir.
        let archive_dir = txlog_dir
            .parent()
            .unwrap_or(txlog_dir)
            .join("txlog_archive");
        writeln!(
            out,
            r#"archive_directory = "{}""#,
            archive_dir.display().to_string().replace('\\', "/")
        )
        .unwrap();
        writeln!(out, "segment_size = {}", archive.segment_size).unwrap();
        if archive.compress {
            writeln!(out, "archive_compress = true").unwrap();
        }
    }
    writeln!(out).unwrap();

    for spec in topics {
        writeln!(out, "[[topics]]").unwrap();
        writeln!(out, r#"name = "{}""#, spec.name).unwrap();
        let keys: Vec<String> = spec.key.iter().map(|k| format!("\"{k}\"")).collect();
        writeln!(out, "key = [{}]", keys.join(", ")).unwrap();
        writeln!(out, "persist = {}", spec.persist).unwrap();
        if let Some(ms) = spec.conflation_ms {
            writeln!(out, "conflation_ms = {ms}").unwrap();
        }
        writeln!(out, "initial_capacity = {}", spec.initial_capacity).unwrap();
        if let Some(schema_value) = &spec.schema_json {
            let safe_name = spec.name.trim_start_matches('/').replace('/', "_");
            let file_path = schemas_dir.join(format!("{safe_name}.json"));
            std::fs::write(&file_path, serde_json::to_string_pretty(schema_value).unwrap())
                .expect("write schema");
            // schema_file is relative to config dir (parent of schemas/).
            writeln!(out, r#"schema_file = "schemas/{safe_name}.json""#).unwrap();
        } else if !spec.inline_columns.is_empty() {
            // Render inline `columns = [...]` form. The server parses
            // the same field shape as the inline table-of-tables, but
            // an inline array is more compact for tests.
            write!(out, "columns = [").unwrap();
            let mut first = true;
            for (name, ty) in &spec.inline_columns {
                if !first {
                    write!(out, ", ").unwrap();
                }
                first = false;
                write!(out, r#"{{ name = "{name}", type = "{ty}" }}"#).unwrap();
            }
            writeln!(out, "]").unwrap();
        }
        if !spec.index_columns.is_empty() {
            let cols: Vec<String> = spec
                .index_columns
                .iter()
                .map(|c| format!("\"{c}\""))
                .collect();
            writeln!(out, "index_columns = [{}]", cols.join(", ")).unwrap();
        }
        if let Some(ttl) = spec.expire_seconds {
            writeln!(out, "expire_seconds = {ttl}").unwrap();
        }
        writeln!(out).unwrap();
    }

    // [auth] section + [[auth.users]] entries when configured.
    if let Some(auth_opts) = &opts.auth {
        writeln!(out, "[auth]").unwrap();
        writeln!(out, "required = true").unwrap();
        for u in &auth_opts.users {
            writeln!(out, "[[auth.users]]").unwrap();
            writeln!(out, r#"username = "{}""#, u.username).unwrap();
            writeln!(out, r#"password_hash = "{}""#, u.password_hash).unwrap();
            if !u.entitlements.is_empty() {
                let es: Vec<String> = u
                    .entitlements
                    .iter()
                    .map(|e| format!("\"{e}\""))
                    .collect();
                writeln!(out, "entitlements = [{}]", es.join(", ")).unwrap();
            }
            if let Some(rf) = &u.row_filter {
                // SQL row filters often contain single-quoted string
                // literals (`desk = 'RATES'`). TOML basic strings
                // (double-quoted) don't process single quotes
                // specially, so embed verbatim. We do still escape
                // backslashes and double-quotes since TOML basic
                // strings interpret those.
                let escaped = rf.replace('\\', "\\\\").replace('"', "\\\"");
                writeln!(out, r#"row_filter = "{escaped}""#).unwrap();
            }
            writeln!(out).unwrap();
        }
    }

    // [[queues]] entries — only emitted when the caller actually
    // configured queues. Keeps the default TOML clean.
    for q in &opts.queues {
        writeln!(out, "[[queues]]").unwrap();
        writeln!(out, r#"name = "{}""#, q.name).unwrap();
        if let Some(ms) = q.lease_ms {
            writeln!(out, "lease_ms = {ms}").unwrap();
        }
        if let Some(n) = q.max_delivery_count {
            writeln!(out, "max_delivery_count = {n}").unwrap();
        }
        if let Some(dlq) = &q.dlq {
            writeln!(out, r#"dlq = "{dlq}""#).unwrap();
        }
        writeln!(out).unwrap();
    }
    out
}

fn locate_server_binary() -> PathBuf {
    // CARGO_BIN_EXE_cqserver is set when this crate depends on cq-server
    // as a `dev-dependencies` binary, but we don't want that coupling so
    // we just look in the workspace target directory. Tests must be run
    // after `cargo build --release -p cq-server`.
    let workspace_root = workspace_root();
    let release = workspace_root.join("target/release/cqserver");
    let debug = workspace_root.join("target/debug/cqserver");
    if release.exists() {
        release
    } else if debug.exists() {
        debug
    } else {
        panic!(
            "cqserver binary not found; run `cargo build --release -p cq-server` first \
             (looked at {} and {})",
            release.display(),
            debug.display()
        );
    }
}

fn workspace_root() -> PathBuf {
    // Walk up from this file until we find the workspace Cargo.toml.
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    loop {
        if p.join("Cargo.toml").exists()
            && std::fs::read_to_string(p.join("Cargo.toml"))
                .map(|s| s.contains("[workspace]"))
                .unwrap_or(false)
        {
            return p;
        }
        if !p.pop() {
            panic!("workspace root not found");
        }
    }
}

async fn wait_for_healthz(h: &ServerHandle) {
    let url = format!("{}/healthz", h.admin_url());
    let deadline = Instant::now() + Duration::from_secs(15);
    let client = reqwest::Client::new();
    loop {
        if let Ok(resp) = client.get(&url).send().await {
            if resp.status().is_success() {
                return;
            }
        }
        if Instant::now() > deadline {
            panic!("cqserver did not become ready at {url}");
        }
        tokio::time::sleep(Duration::from_millis(80)).await;
    }
}

/// Helper: read `/topics` admin endpoint and locate a topic's stats.
pub async fn topic_stats(h: &ServerHandle, name: &str) -> Option<Value> {
    let url = format!("{}/topics", h.admin_url());
    let resp = reqwest::get(&url).await.ok()?.json::<Value>().await.ok()?;
    resp.as_array()?
        .iter()
        .find(|t| t.get("name").and_then(|v| v.as_str()) == Some(name))
        .cloned()
}

/// Generator for a deeply-nested 300+ field schema. Used by the
/// "wide-row" e2e tests.
pub fn build_wide_schema() -> Value {
    use serde_json::json;
    let mut root = serde_json::Map::new();
    root.insert("positionKey".into(), json!("string"));
    root.insert("book".into(), json!("string"));
    root.insert("trader".into(), json!("string"));

    // instrument.*
    let mut instrument = serde_json::Map::new();
    for (name, ty) in [
        ("cusip", "string"),
        ("ticker", "string"),
        ("issuer", "string"),
        ("assetClass", "string"),
        ("sector", "string"),
        ("subSector", "string"),
        ("rating", "string"),
        ("currency", "string"),
        ("country", "string"),
        ("exchange", "string"),
        ("couponPct", "double"),
        ("maturity", "string"),
        ("callable", "string"),
        ("putable", "string"),
        ("seniority", "string"),
    ] {
        instrument.insert(name.into(), json!(ty));
    }
    root.insert("instrument".into(), Value::Object(instrument));

    // position.*
    let mut position = serde_json::Map::new();
    for n in [
        "netQty",
        "longQty",
        "shortQty",
        "avgCost",
        "lastMid",
        "lastBid",
        "lastAsk",
        "marketValue",
        "unrealizedPnl",
        "realizedPnl",
        "dayPnl",
        "mtdPnl",
        "ytdPnl",
        "carry",
        "rollDown",
    ] {
        position.insert(n.into(), json!("double"));
    }
    position.insert("tradeCount".into(), json!("long"));
    position.insert("lastTradeTs".into(), json!("string"));
    root.insert("position".into(), Value::Object(position));

    // risk.* — duration / convexity / DV01 across many curves
    let mut risk = serde_json::Map::new();
    for n in [
        "duration",
        "modifiedDuration",
        "effectiveDuration",
        "convexity",
        "modConvexity",
        "ytm",
        "ytw",
        "ytc",
        "ytp",
        "spreadDv01",
        "creditDv01",
        "rateDv01",
        "asw",
        "zspread",
        "oasSpread",
        "iSpread",
        "swapSpread",
        "treasurySpread",
        "benchmarkSpread",
        "currentYield",
    ] {
        risk.insert(n.into(), json!("double"));
    }
    // Per-currency ir01s
    for ccy in [
        "USD", "EUR", "GBP", "JPY", "CHF", "CAD", "AUD", "NZD", "SEK", "NOK", "DKK", "CNY",
    ] {
        risk.insert(format!("ir01_{ccy}"), json!("double"));
    }
    // Tenor-bucketed key rates
    for tenor in [
        "3m", "6m", "1y", "2y", "3y", "5y", "7y", "10y", "15y", "20y", "30y",
    ] {
        risk.insert(format!("kr01_{tenor}"), json!("double"));
    }
    root.insert("risk".into(), Value::Object(risk));

    // greeks.* (for option-ish things; mostly zero for cash bonds)
    let mut greeks = serde_json::Map::new();
    for n in ["delta", "gamma", "vega", "theta", "rho", "vanna", "volga"] {
        greeks.insert(n.into(), json!("double"));
    }
    root.insert("greeks".into(), Value::Object(greeks));

    // var.*
    let mut var = serde_json::Map::new();
    for horizon in ["1d", "5d", "10d", "20d"] {
        for ci in ["95", "99", "99_5"] {
            var.insert(format!("var_{horizon}_{ci}"), json!("double"));
            var.insert(format!("es_{horizon}_{ci}"), json!("double"));
        }
    }
    root.insert("var".into(), Value::Object(var));

    // stress.parallel_*, stress.curve_*, stress.credit_*
    let mut stress = serde_json::Map::new();
    for shift in ["10bp", "25bp", "50bp", "100bp", "200bp"] {
        for dir in ["up", "down"] {
            stress.insert(format!("parallel_{dir}_{shift}"), json!("double"));
            stress.insert(format!("credit_{dir}_{shift}"), json!("double"));
        }
    }
    for tenor in ["2y", "5y", "10y", "30y"] {
        stress.insert(format!("steepener_{tenor}"), json!("double"));
        stress.insert(format!("flattener_{tenor}"), json!("double"));
    }
    root.insert("stress".into(), Value::Object(stress));

    // limits.*
    let mut limits = serde_json::Map::new();
    for n in [
        "grossLimit",
        "netLimit",
        "dv01Limit",
        "creditLimit",
        "varLimit",
        "concentrationLimit",
    ] {
        limits.insert(n.into(), json!("double"));
        limits.insert(format!("{n}Used"), json!("double"));
        limits.insert(format!("{n}Pct"), json!("double"));
    }
    root.insert("limits".into(), Value::Object(limits));

    // attribution.* — daily/mtd/ytd × component
    let mut attribution = serde_json::Map::new();
    for n in [
        "carry",
        "rollDown",
        "rates",
        "spread",
        "fx",
        "default",
        "convexity",
        "residual",
    ] {
        attribution.insert(format!("daily_{n}"), json!("double"));
        attribution.insert(format!("mtd_{n}"), json!("double"));
        attribution.insert(format!("ytd_{n}"), json!("double"));
    }
    root.insert("attribution".into(), Value::Object(attribution));

    // exposures.* — net by sector, by rating, by maturity bucket
    let mut exposures = serde_json::Map::new();
    for s in [
        "Treasury",
        "Tech",
        "Banks",
        "Energy",
        "Pharma",
        "Telecom",
        "Media",
        "Auto",
        "Utilities",
        "REIT",
        "Industrial",
        "Consumer",
        "Agency",
        "Muni",
    ] {
        exposures.insert(format!("sector_{s}"), json!("double"));
    }
    for r in ["AAA", "AA", "A", "BBB", "BB", "B", "CCC", "D", "NR"] {
        exposures.insert(format!("rating_{r}"), json!("double"));
    }
    for b in ["0_1y", "1_3y", "3_5y", "5_7y", "7_10y", "10_20y", "20_30y", "30y_plus"] {
        exposures.insert(format!("maturity_{b}"), json!("double"));
    }
    root.insert("exposures".into(), Value::Object(exposures));

    // liquidity.*
    let mut liquidity = serde_json::Map::new();
    for n in [
        "adv30d",
        "adv90d",
        "lastTradeMinsAgo",
        "bidAskBps",
        "ttl",
        "isOnTheRun",
        "issueSize",
        "freeFloat",
        "amountOutstanding",
    ] {
        liquidity.insert(n.into(), json!("double"));
    }
    root.insert("liquidity".into(), Value::Object(liquidity));

    // counterparty.* and trade-allocation by counterparty
    let mut counterparty = serde_json::Map::new();
    for cp in [
        "CITI", "GS", "JPM", "MS", "BAC", "BARC", "DB", "UBS", "HSBC", "NOMURA", "RBC", "CS",
    ] {
        counterparty.insert(format!("exposure_{cp}"), json!("double"));
        counterparty.insert(format!("count_{cp}"), json!("long"));
    }
    root.insert("counterparty".into(), Value::Object(counterparty));

    // collateral.*
    let mut collateral = serde_json::Map::new();
    for n in [
        "posted",
        "received",
        "haircut",
        "vmThreshold",
        "imRequired",
        "imPosted",
        "imShortfall",
    ] {
        collateral.insert(n.into(), json!("double"));
    }
    root.insert("collateral".into(), Value::Object(collateral));

    // valuation.* — alternate marks
    let mut valuation = serde_json::Map::new();
    for src in [
        "bloomberg",
        "refinitiv",
        "ice",
        "marketAxess",
        "tradeweb",
        "internal",
        "consensus",
    ] {
        valuation.insert(format!("mid_{src}"), json!("double"));
        valuation.insert(format!("ageMins_{src}"), json!("long"));
    }
    root.insert("valuation".into(), Value::Object(valuation));

    // pnl_attribution.* — additional finer breakdown
    let mut pnl_attr = serde_json::Map::new();
    for src in ["carry", "rollDown", "curve", "spread", "fx", "convexity"] {
        for horizon in ["t1", "t5", "wtd", "mtd", "qtd", "ytd"] {
            pnl_attr.insert(format!("{src}_{horizon}"), json!("double"));
        }
    }
    root.insert("pnl_attribution".into(), Value::Object(pnl_attr));

    Value::Object(root)
}

/// Count the leaves in a nested schema JSON (i.e. the number of flat
/// columns that schema will produce after server-side flattening).
pub fn count_leaves(v: &Value) -> usize {
    match v {
        Value::Object(map) => map.values().map(count_leaves).sum(),
        _ => 1,
    }
}

/// Tiny shim: write something to a debug log file for tests that want
/// to capture state. Not strictly required, but useful when a test
/// flakes and you want to see what the harness saw.
#[allow(dead_code)]
fn write_debug(path: &Path, content: &str) {
    if let Ok(mut f) = std::fs::File::create(path) {
        let _ = f.write_all(content.as_bytes());
    }
}

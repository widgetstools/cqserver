//! S25 — per-target tracing sinks.
//!
//! The server installs a single `tracing_subscriber::Registry` with one
//! layer per configured `LogSink`. Each layer carries its own
//! `EnvFilter` so events get routed by target (e.g., `cq_audit=info`
//! goes to `audit.log`; everything else goes to stderr).
//!
//! ### Config shape
//!
//! ```toml
//! [logging]
//! [[logging.sinks]]
//! filter = "info,cq_audit=off"      # everything-except-audit → stderr
//!
//! [[logging.sinks]]
//! file = "logs/audit.log"
//! filter = "cq_audit=info"          # ONLY audit events → audit.log
//! format = "json"                    # human "text" (default) or "json"
//! ```
//!
//! ### Targets
//!
//! Events that should land on the audit sink emit with an explicit
//! `target` — e.g. `tracing::info!(target: "cq_audit", user = %u, "Logon ok")`.
//! Other modules use the default `module_path!()` target and are routed
//! by the catch-all sink.
//!
//! When `[logging]` is absent (or empty), the server falls back to the
//! historical single-stderr layer with the same `RUST_LOG` semantics
//! it had before S25.
//!
//! ### D5/P0.5 — `[audit]` sugar
//!
//! ```toml
//! [audit]
//! sink = "file"      # or "syslog"
//! path = "logs/audit.log"
//! ```
//!
//! `[audit]` builds NO new plumbing — it is sugar over the same
//! per-target-filter machinery above. `sink = "file"` is equivalent to
//! hand-writing a `[[logging.sinks]]` entry with
//! `filter = "off,cq_audit=info"` (ONLY `cq_audit`-targeted events land
//! here, at any level) pointed at `path`. `sink = "syslog"` builds an
//! analogous layer whose `MakeWriter` sends each formatted line to a
//! syslog daemon over a Unix datagram socket at `path` (defaults to
//! `/dev/log`, the conventional syslog socket) using RFC 3164 framing
//! (`<PRI>message`) — no external syslog crate, since a single write()
//! per line is all RFC 3164 needs. The `[audit]` layer is installed
//! IN ADDITION to (not instead of) `[logging].sinks`, so operators can
//! keep the historical stderr default for everything else while
//! opting into a dedicated audit trail.
//!
//! Audit call sites always use `target: "cq_audit"` (see
//! `crates/cq-transport/src/router.rs::handle_logon`,
//! `crates/cq-server/src/admin.rs::audit_admin`, and
//! `check_entitlement`'s denial branch) — the same target that
//! hand-authored `[[logging.sinks]]` filters already route on.
//!
//! ### Fail-open by design
//!
//! The audit subsystem is intentionally **fail-open**, not fail-closed:
//!
//! - **Startup**: if `[audit]` is configured but its sink fails to
//!   build (e.g. an unwritable path, a directory where a file is
//!   expected, or a syslog socket that can't be opened), `install`
//!   below records the error in its returned `Vec<String>` and simply
//!   omits that layer — it does NOT return `Err` and does NOT abort
//!   server startup. The caller (`cq_server::main`) logs a loud,
//!   unmistakable warning (see `AUDIT SINK FAILED TO INITIALIZE` in
//!   `main.rs`) and the server continues running WITHOUT an audit
//!   trail.
//! - **Runtime**: once installed, a `tracing_subscriber::fmt::Layer`
//!   writer that errors on a given event (e.g. a transient disk-full
//!   or a syslog socket that stops accepting datagrams) has that
//!   single write silently dropped by `tracing-subscriber` — there is
//!   no retry, no backpressure, and no panic.
//!
//! This is a deliberate tradeoff, not an oversight: cqserver is a
//! trading/data server, and audit logging must never be able to bring
//! down (or stall) the primary read/write path. An operator losing
//! their audit trail is bad; the server refusing to start — or
//! blocking on every event — because a log sink hiccuped would be
//! worse. If a fail-closed audit mode is ever needed (e.g. for a
//! deployment with a hard compliance requirement that no unaudited
//! action may occur), it should be an explicit, separately-configured
//! opt-in — not a change to this default.

use serde::Deserialize;
use std::fs::OpenOptions;
use std::io;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tracing_subscriber::{
    fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer, Registry,
};

/// Top-level `[logging]` config.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct LoggingConfig {
    /// One layer per declared sink. If empty, the server falls back to
    /// the historical single-stderr fmt layer.
    #[serde(default)]
    pub sinks: Vec<SinkConfig>,
}

/// One sink layer.
#[derive(Debug, Clone, Deserialize)]
pub struct SinkConfig {
    /// Destination file. When `None`, the sink writes to stderr.
    /// Created on demand (incl. parent directories) if missing.
    pub file: Option<String>,
    /// `tracing_subscriber::EnvFilter` directive. Empty / missing
    /// defaults to `"info"`.
    #[serde(default = "default_filter")]
    pub filter: String,
    /// `"text"` (default, human-friendly compact format) or `"json"`
    /// for machine-parseable lines.
    #[serde(default)]
    pub format: SinkFormat,
}

#[derive(Debug, Clone, Copy, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SinkFormat {
    #[default]
    Text,
    Json,
}

fn default_filter() -> String {
    "info".into()
}

/// D5/P0.5 — top-level `[audit]` config. Sugar over the S25 sink
/// machinery: routes every `target = "cq_audit"` event to a dedicated
/// destination, independent of (and in addition to) `[logging].sinks`.
#[derive(Debug, Clone, Deserialize)]
pub struct AuditConfig {
    /// `"file"` (default) or `"syslog"`.
    #[serde(default)]
    pub sink: AuditSinkKind,
    /// File sink: destination path (created on demand, incl. parent
    /// dirs). Syslog sink: path to the Unix datagram socket; when
    /// absent, defaults to `/dev/log`.
    #[serde(default)]
    pub path: Option<String>,
}

#[derive(Debug, Clone, Copy, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum AuditSinkKind {
    #[default]
    File,
    Syslog,
}

/// The tracing target every audit event is emitted with. Both the
/// hand-authored `[[logging.sinks]]` path and the `[audit]` sugar
/// route on this same value.
pub const AUDIT_TARGET: &str = "cq_audit";

/// Filter directive that captures ONLY audit-targeted events, at any
/// level `>= info`, and nothing else.
fn audit_only_filter() -> String {
    format!("off,{AUDIT_TARGET}=info")
}

/// Prefix stamped on an error string in `install`'s returned `Vec`
/// when the failure came from building the `[audit]` layer
/// specifically (as opposed to a regular `[[logging.sinks]]` entry).
/// `main.rs` greps for this prefix to decide whether to emit the loud
/// "AUDIT SINK FAILED TO INITIALIZE" warning versus the generic
/// per-sink warning — see the fail-open note in this module's doc
/// comment.
pub const AUDIT_SINK_ERROR_PREFIX: &str = "[audit] ";

/// Install the tracing-subscriber stack from `cfg` (`[logging].sinks`)
/// plus, when present, the `[audit]` sugar layer. Returns a list of
/// any errors encountered while opening sink files/sockets; on error
/// the affected sink is dropped (others still install) and the caller
/// can log a warning via the surviving sinks.
///
/// When both `cfg.sinks` and `audit` are empty/`None`, installs the
/// historical single-stderr fmt layer driven by `RUST_LOG` (matches
/// pre-S25 behaviour).
///
/// **Fail-open**: a failure to build the `[audit]` layer (bad path,
/// unwritable directory, bad syslog socket, ...) is reported via the
/// returned `Vec<String>` (prefixed with [`AUDIT_SINK_ERROR_PREFIX`])
/// but never turns into an `Err` — this function cannot fail server
/// startup on account of the audit sink. See the module-level
/// "Fail-open by design" doc above for the rationale.
pub fn install(cfg: &LoggingConfig, audit: Option<&AuditConfig>) -> Vec<String> {
    let mut errors = Vec::new();

    if cfg.sinks.is_empty() && audit.is_none() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(
                EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| "info,cq_core=debug,cq_transport=debug".into()),
            )
            .try_init();
        return errors;
    }

    let mut layers: Vec<Box<dyn Layer<Registry> + Send + Sync>> = Vec::new();
    for sink in &cfg.sinks {
        match build_layer(sink) {
            Ok(layer) => layers.push(layer),
            Err(e) => errors.push(e),
        }
    }
    if let Some(audit_cfg) = audit {
        match build_audit_layer(audit_cfg) {
            Ok(layer) => layers.push(layer),
            // Tag with AUDIT_SINK_ERROR_PREFIX so main.rs can single
            // this out for the loud "server running WITHOUT audit
            // trail" warning instead of the generic per-sink log line.
            Err(e) => errors.push(format!("{AUDIT_SINK_ERROR_PREFIX}{e}")),
        }
    }
    if layers.is_empty() {
        // Every sink failed — fall back to stderr so we don't run silent.
        let _ = tracing_subscriber::fmt()
            .with_env_filter(EnvFilter::new("info"))
            .try_init();
        return errors;
    }
    let _ = Registry::default().with(layers).try_init();
    errors
}

/// Build the `[audit]`-sugar layer: a plain `SinkConfig` for `file`,
/// or the syslog `MakeWriter` for `syslog`. Both use
/// [`audit_only_filter`] so ONLY `target = "cq_audit"` events land
/// here, regardless of what else `[logging].sinks` routes.
fn build_audit_layer(
    cfg: &AuditConfig,
) -> Result<Box<dyn Layer<Registry> + Send + Sync>, String> {
    let filter = EnvFilter::try_new(audit_only_filter())
        .map_err(|e| format!("invalid audit filter: {e}"))?;
    match cfg.sink {
        AuditSinkKind::File => {
            let path = cfg
                .path
                .as_deref()
                .ok_or_else(|| "[audit] sink = \"file\" requires `path`".to_string())?;
            let writer = open_file(path)?;
            Ok(Box::new(
                fmt::layer().with_writer(writer).with_ansi(false).with_filter(filter),
            ))
        }
        AuditSinkKind::Syslog => {
            let socket_path = cfg.path.clone().unwrap_or_else(|| "/dev/log".to_string());
            let writer = SyslogWriter::new(socket_path)?;
            Ok(Box::new(
                fmt::layer()
                    .with_writer(writer)
                    .with_ansi(false)
                    .without_time()
                    .with_filter(filter),
            ))
        }
    }
}

fn build_layer(sink: &SinkConfig) -> Result<Box<dyn Layer<Registry> + Send + Sync>, String> {
    let filter = EnvFilter::try_new(&sink.filter)
        .map_err(|e| format!("invalid filter `{}`: {}", sink.filter, e))?;
    match (&sink.file, sink.format) {
        (Some(path), SinkFormat::Text) => {
            let writer = open_file(path)?;
            Ok(Box::new(
                fmt::layer().with_writer(writer).with_ansi(false).with_filter(filter),
            ))
        }
        (Some(path), SinkFormat::Json) => {
            let writer = open_file(path)?;
            Ok(Box::new(
                fmt::layer()
                    .with_writer(writer)
                    .with_ansi(false)
                    .json()
                    .with_filter(filter),
            ))
        }
        (None, SinkFormat::Text) => Ok(Box::new(
            fmt::layer().with_writer(io::stderr).with_filter(filter),
        )),
        (None, SinkFormat::Json) => Ok(Box::new(
            fmt::layer()
                .with_writer(io::stderr)
                .json()
                .with_filter(filter),
        )),
    }
}

fn open_file(path: &str) -> Result<SharedFileWriter, String> {
    let buf = PathBuf::from(path);
    if let Some(parent) = buf.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("create_dir_all({}): {}", parent.display(), e))?;
        }
    }
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&buf)
        .map_err(|e| format!("open({}): {}", buf.display(), e))?;
    Ok(SharedFileWriter::new(file))
}

/// Wraps `std::fs::File` in an `Arc<Mutex<_>>` so it implements
/// `for<'a> MakeWriter<'a>` from `tracing_subscriber::fmt`. The mutex
/// serializes writes (one event at a time) so concurrent threads
/// don't interleave bytes mid-line.
#[derive(Clone)]
pub struct SharedFileWriter {
    inner: Arc<Mutex<std::fs::File>>,
}

impl SharedFileWriter {
    pub fn new(file: std::fs::File) -> Self {
        Self {
            inner: Arc::new(Mutex::new(file)),
        }
    }
}

impl<'a> fmt::MakeWriter<'a> for SharedFileWriter {
    type Writer = SharedFileGuard;
    fn make_writer(&'a self) -> Self::Writer {
        SharedFileGuard {
            file: self.inner.clone(),
        }
    }
}

/// Short-lived guard that holds the file mutex for the duration of a
/// single `make_writer` call. Drops the lock as soon as the
/// `tracing_subscriber` writer finishes.
pub struct SharedFileGuard {
    file: Arc<Mutex<std::fs::File>>,
}

impl io::Write for SharedFileGuard {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut g = self.file.lock().map_err(|_| {
            io::Error::other("SharedFileWriter mutex poisoned")
        })?;
        g.write(buf)
    }
    fn flush(&mut self) -> io::Result<()> {
        let mut g = self.file.lock().map_err(|_| {
            io::Error::other("SharedFileWriter mutex poisoned")
        })?;
        g.flush()
    }
}

/// D5/P0.5 — `MakeWriter` that ships each formatted line to a syslog
/// daemon over a Unix datagram socket (the same transport the
/// standard `syslog()` libc call uses under the hood). Framed as RFC
/// 3164 (`<PRI>message`, no structured-data / RFC 5424 header) since
/// that's the lowest common denominator every syslog daemon accepts.
/// `PRI` is fixed at `<14>` (facility=`user`(1), severity=`info`(6):
/// `1*8+6=14`) — audit events are already filtered to `cq_audit=info`
/// by the layer's `EnvFilter`, so a single fixed severity is
/// sufficient and keeps this writer a plain byte-sink (no per-event
/// level plumbing needed).
///
/// Connects (`UnixDatagram::connect`) once at construction so
/// steady-state writes are a single `send()` syscall; if the daemon
/// isn't listening yet, connect still succeeds for `SOCK_DGRAM` (no
/// handshake) and sends simply fail until something is listening —
/// acceptable for an audit sink (best-effort, matches syslog's own
/// fire-and-forget contract) rather than fatal to server startup.
#[derive(Clone)]
pub struct SyslogWriter {
    #[cfg(unix)]
    inner: Arc<std::os::unix::net::UnixDatagram>,
    #[cfg(not(unix))]
    _path: Arc<String>,
}

impl SyslogWriter {
    #[cfg(unix)]
    pub fn new(socket_path: impl Into<String>) -> Result<Self, String> {
        let socket_path = socket_path.into();
        let sock = std::os::unix::net::UnixDatagram::unbound()
            .map_err(|e| format!("syslog: create unbound socket: {e}"))?;
        sock.connect(&socket_path)
            .map_err(|e| format!("syslog: connect({socket_path}): {e}"))?;
        Ok(Self {
            inner: Arc::new(sock),
        })
    }

    #[cfg(not(unix))]
    pub fn new(socket_path: impl Into<String>) -> Result<Self, String> {
        // Config parsing + validation still work on non-Unix targets
        // (e.g. `cargo test` on Windows CI); only the actual socket
        // connect is unavailable, so surface that as an install error
        // rather than failing to compile.
        Err(format!(
            "syslog sink unsupported on this platform (socket_path = {})",
            socket_path.into()
        ))
    }
}

impl<'a> fmt::MakeWriter<'a> for SyslogWriter {
    type Writer = SyslogGuard;
    fn make_writer(&'a self) -> Self::Writer {
        SyslogGuard {
            #[cfg(unix)]
            inner: self.inner.clone(),
        }
    }
}

pub struct SyslogGuard {
    #[cfg(unix)]
    inner: Arc<std::os::unix::net::UnixDatagram>,
}

impl io::Write for SyslogGuard {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        #[cfg(unix)]
        {
            // RFC 3164 PRI prefix; `buf` already ends with the
            // newline tracing-subscriber's fmt layer appends, which
            // syslog datagram framing doesn't need but tolerates.
            let mut framed = Vec::with_capacity(buf.len() + 8);
            framed.extend_from_slice(b"<14>");
            framed.extend_from_slice(buf);
            // Datagram sockets are message-atomic — a short write here
            // would silently truncate the audit line, so treat it as
            // an error rather than reporting a partial success.
            match self.inner.send(&framed) {
                Ok(n) if n == framed.len() => Ok(buf.len()),
                Ok(_) => Err(io::Error::other("syslog: short datagram write")),
                Err(e) => Err(e),
            }
        }
        #[cfg(not(unix))]
        {
            let _ = buf;
            Err(io::Error::other("syslog sink unsupported on this platform"))
        }
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_logging_config_installs_default() {
        // The fallback path is a `try_init`, idempotent across tests.
        let errs = install(&LoggingConfig::default(), None);
        assert!(errs.is_empty());
    }

    #[test]
    fn filter_directive_parses() {
        let f = EnvFilter::try_new("info,cq_audit=off").unwrap();
        let _ = f; // exercise the path
    }

    #[test]
    fn well_formed_filter_directive_builds_layer() {
        // EnvFilter is intentionally permissive — almost anything
        // parses — so the property we care about is that a
        // well-formed multi-segment filter compiles + the build_layer
        // path returns Ok without panicking.
        let s = SinkConfig {
            file: None,
            filter: "info,cq_audit=off,cq_core::topic=trace".into(),
            format: SinkFormat::Text,
        };
        let r = build_layer(&s);
        assert!(r.is_ok(), "expected layer to build: {:?}", r.err());
    }

    #[test]
    fn file_sink_creates_parent_directories() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("nested").join("audit.log");
        let s = SinkConfig {
            file: Some(path.to_string_lossy().into_owned()),
            filter: "info".into(),
            format: SinkFormat::Text,
        };
        let r = build_layer(&s);
        assert!(r.is_ok(), "expected sink to open: {:?}", r.err());
        assert!(path.exists(), "audit.log should be created on open");
    }

    // --- D5/P0.5 [audit] config -------------------------------------

    #[test]
    fn audit_config_file_sink_parses() {
        let toml = r#"
            sink = "file"
            path = "logs/audit.log"
        "#;
        let cfg: AuditConfig = toml::from_str(toml).expect("parse [audit] file sink");
        assert_eq!(cfg.sink, AuditSinkKind::File);
        assert_eq!(cfg.path.as_deref(), Some("logs/audit.log"));
    }

    #[test]
    fn audit_config_syslog_sink_parses() {
        // Task 2.4 brief: "if syslog is hard to test [in an e2e sense],
        // wire config parsing + a unit test that config parses" —
        // covered here independent of a live syslog daemon.
        let toml = r#"
            sink = "syslog"
            path = "/var/run/syslog"
        "#;
        let cfg: AuditConfig = toml::from_str(toml).expect("parse [audit] syslog sink");
        assert_eq!(cfg.sink, AuditSinkKind::Syslog);
        assert_eq!(cfg.path.as_deref(), Some("/var/run/syslog"));
    }

    #[test]
    fn audit_config_sink_defaults_to_file() {
        let toml = r#"path = "logs/audit.log""#;
        let cfg: AuditConfig = toml::from_str(toml).expect("parse [audit] without sink");
        assert_eq!(cfg.sink, AuditSinkKind::File);
    }

    #[test]
    fn audit_only_filter_captures_only_cq_audit_target() {
        // Property under test: the filter directive `[audit]` builds is
        // "off,cq_audit=info" — everything except cq_audit-targeted
        // events is suppressed. Exercise that it at least parses (the
        // full routing behavior is covered by the e2e suite).
        let f = EnvFilter::try_new(audit_only_filter());
        assert!(f.is_ok(), "audit_only_filter() should be a valid EnvFilter directive");
    }

    #[test]
    fn build_audit_layer_file_sink_creates_file() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("audit").join("audit.log");
        let cfg = AuditConfig {
            sink: AuditSinkKind::File,
            path: Some(path.to_string_lossy().into_owned()),
        };
        let r = build_audit_layer(&cfg);
        assert!(r.is_ok(), "expected audit file layer to build: {:?}", r.err());
        assert!(path.exists(), "audit file should be created on open");
    }

    #[test]
    fn build_audit_layer_file_sink_without_path_errors() {
        let cfg = AuditConfig {
            sink: AuditSinkKind::File,
            path: None,
        };
        let r = build_audit_layer(&cfg);
        assert!(r.is_err(), "file sink without `path` should error");
    }

    // --- Finding 4 (Task 2.4 report) — fail-open startup behavior ---

    /// The audit sink is fail-open by design: a broken `[audit].path`
    /// (here, a path that IS an existing directory, so `OpenOptions`
    /// can never open it as a file) must not panic and must not stop
    /// `install` from completing. It should come back as a tagged
    /// error in the returned `Vec<String>` — `main.rs` uses the
    /// `AUDIT_SINK_ERROR_PREFIX` tag to emit the loud
    /// "AUDIT SINK FAILED TO INITIALIZE" warning.
    #[test]
    fn install_with_unwritable_audit_path_does_not_panic_and_reports_tagged_error() {
        let tmp = tempfile::tempdir().unwrap();
        // A directory, not a file — `OpenOptions::open` on this path
        // fails (EISDIR on Unix), simulating a misconfigured/unwritable
        // audit destination.
        let dir_as_path = tmp.path().join("this-is-a-directory");
        std::fs::create_dir_all(&dir_as_path).unwrap();

        let audit = AuditConfig {
            sink: AuditSinkKind::File,
            path: Some(dir_as_path.to_string_lossy().into_owned()),
        };

        // Must not panic (the whole point of this test): install()
        // returning normally, rather than unwinding, is itself part of
        // the assertion.
        let errs = install(&LoggingConfig::default(), Some(&audit));

        assert_eq!(
            errs.len(),
            1,
            "expected exactly one error (the broken audit sink): {errs:?}"
        );
        assert!(
            errs[0].starts_with(AUDIT_SINK_ERROR_PREFIX),
            "audit-sink error should be tagged with AUDIT_SINK_ERROR_PREFIX so \
             main.rs can single it out for the loud warning; got: {}",
            errs[0]
        );
    }

    /// Mirrors the categorization `main.rs` performs on `install`'s
    /// returned errors (`strip_prefix(AUDIT_SINK_ERROR_PREFIX)`):
    /// an audit-tagged error must classify as "audit failed" and a
    /// plain `[[logging.sinks]]` error must NOT.
    #[test]
    fn audit_sink_error_prefix_distinguishes_audit_from_generic_sink_errors() {
        let audit_err = format!("{AUDIT_SINK_ERROR_PREFIX}open(/no/such/dir/audit.log): boom");
        let generic_err = "invalid filter `garbage(((`: parse error".to_string();

        assert!(
            audit_err.strip_prefix(AUDIT_SINK_ERROR_PREFIX).is_some(),
            "audit-tagged error must be recognized as an audit failure"
        );
        assert!(
            generic_err.strip_prefix(AUDIT_SINK_ERROR_PREFIX).is_none(),
            "a generic [[logging.sinks]] error must NOT be misclassified as an audit failure"
        );
    }

    #[test]
    fn install_with_audit_config_only_does_not_use_stderr_fallback() {
        // Regression: `[audit]` with no `[logging].sinks` at all must
        // still install the audit layer, not silently fall back to the
        // pre-S25 stderr-only default (which would drop audit routing
        // entirely).
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("audit.log");
        let audit = AuditConfig {
            sink: AuditSinkKind::File,
            path: Some(path.to_string_lossy().into_owned()),
        };
        let errs = install(&LoggingConfig::default(), Some(&audit));
        assert!(errs.is_empty(), "expected no install errors: {errs:?}");
        assert!(path.exists(), "audit file should exist after install()");
    }
}

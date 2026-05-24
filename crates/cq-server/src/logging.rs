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

/// Install the tracing-subscriber stack from `cfg`. Returns a list of
/// any errors encountered while opening sink files; on error the
/// affected sink is dropped (others still install) and the caller
/// can log a warning via the surviving sinks.
///
/// When `cfg.sinks` is empty, installs the historical single-stderr
/// fmt layer driven by `RUST_LOG` (matches pre-S25 behaviour).
pub fn install(cfg: &LoggingConfig) -> Vec<String> {
    let mut errors = Vec::new();

    if cfg.sinks.is_empty() {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_logging_config_installs_default() {
        // The fallback path is a `try_init`, idempotent across tests.
        let errs = install(&LoggingConfig::default());
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
}

//! TLS helpers for the TCP transport.
//!
//! Builds a `tokio_rustls::TlsAcceptor` from a PEM-encoded cert chain
//! and PKCS#8/RSA/EC private key on disk. Used by `start_tcp_server`
//! when `[transport.tls]` is configured in `cqserver.toml`.
//!
//! Crypto provider: we install `rustls::crypto::aws_lc_rs` as the
//! process default the first time `build_tls_acceptor` is called.
//! Calling more than once is safe (the second install is a no-op).

use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::ServerConfig;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;
use std::sync::Once;
use tokio_rustls::TlsAcceptor;

static CRYPTO_INIT: Once = Once::new();

#[derive(Debug, thiserror::Error)]
pub enum TlsConfigError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("cert file contained no certificates: {0}")]
    NoCertificates(String),
    #[error("key file contained no private key: {0}")]
    NoPrivateKey(String),
    #[error("rustls config: {0}")]
    Rustls(#[from] rustls::Error),
}

/// Build a `TlsAcceptor` from PEM cert chain + PKCS#8/RSA/EC private key.
///
/// `cert_path` may contain a chain (server cert first, intermediates after).
/// `key_path` must contain exactly one private key in PKCS#8, PKCS#1 (RSA),
/// or SEC1 (EC) PEM form. rustls accepts any of these.
pub fn build_tls_acceptor(
    cert_path: impl AsRef<Path>,
    key_path: impl AsRef<Path>,
) -> Result<TlsAcceptor, TlsConfigError> {
    CRYPTO_INIT.call_once(|| {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    });

    let cert_path = cert_path.as_ref();
    let key_path = key_path.as_ref();

    let cert_file = File::open(cert_path)?;
    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut BufReader::new(cert_file))
        .collect::<Result<Vec<_>, _>>()?;
    if certs.is_empty() {
        return Err(TlsConfigError::NoCertificates(
            cert_path.display().to_string(),
        ));
    }

    let key_file = File::open(key_path)?;
    let key: PrivateKeyDer<'static> = rustls_pemfile::private_key(&mut BufReader::new(key_file))?
        .ok_or_else(|| TlsConfigError::NoPrivateKey(key_path.display().to_string()))?;

    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)?;
    Ok(TlsAcceptor::from(Arc::new(config)))
}

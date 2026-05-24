//! Transport abstraction: TCP, TLS, or WebSocket.
//!
//! Both raw TCP and TLS carry length-prefixed bytes from the codec
//! layer; the WebSocket transport additionally distinguishes text
//! (JSON) from binary (MessagePack) frames so the server's per-frame
//! codec detection works correctly.

use crate::error::{ClientError, ClientResult};
use bytes::BytesMut;
use cq_protocol::codec::decode_frame;
use cq_protocol::serialization::Codec;
use futures_util::{SinkExt, StreamExt};
use std::sync::Arc;
use std::sync::Once;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::rustls::pki_types::ServerName;
use tokio_rustls::rustls::ClientConfig as RustlsClientConfig;
use tokio_rustls::TlsConnector;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

static CRYPTO_INIT: Once = Once::new();

/// Lazily install the rustls process default crypto provider. Safe to
/// call from multiple `connect_tls` paths; the second install is a no-op.
fn ensure_crypto_provider() {
    CRYPTO_INIT.call_once(|| {
        let _ = tokio_rustls::rustls::crypto::aws_lc_rs::default_provider().install_default();
    });
}

type TlsStream = tokio_rustls::client::TlsStream<TcpStream>;
type TlsRead = tokio::io::ReadHalf<TlsStream>;
type TlsWrite = tokio::io::WriteHalf<TlsStream>;

pub enum Transport {
    Tcp {
        read: tokio::net::tcp::OwnedReadHalf,
        write: tokio::net::tcp::OwnedWriteHalf,
        buf: BytesMut,
    },
    Tls {
        read: TlsRead,
        write: TlsWrite,
        buf: BytesMut,
    },
    Ws {
        stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
    },
}

impl Transport {
    pub async fn connect_tcp(addr: &str) -> ClientResult<Self> {
        let stream = TcpStream::connect(addr).await?;
        let (read, write) = stream.into_split();
        Ok(Transport::Tcp {
            read,
            write,
            buf: BytesMut::with_capacity(8192),
        })
    }

    /// Connect over TLS. `server_name` is the SNI hostname the client
    /// presents and the cert chain is validated against; `roots` is the
    /// trust anchor set (a `RustlsClientConfig` built by the caller via
    /// `tls_client_config_with_roots` or `tls_client_config_dangerous_no_verify`).
    pub async fn connect_tls(
        addr: &str,
        server_name: &str,
        client_config: Arc<RustlsClientConfig>,
    ) -> ClientResult<Self> {
        ensure_crypto_provider();
        let tcp = TcpStream::connect(addr).await?;
        let connector = TlsConnector::from(client_config);
        let dns_name: ServerName<'static> = ServerName::try_from(server_name.to_string())
            .map_err(|e| ClientError::UnexpectedResponse(format!("invalid SNI name: {e}")))?;
        let tls = connector
            .connect(dns_name, tcp)
            .await
            .map_err(ClientError::Io)?;
        let (read, write) = tokio::io::split(tls);
        Ok(Transport::Tls {
            read,
            write,
            buf: BytesMut::with_capacity(8192),
        })
    }

    pub async fn connect_ws(url: &str) -> ClientResult<Self> {
        let (stream, _resp) = tokio_tungstenite::connect_async(url).await?;
        Ok(Transport::Ws { stream })
    }
}

/// Build a TLS client config that validates the server cert against
/// a caller-supplied trust root (in DER form). Use this for any
/// non-test deployment.
pub fn tls_client_config_with_roots(
    trust_roots_der: impl IntoIterator<Item = Vec<u8>>,
) -> ClientResult<Arc<RustlsClientConfig>> {
    ensure_crypto_provider();
    let mut roots = tokio_rustls::rustls::RootCertStore::empty();
    for der in trust_roots_der {
        roots
            .add(tokio_rustls::rustls::pki_types::CertificateDer::from(der))
            .map_err(|e| ClientError::UnexpectedResponse(format!("invalid root cert: {e}")))?;
    }
    let cfg = RustlsClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(Arc::new(cfg))
}

/// TLS client config that accepts *any* server cert without verification.
/// **Use only for tests** against self-signed servers. Refuses to even
/// compile out of `cfg(any(test, feature = "dangerous-tls"))` once we
/// add that gate; for now we expose it freely since this is an
/// in-tree crate.
pub fn tls_client_config_dangerous_no_verify() -> ClientResult<Arc<RustlsClientConfig>> {
    ensure_crypto_provider();
    let verifier = Arc::new(danger::NoVerify);
    let cfg = RustlsClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(verifier)
        .with_no_client_auth();
    Ok(Arc::new(cfg))
}

mod danger {
    use tokio_rustls::rustls::client::danger::{
        HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier,
    };
    use tokio_rustls::rustls::pki_types::{CertificateDer, ServerName, UnixTime};
    use tokio_rustls::rustls::{DigitallySignedStruct, Error as RustlsError, SignatureScheme};

    #[derive(Debug)]
    pub(super) struct NoVerify;

    impl ServerCertVerifier for NoVerify {
        fn verify_server_cert(
            &self,
            _end_entity: &CertificateDer<'_>,
            _intermediates: &[CertificateDer<'_>],
            _server_name: &ServerName<'_>,
            _ocsp_response: &[u8],
            _now: UnixTime,
        ) -> Result<ServerCertVerified, RustlsError> {
            Ok(ServerCertVerified::assertion())
        }

        fn verify_tls12_signature(
            &self,
            _message: &[u8],
            _cert: &CertificateDer<'_>,
            _dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, RustlsError> {
            Ok(HandshakeSignatureValid::assertion())
        }

        fn verify_tls13_signature(
            &self,
            _message: &[u8],
            _cert: &CertificateDer<'_>,
            _dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, RustlsError> {
            Ok(HandshakeSignatureValid::assertion())
        }

        fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
            // The full canonical list — we accept everything anyway.
            vec![
                SignatureScheme::RSA_PKCS1_SHA256,
                SignatureScheme::RSA_PKCS1_SHA384,
                SignatureScheme::RSA_PKCS1_SHA512,
                SignatureScheme::ECDSA_NISTP256_SHA256,
                SignatureScheme::ECDSA_NISTP384_SHA384,
                SignatureScheme::ECDSA_NISTP521_SHA512,
                SignatureScheme::RSA_PSS_SHA256,
                SignatureScheme::RSA_PSS_SHA384,
                SignatureScheme::RSA_PSS_SHA512,
                SignatureScheme::ED25519,
                SignatureScheme::ED448,
            ]
        }
    }
}

impl Transport {
    /// Send a frame encoded with `codec`. For TCP and TLS, both codec
    /// variants go out as length-prefixed bytes. For WebSocket, JSON
    /// goes out as a Text frame and MessagePack as a Binary frame.
    /// `compression` is the wire-level compression mode negotiated on
    /// Logon — only honoured for TCP/TLS sends; WebSocket frames
    /// stay uncompressed at the protocol layer (rely on
    /// permessage-deflate on the WS extension if compression is
    /// desired).
    pub async fn send(
        &mut self,
        codec: Codec,
        body: &[u8],
        compression: cq_protocol::compression::Compression,
    ) -> ClientResult<()> {
        match self {
            Transport::Tcp { write, .. } => {
                let mut out = BytesMut::new();
                cq_protocol::codec::encode_frame_with(body, &mut out, compression);
                write.write_all(&out).await?;
                Ok(())
            }
            Transport::Tls { write, .. } => {
                let mut out = BytesMut::new();
                cq_protocol::codec::encode_frame_with(body, &mut out, compression);
                write.write_all(&out).await?;
                Ok(())
            }
            Transport::Ws { stream } => {
                let msg = match codec {
                    Codec::Json => {
                        let s =
                            std::str::from_utf8(body).map_err(|_| {
                                ClientError::UnexpectedResponse(
                                    "non-UTF8 JSON body".into(),
                                )
                            })?;
                        Message::Text(s.into())
                    }
                    Codec::MessagePack | Codec::Bson | Codec::Fix => {
                        Message::Binary(body.to_vec().into())
                    }
                };
                stream.send(msg).await?;
                Ok(())
            }
        }
    }

    /// Read the next inbound frame. Returns `(codec, bytes)`. JSON is
    /// reported for TCP/TLS and for WS Text frames; MessagePack for WS
    /// Binary frames.
    pub async fn recv(&mut self, default_codec: Codec) -> ClientResult<(Codec, Vec<u8>)> {
        match self {
            Transport::Tcp { read, buf, .. } => loop {
                if let Some(payload) = decode_frame(buf)
                    .map_err(|e| ClientError::UnexpectedResponse(e.to_string()))?
                {
                    return Ok((default_codec, payload.to_vec()));
                }
                let n = read.read_buf(buf).await?;
                if n == 0 {
                    return Err(ClientError::ConnectionClosed);
                }
            },
            Transport::Tls { read, buf, .. } => loop {
                if let Some(payload) = decode_frame(buf)
                    .map_err(|e| ClientError::UnexpectedResponse(e.to_string()))?
                {
                    return Ok((default_codec, payload.to_vec()));
                }
                let n = read.read_buf(buf).await?;
                if n == 0 {
                    return Err(ClientError::ConnectionClosed);
                }
            },
            Transport::Ws { stream } => loop {
                match stream.next().await {
                    Some(Ok(Message::Text(t))) => {
                        return Ok((Codec::Json, t.as_bytes().to_vec()));
                    }
                    Some(Ok(Message::Binary(b))) => {
                        return Ok((Codec::MessagePack, b.to_vec()));
                    }
                    Some(Ok(Message::Ping(_) | Message::Pong(_) | Message::Frame(_))) => {
                        // ignore control frames, keep looping
                    }
                    Some(Ok(Message::Close(_))) => {
                        return Err(ClientError::ConnectionClosed);
                    }
                    Some(Err(e)) => return Err(ClientError::Ws(e)),
                    None => return Err(ClientError::ConnectionClosed),
                }
            },
        }
    }
}

use cq_protocol::serialization::CodecError;

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("ws: {0}")]
    Ws(#[from] tokio_tungstenite::tungstenite::Error),
    #[error("codec: {0}")]
    Codec(#[from] CodecError),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid url: {0}")]
    InvalidUrl(String),
    #[error("server error: {0}")]
    Server(String),
    #[error("connection closed")]
    ConnectionClosed,
    #[error("timeout waiting for ack")]
    Timeout,
    #[error("unexpected response: {0}")]
    UnexpectedResponse(String),
}

pub type ClientResult<T> = Result<T, ClientError>;

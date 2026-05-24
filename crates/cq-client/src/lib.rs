//! Async Rust client SDK for cqserver.
//!
//! ```no_run
//! use cq_client::{Client, ClientConfig};
//! use serde_json::json;
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let mut client = Client::connect("tcp://127.0.0.1:9007").await?;
//! client.logon("alice", "s3cret").await?;
//!
//! let seq = client.publish("/market-data", json!({
//!     "symbol": "AAPL", "price": 150.0
//! })).await?;
//! println!("published at sequence {}", seq);
//!
//! let mut sub = client.sow_and_subscribe("/market-data", Some("price > 100"), None).await?;
//! while let Some(delta) = sub.next_delta().await {
//!     println!("{:?}: {:?}", delta.delta_type, delta.data);
//! }
//! # Ok(()) }
//! ```

pub mod admin;
pub mod bookmark;
pub mod client;
pub mod error;
pub mod publish_store;
pub mod subscription;
pub mod transport;

pub use admin::AdminClient;
pub use bookmark::{LocalBookmarkError, LocalBookmarkStore};
pub use client::{Client, ClientConfig};
pub use error::ClientError;
pub use publish_store::{LocalPublishError, LocalPublishStore, PendingPublish};
pub use subscription::{Delta, DeltaKind, Subscription};
pub use transport::{tls_client_config_dangerous_no_verify, tls_client_config_with_roots};

// Re-exports for users who want to construct queries or NVFIX payloads
// without a separate dep on cq-protocol.
pub use cq_protocol::nvfix;
pub use cq_protocol::serialization::Codec;

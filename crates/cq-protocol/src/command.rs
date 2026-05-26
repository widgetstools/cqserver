//! CQServer command types — the vocabulary of client-server interaction.

use serde::{Deserialize, Serialize};

/// All commands in the CQServer protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Command {
    /// Publish/upsert a record to a topic.
    Publish,
    /// Q2 — atomic publish of N records to one topic. Carries the
    /// batch in `CqMessage::batch` (array of payload objects). The
    /// server commits all N rows under one `state.write()` and
    /// returns a single Ack whose `seqs` field carries the assigned
    /// sequence per row, in input order. One txlog frame, one ack,
    /// one mutation-event burst — saves N-1 round-trips compared to
    /// the SDK-level pipelining of P16.
    PublishBatch,
    /// Publish a sparse update: the payload contains the row key
    /// plus only the fields that changed. The server merges those
    /// fields into the existing SOW record (or inserts a brand-new
    /// record if no row matches the key). Saves wire bandwidth and
    /// publisher CPU on wide-row topics where most fields are
    /// stable between updates.
    DeltaPublish,
    /// Subscribe to future publishes (optionally content-filtered).
    Subscribe,
    /// One-shot SOW query.
    Sow,
    /// Snapshot + continuous deltas (the primary pattern).
    SowAndSubscribe,
    /// Like sow_and_subscribe but deltas only contain changed fields.
    DeltaSubscribe,
    /// Remove a subscription.
    Unsubscribe,
    /// Delete a record from SOW by key.
    SowDelete,
    /// Keep-alive heartbeat.
    Heartbeat,
    /// Client logon / authentication.
    Logon,
    /// Server acknowledgment.
    Ack,
    /// Start of SOW snapshot batch.
    GroupBegin,
    /// End of SOW snapshot batch.
    GroupEnd,
    /// Chunked SOW: a batch of N row objects in a single frame. Used
    /// by the streaming SOW path so a 40k-row snapshot can be delivered
    /// as ~200 frames instead of ~40,000. The frame's `rows` array
    /// holds the row objects; recipients explode it back into per-row
    /// state on their side.
    SowBatch,
    /// Pause an in-flight subscription. Carries `sub_id` of the
    /// subscription to pause. The server holds the replay cursor;
    /// no further deltas flow until a matching `Resume`.
    Pause,
    /// Resume a paused subscription. Carries `sub_id`.
    Resume,
    /// Server → client schema-change announcement (S44).
    ///
    /// The payload lives in `CqMessage::schema_change` and carries
    /// the list of added columns, removed columns, and a monotonic
    /// per-subscription schema version. Emitted by the server
    /// **before** any data delta whose body references the new
    /// columns — so clients always learn about a column before they
    /// have to bind a value to it. Used today for dynamic PIVOT
    /// (S45 follow-up) and any future operator whose result schema
    /// changes mid-stream.
    SchemaChange,
}

/// Acknowledgment level requested by client.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AckType {
    /// No acknowledgment needed.
    None,
    /// Acknowledge when message is received.
    Received,
    /// Acknowledge when message is processed (SOW updated).
    Processed,
    /// Acknowledge when message is persisted to txlog.
    Persisted,
    /// Acknowledge after replication to standby.
    Replicated,
}

/// Status codes for server responses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    Ok,
    Error,
    NotFound,
    Unauthorized,
    InvalidFilter,
    TopicNotFound,
    SubscriptionNotFound,
}

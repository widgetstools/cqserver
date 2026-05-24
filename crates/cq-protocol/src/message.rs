//! CQServer message format — the envelope for all client-server communication.

use crate::command::{AckType, Command, Status};
use crate::schema_change::SchemaChangeBody;
use serde::{Deserialize, Serialize};

/// A CQServer message (client → server or server → client).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CqMessage {
    /// The command type.
    #[serde(rename = "c")]
    pub command: Command,

    /// Client-assigned correlation ID.
    #[serde(rename = "cid", skip_serializing_if = "Option::is_none")]
    pub command_id: Option<String>,

    /// Topic name.
    #[serde(rename = "t", skip_serializing_if = "Option::is_none")]
    pub topic: Option<String>,

    /// Subscription ID (assigned by server, echoed by client for unsub).
    #[serde(rename = "sid", skip_serializing_if = "Option::is_none")]
    pub sub_id: Option<String>,

    /// SQL filter (WHERE clause).
    #[serde(rename = "f", skip_serializing_if = "Option::is_none")]
    pub filter: Option<String>,

    /// Options string (projection, order, limit, conflation).
    #[serde(rename = "o", skip_serializing_if = "Option::is_none")]
    pub options: Option<String>,

    /// Acknowledgment type requested.
    #[serde(rename = "a", skip_serializing_if = "Option::is_none")]
    pub ack_type: Option<AckType>,

    /// Response status.
    #[serde(rename = "s", skip_serializing_if = "Option::is_none")]
    pub status: Option<Status>,

    /// Reason / error message.
    #[serde(rename = "r", skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,

    /// Payload data (the record being published or returned).
    #[serde(rename = "d", skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,

    /// Number of records (used in group_begin).
    #[serde(rename = "n", skip_serializing_if = "Option::is_none")]
    pub record_count: Option<usize>,

    /// Timestamp (server-assigned, epoch millis).
    #[serde(rename = "ts", skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<u64>,

    /// Delta type for subscription updates.
    #[serde(rename = "dt", skip_serializing_if = "Option::is_none")]
    pub delta_type: Option<String>,

    /// Monotonic per-topic sequence stamp on every delta. Clients should
    /// record the highest `sequence` they have successfully processed and
    /// pass it back as `bookmark` on reconnect.
    #[serde(rename = "seq", skip_serializing_if = "Option::is_none")]
    pub sequence: Option<u64>,

    /// Subscriber-supplied bookmark on `subscribe` / `sow_and_subscribe` /
    /// `delta_subscribe`. The server replays log entries with sequence
    /// strictly greater than this value before transitioning to live
    /// delivery.
    #[serde(rename = "bm", skip_serializing_if = "Option::is_none")]
    pub bookmark: Option<u64>,

    /// Inline raw SQL. When set, the server uses this as the query
    /// verbatim — overriding any `topic`/`filter`/`options`-derived
    /// SQL — which is required for aggregate queries (`SELECT
    /// SUM(...) ... GROUP BY ...`) that can't be expressed by the
    /// projection-only options string. The FROM table name is
    /// rewritten to the canonical placeholder server-side so a
    /// caller can write either `FROM t` or the topic name itself.
    #[serde(rename = "sql", skip_serializing_if = "Option::is_none")]
    pub sql: Option<String>,

    /// Timestamp seek: when set on `subscribe`/`sow_and_subscribe`,
    /// the server scans the topic's txlog to find the highest
    /// sequence whose write happened **before** this wall-clock
    /// (epoch millis) and uses it as the implicit bookmark. The
    /// subscriber then receives every entry from that point onward
    /// (replay) before transitioning to live.
    ///
    /// Ignored when `bookmark` is also set (explicit sequence wins).
    /// Requires the topic to be persistent — there's no history to
    /// scan otherwise.
    #[serde(rename = "since_ts", skip_serializing_if = "Option::is_none")]
    pub since_timestamp_ms: Option<u64>,

    /// `MOST_RECENT` replay mode. When `true` and `client_name` is
    /// set on this connection (via logon), the server looks up the
    /// last sequence it delivered to this client for this topic
    /// (across previous sessions) and uses that as the implicit
    /// bookmark. Ignored if no prior delivery is recorded — the
    /// subscription falls through to a live-only stream.
    #[serde(rename = "mr", default, skip_serializing_if = "is_false")]
    pub most_recent: bool,

    /// Stable client identity used by `MOST_RECENT` replay. Set on
    /// `logon`; the server keys per-client bookmarks by this value.
    /// Distinct from the auto-generated `session_id`, which changes
    /// every reconnect.
    #[serde(rename = "client", skip_serializing_if = "Option::is_none")]
    pub client_name: Option<String>,

    /// On `delta_subscribe`, deliver only the topic's key columns in
    /// the initial snapshot — no payload fields. Subsequent updates
    /// remain sparse (changed fields only). Useful for grid clients
    /// that just want the key inventory upfront and will pull full
    /// rows on demand.
    #[serde(rename = "sk", default, skip_serializing_if = "is_false")]
    pub send_keys: bool,

    /// Per-message lease id assigned by the server when delivering a
    /// queue message. The consumer echoes this in an `Ack` command
    /// to commit the lease; on lease timeout the server redelivers
    /// the message (possibly to a different consumer).
    #[serde(rename = "did", skip_serializing_if = "Option::is_none")]
    pub delivery_id: Option<u64>,

    /// Structured payload for `Command::SchemaChange` frames (S44).
    /// `None` on every other command — kept Optional so older
    /// clients deserializing a SchemaChange-free wire byte stream
    /// don't see an unknown field.
    #[serde(rename = "sc", skip_serializing_if = "Option::is_none")]
    pub schema_change: Option<SchemaChangeBody>,

    /// S28 wire-protocol version negotiation. On `Logon`, clients send
    /// the set of protocol versions they support; the server picks the
    /// highest mutually supported and echoes it back as a single-entry
    /// vec on the ack. Both sides then know which version is active.
    /// Absent on every non-Logon frame (the negotiated version is
    /// stored per-session). Older clients/servers that don't send this
    /// field default to version 1.
    #[serde(rename = "pv", skip_serializing_if = "Option::is_none")]
    pub protocol_versions: Option<Vec<u32>>,

    /// S27 wire-compression negotiation. On `Logon`, clients send a
    /// preference-ordered list of compression algorithms; the server
    /// picks the first one it supports and echoes it back as a
    /// single-entry vec on the ack. Absent on every non-Logon frame.
    /// Pre-S27 clients that omit this field negotiate to
    /// [`crate::compression::DEFAULT_LEGACY_COMPRESSION`] (i.e.
    /// `Compression::None`).
    #[serde(rename = "comp", skip_serializing_if = "Option::is_none")]
    pub compressions: Option<Vec<crate::compression::Compression>>,
}

fn is_false(b: &bool) -> bool {
    !*b
}

impl CqMessage {
    /// Create a minimal message with just a command.
    pub fn new(command: Command) -> Self {
        CqMessage {
            command,
            command_id: None,
            topic: None,
            sub_id: None,
            filter: None,
            options: None,
            ack_type: None,
            status: None,
            reason: None,
            data: None,
            record_count: None,
            timestamp: None,
            delta_type: None,
            sequence: None,
            bookmark: None,
            sql: None,
            since_timestamp_ms: None,
            most_recent: false,
            client_name: None,
            send_keys: false,
            delivery_id: None,
            schema_change: None,
            protocol_versions: None,
            compressions: None,
        }
    }

    /// Construct a `SchemaChange` frame for `sub_id` carrying `body`.
    /// The frame is server-emitted; clients deserialize and surface
    /// it via the SDK's schema-change callback. See [`crate::schema_change`].
    pub fn schema_change_msg(
        sub_id: &str,
        body: SchemaChangeBody,
    ) -> Self {
        CqMessage {
            command: Command::SchemaChange,
            sub_id: Some(sub_id.to_string()),
            schema_change: Some(body),
            ..CqMessage::new(Command::SchemaChange)
        }
    }

    /// Create an error response.
    pub fn error(command_id: Option<String>, reason: &str) -> Self {
        CqMessage {
            command: Command::Ack,
            command_id,
            status: Some(Status::Error),
            reason: Some(reason.to_string()),
            ..CqMessage::new(Command::Ack)
        }
    }

    /// Create an OK ack.
    pub fn ack_ok(command_id: Option<String>) -> Self {
        CqMessage {
            command: Command::Ack,
            command_id,
            status: Some(Status::Ok),
            ..CqMessage::new(Command::Ack)
        }
    }

    /// Create a group_begin marker.
    pub fn group_begin(sub_id: &str, count: usize) -> Self {
        CqMessage {
            command: Command::GroupBegin,
            sub_id: Some(sub_id.to_string()),
            record_count: Some(count),
            ..CqMessage::new(Command::GroupBegin)
        }
    }

    /// Create a group_end marker.
    pub fn group_end(sub_id: &str) -> Self {
        CqMessage {
            command: Command::GroupEnd,
            sub_id: Some(sub_id.to_string()),
            ..CqMessage::new(Command::GroupEnd)
        }
    }

    /// Create a SOW row message (part of a snapshot).
    pub fn sow_row(
        sub_id: &str,
        data: serde_json::Map<String, serde_json::Value>,
    ) -> Self {
        CqMessage {
            command: Command::Sow,
            sub_id: Some(sub_id.to_string()),
            data: Some(serde_json::Value::Object(data)),
            ..CqMessage::new(Command::Sow)
        }
    }

    /// Create a SOW batch message — `rows` is delivered as a JSON
    /// array in the `data` field. Recipients explode it back into N
    /// per-row events. Used by the streaming SOW path to amortize wire
    /// overhead across many rows per frame.
    pub fn sow_batch(
        sub_id: &str,
        rows: Vec<serde_json::Map<String, serde_json::Value>>,
    ) -> Self {
        let n = rows.len();
        let values: Vec<serde_json::Value> =
            rows.into_iter().map(serde_json::Value::Object).collect();
        CqMessage {
            command: Command::SowBatch,
            sub_id: Some(sub_id.to_string()),
            data: Some(serde_json::Value::Array(values)),
            record_count: Some(n),
            ..CqMessage::new(Command::SowBatch)
        }
    }

    /// Create a delta message.
    pub fn delta(
        sub_id: &str,
        delta_type: &str,
        data: serde_json::Map<String, serde_json::Value>,
    ) -> Self {
        CqMessage {
            command: Command::Publish,
            sub_id: Some(sub_id.to_string()),
            delta_type: Some(delta_type.to_string()),
            data: Some(serde_json::Value::Object(data)),
            ..CqMessage::new(Command::Publish)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialize_message() {
        let msg = CqMessage {
            command: Command::SowAndSubscribe,
            command_id: Some("cmd-1".into()),
            topic: Some("/market-data".into()),
            filter: Some("desk = 'RATES'".into()),
            ..CqMessage::new(Command::SowAndSubscribe)
        };
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("sow_and_subscribe"));
        assert!(json.contains("/market-data"));
    }

    #[test]
    fn test_deserialize_message() {
        let json = r#"{
            "c": "publish",
            "t": "/orders",
            "d": {"orderId": "O001", "symbol": "AAPL", "qty": 100}
        }"#;
        let msg: CqMessage = serde_json::from_str(json).unwrap();
        assert_eq!(msg.command, Command::Publish);
        assert_eq!(msg.topic.unwrap(), "/orders");
        assert!(msg.data.is_some());
    }
}

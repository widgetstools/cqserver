//! Wire-format tests for the `SchemaChange` frame (worklog S44).
//!
//! Covers:
//!   - Encode/decode round-trip of a full SchemaChange `CqMessage`.
//!   - Backward compatibility: a wire byte stream WITHOUT the
//!     `schema_change` field still deserializes cleanly into the
//!     extended `CqMessage` struct.
//!   - Encoding of a SchemaChange-free message OMITS the new field
//!     entirely (the `Option` + `skip_serializing_if = "Option::is_none"`
//!     contract), so older readers see byte-identical output to
//!     what they saw before S44 landed.

use cq_protocol::command::Command;
use cq_protocol::message::CqMessage;
use cq_protocol::schema_change::{ColumnDef, SchemaChangeBody};

#[test]
fn schema_change_message_round_trips() {
    let body = SchemaChangeBody::new(42)
        .with_added(vec![
            ColumnDef { name: "RATES".into(), ty: "double".into() },
            ColumnDef { name: "FX".into(), ty: "double".into() },
        ])
        .with_removed(vec!["EQUITIES".into()]);
    let msg = CqMessage::schema_change_msg("sub-42", body.clone());

    let json = serde_json::to_string(&msg).expect("encode");
    let back: CqMessage = serde_json::from_str(&json).expect("decode");

    assert_eq!(back.command, Command::SchemaChange);
    assert_eq!(back.sub_id.as_deref(), Some("sub-42"));
    let sc = back.schema_change.expect("schema_change present");
    assert_eq!(sc, body);
}

#[test]
fn schema_change_wire_uses_short_key_sc() {
    // The wire key is `"sc"` (matches the rest of this protocol's
    // 2-letter envelope keys). Older byte streams that don't carry
    // this key still parse; the field is `Option<...>` with skip
    // semantics, so a SchemaChange-free message produces a
    // SchemaChange-free byte stream.
    let body = SchemaChangeBody::new(1);
    let msg = CqMessage::schema_change_msg("s", body);
    let json = serde_json::to_string(&msg).expect("encode");
    assert!(json.contains("\"sc\":"), "expected `sc` key in {json}");
    assert!(json.contains("\"c\":\"schema_change\""), "command discriminator missing in {json}");
}

#[test]
fn old_clients_emitting_msg_without_sc_still_parse_post_s44() {
    // Backward compatibility: a byte stream a pre-S44 client would
    // have produced (no `sc` field) must still deserialize into
    // the extended struct with `schema_change = None`. This is the
    // contract that keeps older clients from breaking when the
    // server adds SchemaChange support.
    let pre_s44_json = r#"{
        "c": "publish",
        "t": "/orders",
        "d": {"k": "o1", "v": 100}
    }"#;
    let msg: CqMessage = serde_json::from_str(pre_s44_json).expect("decode");
    assert_eq!(msg.command, Command::Publish);
    assert!(msg.schema_change.is_none());
}

#[test]
fn schema_change_free_messages_dont_emit_sc_key() {
    // The flip side of the backward-compat test: a non-SchemaChange
    // message must NOT emit `sc` on the wire. Pre-S44 readers see
    // identical bytes to what they saw before.
    let msg = CqMessage::new(Command::Publish);
    let json = serde_json::to_string(&msg).expect("encode");
    assert!(
        !json.contains("\"sc\""),
        "non-SchemaChange msg leaked `sc` key: {json}"
    );
}

#[test]
fn schema_change_with_no_columns_just_advances_version() {
    // A version-only SchemaChange (`new_columns = []`, `removed_columns = []`)
    // is legal — useful when a server wants to acknowledge a
    // logical schema-version bump without actually changing
    // columns (e.g., column reorder). Wire payload omits both
    // empty lists to stay minimal.
    let body = SchemaChangeBody::new(99);
    let msg = CqMessage::schema_change_msg("s", body);
    let json = serde_json::to_string(&msg).expect("encode");
    assert!(json.contains("\"version\":99"));
    assert!(!json.contains("new_columns"), "empty added list leaked: {json}");
    assert!(!json.contains("removed_columns"), "empty removed list leaked: {json}");

    // Round-trip still works — defaults populate both lists.
    let back: CqMessage = serde_json::from_str(&json).expect("decode");
    let sc = back.schema_change.expect("present");
    assert_eq!(sc.version, 99);
    assert!(sc.new_columns.is_empty());
    assert!(sc.removed_columns.is_empty());
}

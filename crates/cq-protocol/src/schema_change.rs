//! Wire-protocol `SchemaChange` body (worklog S44, prep for S45
//! dynamic PIVOT).
//!
//! A `SchemaChange` frame announces that the result schema of a
//! continuous query just gained or lost columns. The server emits
//! it **before** any data delta that references the new columns,
//! and the client SDK is expected to surface it as a structured
//! callback so application code can adjust grids / typed bindings
//! before processing the delta payload.
//!
//! Landing the wire format ahead of the dynamic-PIVOT executor
//! (S45) means clients can absorb the protocol-side breakage in a
//! separate release from the operator's first use — no big-bang.

use serde::{Deserialize, Serialize};

/// One column in a `SchemaChange::new_columns` list. The `ty` value
/// is a string for forward compatibility — clients that don't know
/// a new type yet can still surface the column with an "unknown"
/// renderer and continue receiving data.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    #[serde(rename = "type")]
    pub ty: String,
}

/// Structured payload for a `Command::SchemaChange` message.
///
/// `version` is monotonic per (topic, subscription) pair so a client
/// reconnecting mid-flight can detect missed schema changes by
/// comparing the version it last saw against the version on the
/// first post-reconnect delta.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaChangeBody {
    /// Columns added in this version. Order matches the server's
    /// preferred display order. Empty when this frame only removes.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub new_columns: Vec<ColumnDef>,
    /// Column names removed in this version. The client should drop
    /// any cached binding for these names.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub removed_columns: Vec<String>,
    /// Monotonic per-subscription schema version. A delta carrying
    /// the same `sub_id` and a `sequence` AFTER this frame's
    /// `sequence` is guaranteed to be against this schema version
    /// or higher.
    pub version: u64,
}

impl SchemaChangeBody {
    pub fn new(version: u64) -> Self {
        Self {
            new_columns: Vec::new(),
            removed_columns: Vec::new(),
            version,
        }
    }

    pub fn with_added<I: Into<Vec<ColumnDef>>>(mut self, cols: I) -> Self {
        self.new_columns = cols.into();
        self
    }

    pub fn with_removed<I: Into<Vec<String>>>(mut self, names: I) -> Self {
        self.removed_columns = names.into();
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_change_serializes_omits_empty_columns() {
        // Frame that only adds a column: removed_columns is empty
        // and must be skipped on the wire (saves bytes; older
        // clients see a leaner payload).
        let body = SchemaChangeBody::new(7).with_added(vec![ColumnDef {
            name: "RATES".into(),
            ty: "double".into(),
        }]);
        let json = serde_json::to_string(&body).expect("serialize");
        assert!(json.contains("\"version\":7"));
        assert!(json.contains("RATES"));
        assert!(!json.contains("removed_columns"), "empty list should be omitted: {json}");
    }

    #[test]
    fn schema_change_round_trips_with_both_added_and_removed() {
        let body = SchemaChangeBody::new(12)
            .with_added(vec![
                ColumnDef { name: "FX".into(), ty: "double".into() },
                ColumnDef { name: "CREDIT".into(), ty: "double".into() },
            ])
            .with_removed(vec!["EQUITIES".into()]);
        let json = serde_json::to_string(&body).unwrap();
        let back: SchemaChangeBody = serde_json::from_str(&json).unwrap();
        assert_eq!(back, body);
    }

    #[test]
    fn schema_change_round_trip_via_serde_value() {
        // Common wire path: nested inside CqMessage.data via
        // serde_json::Value. Verify that path round-trips too.
        let body = SchemaChangeBody::new(3)
            .with_added(vec![ColumnDef { name: "A".into(), ty: "long".into() }]);
        let v: serde_json::Value = serde_json::to_value(&body).unwrap();
        let back: SchemaChangeBody = serde_json::from_value(v).unwrap();
        assert_eq!(back, body);
    }
}

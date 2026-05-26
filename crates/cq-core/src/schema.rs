//! Schema model: column names, types, and layout metadata.

use compact_str::CompactString;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Supported column types for the columnar store.
///
/// Mirrors the AMPS type lattice (Null, Bool, Int64, Float64, String,
/// Timestamp) that the project spec calls for. The columnar store keeps
/// one typed `Vec<T>` per primitive — adding a new variant means adding
/// a parallel backing array in `ColumnStore` and a matching arm
/// everywhere `ColumnType` is matched.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ColumnType {
    Double,
    Long,
    Int,
    String,
    Bool,
    /// i64 microseconds since UNIX epoch. RFC 3339 / ISO 8601 strings
    /// parse on ingest; output renders as `2026-05-25T22:48:43.201Z`.
    Timestamp,
    /// Q10 — opaque binary blob. JSON wire form is a base64-encoded
    /// string (input + output). Supports equality (`= '<base64>'`)
    /// and `IS NULL`; no arithmetic, no sort, no aggregation —
    /// downstream consumers that need richer ops should decode
    /// client-side. Backed by `Vec<Option<Vec<u8>>>` in the column
    /// store.
    Bytes,
}

impl ColumnType {
    /// Widen two types to the most permissive common type.
    /// LONG + DOUBLE → DOUBLE, BOOL + anything-but-bool → STRING,
    /// anything else mixed → STRING.
    pub fn widen(self, other: ColumnType) -> ColumnType {
        if self == other {
            return self;
        }
        match (self, other) {
            (ColumnType::Long, ColumnType::Double) | (ColumnType::Double, ColumnType::Long) => {
                ColumnType::Double
            }
            (ColumnType::Int, ColumnType::Long) | (ColumnType::Long, ColumnType::Int) => {
                ColumnType::Long
            }
            (ColumnType::Int, ColumnType::Double) | (ColumnType::Double, ColumnType::Int) => {
                ColumnType::Double
            }
            // Bool only co-exists with itself; anything mixed in
            // collapses to String (matches AMPS coercion rules — once a
            // column has seen non-bool values it has to stringify).
            _ => ColumnType::String,
        }
    }

    /// Infer type from a JSON value.
    pub fn from_json(value: &serde_json::Value) -> ColumnType {
        match value {
            serde_json::Value::Number(n) => {
                if n.is_f64() {
                    ColumnType::Double
                } else {
                    ColumnType::Long
                }
            }
            serde_json::Value::Bool(_) => ColumnType::Bool,
            _ => ColumnType::String,
        }
    }

    /// Infer type from a string sample (for CSV loading).
    pub fn infer_from_str(s: &str) -> ColumnType {
        if s.is_empty() {
            return ColumnType::String;
        }
        if s.parse::<i64>().is_ok() {
            return ColumnType::Long;
        }
        if s.parse::<f64>().is_ok() {
            return ColumnType::Double;
        }
        ColumnType::String
    }
}

/// Metadata for a single column.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Column {
    name: CompactString,
    col_type: ColumnType,
    index: usize,
}

impl Column {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn col_type(&self) -> ColumnType {
        self.col_type
    }

    pub fn index(&self) -> usize {
        self.index
    }
}

/// Internal mapping from schema column index to the backing typed array.
#[derive(Debug, Clone, Copy)]
pub struct ColumnMapping {
    pub kind: ColumnType,
    pub array_index: usize,
}

/// Immutable schema describing the column layout of a topic's SOW store.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schema {
    columns: Vec<Column>,
    #[serde(skip)]
    name_to_index: HashMap<CompactString, usize>,
}

impl Schema {
    /// Create a schema from parallel name and type vectors.
    pub fn new(names: Vec<CompactString>, types: Vec<ColumnType>) -> Self {
        assert_eq!(names.len(), types.len(), "names and types length mismatch");
        let mut name_to_index = HashMap::with_capacity(names.len());
        let columns: Vec<Column> = names
            .into_iter()
            .zip(types)
            .enumerate()
            .map(|(i, (name, col_type))| {
                name_to_index.insert(name.clone(), i);
                Column {
                    name,
                    col_type,
                    index: i,
                }
            })
            .collect();

        Schema {
            columns,
            name_to_index,
        }
    }

    /// Build from string slices (convenience).
    pub fn from_strs(names: &[&str], types: &[ColumnType]) -> Self {
        let names: Vec<CompactString> = names.iter().map(|s| CompactString::new(s)).collect();
        Self::new(names, types.to_vec())
    }

    /// Rebuild the name-to-index map after deserialization.
    pub fn rebuild_index(&mut self) {
        self.name_to_index.clear();
        for col in &self.columns {
            self.name_to_index.insert(col.name.clone(), col.index);
        }
    }

    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    pub fn has_column(&self, name: &str) -> bool {
        self.name_to_index.contains_key(name)
    }

    pub fn index_of(&self, name: &str) -> Option<usize> {
        self.name_to_index.get(name).copied()
    }

    pub fn column_at(&self, index: usize) -> &Column {
        &self.columns[index]
    }

    pub fn column_type(&self, index: usize) -> ColumnType {
        self.columns[index].col_type
    }

    pub fn column_name(&self, index: usize) -> &str {
        &self.columns[index].name
    }

    /// Compute the mapping from schema column index to typed backing array index.
    /// Called once when creating a ColumnStore.
    pub fn compute_mappings(&self) -> Vec<ColumnMapping> {
        let mut double_count = 0usize;
        let mut long_count = 0usize;
        let mut int_count = 0usize;
        let mut string_count = 0usize;
        let mut bool_count = 0usize;
        let mut timestamp_count = 0usize;
        let mut bytes_count = 0usize;

        self.columns
            .iter()
            .map(|col| {
                let array_index = match col.col_type {
                    ColumnType::Double => {
                        let idx = double_count;
                        double_count += 1;
                        idx
                    }
                    ColumnType::Long => {
                        let idx = long_count;
                        long_count += 1;
                        idx
                    }
                    ColumnType::Int => {
                        let idx = int_count;
                        int_count += 1;
                        idx
                    }
                    ColumnType::String => {
                        let idx = string_count;
                        string_count += 1;
                        idx
                    }
                    ColumnType::Bool => {
                        let idx = bool_count;
                        bool_count += 1;
                        idx
                    }
                    ColumnType::Timestamp => {
                        let idx = timestamp_count;
                        timestamp_count += 1;
                        idx
                    }
                    ColumnType::Bytes => {
                        let idx = bytes_count;
                        bytes_count += 1;
                        idx
                    }
                };
                ColumnMapping {
                    kind: col.col_type,
                    array_index,
                }
            })
            .collect()
    }

    /// Per-primitive column counts. Returned as a struct rather than a
    /// tuple so adding a new variant doesn't break every caller's
    /// destructuring at the call site.
    pub fn type_counts(&self) -> TypeCounts {
        let mut counts = TypeCounts::default();
        for col in &self.columns {
            match col.col_type {
                ColumnType::Double => counts.double += 1,
                ColumnType::Long => counts.long += 1,
                ColumnType::Int => counts.int += 1,
                ColumnType::String => counts.string += 1,
                ColumnType::Bool => counts.bool += 1,
                ColumnType::Timestamp => counts.timestamp += 1,
                ColumnType::Bytes => counts.bytes += 1,
            }
        }
        counts
    }
}

/// Per-primitive column counts used by `ColumnStore::new` to size the
/// backing arrays.
#[derive(Debug, Clone, Copy, Default)]
pub struct TypeCounts {
    pub double: usize,
    pub long: usize,
    pub int: usize,
    pub string: usize,
    pub bool: usize,
    pub timestamp: usize,
    /// Q10 — Bytes column count.
    pub bytes: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_basics() {
        let schema = Schema::from_strs(
            &["tradeId", "price", "quantity", "desk"],
            &[
                ColumnType::String,
                ColumnType::Double,
                ColumnType::Long,
                ColumnType::String,
            ],
        );
        assert_eq!(schema.column_count(), 4);
        assert_eq!(schema.index_of("price"), Some(1));
        assert_eq!(schema.column_type(1), ColumnType::Double);
        assert!(schema.has_column("desk"));
        assert!(!schema.has_column("nonexistent"));
    }

    #[test]
    fn test_type_widening() {
        assert_eq!(ColumnType::Long.widen(ColumnType::Double), ColumnType::Double);
        assert_eq!(ColumnType::Int.widen(ColumnType::Long), ColumnType::Long);
        assert_eq!(ColumnType::Long.widen(ColumnType::String), ColumnType::String);
        assert_eq!(ColumnType::Double.widen(ColumnType::Double), ColumnType::Double);
    }

    #[test]
    fn test_column_mappings() {
        let schema = Schema::from_strs(
            &["a", "b", "c", "d", "e"],
            &[
                ColumnType::Double,
                ColumnType::String,
                ColumnType::Long,
                ColumnType::Double,
                ColumnType::String,
            ],
        );
        let mappings = schema.compute_mappings();
        // a → Double[0], b → String[0], c → Long[0], d → Double[1], e → String[1]
        assert_eq!(mappings[0].array_index, 0); // a: Double[0]
        assert_eq!(mappings[1].array_index, 0); // b: String[0]
        assert_eq!(mappings[2].array_index, 0); // c: Long[0]
        assert_eq!(mappings[3].array_index, 1); // d: Double[1]
        assert_eq!(mappings[4].array_index, 1); // e: String[1]
    }

    #[test]
    fn test_bool_column_inference_widen_and_mapping() {
        // Inference: native JSON booleans become Bool, not String.
        assert_eq!(
            ColumnType::from_json(&serde_json::Value::Bool(true)),
            ColumnType::Bool
        );

        // Widening: Bool only co-exists with itself.
        assert_eq!(ColumnType::Bool.widen(ColumnType::Bool), ColumnType::Bool);
        assert_eq!(
            ColumnType::Bool.widen(ColumnType::Long),
            ColumnType::String
        );

        // Schema with a mix of types places Bool columns in their own arena.
        let schema = Schema::from_strs(
            &["a", "b", "c", "d"],
            &[
                ColumnType::Bool,
                ColumnType::String,
                ColumnType::Bool,
                ColumnType::Long,
            ],
        );
        let mappings = schema.compute_mappings();
        assert_eq!(mappings[0].array_index, 0); // a: Bool[0]
        assert_eq!(mappings[2].array_index, 1); // c: Bool[1]
        let counts = schema.type_counts();
        assert_eq!(counts.bool, 2);
        assert_eq!(counts.string, 1);
        assert_eq!(counts.long, 1);
    }
}

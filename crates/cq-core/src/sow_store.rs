//! `SowStore` — the shard-or-not abstraction over a SOW (review C4 /
//! worklog S39).
//!
//! Today's production `ColumnStore` is a single-shard SOW. Tomorrow's
//! per-CPU sharding (S29) wants multiple shards keyed by row-key hash.
//! Without this abstraction, S29 would have to refactor every
//! call-site that touches the store; with it, S29 swaps the
//! `SowStore::Single(...)` enum variant for `SowStore::Sharded { ... }`
//! and the rest of the system keeps working.
//!
//! Scope today: the abstraction lives in this module as a
//! self-contained mini-store, independent of `Topic` + `StoreState`.
//! That keeps S39 small while still letting the proptest suite verify
//! the load-bearing property — that `Single`, `Sharded(2)`, and
//! `Sharded(16)` all produce identical materialized state for any op
//! stream. When S29 lands, `Topic` migrates from its current
//! `RwLock<StoreState>` to a `RwLock<SowStore>` and the `Sharded`
//! variant takes over.

use std::collections::HashMap;
use std::hash::{Hash, Hasher};

use ahash::AHasher;

/// A SOW shard. The minimal surface needed for the upcoming S29 hot
/// path: key-addressed upsert / delete and a way to materialize every
/// live row. Real `ColumnStore` integration (predicates, projections,
/// secondary indexes) layers on top in S29 — those operations
/// trivially fan out across shards by iterating them.
pub trait SowShard {
    /// Insert or overwrite the row identified by `key`.
    fn upsert(&mut self, key: &str, row: Row);
    /// Remove the row. Returns `true` if a row was present.
    fn delete(&mut self, key: &str) -> bool;
    /// Look up the current row for `key`, if any.
    fn get(&self, key: &str) -> Option<&Row>;
    /// Number of live rows in this shard.
    fn live_row_count(&self) -> usize;
    /// Iterate every (key, row) pair currently in the shard.
    fn materialize<'a>(&'a self) -> Box<dyn Iterator<Item = (&'a String, &'a Row)> + 'a>;
}

/// Row body. Production `ColumnStore` represents this as one entry per
/// typed column array; this mini-store uses a plain `HashMap` keyed
/// by column name. Behaviorally equivalent for the equivalence
/// property we're verifying (`materialize_all_sorted()` is a set of
/// `(key, row)` pairs, not a column-oriented projection).
pub type Row = HashMap<String, RowValue>;

#[derive(Debug, Clone, PartialEq)]
pub enum RowValue {
    Null,
    Long(i64),
    Double(f64),
    String(String),
}

impl Eq for RowValue {}

impl Hash for RowValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            RowValue::Null => 0u8.hash(state),
            RowValue::Long(v) => {
                1u8.hash(state);
                v.hash(state);
            }
            RowValue::Double(v) => {
                2u8.hash(state);
                v.to_bits().hash(state);
            }
            RowValue::String(s) => {
                3u8.hash(state);
                s.hash(state);
            }
        }
    }
}

/// Single-shard SOW. Mirrors the production `ColumnStore` semantics
/// at the abstraction level used by `SowStore`.
#[derive(Default)]
pub struct SingleShard {
    rows: HashMap<String, Row>,
}

impl SingleShard {
    pub fn new() -> Self {
        Self {
            rows: HashMap::new(),
        }
    }
}

impl SowShard for SingleShard {
    fn upsert(&mut self, key: &str, row: Row) {
        self.rows.insert(key.to_string(), row);
    }
    fn delete(&mut self, key: &str) -> bool {
        self.rows.remove(key).is_some()
    }
    fn get(&self, key: &str) -> Option<&Row> {
        self.rows.get(key)
    }
    fn live_row_count(&self) -> usize {
        self.rows.len()
    }
    fn materialize<'a>(&'a self) -> Box<dyn Iterator<Item = (&'a String, &'a Row)> + 'a> {
        Box::new(self.rows.iter())
    }
}

/// `SowStore` selects between a single shard (today) and a sharded
/// fan-out across N shards (S29). Routing decisions live here so
/// callers don't have to know which mode they're in.
pub enum SowStore {
    Single(SingleShard),
    Sharded { shards: Vec<SingleShard> },
}

impl SowStore {
    pub fn single() -> Self {
        Self::Single(SingleShard::new())
    }

    /// New sharded store with `n` shards. `n` must be ≥ 1; `n = 1`
    /// is functionally equivalent to `Single` but tests the
    /// `Sharded` code path.
    pub fn sharded(n: usize) -> Self {
        assert!(n >= 1, "shard count must be >= 1");
        let shards = (0..n).map(|_| SingleShard::new()).collect();
        Self::Sharded { shards }
    }

    /// Hash a row key to a shard index. Uses the same hasher family
    /// the production secondary index uses (`ahash`) so we don't
    /// need a second seed when S29 wires shard routing into the
    /// real `ColumnStore`.
    fn shard_for<'a>(shards: &'a mut [SingleShard], key: &str) -> &'a mut SingleShard {
        let mut h = AHasher::default();
        key.hash(&mut h);
        let idx = (h.finish() as usize) % shards.len();
        &mut shards[idx]
    }

    fn shard_for_ref<'a>(shards: &'a [SingleShard], key: &str) -> &'a SingleShard {
        let mut h = AHasher::default();
        key.hash(&mut h);
        let idx = (h.finish() as usize) % shards.len();
        &shards[idx]
    }

    pub fn upsert(&mut self, key: &str, row: Row) {
        match self {
            SowStore::Single(s) => s.upsert(key, row),
            SowStore::Sharded { shards } => Self::shard_for(shards, key).upsert(key, row),
        }
    }

    pub fn delete(&mut self, key: &str) -> bool {
        match self {
            SowStore::Single(s) => s.delete(key),
            SowStore::Sharded { shards } => Self::shard_for(shards, key).delete(key),
        }
    }

    pub fn get(&self, key: &str) -> Option<&Row> {
        match self {
            SowStore::Single(s) => s.get(key),
            SowStore::Sharded { shards } => Self::shard_for_ref(shards, key).get(key),
        }
    }

    pub fn live_row_count(&self) -> usize {
        match self {
            SowStore::Single(s) => s.live_row_count(),
            SowStore::Sharded { shards } => shards.iter().map(|s| s.live_row_count()).sum(),
        }
    }

    /// Materialize the entire SOW state as a sorted `(key, row)`
    /// list. Sorting collapses shard iteration order so two stores
    /// with the same logical state compare equal regardless of how
    /// many shards each uses internally. This is the property the
    /// S39 proptests verify.
    pub fn materialize_sorted(&self) -> Vec<(String, Row)> {
        let mut out: Vec<(String, Row)> = match self {
            SowStore::Single(s) => s.materialize().map(|(k, v)| (k.clone(), v.clone())).collect(),
            SowStore::Sharded { shards } => shards
                .iter()
                .flat_map(|s| s.materialize().map(|(k, v)| (k.clone(), v.clone())))
                .collect(),
        };
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    /// Materialize only rows that satisfy `predicate`. Used to verify
    /// that filtered queries return the same set across shard counts.
    pub fn materialize_filtered<F>(&self, predicate: F) -> Vec<(String, Row)>
    where
        F: Fn(&Row) -> bool,
    {
        let mut out: Vec<(String, Row)> = match self {
            SowStore::Single(s) => s
                .materialize()
                .filter(|(_, r)| predicate(r))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            SowStore::Sharded { shards } => shards
                .iter()
                .flat_map(|s| {
                    s.materialize()
                        .filter(|(_, r)| predicate(r))
                        .map(|(k, v)| (k.clone(), v.clone()))
                })
                .collect(),
        };
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(v: i64) -> Row {
        let mut r = Row::new();
        r.insert("v".into(), RowValue::Long(v));
        r
    }

    #[test]
    fn single_and_sharded_have_identical_state_after_same_ops() {
        let mut single = SowStore::single();
        let mut sharded = SowStore::sharded(8);
        for i in 0..20 {
            let k = format!("k{i:03}");
            single.upsert(&k, row(i));
            sharded.upsert(&k, row(i));
        }
        single.delete("k005");
        sharded.delete("k005");
        single.upsert("k010", row(999));
        sharded.upsert("k010", row(999));
        assert_eq!(single.materialize_sorted(), sharded.materialize_sorted());
        assert_eq!(single.live_row_count(), sharded.live_row_count());
    }

    #[test]
    fn sharded_routes_to_at_least_two_shards_for_diverse_keys() {
        // Sanity: 100 distinct keys hashed into 8 shards should not
        // all land in the same shard. Catches a silently-broken
        // hasher.
        let mut sharded = SowStore::sharded(8);
        for i in 0..100 {
            sharded.upsert(&format!("k{i:03}"), row(i));
        }
        if let SowStore::Sharded { shards } = &sharded {
            let counts: Vec<usize> = shards.iter().map(|s| s.live_row_count()).collect();
            let nonzero = counts.iter().filter(|n| **n > 0).count();
            assert!(
                nonzero >= 4,
                "ahash-keyed routing landed all 100 keys in {nonzero}/8 shards — bad spread"
            );
        }
    }
}

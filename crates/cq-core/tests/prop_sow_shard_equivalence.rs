//! Proptests for the `SowShard` abstraction (worklog S39, review C4).
//!
//! The load-bearing property: for any sequence of upserts and deletes,
//! the materialized state of a `SowStore::Single(...)` equals the
//! materialized state of a `SowStore::Sharded { shards }` for any N
//! shard count. Without this guarantee, S29 can't ship sharding
//! without observable behavior change.

use cq_core::sow_store::{Row, RowValue, SowStore};
use proptest::prelude::*;

#[derive(Debug, Clone)]
enum Op {
    Upsert { key: u8, value: i64 },
    Delete { key: u8 },
}

prop_compose! {
    fn any_op()(
        tag in 0..4u8,
        key in 0u8..32u8,
        value in any::<i64>(),
    ) -> Op {
        if tag < 3 {
            Op::Upsert { key, value }
        } else {
            Op::Delete { key }
        }
    }
}

fn row_of(value: i64) -> Row {
    let mut r = Row::new();
    r.insert("v".into(), RowValue::Long(value));
    r
}

fn apply(store: &mut SowStore, op: &Op) {
    let k = format!("k{:03}", op_key(op));
    match op {
        Op::Upsert { value, .. } => store.upsert(&k, row_of(*value)),
        Op::Delete { .. } => {
            store.delete(&k);
        }
    }
}

fn op_key(op: &Op) -> u8 {
    match op {
        Op::Upsert { key, .. } | Op::Delete { key } => *key,
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// C4.1 — `Single` and `Sharded(N)` produce identical materialized
    /// state across every random op stream. Verifies that the sharding
    /// abstraction preserves SOW semantics regardless of shard count.
    #[test]
    fn materialized_state_is_identical_across_shard_counts(
        ops in prop::collection::vec(any_op(), 1..120),
    ) {
        let mut single = SowStore::single();
        let mut sharded_2 = SowStore::sharded(2);
        let mut sharded_16 = SowStore::sharded(16);
        for op in &ops {
            apply(&mut single, op);
            apply(&mut sharded_2, op);
            apply(&mut sharded_16, op);
        }
        let s = single.materialize_sorted();
        let s2 = sharded_2.materialize_sorted();
        let s16 = sharded_16.materialize_sorted();
        prop_assert_eq!(&s, &s2);
        prop_assert_eq!(&s, &s16);
    }

    /// C4.3 — A predicate-filtered query returns identical row sets
    /// across shard counts. The predicate here is "value > threshold",
    /// which mirrors the kind of WHERE clause the production
    /// `Topic::query` would fan out across shards via per-shard
    /// `execute_query` + merge.
    #[test]
    fn predicate_filter_is_identical_across_shard_counts(
        ops in prop::collection::vec(any_op(), 1..120),
        threshold in any::<i64>(),
    ) {
        let mut single = SowStore::single();
        let mut sharded_2 = SowStore::sharded(2);
        let mut sharded_16 = SowStore::sharded(16);
        for op in &ops {
            apply(&mut single, op);
            apply(&mut sharded_2, op);
            apply(&mut sharded_16, op);
        }
        let pred = |row: &Row| {
            matches!(row.get("v"), Some(RowValue::Long(v)) if *v > threshold)
        };
        let s = single.materialize_filtered(pred);
        let s2 = sharded_2.materialize_filtered(pred);
        let s16 = sharded_16.materialize_filtered(pred);
        prop_assert_eq!(&s, &s2);
        prop_assert_eq!(&s, &s16);
    }
}

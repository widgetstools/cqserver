//! Property test for the `PredicateIndex` correctness contract
//! (worklog S46, review C3 test C3.3).
//!
//! Contract: for any random set of predicates + any random
//! `changed_cols`, the index's `affected(...)` set is a **superset**
//! of the actually-affected subs. False positives (returning a sub
//! that wouldn't have produced a delta) are tolerated — the
//! evaluator just does extra work. False negatives are a correctness
//! bug — a sub that depends on a changed column gets skipped, and
//! the subscriber misses a delta.
//!
//! "Actually affected" is defined here as: the sub's predicate
//! references at least one column in `changed_cols`. That's exactly
//! the property the index claims to compute, so the proptest pins
//! the relationship between the predicate's column-walking and the
//! index's column→subs map.

use std::collections::HashSet;

use compact_str::CompactString;
use cq_core::predicate::CompiledPredicate;
use cq_core::predicate_index::PredicateIndex;
use proptest::prelude::*;

/// Generate a random predicate over column indices 0..16. Includes
/// a mix of leaves (numeric + string variants), And/Or/Not
/// combinators, and the `True` no-op. Doesn't generate string-expr
/// variants — those exercise a distinct code path that the
/// `referenced_columns` walker handles via `StringExpr` recursion,
/// covered by the unit tests in `predicate_index::tests`.
fn any_predicate() -> impl Strategy<Value = CompiledPredicate> {
    let leaf = prop_oneof![
        Just(CompiledPredicate::True),
        (0u8..16u8, any::<i64>())
            .prop_map(|(c, v)| CompiledPredicate::EqLong { col: c as usize, value: v }),
        (0u8..16u8, any::<i64>())
            .prop_map(|(c, v)| CompiledPredicate::GtLong { col: c as usize, value: v }),
        (0u8..16u8).prop_map(|c| CompiledPredicate::IsNull { col: c as usize }),
        (0u8..16u8, "[a-z]{1,5}").prop_map(|(c, s)| CompiledPredicate::EqString {
            col: c as usize,
            value: CompactString::new(s),
        }),
    ];
    leaf.prop_recursive(3, 16, 2, |inner| {
        prop_oneof![
            (inner.clone(), inner.clone())
                .prop_map(|(a, b)| CompiledPredicate::And(Box::new(a), Box::new(b))),
            (inner.clone(), inner.clone())
                .prop_map(|(a, b)| CompiledPredicate::Or(Box::new(a), Box::new(b))),
            inner.prop_map(|a| CompiledPredicate::Not(Box::new(a))),
        ]
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// C3.3 — `PredicateIndex::affected(changed)` is a superset of
    /// the truly-affected sub set (subs whose predicate's
    /// `referenced_columns()` intersects `changed`).
    ///
    /// The test takes 1..50 random predicates, indexes them all,
    /// then for a random changed-column set asks `affected(...)`
    /// and walks every predicate independently to compute the
    /// reference set. Index ⊇ reference must always hold.
    #[test]
    fn affected_is_superset_of_truly_affected(
        predicates in prop::collection::vec(any_predicate(), 1..50),
        changed in prop::collection::vec(0u8..16u8, 0..16),
    ) {
        // Build the index.
        let mut idx = PredicateIndex::new();
        for (i, pred) in predicates.iter().enumerate() {
            idx.add(&format!("sub-{i:03}"), pred);
        }
        let changed_cols: Vec<usize> =
            changed.iter().map(|c| *c as usize).collect();

        // Reference: walk each predicate and check intersection.
        // Subs whose predicate references zero columns (`True`)
        // always match — they're the "always" set in the index
        // and must be returned regardless of changed_cols.
        let changed_set: HashSet<usize> = changed_cols.iter().copied().collect();
        let truly_affected: HashSet<String> = predicates
            .iter()
            .enumerate()
            .filter_map(|(i, pred)| {
                let cols = pred.referenced_columns();
                if cols.is_empty() {
                    // True / always-match subs.
                    Some(format!("sub-{i:03}"))
                } else if cols.iter().any(|c| changed_set.contains(c)) {
                    Some(format!("sub-{i:03}"))
                } else {
                    None
                }
            })
            .collect();

        let indexed: HashSet<String> =
            idx.affected(Some(&changed_cols)).into_iter().collect();

        // Superset: every truly-affected sub must appear in the
        // indexed set. (Equality would also be acceptable; the
        // index can be tighter if it wants. But it must never miss.)
        for id in &truly_affected {
            prop_assert!(
                indexed.contains(id),
                "FALSE NEGATIVE: sub {id} is truly affected (its predicate \
                 references at least one changed column) but the index did \
                 not return it.\n  changed_cols = {changed_cols:?}\n  predicate = {:?}",
                predicates[id.strip_prefix("sub-").unwrap().parse::<usize>().unwrap()]
            );
        }
    }
}

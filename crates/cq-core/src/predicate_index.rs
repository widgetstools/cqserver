//! `PredicateIndex` — selective fan-out for sparse mutations (worklog
//! S46, review concern C3).
//!
//! Without an index, every mutation event walks every subscription's
//! predicate, regardless of whether the mutation could possibly have
//! changed the predicate's outcome. For wide-row topics with
//! delta-publish (`delta_upsert_map` — only a couple of columns
//! change per event), this is a massive amount of wasted work as
//! sub count climbs.
//!
//! `PredicateIndex` maintains a reverse mapping:
//!
//!   column index → set of subscription ids whose predicate
//!                  references that column
//!
//! When a mutation reports its `changed_cols`, the dispatcher unions
//! the column→subs sets across those columns and only evaluates
//! that union. The set is a superset of actually-affected subs (a
//! sub that references `qty` but is currently false-for-this-row
//! still gets evaluated — see the proptest); false positives are
//! free, false negatives would be a correctness bug.
//!
//! `True`-predicate subscriptions (no WHERE clause) reference zero
//! columns. Those subs must STILL receive every event regardless of
//! which columns changed, so the index keeps an "always" set that's
//! unioned in unconditionally.

use std::collections::HashMap;

use crate::predicate::CompiledPredicate;

#[derive(Default)]
pub struct PredicateIndex {
    /// `col_idx → set of sub IDs whose predicate references col_idx`.
    col_to_subs: HashMap<usize, Vec<String>>,
    /// Subs whose predicate references zero columns (`CompiledPredicate::True`).
    /// These match every row, so every mutation must dispatch to them.
    always_subs: Vec<String>,
    /// Reverse map for cheap `remove(sub_id)`: which columns did we
    /// register this sub under?
    sub_to_cols: HashMap<String, Vec<usize>>,
}

impl PredicateIndex {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register `sub_id` with the columns its predicate references.
    /// Safe to call repeatedly with the same id — the second call
    /// removes the prior registration first (so a re-subscribe
    /// with a different predicate doesn't double-dispatch).
    pub fn add(&mut self, sub_id: &str, predicate: &CompiledPredicate) {
        // If the sub was already registered, drop its prior entries.
        if self.sub_to_cols.contains_key(sub_id) {
            self.remove(sub_id);
        }
        let cols = predicate.referenced_columns();
        if cols.is_empty() {
            self.always_subs.push(sub_id.to_string());
        } else {
            for col in &cols {
                self.col_to_subs
                    .entry(*col)
                    .or_default()
                    .push(sub_id.to_string());
            }
        }
        self.sub_to_cols.insert(sub_id.to_string(), cols);
    }

    /// Remove a subscription from every column's set + the always
    /// set. Cheap because we kept the reverse map.
    pub fn remove(&mut self, sub_id: &str) {
        if let Some(cols) = self.sub_to_cols.remove(sub_id) {
            if cols.is_empty() {
                self.always_subs.retain(|id| id != sub_id);
            } else {
                for col in cols {
                    if let Some(list) = self.col_to_subs.get_mut(&col) {
                        list.retain(|id| id != sub_id);
                        if list.is_empty() {
                            self.col_to_subs.remove(&col);
                        }
                    }
                }
            }
        }
    }

    /// Return the set of subscription IDs that need to be evaluated
    /// when a mutation reports it changed `changed_cols`. The
    /// returned vec includes:
    ///   - Every sub from `always_subs` (predicate references zero
    ///     columns — matches every row).
    ///   - Every sub registered against at least one column in
    ///     `changed_cols`.
    ///
    /// `None` for `changed_cols` means "I don't know which columns
    /// changed" — used by full upserts and recovery paths. In that
    /// case we return EVERY known sub (`always` + all of `col_to_subs`),
    /// preserving the pre-S46 behavior. The dispatcher still saves
    /// work on the common sparse-publish path.
    pub fn affected(&self, changed_cols: Option<&[usize]>) -> Vec<String> {
        let mut out: Vec<String> = self.always_subs.clone();
        match changed_cols {
            Some(cols) => {
                for c in cols {
                    if let Some(list) = self.col_to_subs.get(c) {
                        out.extend(list.iter().cloned());
                    }
                }
            }
            None => {
                // Unknown changed set: return every registered sub.
                for (_col, list) in &self.col_to_subs {
                    out.extend(list.iter().cloned());
                }
            }
        }
        out.sort();
        out.dedup();
        out
    }

    /// Total registered subscriptions (always + per-column union).
    pub fn len(&self) -> usize {
        self.sub_to_cols.len()
    }

    pub fn is_empty(&self) -> bool {
        self.sub_to_cols.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::predicate::CompiledPredicate;

    fn eq_long(col: usize) -> CompiledPredicate {
        CompiledPredicate::EqLong { col, value: 0 }
    }
    fn and(a: CompiledPredicate, b: CompiledPredicate) -> CompiledPredicate {
        CompiledPredicate::And(Box::new(a), Box::new(b))
    }

    #[test]
    fn always_sub_receives_every_event() {
        let mut idx = PredicateIndex::new();
        idx.add("always", &CompiledPredicate::True);
        idx.add("col_5", &eq_long(5));

        // Mutation on column 5 — both subs should match.
        let mut affected = idx.affected(Some(&[5]));
        affected.sort();
        assert_eq!(affected, vec!["always".to_string(), "col_5".to_string()]);

        // Mutation on a column nobody cares about — only the
        // always-sub should match.
        let affected = idx.affected(Some(&[99]));
        assert_eq!(affected, vec!["always".to_string()]);
    }

    #[test]
    fn predicate_touching_multiple_cols_indexed_under_each() {
        let mut idx = PredicateIndex::new();
        idx.add("multi", &and(eq_long(2), eq_long(7)));

        // Either column triggers the sub.
        assert_eq!(idx.affected(Some(&[2])), vec!["multi".to_string()]);
        assert_eq!(idx.affected(Some(&[7])), vec!["multi".to_string()]);
        // Both at once doesn't duplicate.
        assert_eq!(idx.affected(Some(&[2, 7])), vec!["multi".to_string()]);
    }

    #[test]
    fn remove_cleans_every_column_entry() {
        let mut idx = PredicateIndex::new();
        idx.add("a", &and(eq_long(1), eq_long(2)));
        idx.remove("a");
        assert_eq!(idx.affected(Some(&[1])), Vec::<String>::new());
        assert_eq!(idx.affected(Some(&[2])), Vec::<String>::new());
        assert!(idx.is_empty());
    }

    #[test]
    fn re_add_same_sub_id_drops_prior_predicate() {
        // Re-subscribing with a different predicate replaces the
        // prior registration — no stale entries.
        let mut idx = PredicateIndex::new();
        idx.add("s", &eq_long(1));
        idx.add("s", &eq_long(9));
        assert_eq!(idx.affected(Some(&[1])), Vec::<String>::new());
        assert_eq!(idx.affected(Some(&[9])), vec!["s".to_string()]);
    }

    #[test]
    fn changed_cols_none_returns_every_registered_sub() {
        // The "I don't know which columns changed" path (full
        // upserts) must dispatch to every sub — no false negatives.
        let mut idx = PredicateIndex::new();
        idx.add("a", &eq_long(1));
        idx.add("b", &eq_long(2));
        idx.add("always", &CompiledPredicate::True);
        let mut affected = idx.affected(None);
        affected.sort();
        assert_eq!(affected, vec!["a", "always", "b"]);
    }
}

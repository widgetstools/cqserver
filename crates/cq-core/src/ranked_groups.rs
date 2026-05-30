//! Ranked groups — the core of a live, sorted, windowed GROUP BY level for
//! AG-Grid SSRM (see `docs/AGGRID_SSRM_PLAN.md`, Phase 3).
//!
//! A grouped grid level shows groups ordered by an aggregate (e.g. `SUM(qty)
//! DESC`) and only displays a window `[offset, offset+limit)`. As underlying
//! rows tick, a group's aggregate changes and the group may move in the
//! ordering — possibly into or out of the visible window. This structure
//! maintains that window incrementally:
//!
//!   - `upsert(group, value)` — a group's aggregate value changed → re-rank in
//!     **O(log G)** and return only the visible-window changes.
//!   - `remove(group)` — the group's last member left.
//!
//! Recomputing + diffing the window is **O(limit)** — independent of the total
//! group count G. So a tick in one of 5,000 groups costs ~`O(log G) + O(limit)`,
//! not `O(rows)`.
//!
//! This is the group-level analogue of the row-level `TopNState`: a
//! `BTreeSet<(SortKey, group)>` plus the aggregate machinery that recomputes
//! only the dirty group. It is intentionally storage-agnostic (groups are
//! identified by their canonical key string and carry a single ordering
//! value) so it composes with the existing aggregate evaluator, which supplies
//! the recomputed value per dirty group.

use std::collections::{BTreeSet, HashMap};

/// Total-ordered wrapper for the ordering value of a group. Direction is baked
/// in by [`RankedGroups`] (it negates for descending), so the `BTreeSet`'s
/// natural ascending order is always the display order.
#[derive(Clone, Copy, PartialEq)]
struct OrdF64(f64);
impl Eq for OrdF64 {}
impl PartialOrd for OrdF64 {
    fn partial_cmp(&self, o: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(o))
    }
}
impl Ord for OrdF64 {
    fn cmp(&self, o: &Self) -> std::cmp::Ordering {
        self.0.total_cmp(&o.0)
    }
}

/// The change to the visible window produced by one `upsert`/`remove`. The
/// consumer (an AG-Grid SSRM adapter) maps these onto
/// `applyServerSideTransactionAsync` on the matching group-level route.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct WindowDelta {
    /// A group that entered the visible window (group key, ordering value).
    pub added: Vec<(String, f64)>,
    /// A group that left the visible window (group key).
    pub removed: Vec<String>,
    /// A still-visible group whose ordering value changed.
    pub updated: Vec<(String, f64)>,
}

impl WindowDelta {
    pub fn is_empty(&self) -> bool {
        self.added.is_empty() && self.removed.is_empty() && self.updated.is_empty()
    }
}

/// A live, sorted, windowed view over a set of groups keyed by an aggregate
/// ordering value. Ties between equal values are broken by the group key
/// (ascending) for a stable, deterministic order.
pub struct RankedGroups {
    ranked: BTreeSet<(OrdF64, String)>,
    /// group key → current ordering key part (so we can remove/re-insert it).
    key_of: HashMap<String, OrdF64>,
    offset: usize,
    limit: usize,
    /// The visible window as last computed: ordered (group, value).
    visible: Vec<(String, f64)>,
    desc: bool,
}

impl RankedGroups {
    pub fn new(offset: usize, limit: usize, desc: bool) -> Self {
        Self {
            ranked: BTreeSet::new(),
            key_of: HashMap::new(),
            offset,
            limit,
            visible: Vec::new(),
            desc,
        }
    }

    fn ord(&self, value: f64) -> OrdF64 {
        OrdF64(if self.desc { -value } else { value })
    }

    /// Insert or update a group's aggregate ordering value, re-ranking it, and
    /// return the resulting visible-window delta.
    pub fn upsert(&mut self, group: &str, value: f64) -> WindowDelta {
        if let Some(old) = self.key_of.get(group).copied() {
            self.ranked.remove(&(old, group.to_string()));
        }
        let k = self.ord(value);
        self.ranked.insert((k, group.to_string()));
        self.key_of.insert(group.to_string(), k);
        self.recompute_window()
    }

    /// Drop a group (its last member left the filter/group), re-ranking, and
    /// return the resulting visible-window delta.
    pub fn remove(&mut self, group: &str) -> WindowDelta {
        if let Some(old) = self.key_of.remove(group) {
            self.ranked.remove(&(old, group.to_string()));
        }
        self.recompute_window()
    }

    /// The current visible window, ordered (group key, ordering value).
    pub fn window(&self) -> &[(String, f64)] {
        &self.visible
    }

    /// Exact group total for the level's scrollbar — maintained, no scan.
    pub fn group_count(&self) -> usize {
        self.ranked.len()
    }

    fn value_of(&self, k: OrdF64) -> f64 {
        if self.desc {
            -k.0
        } else {
            k.0
        }
    }

    /// Recompute the `[offset, offset+limit)` slice and diff it against the
    /// previously-visible window. O(offset + limit) for the slice (offset is
    /// ~0 for the common "watching the top" case; an order-statistics tree
    /// makes deep windows O(log G) — see the plan doc) + O(limit) for the diff.
    fn recompute_window(&mut self) -> WindowDelta {
        let new: Vec<(String, f64)> = self
            .ranked
            .iter()
            .skip(self.offset)
            .take(self.limit)
            .map(|(k, g)| (g.clone(), self.value_of(*k)))
            .collect();

        let old_map: HashMap<&str, f64> =
            self.visible.iter().map(|(g, v)| (g.as_str(), *v)).collect();
        let new_set: std::collections::HashSet<&str> =
            new.iter().map(|(g, _)| g.as_str()).collect();

        let mut delta = WindowDelta::default();
        for (g, v) in &new {
            match old_map.get(g.as_str()) {
                None => delta.added.push((g.clone(), *v)),
                Some(prev) if *prev != *v => delta.updated.push((g.clone(), *v)),
                Some(_) => {}
            }
        }
        for (g, _) in &self.visible {
            if !new_set.contains(g.as_str()) {
                delta.removed.push(g.clone());
            }
        }

        self.visible = new;
        delta
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// Tiny deterministic xorshift PRNG — no dev-dependency needed.
    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            let mut x = self.0;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            self.0 = x;
            x
        }
        fn below(&mut self, n: usize) -> usize {
            (self.next() % n as u64) as usize
        }
        fn val(&mut self) -> f64 {
            (self.next() % 100_000) as f64 / 10.0
        }
    }

    /// Brute-force oracle: sort all groups, take the window.
    fn brute(
        vals: &HashMap<String, f64>,
        offset: usize,
        limit: usize,
        desc: bool,
    ) -> Vec<(String, f64)> {
        let mut v: Vec<(String, f64)> = vals.iter().map(|(k, &val)| (k.clone(), val)).collect();
        v.sort_by(|a, b| {
            let c = a.1.total_cmp(&b.1);
            let c = if desc { c.reverse() } else { c };
            c.then_with(|| a.0.cmp(&b.0))
        });
        v.into_iter().skip(offset).take(limit).collect()
    }

    fn run_random(seed: u64, n_groups: usize, offset: usize, limit: usize, desc: bool, ops: usize) {
        let mut rng = Rng(seed);
        let mut rg = RankedGroups::new(offset, limit, desc);
        let mut shadow: HashMap<String, f64> = HashMap::new();
        // A client-side mirror reconstructed purely from WindowDelta, to prove
        // the emitted deltas exactly describe the window transitions.
        let mut mirror: HashMap<String, f64> = HashMap::new();

        for _ in 0..ops {
            let g = format!("g{:04}", rng.below(n_groups));
            if rng.below(100) < 18 && shadow.contains_key(&g) {
                shadow.remove(&g);
                let d = rg.remove(&g);
                apply(&mut mirror, &d);
            } else {
                let v = rng.val();
                shadow.insert(g.clone(), v);
                let d = rg.upsert(&g, v);
                apply(&mut mirror, &d);
            }

            // 1. Maintained window must equal the brute-force window exactly.
            let expect = brute(&shadow, offset, limit, desc);
            assert_eq!(rg.window(), expect.as_slice(), "window diverged");

            // 2. The delta-reconstructed mirror must match the visible set.
            let vis: HashMap<&str, f64> =
                rg.window().iter().map(|(g, v)| (g.as_str(), *v)).collect();
            assert_eq!(mirror.len(), vis.len(), "mirror size != window");
            for (g, v) in &mirror {
                assert_eq!(vis.get(g.as_str()), Some(v), "mirror value mismatch for {g}");
            }

            // 3. Exact group count is maintained.
            assert_eq!(rg.group_count(), shadow.len());
        }
    }

    fn apply(mirror: &mut HashMap<String, f64>, d: &WindowDelta) {
        for g in &d.removed {
            mirror.remove(g);
        }
        for (g, v) in &d.added {
            mirror.insert(g.clone(), *v);
        }
        for (g, v) in &d.updated {
            mirror.insert(g.clone(), *v);
        }
    }

    #[test]
    fn window_stays_correct_top_desc() {
        // Top-of-grid window over many groups, sorted by a ticking aggregate.
        run_random(0x1234_5678, 500, 0, 30, true, 30_000);
    }

    #[test]
    fn window_stays_correct_top_asc() {
        run_random(0x9e37_79b9, 500, 0, 25, false, 30_000);
    }

    #[test]
    fn window_stays_correct_deep_offset() {
        // A deep scroll position still tracks exactly (just O(offset) slice).
        run_random(0xdead_beef, 800, 200, 20, true, 30_000);
    }

    #[test]
    fn window_stays_correct_small_universe() {
        // Few groups, window larger than the universe (everything visible).
        run_random(0x0bad_f00d, 8, 0, 50, true, 5_000);
    }

    #[test]
    fn basic_enter_leave_update() {
        let mut rg = RankedGroups::new(0, 2, true); // top 2 by value desc
        assert!(rg.upsert("a", 10.0).added == vec![("a".into(), 10.0)]);
        assert!(rg.upsert("b", 20.0).added == vec![("b".into(), 20.0)]); // window [b,a]
        // c=5 is below the top-2 → no window change.
        assert!(rg.upsert("c", 5.0).is_empty());
        assert_eq!(
            rg.window(),
            &[("b".into(), 20.0), ("a".into(), 10.0)][..]
        );
        // c jumps to 100 → enters top-2, pushes a out.
        let d = rg.upsert("c", 100.0);
        assert_eq!(d.added, vec![("c".into(), 100.0)]);
        assert_eq!(d.removed, vec!["a".to_string()]);
        assert_eq!(rg.window(), &[("c".into(), 100.0), ("b".into(), 20.0)][..]);
        // b updates in place (still visible).
        let d = rg.upsert("b", 25.0);
        assert_eq!(d.updated, vec![("b".into(), 25.0)]);
        assert!(d.added.is_empty() && d.removed.is_empty());
    }
}

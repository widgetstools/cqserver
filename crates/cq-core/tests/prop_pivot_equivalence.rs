//! Property test for S43: PIVOT output equals a hand-rolled
//! "GROUP BY anchor, bucket by pivot value, project" recompute over
//! the same input set.
//!
//! The contract: PIVOT is just sugar over a GROUP BY rewrite. For
//! any random set of (trader, desk, qty) triples + any random
//! pivot IN-list, the output of `PIVOT (SUM(qty) FOR desk IN (...))`
//! must equal the result of:
//!   1. Group rows by `trader`.
//!   2. For each (trader, desk) in the IN-list, sum the qty.
//!   3. Emit one row per trader with one column per pivot value.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use cq_core::schema::{ColumnType, Schema};
use cq_core::topic::{Topic, TopicConfig};
use proptest::prelude::*;
use serde_json::{json, Map, Value};

#[derive(Debug, Clone)]
struct Trade {
    trader: String,
    desk: String,
    qty: i64,
}

prop_compose! {
    fn any_trade()(
        trader_id in 0u8..6u8,
        desk_id in 0u8..5u8,
        qty in 1i64..10_000i64,
    ) -> Trade {
        Trade {
            trader: format!("t{trader_id}"),
            desk: format!("D{desk_id}"),
            qty,
        }
    }
}

fn make_topic() -> Topic {
    let schema = Arc::new(Schema::from_strs(
        &["trader", "desk", "qty"],
        &[ColumnType::String, ColumnType::String, ColumnType::Long],
    ));
    Topic::new(
        TopicConfig {
            name: "/prop-pivot".into(),
            key_fields: vec!["trader".into(), "desk".into()],
            persist: false,
            conflation_ms: None,
            index_columns: vec![],
            expire_seconds: None,
        },
        schema,
        256,
    )
}

fn apply(topic: &Topic, trade: &Trade) {
    let mut m = Map::new();
    m.insert("trader".into(), json!(trade.trader));
    m.insert("desk".into(), json!(trade.desk));
    m.insert("qty".into(), json!(trade.qty));
    topic.upsert_map(&m).expect("publish");
}

/// Reference pivot — apply trades to a HashMap-shaped SOW first
/// (so we get the same overwrite-on-key semantics as the topic),
/// then bucket by trader, filter on the IN-list, sum qty.
fn reference_pivot(trades: &[Trade], in_list: &[&str]) -> HashMap<String, HashMap<String, i64>> {
    // SOW: HashMap<(trader, desk), qty> — each (trader, desk) key
    // is overwritten by later trades.
    let mut sow: HashMap<(String, String), i64> = HashMap::new();
    for t in trades {
        sow.insert((t.trader.clone(), t.desk.clone()), t.qty);
    }

    // Anchors: every trader that has at least one row.
    let mut traders: BTreeSet<String> = BTreeSet::new();
    for (k, _) in &sow {
        traders.insert(k.0.clone());
    }

    let in_set: BTreeSet<&str> = in_list.iter().copied().collect();
    let mut out: HashMap<String, HashMap<String, i64>> = HashMap::new();
    for trader in &traders {
        let mut per_pivot: HashMap<String, i64> = HashMap::new();
        for desk in &in_set {
            if let Some(qty) = sow.get(&(trader.clone(), desk.to_string())) {
                per_pivot.insert(desk.to_string(), *qty);
            }
        }
        out.insert(trader.clone(), per_pivot);
    }
    out
}

/// Pull (trader → desk_col → value) from CQ's pivot output.
fn parse_cq_pivot(rows: &[Map<String, Value>]) -> HashMap<String, HashMap<String, i64>> {
    let mut out = HashMap::new();
    for row in rows {
        let trader = row
            .get("trader")
            .and_then(Value::as_str)
            .map(String::from)
            .unwrap_or_default();
        let mut per_pivot = HashMap::new();
        for (k, v) in row {
            if k == "trader" {
                continue;
            }
            if let Some(n) = v.as_i64() {
                per_pivot.insert(k.clone(), n);
            }
            // null bucket values are dropped — match the reference
        }
        out.insert(trader, per_pivot);
    }
    out
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    /// S43 property: random trade stream + random IN-list →
    /// PIVOT output equals the hand-rolled HashMap recompute.
    #[test]
    fn pivot_output_equals_manual_groupby_rewrite(
        trades in prop::collection::vec(any_trade(), 1..40),
        in_size in 1usize..5usize,
    ) {
        let topic = make_topic();
        for t in &trades {
            apply(&topic, t);
        }
        // Build the IN-list from the first `in_size` distinct desks
        // observed; if the trades didn't generate enough distinct
        // desks, pad with synthetic "D9", "D10" entries that won't
        // match anything (still valid IN-list).
        let mut distinct: BTreeSet<String> =
            trades.iter().map(|t| t.desk.clone()).collect();
        while distinct.len() < in_size {
            distinct.insert(format!("D{}", distinct.len() + 100));
        }
        let in_list: Vec<&str> = distinct.iter().take(in_size).map(String::as_str).collect();
        let in_quoted: Vec<String> =
            in_list.iter().map(|s| format!("'{s}'")).collect();
        let sql = format!(
            "SELECT * FROM t PIVOT (SUM(qty) FOR desk IN ({}))",
            in_quoted.join(", ")
        );

        let result = topic.query(&sql).expect("pivot query");
        let cq = parse_cq_pivot(&result.rows);
        let reference = reference_pivot(&trades, &in_list);

        prop_assert_eq!(cq, reference);
    }
}

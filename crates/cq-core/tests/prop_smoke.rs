//! Smoke test proving the proptest harness compiles and runs.
//!
//! Run with:
//!   cargo test --release --test prop_smoke
//!   PROPTEST_CASES=2000 cargo test --release --test prop_smoke

use proptest::prelude::*;

proptest! {
    /// Any String survives a JSON round-trip. Trivial property — the
    /// point is to prove the proptest scaffold works end-to-end so
    /// real correctness properties (S32, S33, S34) can follow the
    /// same pattern.
    #[test]
    fn json_roundtrip_any_string(s in any::<String>()) {
        let encoded = serde_json::to_string(&s).expect("encode");
        let decoded: String = serde_json::from_str(&encoded).expect("decode");
        prop_assert_eq!(s, decoded);
    }

    /// A Vec<i64> survives a JSON round-trip with element order
    /// preserved. Exercises proptest's collection strategy.
    #[test]
    fn json_roundtrip_vec_i64(v in prop::collection::vec(any::<i64>(), 0..64)) {
        let encoded = serde_json::to_string(&v).expect("encode");
        let decoded: Vec<i64> = serde_json::from_str(&encoded).expect("decode");
        prop_assert_eq!(v, decoded);
    }
}

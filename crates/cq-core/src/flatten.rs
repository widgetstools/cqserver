//! JSON flattener: converts nested JSON objects into flat key-value maps
//! using dot-notation for nested objects and bracket notation for arrays.
//!
//! Example:
//! ```json
//! { "counterparty": { "name": "GS", "addr": { "city": "NYC" } },
//!   "legs": [{ "ccy": "USD" }, { "ccy": "EUR" }] }
//! ```
//! Becomes:
//! ```text
//! counterparty.name     → "GS"
//! counterparty.addr.city → "NYC"
//! legs[0].ccy           → "USD"
//! legs[1].ccy           → "EUR"
//! ```

use serde_json::Value;
use std::collections::BTreeMap;

/// Configuration for the flattener.
#[derive(Debug, Clone)]
pub struct FlattenConfig {
    /// Maximum array index to flatten (prevents schema explosion from huge arrays).
    pub max_array_index: usize,
}

impl Default for FlattenConfig {
    fn default() -> Self {
        FlattenConfig {
            max_array_index: 10,
        }
    }
}

/// Flatten a JSON value into a map of dotted-path keys to scalar values.
pub fn flatten(value: &Value, config: &FlattenConfig) -> BTreeMap<String, Value> {
    let mut result = BTreeMap::new();
    flatten_recursive(value, String::new(), &mut result, config);
    result
}

fn flatten_recursive(
    value: &Value,
    prefix: String,
    result: &mut BTreeMap<String, Value>,
    config: &FlattenConfig,
) {
    match value {
        Value::Object(map) => {
            for (key, val) in map {
                let new_prefix = if prefix.is_empty() {
                    key.clone()
                } else {
                    format!("{}.{}", prefix, key)
                };
                flatten_recursive(val, new_prefix, result, config);
            }
        }
        Value::Array(arr) => {
            for (i, val) in arr.iter().enumerate() {
                if i >= config.max_array_index {
                    break;
                }
                let new_prefix = format!("{}[{}]", prefix, i);
                flatten_recursive(val, new_prefix, result, config);
            }
        }
        // Scalar values — store directly
        _ => {
            if !prefix.is_empty() {
                result.insert(prefix, value.clone());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_flat_object() {
        let input = json!({ "symbol": "AAPL", "price": 150.0 });
        let result = flatten(&input, &FlattenConfig::default());
        assert_eq!(result.get("symbol").unwrap(), "AAPL");
        assert_eq!(result.get("price").unwrap(), 150.0);
    }

    #[test]
    fn test_nested_object() {
        let input = json!({
            "counterparty": {
                "name": "GS",
                "address": { "city": "NYC" }
            }
        });
        let result = flatten(&input, &FlattenConfig::default());
        assert_eq!(result.get("counterparty.name").unwrap(), "GS");
        assert_eq!(result.get("counterparty.address.city").unwrap(), "NYC");
    }

    #[test]
    fn test_array_flattening() {
        let input = json!({
            "legs": [
                { "ccy": "USD", "notional": 1000000 },
                { "ccy": "EUR", "notional": 500000 }
            ]
        });
        let result = flatten(&input, &FlattenConfig::default());
        assert_eq!(result.get("legs[0].ccy").unwrap(), "USD");
        assert_eq!(result.get("legs[1].ccy").unwrap(), "EUR");
        assert_eq!(result.get("legs[0].notional").unwrap(), 1000000);
        assert_eq!(result.get("legs[1].notional").unwrap(), 500000);
    }

    #[test]
    fn test_max_array_index() {
        let input = json!({ "arr": [1, 2, 3, 4, 5] });
        let config = FlattenConfig { max_array_index: 3 };
        let result = flatten(&input, &config);
        assert!(result.contains_key("arr[0]"));
        assert!(result.contains_key("arr[2]"));
        assert!(!result.contains_key("arr[3]"));
    }
}

//! Integration test that runs the entire `corpus/` against both
//! CQServer's `Topic::query` and an in-process DataFusion session,
//! asserting result-set equality.
//!
//! Run with: `cargo test -p cq-differential-tests --release`

use cq_differential_tests::{load_corpus, run_case};

/// Harness self-test #1: a hand-crafted case that should pass.
#[test]
fn harness_passes_a_known_matching_case() {
    let case = serde_yaml::from_str(
        r#"
name: self_test_match
schema:
  - { name: id, type: long }
  - { name: v, type: long }
key: id
publishes:
  - { id: 1, v: 100 }
  - { id: 2, v: 200 }
query: "SELECT id, v FROM t WHERE id = 1"
expected_rows:
  - { id: 1, v: 100 }
"#,
    )
    .expect("parse self-test case");
    let result = run_case(&case);
    assert!(
        result.passed,
        "self-test failed: {}",
        result.failure_reason.unwrap_or_default()
    );
}

/// Harness self-test #2: a deliberately-wrong `expected_rows` should
/// surface as a failure with a useful message.
#[test]
fn harness_flags_a_deliberately_wrong_expected_with_useful_message() {
    let case = serde_yaml::from_str(
        r#"
name: self_test_mismatch
schema:
  - { name: id, type: long }
key: id
publishes:
  - { id: 1 }
  - { id: 2 }
query: "SELECT id FROM t"
expected_rows:
  - { id: 999 }
"#,
    )
    .expect("parse self-test case");
    let result = run_case(&case);
    assert!(
        !result.passed,
        "self-test should have failed but didn't (engines + harness all agreed with id=999)"
    );
    let reason = result.failure_reason.unwrap_or_default();
    assert!(
        reason.contains("999"),
        "failure reason should mention the diverging value; got: {reason}"
    );
}

/// Run the entire on-disk corpus. Each YAML file under `corpus/` is a
/// list of `TestCase`s; every entry runs against both engines.
///
/// A failure aggregates ALL failing cases before panicking, so the
/// developer sees the full delta in one shot rather than fixing one,
/// rerunning, finding the next, etc.
#[test]
fn corpus_against_datafusion() {
    let corpus_dir = format!("{}/corpus", env!("CARGO_MANIFEST_DIR"));
    let corpus = load_corpus(&corpus_dir).expect("load corpus");
    assert!(
        !corpus.is_empty(),
        "no test cases loaded from {corpus_dir}"
    );

    let mut passed = 0usize;
    let mut failures: Vec<(String, String)> = Vec::new();
    for case in &corpus {
        let result = run_case(case);
        if result.passed {
            passed += 1;
        } else {
            failures.push((
                result.name.clone(),
                result.failure_reason.unwrap_or_default(),
            ));
        }
    }
    eprintln!(
        "differential corpus: {} passed / {} failed (of {} total)",
        passed,
        failures.len(),
        corpus.len()
    );
    if !failures.is_empty() {
        let lines: Vec<String> = failures
            .iter()
            .map(|(n, r)| format!("\n[FAIL] {n}:\n{r}"))
            .collect();
        panic!(
            "{} of {} differential cases failed:{}",
            failures.len(),
            corpus.len(),
            lines.join("")
        );
    }
}

//! SQL predicate compiler: parses WHERE clauses into `CompiledPredicate`
//! chains that operate on column indices for zero-allocation hot-path evaluation.

use crate::schema::{ColumnType, Schema};
use crate::store::{ColumnStore, NULL_INT, NULL_LONG};
use compact_str::CompactString;
use std::collections::HashSet;

/// A compiled predicate that can evaluate a row in a ColumnStore.
/// All column references are pre-resolved to indices at parse time.
#[derive(Debug, Clone)]
pub enum CompiledPredicate {
    // ---- Leaf comparisons ----
    EqDouble {
        col: usize,
        value: f64,
    },
    NeqDouble {
        col: usize,
        value: f64,
    },
    LtDouble {
        col: usize,
        value: f64,
    },
    LeDouble {
        col: usize,
        value: f64,
    },
    GtDouble {
        col: usize,
        value: f64,
    },
    GeDouble {
        col: usize,
        value: f64,
    },
    BetweenDouble {
        col: usize,
        low: f64,
        high: f64,
    },

    EqLong {
        col: usize,
        value: i64,
    },
    NeqLong {
        col: usize,
        value: i64,
    },
    LtLong {
        col: usize,
        value: i64,
    },
    LeLong {
        col: usize,
        value: i64,
    },
    GtLong {
        col: usize,
        value: i64,
    },
    GeLong {
        col: usize,
        value: i64,
    },
    BetweenLong {
        col: usize,
        low: i64,
        high: i64,
    },

    EqString {
        col: usize,
        value: CompactString,
    },
    NeqString {
        col: usize,
        value: CompactString,
    },
    InString {
        col: usize,
        values: HashSet<CompactString>,
    },
    /// `col IN (1, 2, 3)` for `Long` and `Int` columns. Pre-S46 the
    /// compiler always emitted `InString` regardless of column type;
    /// applying that to a numeric column then panicked in
    /// `ColumnStore::get_string` because the array_index pointed at
    /// the string-arena slot that didn't exist for the numeric column.
    InLong {
        col: usize,
        values: std::collections::HashSet<i64>,
    },
    /// `col IN (1.0, 2.5)` for `Double` columns. F64 doesn't implement
    /// `Hash` so we key the set by `to_bits()` — exact-bitpattern
    /// equality, matching SQL `=` semantics on floats (NaN never
    /// matches anything, including itself).
    InDouble {
        col: usize,
        values: std::collections::HashSet<u64>,
    },
    /// `flag IN (true, false)` — Bool columns. With only two non-null
    /// values, the set is at most {true, false}, but we keep it as a
    /// HashSet so the matcher stays uniform with the other `In*` arms.
    InBool {
        col: usize,
        values: HashSet<bool>,
    },
    /// `flag = true` — exact boolean equality.
    EqBool { col: usize, value: bool },
    /// `flag != true` — boolean inequality. SQL three-valued logic:
    /// NULL booleans never match either side.
    NeqBool { col: usize, value: bool },

    /// Timestamp ops. `value` is i64 microseconds since UNIX epoch —
    /// the wire form (RFC 3339 string) is parsed at compile time.
    /// Null timestamps (= i64::MIN) never compare equal/less/greater.
    EqTimestamp { col: usize, value: i64 },
    NeqTimestamp { col: usize, value: i64 },
    LtTimestamp { col: usize, value: i64 },
    LeTimestamp { col: usize, value: i64 },
    GtTimestamp { col: usize, value: i64 },
    GeTimestamp { col: usize, value: i64 },
    BetweenTimestamp { col: usize, low: i64, high: i64 },
    InTimestamp { col: usize, values: HashSet<i64> },
    Like {
        col: usize,
        pattern: regex::Regex,
    },
    /// P13 — `MATCHES_REGEX(col, '<pattern>')`. The pattern is
    /// compiled at parse time so an invalid pattern is rejected as
    /// a `PredicateError::InvalidPattern` before any row is read.
    Regex {
        col: usize,
        pattern: regex::Regex,
    },

    /// `UPPER(col) = 'X'` or `LOWER(col) = 'x'`. Applies the case
    /// transform to the row value before comparison; the literal is
    /// pre-normalized at compile time. Saves an allocation per row
    /// vs always allocating a transformed copy.
    EqStringFn {
        col: usize,
        func: StringFn,
        /// Pre-normalized literal (already in the target case).
        value: CompactString,
    },
    NeqStringFn {
        col: usize,
        func: StringFn,
        value: CompactString,
    },
    /// `UPPER(col) LIKE 'X%'` / `LOWER(col) LIKE 'x%'`. The pattern
    /// is compiled once at parse time; we apply the case transform
    /// per row before matching.
    LikeStringFn {
        col: usize,
        func: StringFn,
        pattern: regex::Regex,
    },

    /// `LENGTH(col) <op> N`. Counts UTF-8 chars (not bytes), matching
    /// SQL `CHAR_LENGTH` semantics — most SQL dialects' `LENGTH` on
    /// strings is char-count.
    LengthCmp {
        col: usize,
        op: NumCmpOp,
        value: i64,
    },

    /// Compare a structured string expression (SUBSTR / CONCAT and
    /// arbitrary nests of them, plus UPPER / LOWER) to a literal.
    /// Slower than the fast-path single-column variants above
    /// because every match allocates the intermediate `String`; only
    /// fires when the LHS isn't a single column or single UPPER /
    /// LOWER call.
    EqStringExpr {
        expr: StringExpr,
        value: CompactString,
    },
    NeqStringExpr {
        expr: StringExpr,
        value: CompactString,
    },
    LikeStringExpr {
        expr: StringExpr,
        pattern: regex::Regex,
    },

    IsNull {
        col: usize,
    },
    IsNotNull {
        col: usize,
    },
    /// R12 — `<bool-col> IS TRUE / IS NOT TRUE / IS FALSE / IS NOT FALSE`.
    /// `IS TRUE` is `value == true` (NULL is NOT true), `IS NOT TRUE`
    /// is `value != true OR NULL` (matches NULL too). Same for FALSE.
    /// This mirrors SQL three-valued logic.
    IsBool {
        col: usize,
        want_true: bool,
        negated: bool,
    },

    /// R2 — Generic numeric-expression comparison. Used by anything
    /// the simpler `EqLong/GtDouble/...` variants can't cover:
    ///   - Scalar functions: `WHERE ABS(col) > N`, `ROUND(x, 2) >= K`
    ///   - WHERE-side arithmetic: `WHERE a + b > 100`
    ///   - Two-column comparison: `WHERE col1 > col2`
    /// The RHS is always reduced to an f64 at compile time (literal
    /// or constant-folded expression); the LHS NumExpr is walked
    /// per-row.
    CompareNum {
        expr: NumExpr,
        op: NumCmpOp,
        value: f64,
    },

    /// R2 — `NumExpr BETWEEN low AND high`. Inclusive on both sides
    /// to match SQL semantics.
    BetweenNum {
        expr: NumExpr,
        low: f64,
        high: f64,
    },

    // ---- Combinators ----
    And(Box<CompiledPredicate>, Box<CompiledPredicate>),
    Or(Box<CompiledPredicate>, Box<CompiledPredicate>),
    Not(Box<CompiledPredicate>),

    /// Always true (no WHERE clause).
    True,
}

impl CompiledPredicate {
    /// Collect every column index referenced by this predicate. Used by
    /// `PredicateIndex` (S46 / review concern C3) — a sparse update
    /// that only touches columns C can skip evaluation for any
    /// subscription whose predicate doesn't reference any column in C.
    ///
    /// Returns a sorted dedup'd `Vec<usize>` (small N — typical
    /// predicates touch 1-5 columns; Vec is cheaper than HashSet).
    pub fn referenced_columns(&self) -> Vec<usize> {
        let mut out = Vec::new();
        self.collect_referenced(&mut out);
        out.sort_unstable();
        out.dedup();
        out
    }

    pub fn collect_referenced(&self, out: &mut Vec<usize>) {
        use CompiledPredicate::*;
        match self {
            EqDouble { col, .. }
            | NeqDouble { col, .. }
            | LtDouble { col, .. }
            | LeDouble { col, .. }
            | GtDouble { col, .. }
            | GeDouble { col, .. }
            | BetweenDouble { col, .. }
            | EqLong { col, .. }
            | NeqLong { col, .. }
            | LtLong { col, .. }
            | LeLong { col, .. }
            | GtLong { col, .. }
            | GeLong { col, .. }
            | BetweenLong { col, .. }
            | EqString { col, .. }
            | NeqString { col, .. }
            | InString { col, .. }
            | InLong { col, .. }
            | InDouble { col, .. }
            | InBool { col, .. }
            | EqBool { col, .. }
            | NeqBool { col, .. }
            | EqTimestamp { col, .. }
            | NeqTimestamp { col, .. }
            | LtTimestamp { col, .. }
            | LeTimestamp { col, .. }
            | GtTimestamp { col, .. }
            | GeTimestamp { col, .. }
            | BetweenTimestamp { col, .. }
            | InTimestamp { col, .. }
            | Like { col, .. }
            | Regex { col, .. }
            | EqStringFn { col, .. }
            | NeqStringFn { col, .. }
            | LikeStringFn { col, .. }
            | LengthCmp { col, .. }
            | IsNull { col }
            | IsNotNull { col }
            | IsBool { col, .. } => out.push(*col),
            EqStringExpr { expr, .. } | NeqStringExpr { expr, .. } | LikeStringExpr { expr, .. } => {
                expr.referenced_columns(out);
            }
            CompareNum { expr, .. } | BetweenNum { expr, .. } => {
                expr.referenced_columns(out);
            }
            And(a, b) | Or(a, b) => {
                a.collect_referenced(out);
                b.collect_referenced(out);
            }
            Not(inner) => inner.collect_referenced(out),
            True => {}
        }
    }
}

/// Case-folding transform used by `UPPER` / `LOWER` predicates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StringFn {
    Upper,
    Lower,
}

/// Mini AST for string-valued expressions in WHERE clauses. Used by
/// `EqStringExpr` / `LikeStringExpr` to evaluate function calls like
/// `SUBSTR(col, 1, 3)` or `CONCAT(desk, '-', book)` over each row.
#[derive(Debug, Clone)]
pub enum StringExpr {
    Col(usize),
    Lit(CompactString),
    Upper(Box<StringExpr>),
    Lower(Box<StringExpr>),
    /// `SUBSTR(inner, start, len)` — 1-based start; `len = None` means
    /// "to the end". Negative / out-of-range values clamp to empty,
    /// matching the most permissive SQL dialect (Postgres/SQLite).
    Substr {
        inner: Box<StringExpr>,
        start: i64,
        len: Option<i64>,
    },
    /// `CONCAT(a, b, ...)`. Nulls in any part are treated as the empty
    /// string (Postgres-style `CONCAT`, not `||`).
    Concat(Vec<StringExpr>),
}

impl StringExpr {
    /// Collect every column index referenced by this expression
    /// (transitively through Upper/Lower/Substr/Concat). Used by
    /// `PredicateIndex` (S46 / C3) to route mutations: a sparse
    /// update that touches column set C only needs to evaluate
    /// subscriptions whose predicate references at least one
    /// column in C.
    pub fn referenced_columns(&self, out: &mut Vec<usize>) {
        match self {
            StringExpr::Col(c) => out.push(*c),
            StringExpr::Lit(_) => {}
            StringExpr::Upper(inner) | StringExpr::Lower(inner) => {
                inner.referenced_columns(out);
            }
            StringExpr::Substr { inner, .. } => inner.referenced_columns(out),
            StringExpr::Concat(parts) => {
                for p in parts {
                    p.referenced_columns(out);
                }
            }
        }
    }

    /// Evaluate the expression against `row`. Returns `None` only when
    /// a referenced column is null *and* the expression doesn't define
    /// a defaulting rule (e.g., `Col(c)` returns `None` on null;
    /// `Concat` returns `Some("")` because every null part is the
    /// empty string).
    pub fn eval(&self, store: &ColumnStore, row: u32) -> Option<String> {
        match self {
            StringExpr::Col(c) => store.get_string(*c, row).map(|s| s.to_string()),
            StringExpr::Lit(s) => Some(s.to_string()),
            StringExpr::Upper(inner) => Some(inner.eval(store, row)?.to_uppercase()),
            StringExpr::Lower(inner) => Some(inner.eval(store, row)?.to_lowercase()),
            StringExpr::Substr { inner, start, len } => {
                let s = inner.eval(store, row)?;
                // Char-based slicing keeps multibyte input safe.
                let chars: Vec<char> = s.chars().collect();
                let n = chars.len();
                let start_idx = if *start <= 0 {
                    0
                } else {
                    ((*start as usize) - 1).min(n)
                };
                let end_idx = match len {
                    Some(l) if *l <= 0 => start_idx,
                    Some(l) => start_idx.saturating_add(*l as usize).min(n),
                    None => n,
                };
                Some(chars[start_idx..end_idx].iter().collect())
            }
            StringExpr::Concat(parts) => {
                let mut out = String::new();
                for p in parts {
                    if let Some(s) = p.eval(store, row) {
                        out.push_str(&s);
                    }
                }
                Some(out)
            }
        }
    }
}

/// Comparison operator for numeric expressions like `LENGTH(col) > 5`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NumCmpOp {
    Eq,
    Neq,
    Lt,
    Le,
    Gt,
    Ge,
}

impl NumCmpOp {
    fn apply(&self, lhs: i64, rhs: i64) -> bool {
        match self {
            NumCmpOp::Eq => lhs == rhs,
            NumCmpOp::Neq => lhs != rhs,
            NumCmpOp::Lt => lhs < rhs,
            NumCmpOp::Le => lhs <= rhs,
            NumCmpOp::Gt => lhs > rhs,
            NumCmpOp::Ge => lhs >= rhs,
        }
    }

    /// R2 — f64 comparison used by the scalar-expression predicate.
    /// NaN never satisfies any comparison (mirrors SQL three-valued
    /// logic: NULL ≠ anything, including itself).
    fn apply_f64(&self, lhs: f64, rhs: f64) -> bool {
        if lhs.is_nan() || rhs.is_nan() {
            return false;
        }
        match self {
            NumCmpOp::Eq => lhs == rhs,
            NumCmpOp::Neq => lhs != rhs,
            NumCmpOp::Lt => lhs < rhs,
            NumCmpOp::Le => lhs <= rhs,
            NumCmpOp::Gt => lhs > rhs,
            NumCmpOp::Ge => lhs >= rhs,
        }
    }
}

/// R2 — Mini AST for numeric expressions in WHERE / HAVING clauses.
/// Mirrors `StringExpr` in shape. Used by `CompareNum` /
/// `BetweenNum` to evaluate `ABS(col)`, `ROUND(col, 2)`, `a + b`,
/// `col1 - col2`, etc. against each row. Every leaf is either a
/// column read (coerced to f64 via the store's `value_as_f64`) or a
/// literal; intermediate ops are pure f64 arithmetic.
///
/// Why f64 throughout: AMPS evaluates scalar arithmetic in
/// double-precision regardless of the column's storage type. Mixing
/// Long + Double in `WHERE qty * price > N` is the common case;
/// promoting everything to f64 matches AMPS's behaviour and
/// sidesteps a combinatorial expansion of typed variants.
#[derive(Debug, Clone)]
pub enum NumExpr {
    Col(usize),
    Lit(f64),
    Add(Box<NumExpr>, Box<NumExpr>),
    Sub(Box<NumExpr>, Box<NumExpr>),
    Mul(Box<NumExpr>, Box<NumExpr>),
    Div(Box<NumExpr>, Box<NumExpr>),
    Neg(Box<NumExpr>),
    Abs(Box<NumExpr>),
    Round(Box<NumExpr>),
    Floor(Box<NumExpr>),
    Ceil(Box<NumExpr>),
    /// R4 — `COALESCE(a, b, …)`: first non-NaN argument. Empty list
    /// rejected at parse time.
    Coalesce(Vec<NumExpr>),
    /// R4 — `NULLIF(a, b)`: NaN when a == b, else a. The "guard
    /// against division by zero" pattern: `SUM(x) / NULLIF(SUM(y), 0)`.
    NullIf(Box<NumExpr>, Box<NumExpr>),
    /// R7 — `NOW()`: current wall-clock time in microseconds since
    /// the UNIX epoch, evaluated ONCE at compile time. AMPS's
    /// time-window queries (`WHERE ts > NOW() - 86400000000` for
    /// "last 24h") use this together with NumExpr arithmetic. The
    /// constant frozen at compile time matches AMPS's "snapshot
    /// clock" semantics — every row in one SOW sees the same NOW.
    NowMicros(i64),
}

impl NumExpr {
    /// Walk the expression and append every referenced column index
    /// to `out`. Used by `collect_referenced` + view evaluators.
    pub fn referenced_columns(&self, out: &mut Vec<usize>) {
        match self {
            NumExpr::Col(c) => out.push(*c),
            NumExpr::Lit(_) => {}
            NumExpr::Add(a, b) | NumExpr::Sub(a, b) | NumExpr::Mul(a, b) | NumExpr::Div(a, b) => {
                a.referenced_columns(out);
                b.referenced_columns(out);
            }
            NumExpr::Neg(x)
            | NumExpr::Abs(x)
            | NumExpr::Round(x)
            | NumExpr::Floor(x)
            | NumExpr::Ceil(x) => x.referenced_columns(out),
            NumExpr::Coalesce(parts) => {
                for p in parts {
                    p.referenced_columns(out);
                }
            }
            NumExpr::NullIf(a, b) => {
                a.referenced_columns(out);
                b.referenced_columns(out);
            }
            NumExpr::NowMicros(_) => {}
        }
    }

    /// Evaluate against the store at `row`. Returns NaN for any
    /// operand that's NULL (so downstream comparisons via
    /// `NumCmpOp::apply_f64` correctly produce false — matching
    /// AMPS's "NULL propagates" semantics for arithmetic).
    pub fn eval(&self, store: &ColumnStore, row: u32) -> f64 {
        match self {
            NumExpr::Col(c) => {
                let v = store.get(*c, row);
                v.as_f64().unwrap_or(f64::NAN)
            }
            NumExpr::Lit(v) => *v,
            NumExpr::Add(a, b) => a.eval(store, row) + b.eval(store, row),
            NumExpr::Sub(a, b) => a.eval(store, row) - b.eval(store, row),
            NumExpr::Mul(a, b) => a.eval(store, row) * b.eval(store, row),
            NumExpr::Div(a, b) => {
                let rhs = b.eval(store, row);
                if rhs == 0.0 {
                    f64::NAN
                } else {
                    a.eval(store, row) / rhs
                }
            }
            NumExpr::Neg(x) => -x.eval(store, row),
            NumExpr::Abs(x) => x.eval(store, row).abs(),
            NumExpr::Round(x) => x.eval(store, row).round(),
            NumExpr::Floor(x) => x.eval(store, row).floor(),
            NumExpr::Ceil(x) => x.eval(store, row).ceil(),
            NumExpr::Coalesce(parts) => {
                for p in parts {
                    let v = p.eval(store, row);
                    if !v.is_nan() {
                        return v;
                    }
                }
                f64::NAN
            }
            NumExpr::NullIf(a, b) => {
                let av = a.eval(store, row);
                let bv = b.eval(store, row);
                if !av.is_nan() && !bv.is_nan() && av == bv {
                    f64::NAN
                } else {
                    av
                }
            }
            NumExpr::NowMicros(t) => *t as f64,
        }
    }
}

/// SQL three-valued logic result. `Unknown` arises whenever a
/// comparison touches a NULL operand. A WHERE clause keeps a row only
/// when the predicate evaluates to `True` — both `False` and `Unknown`
/// exclude it.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Tri {
    True,
    False,
    Unknown,
}

#[inline]
fn tri(b: bool) -> Tri {
    if b {
        Tri::True
    } else {
        Tri::False
    }
}

/// Whether column `col` holds SQL NULL at `row`, per the column's type
/// sentinel. Mirrors the `IsNull` leaf logic.
#[inline]
fn column_is_null(store: &ColumnStore, col: usize, row: u32) -> bool {
    match store.schema().column_type(col) {
        ColumnType::Double => store.get_double(col, row).is_nan(),
        ColumnType::Long => store.get_long(col, row) == NULL_LONG,
        ColumnType::Int => store.get_int(col, row) == NULL_INT,
        ColumnType::String => store.get_string(col, row).is_none(),
        ColumnType::Bool => store.get_bool(col, row).is_none(),
        ColumnType::Timestamp => store.get_timestamp(col, row) == crate::store::NULL_TIMESTAMP,
        ColumnType::Bytes => store.get(col, row).is_null(),
    }
}

impl CompiledPredicate {
    /// Evaluate against a row for WHERE semantics: `true` only when the
    /// predicate is definitely TRUE. UNKNOWN (NULL operand) excludes
    /// the row, matching SQL three-valued logic.
    #[inline]
    pub fn matches(&self, store: &ColumnStore, row: u32) -> bool {
        self.matches_3vl(store, row) == Tri::True
    }

    /// Three-valued evaluation. Combinators follow SQL Kleene logic;
    /// comparison leaves return `Unknown` when their operand is NULL.
    /// `IS [NOT] NULL` and `IS [NOT] TRUE/FALSE` are total predicates —
    /// they map NULL to a definite TRUE/FALSE.
    pub fn matches_3vl(&self, store: &ColumnStore, row: u32) -> Tri {
        match self {
            CompiledPredicate::And(left, right) => {
                match (
                    left.matches_3vl(store, row),
                    right.matches_3vl(store, row),
                ) {
                    (Tri::False, _) | (_, Tri::False) => Tri::False,
                    (Tri::True, Tri::True) => Tri::True,
                    _ => Tri::Unknown,
                }
            }
            CompiledPredicate::Or(left, right) => {
                match (
                    left.matches_3vl(store, row),
                    right.matches_3vl(store, row),
                ) {
                    (Tri::True, _) | (_, Tri::True) => Tri::True,
                    (Tri::False, Tri::False) => Tri::False,
                    _ => Tri::Unknown,
                }
            }
            CompiledPredicate::Not(inner) => match inner.matches_3vl(store, row) {
                Tri::True => Tri::False,
                Tri::False => Tri::True,
                Tri::Unknown => Tri::Unknown,
            },
            CompiledPredicate::True => Tri::True,
            CompiledPredicate::IsNull { .. }
            | CompiledPredicate::IsNotNull { .. }
            | CompiledPredicate::IsBool { .. } => tri(self.matches_leaf_bool(store, row)),
            leaf => {
                if leaf.operand_is_null(store, row) {
                    Tri::Unknown
                } else {
                    tri(leaf.matches_leaf_bool(store, row))
                }
            }
        }
    }

    /// True when this comparison leaf's operand (column or expression)
    /// is NULL at `row`. Combinators and null-test leaves return false
    /// (they're dispatched before this is reached).
    fn operand_is_null(&self, store: &ColumnStore, row: u32) -> bool {
        use CompiledPredicate::*;
        match self {
            EqDouble { col, .. }
            | NeqDouble { col, .. }
            | LtDouble { col, .. }
            | LeDouble { col, .. }
            | GtDouble { col, .. }
            | GeDouble { col, .. }
            | BetweenDouble { col, .. }
            | EqLong { col, .. }
            | NeqLong { col, .. }
            | LtLong { col, .. }
            | LeLong { col, .. }
            | GtLong { col, .. }
            | GeLong { col, .. }
            | BetweenLong { col, .. }
            | EqString { col, .. }
            | NeqString { col, .. }
            | InString { col, .. }
            | InLong { col, .. }
            | InDouble { col, .. }
            | InBool { col, .. }
            | EqBool { col, .. }
            | NeqBool { col, .. }
            | EqTimestamp { col, .. }
            | NeqTimestamp { col, .. }
            | LtTimestamp { col, .. }
            | LeTimestamp { col, .. }
            | GtTimestamp { col, .. }
            | GeTimestamp { col, .. }
            | BetweenTimestamp { col, .. }
            | InTimestamp { col, .. }
            | Like { col, .. }
            | Regex { col, .. }
            | EqStringFn { col, .. }
            | NeqStringFn { col, .. }
            | LikeStringFn { col, .. }
            | LengthCmp { col, .. } => column_is_null(store, *col, row),
            EqStringExpr { expr, .. } | NeqStringExpr { expr, .. } | LikeStringExpr { expr, .. } => {
                expr.eval(store, row).is_none()
            }
            CompareNum { expr, .. } | BetweenNum { expr, .. } => expr.eval(store, row).is_nan(),
            IsNull { .. } | IsNotNull { .. } | IsBool { .. } | And(..) | Or(..) | Not(..)
            | True => false,
        }
    }

    /// Leaf boolean evaluation without NULL/3VL semantics. Only invoked
    /// for a leaf whose operand is already known non-NULL (or for the
    /// total null-test predicates). Combinators are unreachable here.
    #[inline]
    fn matches_leaf_bool(&self, store: &ColumnStore, row: u32) -> bool {
        match self {
            // ---- Double comparisons ----
            // IEEE 754 equality with a NaN guard. The previous EPSILON
            // fuzz mis-fired for large magnitudes (eg. 1e9 + EPSILON
            // tested equal to 1e9). NaN stored values never match
            // anything by design.
            CompiledPredicate::EqDouble { col, value } => {
                let v = store.get_double(*col, row);
                !v.is_nan() && v == *value
            }
            CompiledPredicate::NeqDouble { col, value } => {
                let v = store.get_double(*col, row);
                v.is_nan() || v != *value
            }
            CompiledPredicate::LtDouble { col, value } => {
                let v = store.get_double(*col, row);
                !v.is_nan() && v < *value
            }
            CompiledPredicate::LeDouble { col, value } => {
                let v = store.get_double(*col, row);
                !v.is_nan() && v <= *value
            }
            CompiledPredicate::GtDouble { col, value } => {
                let v = store.get_double(*col, row);
                !v.is_nan() && v > *value
            }
            CompiledPredicate::GeDouble { col, value } => {
                let v = store.get_double(*col, row);
                !v.is_nan() && v >= *value
            }
            CompiledPredicate::BetweenDouble { col, low, high } => {
                let v = store.get_double(*col, row);
                !v.is_nan() && v >= *low && v <= *high
            }

            // ---- Long comparisons ----
            CompiledPredicate::EqLong { col, value } => {
                let v = store.get_long(*col, row);
                v != NULL_LONG && v == *value
            }
            CompiledPredicate::NeqLong { col, value } => {
                let v = store.get_long(*col, row);
                v == NULL_LONG || v != *value
            }
            CompiledPredicate::LtLong { col, value } => {
                let v = store.get_long(*col, row);
                v != NULL_LONG && v < *value
            }
            CompiledPredicate::LeLong { col, value } => {
                let v = store.get_long(*col, row);
                v != NULL_LONG && v <= *value
            }
            CompiledPredicate::GtLong { col, value } => {
                let v = store.get_long(*col, row);
                v != NULL_LONG && v > *value
            }
            CompiledPredicate::GeLong { col, value } => {
                let v = store.get_long(*col, row);
                v != NULL_LONG && v >= *value
            }
            CompiledPredicate::BetweenLong { col, low, high } => {
                let v = store.get_long(*col, row);
                v != NULL_LONG && v >= *low && v <= *high
            }

            // ---- String comparisons ----
            CompiledPredicate::EqString { col, value } => {
                store.get_string(*col, row).map_or(false, |s| s == value.as_str())
            }
            CompiledPredicate::NeqString { col, value } => {
                store.get_string(*col, row).map_or(true, |s| s != value.as_str())
            }
            CompiledPredicate::InString { col, values } => {
                // `CompactString: Borrow<str>` lets us probe the set
                // with the borrowed `&str` directly — no per-row
                // allocation on the scan hot path.
                store
                    .get_string(*col, row)
                    .map_or(false, |s| values.contains(s))
            }
            CompiledPredicate::InLong { col, values } => {
                let v = store.get_long(*col, row);
                if v == crate::store::NULL_LONG {
                    return false;
                }
                values.contains(&v)
            }
            CompiledPredicate::InDouble { col, values } => {
                let v = store.get_double(*col, row);
                if v.is_nan() {
                    // SQL `IN` returns UNKNOWN for NULL/NaN — treated
                    // as false in a WHERE clause.
                    return false;
                }
                values.contains(&v.to_bits())
            }
            CompiledPredicate::InBool { col, values } => {
                match store.get_bool(*col, row) {
                    Some(b) => values.contains(&b),
                    None => false, // NULL bool — SQL IN returns UNKNOWN.
                }
            }
            CompiledPredicate::EqBool { col, value } => {
                store.get_bool(*col, row).map_or(false, |b| b == *value)
            }
            CompiledPredicate::NeqBool { col, value } => {
                // SQL three-valued logic: NULL != anything is UNKNOWN.
                store.get_bool(*col, row).map_or(false, |b| b != *value)
            }
            CompiledPredicate::EqTimestamp { col, value } => {
                let v = store.get_timestamp(*col, row);
                v != crate::store::NULL_TIMESTAMP && v == *value
            }
            CompiledPredicate::NeqTimestamp { col, value } => {
                let v = store.get_timestamp(*col, row);
                v != crate::store::NULL_TIMESTAMP && v != *value
            }
            CompiledPredicate::LtTimestamp { col, value } => {
                let v = store.get_timestamp(*col, row);
                v != crate::store::NULL_TIMESTAMP && v < *value
            }
            CompiledPredicate::LeTimestamp { col, value } => {
                let v = store.get_timestamp(*col, row);
                v != crate::store::NULL_TIMESTAMP && v <= *value
            }
            CompiledPredicate::GtTimestamp { col, value } => {
                let v = store.get_timestamp(*col, row);
                v != crate::store::NULL_TIMESTAMP && v > *value
            }
            CompiledPredicate::GeTimestamp { col, value } => {
                let v = store.get_timestamp(*col, row);
                v != crate::store::NULL_TIMESTAMP && v >= *value
            }
            CompiledPredicate::BetweenTimestamp { col, low, high } => {
                let v = store.get_timestamp(*col, row);
                v != crate::store::NULL_TIMESTAMP && v >= *low && v <= *high
            }
            CompiledPredicate::InTimestamp { col, values } => {
                let v = store.get_timestamp(*col, row);
                if v == crate::store::NULL_TIMESTAMP {
                    return false;
                }
                values.contains(&v)
            }
            CompiledPredicate::Like { col, pattern } => {
                store.get_string(*col, row).map_or(false, |s| pattern.is_match(s))
            }
            CompiledPredicate::Regex { col, pattern } => {
                // P13 — MATCHES_REGEX uses the regex directly (no
                // LIKE-style `%` / `_` mapping).
                store.get_string(*col, row).map_or(false, |s| pattern.is_match(s))
            }

            CompiledPredicate::EqStringFn { col, func, value } => {
                store.get_string(*col, row).map_or(false, |s| {
                    string_fn_eq(*func, s, value.as_str())
                })
            }
            CompiledPredicate::NeqStringFn { col, func, value } => {
                store.get_string(*col, row).map_or(true, |s| {
                    !string_fn_eq(*func, s, value.as_str())
                })
            }
            CompiledPredicate::LikeStringFn { col, func, pattern } => {
                store.get_string(*col, row).map_or(false, |s| {
                    let transformed = string_fn_apply(*func, s);
                    pattern.is_match(&transformed)
                })
            }
            CompiledPredicate::LengthCmp { col, op, value } => {
                store.get_string(*col, row).map_or(false, |s| {
                    // Char count, not byte count — matches SQL CHAR_LENGTH.
                    let n = s.chars().count() as i64;
                    op.apply(n, *value)
                })
            }

            CompiledPredicate::EqStringExpr { expr, value } => {
                expr.eval(store, row).map_or(false, |s| s == value.as_str())
            }
            CompiledPredicate::NeqStringExpr { expr, value } => {
                expr.eval(store, row).map_or(true, |s| s != value.as_str())
            }
            CompiledPredicate::LikeStringExpr { expr, pattern } => {
                expr.eval(store, row).map_or(false, |s| pattern.is_match(&s))
            }

            // ---- Null checks ----
            CompiledPredicate::IsNull { col } => {
                let schema = store.schema();
                match schema.column_type(*col) {
                    ColumnType::Double => store.get_double(*col, row).is_nan(),
                    ColumnType::Long => store.get_long(*col, row) == NULL_LONG,
                    ColumnType::Int => store.get_int(*col, row) == NULL_INT,
                    ColumnType::String => store.get_string(*col, row).is_none(),
                    ColumnType::Bool => store.get_bool(*col, row).is_none(),
                    ColumnType::Timestamp => {
                        store.get_timestamp(*col, row) == crate::store::NULL_TIMESTAMP
                    }
                    // Q10 — Bytes nullness via generic Value path.
                    ColumnType::Bytes => store.get(*col, row).is_null(),
                }
            }
            CompiledPredicate::IsNotNull { col } => {
                let schema = store.schema();
                match schema.column_type(*col) {
                    ColumnType::Double => !store.get_double(*col, row).is_nan(),
                    ColumnType::Long => store.get_long(*col, row) != NULL_LONG,
                    ColumnType::Int => store.get_int(*col, row) != NULL_INT,
                    ColumnType::String => store.get_string(*col, row).is_some(),
                    ColumnType::Bool => store.get_bool(*col, row).is_some(),
                    ColumnType::Timestamp => {
                        store.get_timestamp(*col, row) != crate::store::NULL_TIMESTAMP
                    }
                    ColumnType::Bytes => !store.get(*col, row).is_null(),
                }
            }
            // R12 — IS TRUE / IS NOT TRUE / IS FALSE / IS NOT FALSE.
            CompiledPredicate::IsBool { col, want_true, negated } => {
                let matches_target = match store.get_bool(*col, row) {
                    Some(v) => v == *want_true,
                    // SQL three-valued logic: NULL is neither true nor
                    // false. `IS TRUE` of NULL is false; `IS NOT TRUE`
                    // of NULL is true (the negated branch handles it).
                    None => false,
                };
                if *negated { !matches_target } else { matches_target }
            }

            // R2 — generic NumExpr comparison + BETWEEN.
            CompiledPredicate::CompareNum { expr, op, value } => {
                let v = expr.eval(store, row);
                op.apply_f64(v, *value)
            }
            CompiledPredicate::BetweenNum { expr, low, high } => {
                let v = expr.eval(store, row);
                !v.is_nan() && v >= *low && v <= *high
            }

            // ---- Combinators ----
            CompiledPredicate::And(left, right) => {
                left.matches(store, row) && right.matches(store, row)
            }
            CompiledPredicate::Or(left, right) => {
                left.matches(store, row) || right.matches(store, row)
            }
            CompiledPredicate::Not(inner) => !inner.matches(store, row),

            CompiledPredicate::True => true,
        }
    }
}

/// Apply `func` to `s`, returning a new owned `String`. Allocation-
/// free fast path when the source is already in the target case.
fn string_fn_apply(func: StringFn, s: &str) -> String {
    match func {
        StringFn::Upper => s.to_uppercase(),
        StringFn::Lower => s.to_lowercase(),
    }
}

/// Equality check `func(s) == literal`. The literal is normalized at
/// compile time so we only need to transform the row value here.
fn string_fn_eq(func: StringFn, s: &str, literal: &str) -> bool {
    // Char-by-char compare with the transform applied per source char
    // is cheaper than allocating a full transformed string for the
    // common short-string case. Fall back to materialized compare for
    // any input where lowercase/uppercase changes char count (e.g.
    // `ß` → `SS`).
    if s.len() != literal.len() {
        // Length mismatch with bytewise compare *might* still be equal
        // under a case-folding that changes byte count. Recheck via
        // materialized form to be safe.
        return string_fn_apply(func, s) == literal;
    }
    match func {
        StringFn::Upper => {
            s.chars().zip(literal.chars()).all(|(a, b)| {
                a.to_uppercase().next().map(|u| u == b).unwrap_or(false)
            })
        }
        StringFn::Lower => {
            s.chars().zip(literal.chars()).all(|(a, b)| {
                a.to_lowercase().next().map(|u| u == b).unwrap_or(false)
            })
        }
    }
}

/// Convert a SQL LIKE pattern to a regex pattern.
///
/// Wildcards: `%` → `.*`, `_` → `.`. The backslash `\` is treated as the
/// ESCAPE character (SQL default in most dialects): `\%` matches a literal
/// `%`, `\_` matches a literal `_`, and `\\` matches a single backslash.
/// A trailing unmatched `\` is dropped.
pub fn like_to_regex(pattern: &str) -> String {
    let mut regex = String::with_capacity(pattern.len() + 8);
    // `(?s)` makes `.` (from `_`) and `.*` (from `%`) match newlines,
    // so LIKE behaves correctly on multiline string values. Anchored
    // with `^`/`$` for a whole-string match.
    regex.push_str("(?s)^");
    let mut chars = pattern.chars();
    while let Some(ch) = chars.next() {
        match ch {
            '\\' => {
                // Escape sequence: emit the next char as a literal.
                if let Some(next) = chars.next() {
                    if regex_needs_escape(next) {
                        regex.push('\\');
                    }
                    regex.push(next);
                }
            }
            '%' => regex.push_str(".*"),
            '_' => regex.push('.'),
            c if regex_needs_escape(c) => {
                regex.push('\\');
                regex.push(c);
            }
            c => regex.push(c),
        }
    }
    regex.push('$');
    regex
}

/// Evaluate a compiled predicate against a JSON map (rather than a live
/// `ColumnStore`). Builds a single-row temporary store and runs the
/// existing matcher. Intended for bookmark replay where the row was
/// historical and may no longer be present in the live store.
pub fn predicate_matches_json(
    predicate: &CompiledPredicate,
    schema: &std::sync::Arc<Schema>,
    map: &serde_json::Map<String, serde_json::Value>,
) -> bool {
    use crate::store::Value;
    let mut tmp = ColumnStore::new(schema.clone(), 1);
    let values: Vec<Value> = schema
        .columns()
        .iter()
        .map(|col| {
            map.get(col.name())
                .map(|v| Value::from_json(v, col.col_type()))
                .unwrap_or(Value::Null)
        })
        .collect();
    tmp.append_row(&values);
    predicate.matches(&tmp, 0)
}

fn regex_needs_escape(c: char) -> bool {
    matches!(
        c,
        '.' | '+' | '*' | '?' | '(' | ')' | '[' | ']' | '{' | '}' | '\\' | '^' | '$' | '|'
    )
}

/// Parse a SQL WHERE expression (from sqlparser AST) into a CompiledPredicate.
pub fn compile_expr(
    expr: &sqlparser::ast::Expr,
    schema: &Schema,
) -> Result<CompiledPredicate, PredicateError> {
    use sqlparser::ast::{BinaryOperator, Expr, UnaryOperator};

    match expr {
        Expr::BinaryOp { left, op, right } => {
            match op {
                BinaryOperator::And => {
                    let l = compile_expr(left, schema)?;
                    let r = compile_expr(right, schema)?;
                    Ok(CompiledPredicate::And(Box::new(l), Box::new(r)))
                }
                BinaryOperator::Or => {
                    let l = compile_expr(left, schema)?;
                    let r = compile_expr(right, schema)?;
                    Ok(CompiledPredicate::Or(Box::new(l), Box::new(r)))
                }
                BinaryOperator::Eq
                | BinaryOperator::NotEq
                | BinaryOperator::Lt
                | BinaryOperator::LtEq
                | BinaryOperator::Gt
                | BinaryOperator::GtEq => compile_comparison(left, op, right, schema),
                _ => Err(PredicateError::UnsupportedOperator(format!("{:?}", op))),
            }
        }

        Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr: inner,
        } => {
            let compiled = compile_expr(inner, schema)?;
            Ok(CompiledPredicate::Not(Box::new(compiled)))
        }

        Expr::IsNull(inner) => {
            let col = resolve_column(inner, schema)?;
            Ok(CompiledPredicate::IsNull { col })
        }

        Expr::IsNotNull(inner) => {
            let col = resolve_column(inner, schema)?;
            Ok(CompiledPredicate::IsNotNull { col })
        }

        // R12 — `<bool-col> IS TRUE / IS NOT TRUE / IS FALSE / IS NOT FALSE`.
        // Per SQL three-valued logic, IS TRUE matches only true (NULL
        // and FALSE both fail); IS NOT TRUE matches false OR NULL.
        Expr::IsTrue(inner) => {
            let col = resolve_column(inner, schema)?;
            Ok(CompiledPredicate::IsBool { col, want_true: true, negated: false })
        }
        Expr::IsNotTrue(inner) => {
            let col = resolve_column(inner, schema)?;
            Ok(CompiledPredicate::IsBool { col, want_true: true, negated: true })
        }
        Expr::IsFalse(inner) => {
            let col = resolve_column(inner, schema)?;
            Ok(CompiledPredicate::IsBool { col, want_true: false, negated: false })
        }
        Expr::IsNotFalse(inner) => {
            let col = resolve_column(inner, schema)?;
            Ok(CompiledPredicate::IsBool { col, want_true: false, negated: true })
        }

        Expr::Like {
            negated,
            expr: col_expr,
            pattern,
            ..
        } => {
            let pat_str = extract_string_value(pattern)?;
            let pred = if let Some((func, col)) = extract_string_fn_target(col_expr, schema)? {
                // UPPER(col) LIKE 'X%' — pre-normalize the pattern to
                // match the case the row will be transformed to.
                let normalized = string_fn_apply(func, &pat_str);
                let regex_str = like_to_regex(&normalized);
                let regex = regex::Regex::new(&regex_str)
                    .map_err(|e| PredicateError::InvalidPattern(e.to_string()))?;
                CompiledPredicate::LikeStringFn { col, func, pattern: regex }
            } else if is_string_expr_function(col_expr) {
                // CONCAT(...) LIKE '...' or SUBSTR(...) LIKE '...'.
                let se = compile_string_expr(col_expr, schema)?;
                let regex_str = like_to_regex(&pat_str);
                let regex = regex::Regex::new(&regex_str)
                    .map_err(|e| PredicateError::InvalidPattern(e.to_string()))?;
                CompiledPredicate::LikeStringExpr { expr: se, pattern: regex }
            } else {
                let col = resolve_column(col_expr, schema)?;
                let regex_str = like_to_regex(&pat_str);
                let regex = regex::Regex::new(&regex_str)
                    .map_err(|e| PredicateError::InvalidPattern(e.to_string()))?;
                CompiledPredicate::Like { col, pattern: regex }
            };
            if *negated {
                Ok(CompiledPredicate::Not(Box::new(pred)))
            } else {
                Ok(pred)
            }
        }

        Expr::InList {
            expr: col_expr,
            list,
            negated,
        } => {
            let col = resolve_column(col_expr, schema)?;
            // Pick the compiled variant based on the COLUMN's type,
            // not the literal type. SQL is type-strict here — `IN`
            // against a numeric column with string literals should
            // be a parse error anyway, but we'd rather surface that
            // as `InvalidLiteral` (caught below by the per-type
            // extractor) than panic in `get_string` on the wrong
            // column arena (the pre-S46 bug this fix closes).
            let pred = match schema.column_type(col) {
                ColumnType::String => {
                    let values: HashSet<CompactString> = list
                        .iter()
                        .map(|e| extract_string_value(e).map(CompactString::new))
                        .collect::<Result<_, _>>()?;
                    CompiledPredicate::InString { col, values }
                }
                ColumnType::Long | ColumnType::Int => {
                    let values: std::collections::HashSet<i64> = list
                        .iter()
                        .map(extract_i64)
                        .collect::<Result<_, _>>()?;
                    CompiledPredicate::InLong { col, values }
                }
                ColumnType::Double => {
                    let values: std::collections::HashSet<u64> = list
                        .iter()
                        .map(|e| extract_f64(e).map(|f| f.to_bits()))
                        .collect::<Result<_, _>>()?;
                    CompiledPredicate::InDouble { col, values }
                }
                ColumnType::Bool => {
                    // Convert each literal to a bool so `flag IN (true, false)`
                    // works without surfacing string-vs-bool type mismatch.
                    let values: HashSet<bool> = list
                        .iter()
                        .map(extract_bool)
                        .collect::<Result<_, _>>()?;
                    CompiledPredicate::InBool { col, values }
                }
                ColumnType::Timestamp => {
                    let values: HashSet<i64> = list
                        .iter()
                        .map(extract_timestamp)
                        .collect::<Result<_, _>>()?;
                    CompiledPredicate::InTimestamp { col, values }
                }
                // Q10 — Bytes doesn't support IN; reject cleanly.
                ColumnType::Bytes => {
                    return Err(PredicateError::TypeMismatch(
                        "IN list on bytes column not supported".into(),
                    ));
                }
            };
            if *negated {
                Ok(CompiledPredicate::Not(Box::new(pred)))
            } else {
                Ok(pred)
            }
        }

        Expr::Between {
            expr: col_expr,
            negated,
            low,
            high,
        } => {
            // R2 — when LHS isn't a bare column (function call,
            // arithmetic, etc.), fall through to BetweenNum which
            // evaluates the NumExpr per row.
            if !matches!(col_expr.as_ref(), Expr::Identifier(_) | Expr::CompoundIdentifier(_)) {
                if let Some(num_lhs) = try_compile_num_expr(col_expr, schema) {
                    let low_f = extract_f64(low)?;
                    let high_f = extract_f64(high)?;
                    let pred = CompiledPredicate::BetweenNum {
                        expr: num_lhs,
                        low: low_f,
                        high: high_f,
                    };
                    return Ok(if *negated {
                        CompiledPredicate::Not(Box::new(pred))
                    } else {
                        pred
                    });
                }
            }
            let col = resolve_column(col_expr, schema)?;
            let col_type = schema.column_type(col);
            let pred = match col_type {
                ColumnType::Double => CompiledPredicate::BetweenDouble {
                    col,
                    low: extract_f64(low)?,
                    high: extract_f64(high)?,
                },
                ColumnType::Long | ColumnType::Int => CompiledPredicate::BetweenLong {
                    col,
                    low: extract_i64(low)?,
                    high: extract_i64(high)?,
                },
                ColumnType::Timestamp => CompiledPredicate::BetweenTimestamp {
                    col,
                    low: extract_timestamp(low)?,
                    high: extract_timestamp(high)?,
                },
                _ => return Err(PredicateError::TypeMismatch("BETWEEN on string column".into())),
            };
            if *negated {
                Ok(CompiledPredicate::Not(Box::new(pred)))
            } else {
                Ok(pred)
            }
        }

        Expr::Nested(inner) => compile_expr(inner, schema),

        // P13 — MATCHES_REGEX(col, '<pattern>').
        Expr::Function(f) => {
            let name = f.name.to_string().to_ascii_uppercase();
            if name != "MATCHES_REGEX" {
                return Err(PredicateError::UnsupportedExpression(format!(
                    "function in WHERE position: {name}"
                )));
            }
            let arg_list = match &f.args {
                sqlparser::ast::FunctionArguments::List(l) => l,
                _ => {
                    return Err(PredicateError::UnsupportedExpression(
                        "MATCHES_REGEX requires (col, pattern) arguments".into(),
                    ))
                }
            };
            if arg_list.args.len() != 2 {
                return Err(PredicateError::UnsupportedExpression(format!(
                    "MATCHES_REGEX expects 2 arguments (col, pattern), got {}",
                    arg_list.args.len()
                )));
            }
            use sqlparser::ast::{FunctionArg, FunctionArgExpr};
            let col_expr = match &arg_list.args[0] {
                FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => e,
                other => {
                    return Err(PredicateError::UnsupportedExpression(format!(
                        "MATCHES_REGEX: first arg must be a column ref, got {other:?}"
                    )))
                }
            };
            let pat_expr = match &arg_list.args[1] {
                FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => e,
                other => {
                    return Err(PredicateError::UnsupportedExpression(format!(
                        "MATCHES_REGEX: second arg must be a string literal, got {other:?}"
                    )))
                }
            };
            let col = resolve_column(col_expr, schema)?;
            let pat_str = extract_string_value(pat_expr)?;
            // Bound the compiled program / DFA size for caller-supplied
            // patterns. The `regex` crate matches in linear time (no
            // catastrophic backtracking), but a pathological pattern can
            // still blow up compiled-program memory; cap it explicitly.
            // 1 MiB program + 1 MiB DFA is generous for legitimate
            // filters and rejects abusive ones at compile time.
            let pattern = regex::RegexBuilder::new(&pat_str)
                .size_limit(1 << 20)
                .dfa_size_limit(1 << 20)
                .build()
                .map_err(|e| PredicateError::InvalidPattern(e.to_string()))?;
            Ok(CompiledPredicate::Regex { col, pattern })
        }

        _ => Err(PredicateError::UnsupportedExpression(format!("{:?}", expr))),
    }
}

/// Compile a simple binary comparison (=, <>, <, >, <=, >=).
fn compile_comparison(
    left: &sqlparser::ast::Expr,
    op: &sqlparser::ast::BinaryOperator,
    right: &sqlparser::ast::Expr,
    schema: &Schema,
) -> Result<CompiledPredicate, PredicateError> {
    use sqlparser::ast::BinaryOperator;

    // LHS as a string function over a column: `UPPER(col)` / `LOWER(col)`.
    if let Some((func, col)) = extract_string_fn_target(left, schema)? {
        if schema.column_type(col) != ColumnType::String {
            return Err(PredicateError::TypeMismatch(format!(
                "UPPER/LOWER requires a string column, got {:?}",
                schema.column_type(col)
            )));
        }
        let raw = extract_string_value(right)?;
        let normalized = CompactString::new(string_fn_apply(func, &raw));
        return Ok(match op {
            BinaryOperator::Eq => CompiledPredicate::EqStringFn {
                col,
                func,
                value: normalized,
            },
            BinaryOperator::NotEq => CompiledPredicate::NeqStringFn {
                col,
                func,
                value: normalized,
            },
            _ => {
                return Err(PredicateError::UnsupportedOperator(format!(
                    "{:?} not supported with UPPER/LOWER",
                    op
                )))
            }
        });
    }

    // LHS as a general string expression (SUBSTR / CONCAT / nested
    // UPPER+LOWER). Falls through to the simpler EqString /
    // EqStringFn variants when the LHS is just a column or a single
    // UPPER/LOWER call, so we only pay the StringExpr allocation
    // overhead for genuinely complex expressions.
    if is_string_expr_function(left) {
        // Skip if the call is already handled by the faster
        // UPPER/LOWER-of-single-column path checked above.
        let is_simple_case_fold = matches!(
            extract_string_fn_target(left, schema)?, Some(_)
        );
        if !is_simple_case_fold {
            let se = compile_string_expr(left, schema)?;
            let raw = extract_string_value(right)?;
            let value = CompactString::new(raw);
            return Ok(match op {
                BinaryOperator::Eq => CompiledPredicate::EqStringExpr { expr: se, value },
                BinaryOperator::NotEq => {
                    CompiledPredicate::NeqStringExpr { expr: se, value }
                }
                _ => {
                    return Err(PredicateError::UnsupportedOperator(format!(
                        "{:?} not supported on string expressions",
                        op
                    )))
                }
            });
        }
    }

    // LHS as `LENGTH(col)` → numeric comparison against an int literal.
    if let Some(col) = extract_length_target(left, schema)? {
        if schema.column_type(col) != ColumnType::String {
            return Err(PredicateError::TypeMismatch(
                "LENGTH expects a string column".into(),
            ));
        }
        let n = extract_i64(right)?;
        let cmp_op = match op {
            BinaryOperator::Eq => NumCmpOp::Eq,
            BinaryOperator::NotEq => NumCmpOp::Neq,
            BinaryOperator::Lt => NumCmpOp::Lt,
            BinaryOperator::LtEq => NumCmpOp::Le,
            BinaryOperator::Gt => NumCmpOp::Gt,
            BinaryOperator::GtEq => NumCmpOp::Ge,
            _ => {
                return Err(PredicateError::UnsupportedOperator(format!(
                    "{:?} not supported with LENGTH",
                    op
                )))
            }
        };
        return Ok(CompiledPredicate::LengthCmp {
            col,
            op: cmp_op,
            value: n,
        });
    }

    // R2 — try compiling LHS as a NumExpr (scalar function / arithmetic
    // over columns). This is what unlocks AMPS-style filters like
    // `WHERE ABS(slippage) > 5`, `WHERE a + b > 100`, `WHERE col1 >
    // col2`. The bare-column-vs-literal path below remains the fast
    // path for the common case; we only enter the NumExpr branch when
    // LHS is structured.
    if !matches!(left, sqlparser::ast::Expr::Identifier(_)) {
        if let Some(num_lhs) = try_compile_num_expr(left, schema) {
            let cmp_op = match op {
                BinaryOperator::Eq => NumCmpOp::Eq,
                BinaryOperator::NotEq => NumCmpOp::Neq,
                BinaryOperator::Lt => NumCmpOp::Lt,
                BinaryOperator::LtEq => NumCmpOp::Le,
                BinaryOperator::Gt => NumCmpOp::Gt,
                BinaryOperator::GtEq => NumCmpOp::Ge,
                _ => {
                    return Err(PredicateError::UnsupportedOperator(format!(
                        "{:?} not supported on numeric expressions",
                        op
                    )));
                }
            };
            // RHS — accept either a literal OR another NumExpr (col vs
            // col). For NumExpr-vs-NumExpr we fold by subtracting and
            // comparing the difference to 0.
            if let Ok(value) = extract_f64(right) {
                return Ok(CompiledPredicate::CompareNum {
                    expr: num_lhs,
                    op: cmp_op,
                    value,
                });
            }
            if let Some(num_rhs) = try_compile_num_expr(right, schema) {
                return Ok(CompiledPredicate::CompareNum {
                    expr: NumExpr::Sub(Box::new(num_lhs), Box::new(num_rhs)),
                    op: cmp_op,
                    value: 0.0,
                });
            }
        }
    }

    // R2 (mirror) — bare-column LHS but NumExpr-shaped RHS: `WHERE
    // col1 > col2 + 5` or `WHERE qty > NULLIF(...)`. The col-vs-col
    // case where both sides resolve to bare columns of the SAME type
    // still goes through the typed fast path; only mixed shapes land
    // here.
    if matches!(left, sqlparser::ast::Expr::Identifier(_))
        && !matches!(right, sqlparser::ast::Expr::Value(_))
    {
        if let (Some(num_lhs), Some(num_rhs)) = (
            try_compile_num_expr(left, schema),
            try_compile_num_expr(right, schema),
        ) {
            // Only take this branch if RHS is actually structured
            // (function / arithmetic / column ref) — a pure literal
            // RHS belongs to the typed fast path below.
            let rhs_is_structured = !matches!(
                right,
                sqlparser::ast::Expr::Value(_)
                    | sqlparser::ast::Expr::UnaryOp {
                        op: sqlparser::ast::UnaryOperator::Minus,
                        expr: _,
                    }
            ) || matches!(right, sqlparser::ast::Expr::BinaryOp { .. });
            if rhs_is_structured {
                let cmp_op = match op {
                    BinaryOperator::Eq => NumCmpOp::Eq,
                    BinaryOperator::NotEq => NumCmpOp::Neq,
                    BinaryOperator::Lt => NumCmpOp::Lt,
                    BinaryOperator::LtEq => NumCmpOp::Le,
                    BinaryOperator::Gt => NumCmpOp::Gt,
                    BinaryOperator::GtEq => NumCmpOp::Ge,
                    _ => {
                        return Err(PredicateError::UnsupportedOperator(format!(
                            "{:?} not supported on numeric expressions",
                            op
                        )));
                    }
                };
                return Ok(CompiledPredicate::CompareNum {
                    expr: NumExpr::Sub(Box::new(num_lhs), Box::new(num_rhs)),
                    op: cmp_op,
                    value: 0.0,
                });
            }
        }
    }

    let col = resolve_column(left, schema)?;
    let col_type = schema.column_type(col);

    match col_type {
        ColumnType::Double => {
            let value = extract_f64(right)?;
            Ok(match op {
                BinaryOperator::Eq => CompiledPredicate::EqDouble { col, value },
                BinaryOperator::NotEq => CompiledPredicate::NeqDouble { col, value },
                BinaryOperator::Lt => CompiledPredicate::LtDouble { col, value },
                BinaryOperator::LtEq => CompiledPredicate::LeDouble { col, value },
                BinaryOperator::Gt => CompiledPredicate::GtDouble { col, value },
                BinaryOperator::GtEq => CompiledPredicate::GeDouble { col, value },
                _ => unreachable!(),
            })
        }
        ColumnType::Long | ColumnType::Int => {
            let value = extract_i64(right)?;
            Ok(match op {
                BinaryOperator::Eq => CompiledPredicate::EqLong { col, value },
                BinaryOperator::NotEq => CompiledPredicate::NeqLong { col, value },
                BinaryOperator::Lt => CompiledPredicate::LtLong { col, value },
                BinaryOperator::LtEq => CompiledPredicate::LeLong { col, value },
                BinaryOperator::Gt => CompiledPredicate::GtLong { col, value },
                BinaryOperator::GtEq => CompiledPredicate::GeLong { col, value },
                _ => unreachable!(),
            })
        }
        ColumnType::String => {
            let value = CompactString::new(extract_string_value(right)?);
            Ok(match op {
                BinaryOperator::Eq => CompiledPredicate::EqString { col, value },
                BinaryOperator::NotEq => CompiledPredicate::NeqString { col, value },
                _ => return Err(PredicateError::TypeMismatch(
                    format!("{:?} not supported on string columns", op),
                )),
            })
        }
        ColumnType::Bool => {
            let value = extract_bool(right)?;
            Ok(match op {
                BinaryOperator::Eq => CompiledPredicate::EqBool { col, value },
                BinaryOperator::NotEq => CompiledPredicate::NeqBool { col, value },
                _ => return Err(PredicateError::TypeMismatch(
                    format!("{:?} not supported on bool columns", op),
                )),
            })
        }
        ColumnType::Timestamp => {
            let value = extract_timestamp(right)?;
            Ok(match op {
                BinaryOperator::Eq => CompiledPredicate::EqTimestamp { col, value },
                BinaryOperator::NotEq => CompiledPredicate::NeqTimestamp { col, value },
                BinaryOperator::Lt => CompiledPredicate::LtTimestamp { col, value },
                BinaryOperator::LtEq => CompiledPredicate::LeTimestamp { col, value },
                BinaryOperator::Gt => CompiledPredicate::GtTimestamp { col, value },
                BinaryOperator::GtEq => CompiledPredicate::GeTimestamp { col, value },
                _ => unreachable!(),
            })
        }
        // Q10 — Bytes column: only `=`/`<>` against a string literal
        // (the base64 form). Other comparisons (ordering) are rejected
        // since bytes have no canonical order semantics.
        ColumnType::Bytes => Err(PredicateError::TypeMismatch(
            "comparisons on bytes columns are not yet supported".into(),
        )),
    }
}

/// Recursive parser for `StringExpr`. Accepts column identifiers,
/// string literals, and the four function heads (`UPPER`, `LOWER`,
/// `SUBSTR`, `CONCAT`). Used when the LHS of a comparison is more
/// complex than a single column or single-arg case-fold.
fn compile_string_expr(
    expr: &sqlparser::ast::Expr,
    schema: &Schema,
) -> Result<StringExpr, PredicateError> {
    use sqlparser::ast::{Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Value};
    match expr {
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => {
            let col = resolve_column(expr, schema)?;
            if schema.column_type(col) != ColumnType::String {
                return Err(PredicateError::TypeMismatch(format!(
                    "string expression references non-string column {}",
                    schema.column_name(col)
                )));
            }
            Ok(StringExpr::Col(col))
        }
        Expr::Value(sqlparser::ast::ValueWithSpan { value, .. }) => match value {
            Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => {
                Ok(StringExpr::Lit(CompactString::new(s)))
            }
            _ => Err(PredicateError::InvalidLiteral(format!(
                "non-string literal in string expression: {value:?}"
            ))),
        },
        // sqlparser produces a dedicated AST node for `SUBSTRING(... FROM ... FOR ...)`
        // and also for `SUBSTR(...)` (depending on dialect). Handle both.
        Expr::Substring {
            expr: inner_expr,
            substring_from,
            substring_for,
            ..
        } => {
            let inner = compile_string_expr(inner_expr, schema)?;
            let start = match substring_from {
                Some(e) => extract_i64(e)?,
                None => 1,
            };
            let len = match substring_for {
                Some(e) => Some(extract_i64(e)?),
                None => None,
            };
            Ok(StringExpr::Substr {
                inner: Box::new(inner),
                start,
                len,
            })
        }
        Expr::Function(f) => {
            let name = f.name.to_string().to_ascii_uppercase();
            let arg_list = match &f.args {
                FunctionArguments::List(l) => l,
                _ => {
                    return Err(PredicateError::UnsupportedExpression(format!(
                        "{name} expects an argument list"
                    )))
                }
            };
            let exprs: Vec<&Expr> = arg_list
                .args
                .iter()
                .map(|a| match a {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => Ok(e),
                    other => Err(PredicateError::UnsupportedExpression(format!(
                        "{name}: unsupported argument shape: {other:?}"
                    ))),
                })
                .collect::<Result<Vec<_>, _>>()?;
            match name.as_str() {
                "UPPER" => {
                    if exprs.len() != 1 {
                        return Err(PredicateError::UnsupportedExpression(
                            "UPPER takes 1 argument".into(),
                        ));
                    }
                    Ok(StringExpr::Upper(Box::new(compile_string_expr(
                        exprs[0], schema,
                    )?)))
                }
                "LOWER" => {
                    if exprs.len() != 1 {
                        return Err(PredicateError::UnsupportedExpression(
                            "LOWER takes 1 argument".into(),
                        ));
                    }
                    Ok(StringExpr::Lower(Box::new(compile_string_expr(
                        exprs[0], schema,
                    )?)))
                }
                "SUBSTR" | "SUBSTRING" => {
                    if exprs.len() != 2 && exprs.len() != 3 {
                        return Err(PredicateError::UnsupportedExpression(
                            "SUBSTR expects (str, start) or (str, start, len)".into(),
                        ));
                    }
                    let inner = compile_string_expr(exprs[0], schema)?;
                    let start = extract_i64(exprs[1])?;
                    let len = if exprs.len() == 3 {
                        Some(extract_i64(exprs[2])?)
                    } else {
                        None
                    };
                    Ok(StringExpr::Substr {
                        inner: Box::new(inner),
                        start,
                        len,
                    })
                }
                "CONCAT" => {
                    if exprs.is_empty() {
                        return Err(PredicateError::UnsupportedExpression(
                            "CONCAT requires at least one argument".into(),
                        ));
                    }
                    let parts = exprs
                        .into_iter()
                        .map(|e| compile_string_expr(e, schema))
                        .collect::<Result<Vec<_>, _>>()?;
                    Ok(StringExpr::Concat(parts))
                }
                other => Err(PredicateError::UnsupportedExpression(format!(
                    "unsupported function in string expression: {other}"
                ))),
            }
        }
        _ => Err(PredicateError::UnsupportedExpression(format!(
            "not a string expression: {expr:?}"
        ))),
    }
}

/// True iff `expr` is a structured string expression
/// (UPPER/LOWER/SUBSTR/SUBSTRING/CONCAT, in either `Expr::Function`
/// or `Expr::Substring` form) — i.e., something the general
/// `compile_string_expr` should handle. A bare column or string
/// literal returns `false` so the fast EqString / NeqString paths
/// still trigger.
fn is_string_expr_function(expr: &sqlparser::ast::Expr) -> bool {
    use sqlparser::ast::Expr;
    match expr {
        Expr::Function(f) => {
            let name = f.name.to_string().to_ascii_uppercase();
            matches!(
                name.as_str(),
                "UPPER" | "LOWER" | "SUBSTR" | "SUBSTRING" | "CONCAT"
            )
        }
        Expr::Substring { .. } => true,
        _ => false,
    }
}

/// Recognize `UPPER(col)` or `LOWER(col)`. Returns the function +
/// resolved column index, or `Ok(None)` if `expr` isn't a recognized
/// case-fold over a column.
fn extract_string_fn_target(
    expr: &sqlparser::ast::Expr,
    schema: &Schema,
) -> Result<Option<(StringFn, usize)>, PredicateError> {
    use sqlparser::ast::{Expr, FunctionArg, FunctionArgExpr, FunctionArguments};
    let f = match expr {
        Expr::Function(f) => f,
        _ => return Ok(None),
    };
    let name = f.name.to_string().to_ascii_uppercase();
    let func = match name.as_str() {
        "UPPER" => StringFn::Upper,
        "LOWER" => StringFn::Lower,
        _ => return Ok(None),
    };
    let arg_list = match &f.args {
        FunctionArguments::List(l) => l,
        _ => return Err(PredicateError::UnsupportedExpression(format!(
            "{name} expects a column argument"
        ))),
    };
    if arg_list.args.len() != 1 {
        return Err(PredicateError::UnsupportedExpression(format!(
            "{name} expects exactly one argument"
        )));
    }
    let inner = match &arg_list.args[0] {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => e,
        _ => return Err(PredicateError::UnsupportedExpression(format!(
            "{name}: unsupported argument shape"
        ))),
    };
    // If the inner isn't a bare column (e.g., `UPPER(SUBSTR(...))`),
    // bail to the slower general string-expression path rather than
    // erroring. `resolve_column` is the strictest test for "bare
    // column", so its `Err` is what we treat as "not a fast path".
    let col = match resolve_column(inner, schema) {
        Ok(c) => c,
        Err(_) => return Ok(None),
    };
    Ok(Some((func, col)))
}

/// Recognize `LENGTH(col)`. Returns the column index or `Ok(None)`.
fn extract_length_target(
    expr: &sqlparser::ast::Expr,
    schema: &Schema,
) -> Result<Option<usize>, PredicateError> {
    use sqlparser::ast::{Expr, FunctionArg, FunctionArgExpr, FunctionArguments};
    let f = match expr {
        Expr::Function(f) => f,
        _ => return Ok(None),
    };
    let name = f.name.to_string().to_ascii_uppercase();
    if name != "LENGTH" && name != "CHAR_LENGTH" {
        return Ok(None);
    }
    let arg_list = match &f.args {
        FunctionArguments::List(l) => l,
        _ => return Err(PredicateError::UnsupportedExpression(
            "LENGTH expects a column argument".into(),
        )),
    };
    if arg_list.args.len() != 1 {
        return Err(PredicateError::UnsupportedExpression(
            "LENGTH expects exactly one argument".into(),
        ));
    }
    let inner = match &arg_list.args[0] {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => e,
        _ => return Err(PredicateError::UnsupportedExpression(
            "LENGTH: unsupported argument shape".into(),
        )),
    };
    Ok(Some(resolve_column(inner, schema)?))
}

/// Resolve an expression to a column index.
fn resolve_column(
    expr: &sqlparser::ast::Expr,
    schema: &Schema,
) -> Result<usize, PredicateError> {
    match expr {
        sqlparser::ast::Expr::Identifier(ident) => {
            let name = &ident.value;
            schema
                .index_of(name)
                .ok_or_else(|| PredicateError::UnknownColumn(name.clone()))
        }
        sqlparser::ast::Expr::CompoundIdentifier(parts) => {
            // Handle dotted names like "counterparty.name"
            let name = parts
                .iter()
                .map(|p| p.value.as_str())
                .collect::<Vec<_>>()
                .join(".");
            schema
                .index_of(&name)
                .ok_or_else(|| PredicateError::UnknownColumn(name))
        }
        _ => Err(PredicateError::UnsupportedExpression(
            "Expected column reference".into(),
        )),
    }
}

/// R2 — try to compile a SQL expression as a numeric expression
/// (column read / literal / arithmetic / scalar function). Returns
/// `None` if the expression isn't num-shaped (e.g. a string literal,
/// a string column) so the caller can fall back to its other paths.
///
/// Supported:
///   - Bare column reference on a numeric column (Long / Int / Double)
///   - Numeric literal (integer or float)
///   - Unary `-` and `+`
///   - Binary `+ - * /` (any combination of NumExprs)
///   - `ABS(x)`, `ROUND(x)`, `FLOOR(x)`, `CEIL(x)`
///   - Parenthesised expressions
///
/// Why use a try-compile (return Option) instead of an error: the
/// caller (compile_comparison) needs to fall back to the
/// string-fn / bare-column paths if the LHS isn't num-shaped. An
/// error would mask those legitimate paths.
pub(crate) fn try_compile_num_expr(
    expr: &sqlparser::ast::Expr,
    schema: &Schema,
) -> Option<NumExpr> {
    use sqlparser::ast::{BinaryOperator, Expr, UnaryOperator, Value, ValueWithSpan};
    match expr {
        Expr::Nested(inner) => try_compile_num_expr(inner, schema),
        Expr::Identifier(id) => {
            let col = schema.index_of(&id.value)?;
            match schema.column_type(col) {
                ColumnType::Long
                | ColumnType::Int
                | ColumnType::Double
                | ColumnType::Timestamp => Some(NumExpr::Col(col)),
                _ => None,
            }
        }
        Expr::CompoundIdentifier(parts) => {
            // P1's qualified-ref rewriter strips alias prefixes before
            // we get here, so any compound identifier that survives is
            // either a single-part fallback or something we can't
            // resolve.
            if parts.len() == 1 {
                let col = schema.index_of(&parts[0].value)?;
                match schema.column_type(col) {
                    ColumnType::Long | ColumnType::Int | ColumnType::Double => Some(NumExpr::Col(col)),
                    _ => None,
                }
            } else {
                None
            }
        }
        Expr::Value(ValueWithSpan { value, .. }) => match value {
            Value::Number(s, _) => s.parse::<f64>().ok().map(NumExpr::Lit),
            _ => None,
        },
        Expr::UnaryOp { op, expr: inner } => match op {
            UnaryOperator::Minus => {
                Some(NumExpr::Neg(Box::new(try_compile_num_expr(inner, schema)?)))
            }
            UnaryOperator::Plus => try_compile_num_expr(inner, schema),
            _ => None,
        },
        Expr::BinaryOp { left, op, right } => {
            let ctor: fn(Box<NumExpr>, Box<NumExpr>) -> NumExpr = match op {
                BinaryOperator::Plus => |a, b| NumExpr::Add(a, b),
                BinaryOperator::Minus => |a, b| NumExpr::Sub(a, b),
                BinaryOperator::Multiply => |a, b| NumExpr::Mul(a, b),
                BinaryOperator::Divide => |a, b| NumExpr::Div(a, b),
                _ => return None,
            };
            let l = try_compile_num_expr(left, schema)?;
            let r = try_compile_num_expr(right, schema)?;
            Some(ctor(Box::new(l), Box::new(r)))
        }
        Expr::Function(f) => {
            let name = f.name.to_string().to_ascii_uppercase();
            let args = match &f.args {
                sqlparser::ast::FunctionArguments::List(list) => &list.args,
                _ => return None,
            };
            // R7 — NOW() takes zero arguments and returns the
            // current time in microseconds since UNIX epoch.
            // Evaluated AT COMPILE TIME so the resulting predicate
            // has a stable reference clock.
            if name == "NOW" && args.is_empty() {
                let micros = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_micros() as i64)
                    .unwrap_or(0);
                return Some(NumExpr::NowMicros(micros));
            }
            // R4 — multi-arg variants (COALESCE / NULLIF) first.
            if matches!(name.as_str(), "COALESCE" | "NULLIF") {
                return try_compile_multi_arg_num_fn(f, schema);
            }
            // Single-arg numeric scalar functions (ABS / ROUND).
            if args.len() != 1 {
                return None;
            }
            let arg_expr = match &args[0] {
                sqlparser::ast::FunctionArg::Unnamed(
                    sqlparser::ast::FunctionArgExpr::Expr(e),
                ) => e,
                _ => return None,
            };
            let inner = try_compile_num_expr(arg_expr, schema)?;
            match name.as_str() {
                "ABS" => Some(NumExpr::Abs(Box::new(inner))),
                "ROUND" => Some(NumExpr::Round(Box::new(inner))),
                _ => None,
            }
        }
        // sqlparser 0.56 promotes FLOOR/CEIL/CEILING to dedicated
        // AST variants rather than generic Function nodes, so we
        // dispatch on them separately. `field` is the (rarely-used)
        // `FLOOR(x TO MONTH)` ANSI extension — ignored for now;
        // returning None for non-empty field would silently fall back
        // to the bare-column path and produce a confusing error.
        Expr::Floor { expr, .. } => Some(NumExpr::Floor(Box::new(
            try_compile_num_expr(expr, schema)?,
        ))),
        Expr::Ceil { expr, .. } => Some(NumExpr::Ceil(Box::new(
            try_compile_num_expr(expr, schema)?,
        ))),
        _ => None,
    }
}

/// R4 — multi-arg numeric scalar functions (COALESCE / NULLIF).
/// Called by `try_compile_num_expr`'s function arm before the
/// single-arg gate. Returns None for unrecognised names so the
/// outer caller can fall back to its other paths.
fn try_compile_multi_arg_num_fn(
    f: &sqlparser::ast::Function,
    schema: &Schema,
) -> Option<NumExpr> {
    let name = f.name.to_string().to_ascii_uppercase();
    let args = match &f.args {
        sqlparser::ast::FunctionArguments::List(list) => &list.args,
        _ => return None,
    };
    let arg_exprs = args
        .iter()
        .map(|a| match a {
            sqlparser::ast::FunctionArg::Unnamed(
                sqlparser::ast::FunctionArgExpr::Expr(e),
            ) => Some(e),
            _ => None,
        })
        .collect::<Option<Vec<_>>>()?;
    match name.as_str() {
        "COALESCE" => {
            if arg_exprs.is_empty() {
                return None;
            }
            let parts = arg_exprs
                .iter()
                .map(|e| try_compile_num_expr(e, schema))
                .collect::<Option<Vec<_>>>()?;
            Some(NumExpr::Coalesce(parts))
        }
        "NULLIF" => {
            if arg_exprs.len() != 2 {
                return None;
            }
            let a = try_compile_num_expr(arg_exprs[0], schema)?;
            let b = try_compile_num_expr(arg_exprs[1], schema)?;
            Some(NumExpr::NullIf(Box::new(a), Box::new(b)))
        }
        _ => None,
    }
}

/// Extract a string value from a SQL literal. Handles a unary minus
/// in front of a number literal (`-44`) by prepending `-` to the
/// stringified number; sqlparser models that as `UnaryOp { Minus, ... }`
/// rather than a signed `Number`, and downstream parsers (`extract_i64`,
/// `extract_f64`) handle the leading sign natively.
pub(crate) fn extract_string_value(expr: &sqlparser::ast::Expr) -> Result<String, PredicateError> {
    match expr {
        sqlparser::ast::Expr::Value(sqlparser::ast::ValueWithSpan { value, .. }) => match value {
            sqlparser::ast::Value::SingleQuotedString(s) => Ok(s.clone()),
            sqlparser::ast::Value::DoubleQuotedString(s) => Ok(s.clone()),
            sqlparser::ast::Value::Number(s, _) => Ok(s.clone()),
            _ => Err(PredicateError::InvalidLiteral(format!("{:?}", value))),
        },
        sqlparser::ast::Expr::UnaryOp {
            op: sqlparser::ast::UnaryOperator::Minus,
            expr: inner,
        } => Ok(format!("-{}", extract_string_value(inner)?)),
        sqlparser::ast::Expr::UnaryOp {
            op: sqlparser::ast::UnaryOperator::Plus,
            expr: inner,
        } => extract_string_value(inner),
        _ => Err(PredicateError::InvalidLiteral(format!("{:?}", expr))),
    }
}

/// Extract a f64 from a SQL literal.
pub(crate) fn extract_f64(expr: &sqlparser::ast::Expr) -> Result<f64, PredicateError> {
    let s = extract_string_value(expr)?;
    s.parse::<f64>()
        .map_err(|_| PredicateError::InvalidLiteral(format!("Cannot parse '{}' as f64", s)))
}

/// Extract an i64 from a SQL literal.
pub(crate) fn extract_i64(expr: &sqlparser::ast::Expr) -> Result<i64, PredicateError> {
    let s = extract_string_value(expr)?;
    s.parse::<i64>()
        .map_err(|_| PredicateError::InvalidLiteral(format!("Cannot parse '{}' as i64", s)))
}

/// Extract a Timestamp literal (i64 microseconds since UNIX epoch)
/// from a SQL expression. Accepts:
///   * Numeric literals — passed through `parse_timestamp_json` so
///     epoch seconds / ms / μs all work by magnitude detection.
///   * String literals — parsed as RFC 3339 / ISO 8601 with optional
///     timezone, naive datetimes (treated UTC), or date-only.
/// Returns `InvalidLiteral` on a parse failure.
pub(crate) fn extract_timestamp(expr: &sqlparser::ast::Expr) -> Result<i64, PredicateError> {
    let s = extract_string_value(expr)?;
    let json = serde_json::Value::String(s.clone());
    let v = crate::store::parse_timestamp_json(&json);
    if v == crate::store::NULL_TIMESTAMP {
        // Try treating the literal as a number (epoch).
        if let Ok(n) = s.parse::<i64>() {
            let nj = serde_json::Value::from(n);
            let parsed = crate::store::parse_timestamp_json(&nj);
            if parsed != crate::store::NULL_TIMESTAMP {
                return Ok(parsed);
            }
        }
        return Err(PredicateError::InvalidLiteral(format!(
            "Cannot parse '{}' as timestamp",
            s
        )));
    }
    Ok(v)
}

/// Extract a bool from a SQL literal. Accepts the native `Boolean`
/// AST node as well as the canonical truthy/falsy strings + 0/1
/// numbers — keeps filter writers from having to fight the parser
/// when they say `mifid_flag = 'true'` instead of `mifid_flag = true`.
pub(crate) fn extract_bool(expr: &sqlparser::ast::Expr) -> Result<bool, PredicateError> {
    if let sqlparser::ast::Expr::Value(sqlparser::ast::ValueWithSpan { value, .. }) = expr {
        if let sqlparser::ast::Value::Boolean(b) = value {
            return Ok(*b);
        }
    }
    let s = extract_string_value(expr)?;
    match s.to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "y" => Ok(true),
        "false" | "0" | "no" | "n" => Ok(false),
        _ => Err(PredicateError::InvalidLiteral(format!(
            "Cannot parse '{}' as bool",
            s
        ))),
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PredicateError {
    #[error("Unknown column: {0}")]
    UnknownColumn(String),
    #[error("Unsupported operator: {0}")]
    UnsupportedOperator(String),
    #[error("Unsupported expression: {0}")]
    UnsupportedExpression(String),
    #[error("Type mismatch: {0}")]
    TypeMismatch(String),
    #[error("Invalid literal: {0}")]
    InvalidLiteral(String),
    #[error("Invalid pattern: {0}")]
    InvalidPattern(String),
    #[error("Parse error: {0}")]
    ParseError(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::parse_query;
    use crate::store::Value;
    use std::sync::Arc;

    // ───── P13 — MATCHES_REGEX ───────────────────────────────────────

    #[test]
    fn parses_matches_regex() {
        let (schema, _) = make_store();
        let q = parse_query(
            "SELECT symbol FROM trades WHERE MATCHES_REGEX(symbol, '^A.*')",
            &schema,
        )
        .expect("MATCHES_REGEX must parse");
        assert!(matches!(
            q.predicate,
            CompiledPredicate::Regex { .. }
        ));
    }

    #[test]
    fn matches_regex_filters_rows() {
        // Fixture: AAPL, MSFT, GOOGL.
        let (schema, store) = make_store();
        let q = parse_query(
            "SELECT symbol FROM trades WHERE MATCHES_REGEX(symbol, '^[AM].*')",
            &schema,
        )
        .unwrap();
        let r = crate::query::execute_query(&q, &store);
        let syms: std::collections::HashSet<String> = r
            .rows
            .iter()
            .map(|row| row.get("symbol").unwrap().as_str().unwrap().to_string())
            .collect();
        assert!(syms.contains("AAPL"));
        assert!(syms.contains("MSFT"));
        assert!(!syms.contains("GOOGL"));
    }

    #[test]
    fn matches_regex_invalid_pattern_errors_at_parse() {
        let (schema, _) = make_store();
        // Unclosed character class.
        let r = parse_query(
            "SELECT symbol FROM trades WHERE MATCHES_REGEX(symbol, '[unclosed')",
            &schema,
        );
        assert!(r.is_err(), "invalid regex must error at parse time");
    }

    #[test]
    fn matches_regex_combined_with_and() {
        let (schema, store) = make_store();
        let q = parse_query(
            "SELECT symbol FROM trades \
             WHERE MATCHES_REGEX(symbol, '^A.*') AND desk = 'RATES'",
            &schema,
        )
        .unwrap();
        let r = crate::query::execute_query(&q, &store);
        // AAPL has desk RATES; no other A-prefixed symbols → only AAPL.
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0].get("symbol").unwrap().as_str().unwrap(), "AAPL");
    }


    fn make_store() -> (Arc<Schema>, ColumnStore) {
        let schema = Arc::new(Schema::from_strs(
            &["symbol", "price", "quantity", "desk"],
            &[
                ColumnType::String,
                ColumnType::Double,
                ColumnType::Long,
                ColumnType::String,
            ],
        ));
        let mut store = ColumnStore::new(schema.clone(), 10);

        store.append_row(&[
            Value::String(Some(CompactString::new("AAPL"))),
            Value::Double(150.0),
            Value::Long(100),
            Value::String(Some(CompactString::new("RATES"))),
        ]);
        store.append_row(&[
            Value::String(Some(CompactString::new("MSFT"))),
            Value::Double(300.0),
            Value::Long(50),
            Value::String(Some(CompactString::new("EQUITIES"))),
        ]);
        store.append_row(&[
            Value::String(Some(CompactString::new("GOOGL"))),
            Value::Double(2800.0),
            Value::Long(10),
            Value::String(Some(CompactString::new("RATES"))),
        ]);

        (schema, store)
    }

    fn compile(sql_where: &str, schema: &Schema) -> CompiledPredicate {
        let sql = format!("SELECT * FROM t WHERE {sql_where}");
        let parsed = crate::query::parse_query(&sql, schema).expect("parse");
        parsed.predicate
    }

    /// Row 0 has NULL `desk` and NULL `price`; row 1 is fully populated.
    fn make_store_with_nulls() -> (Arc<Schema>, ColumnStore) {
        let schema = Arc::new(Schema::from_strs(
            &["symbol", "price", "quantity", "desk"],
            &[
                ColumnType::String,
                ColumnType::Double,
                ColumnType::Long,
                ColumnType::String,
            ],
        ));
        let mut store = ColumnStore::new(schema.clone(), 10);
        store.append_row(&[
            Value::String(Some(CompactString::new("AAPL"))),
            Value::Double(f64::NAN), // NULL price
            Value::Long(100),
            Value::String(None), // NULL desk
        ]);
        store.append_row(&[
            Value::String(Some(CompactString::new("MSFT"))),
            Value::Double(300.0),
            Value::Long(50),
            Value::String(Some(CompactString::new("RATES"))),
        ]);
        (schema, store)
    }

    #[test]
    fn null_three_valued_logic_excludes_null_rows() {
        let (schema, store) = make_store_with_nulls();
        // row 0: desk = NULL, price = NULL. row 1: desk = 'RATES', price = 300.

        // Equality on NULL is UNKNOWN -> excluded.
        let eq = compile("desk = 'RATES'", &schema);
        assert!(!eq.matches(&store, 0), "NULL desk must not match `= 'RATES'`");
        assert!(eq.matches(&store, 1));

        // <> on NULL is UNKNOWN, NOT TRUE -> must exclude the NULL row
        // (regression: previously leaked NULLs as `true`).
        let neq = compile("desk <> 'RATES'", &schema);
        assert!(!neq.matches(&store, 0), "NULL desk must not match `<> 'RATES'`");
        assert!(!neq.matches(&store, 1));

        // NOT IN on NULL is UNKNOWN -> excluded (regression).
        let not_in = compile("desk NOT IN ('RATES','FX')", &schema);
        assert!(!not_in.matches(&store, 0), "NULL desk must not match NOT IN");
        assert!(!not_in.matches(&store, 1));

        // NOT (col = lit) on NULL is NOT UNKNOWN = UNKNOWN -> excluded
        // (regression: Not used to flip the leaf's false to true).
        let not_eq = compile("NOT (desk = 'RATES')", &schema);
        assert!(!not_eq.matches(&store, 0), "NOT over a NULL comparison must not match");
        assert!(!not_eq.matches(&store, 1));

        // Numeric <> on NULL (NaN) is UNKNOWN -> excluded (regression).
        let price_neq = compile("price <> 100.0", &schema);
        assert!(!price_neq.matches(&store, 0), "NULL price must not match `<> 100`");
        assert!(price_neq.matches(&store, 1));

        // IS NULL / IS NOT NULL stay total.
        let is_null = compile("desk IS NULL", &schema);
        assert!(is_null.matches(&store, 0));
        assert!(!is_null.matches(&store, 1));
    }

    #[test]
    fn null_kleene_and_or() {
        let (schema, store) = make_store_with_nulls();
        // row 0: desk NULL, quantity = 100.
        // (desk = 'RATES') AND (quantity = 999): Unknown AND False = False.
        let and_false = compile("desk = 'RATES' AND quantity = 999", &schema);
        assert_eq!(and_false.matches_3vl(&store, 0), Tri::False);
        // (desk = 'RATES') OR (quantity = 100): Unknown OR True = True.
        let or_true = compile("desk = 'RATES' OR quantity = 100", &schema);
        assert_eq!(or_true.matches_3vl(&store, 0), Tri::True);
        // (desk = 'RATES') AND (quantity = 100): Unknown AND True = Unknown.
        let and_unknown = compile("desk = 'RATES' AND quantity = 100", &schema);
        assert_eq!(and_unknown.matches_3vl(&store, 0), Tri::Unknown);
    }

    #[test]
    fn upper_eq_matches_case_insensitively_for_literal_already_uppercase() {
        let (schema, store) = make_store();
        let pred = compile("UPPER(desk) = 'RATES'", &schema);
        assert!(pred.matches(&store, 0));
        assert!(!pred.matches(&store, 1));
        assert!(pred.matches(&store, 2));
    }

    #[test]
    fn upper_eq_normalizes_lowercase_literal_at_compile_time() {
        // Literal `rates` is normalized to `RATES` at compile time,
        // then `UPPER(desk)` matches per-row.
        let (schema, store) = make_store();
        let pred = compile("UPPER(desk) = 'rates'", &schema);
        assert!(pred.matches(&store, 0));
        assert!(pred.matches(&store, 2));
    }

    #[test]
    fn lower_eq_works_with_mixed_case() {
        let (schema, store) = make_store();
        let pred = compile("LOWER(desk) = 'Equities'", &schema);
        // Literal lowered to "equities"; row value "EQUITIES" lowered
        // to "equities" → match.
        assert!(pred.matches(&store, 1));
        assert!(!pred.matches(&store, 0));
    }

    #[test]
    fn upper_like_pattern() {
        let (schema, store) = make_store();
        let pred = compile("UPPER(symbol) LIKE 'A%'", &schema);
        assert!(pred.matches(&store, 0)); // AAPL
        assert!(!pred.matches(&store, 1));
    }

    #[test]
    fn length_comparisons() {
        let (schema, store) = make_store();
        let eq4 = compile("LENGTH(symbol) = 4", &schema);
        // AAPL, MSFT, GOOGL → 4, 4, 5
        assert!(eq4.matches(&store, 0));
        assert!(eq4.matches(&store, 1));
        assert!(!eq4.matches(&store, 2));

        let gt4 = compile("LENGTH(symbol) > 4", &schema);
        assert!(!gt4.matches(&store, 0));
        assert!(gt4.matches(&store, 2));
    }

    #[test]
    fn substr_eq_matches_prefix_slice() {
        let (schema, store) = make_store();
        // SUBSTR(symbol, 1, 3) → first 3 chars. AAPL → "AAP", MSFT → "MSF",
        // GOOGL → "GOO".
        let pred = compile("SUBSTR(symbol, 1, 3) = 'AAP'", &schema);
        assert!(pred.matches(&store, 0));
        assert!(!pred.matches(&store, 1));
        assert!(!pred.matches(&store, 2));
    }

    #[test]
    fn substr_without_len_takes_rest() {
        let (schema, store) = make_store();
        let pred = compile("SUBSTR(symbol, 2) = 'OOGL'", &schema);
        assert!(!pred.matches(&store, 0));
        assert!(pred.matches(&store, 2));
    }

    #[test]
    fn substr_clamps_out_of_range() {
        // start past end → empty string.
        let (schema, store) = make_store();
        let pred = compile("SUBSTR(symbol, 100) = ''", &schema);
        assert!(pred.matches(&store, 0));
        assert!(pred.matches(&store, 1));
    }

    #[test]
    fn concat_with_literal_separator() {
        let (schema, store) = make_store();
        // desk-symbol pairs. AAPL/RATES → "RATES:AAPL".
        let pred = compile("CONCAT(desk, ':', symbol) = 'RATES:AAPL'", &schema);
        assert!(pred.matches(&store, 0));
        assert!(!pred.matches(&store, 1));
    }

    #[test]
    fn concat_like_with_pattern() {
        let (schema, store) = make_store();
        let pred = compile("CONCAT(desk, '-', symbol) LIKE 'RATES-%'", &schema);
        assert!(pred.matches(&store, 0)); // RATES-AAPL
        assert!(!pred.matches(&store, 1)); // EQUITIES-MSFT
        assert!(pred.matches(&store, 2)); // RATES-GOOGL
    }

    #[test]
    fn nested_upper_substr() {
        // UPPER(SUBSTR(desk, 1, 4)) — clip to 4 chars then uppercase.
        let (schema, store) = make_store();
        let pred = compile("UPPER(SUBSTR(desk, 1, 4)) = 'RATE'", &schema);
        assert!(pred.matches(&store, 0)); // desk RATES → "RATE"
        assert!(!pred.matches(&store, 1));
    }

    #[test]
    fn substr_with_zero_or_negative_args_returns_empty_segment() {
        let (schema, store) = make_store();
        // start <= 0 normalizes to start=1; len=0 → empty.
        let pred = compile("SUBSTR(symbol, 1, 0) = ''", &schema);
        assert!(pred.matches(&store, 0));
        assert!(pred.matches(&store, 1));
    }

    #[test]
    fn length_on_non_string_errors() {
        let (schema, _store) = make_store();
        let sql = "SELECT * FROM t WHERE LENGTH(price) > 5";
        let r = crate::query::parse_query(sql, &schema);
        assert!(r.is_err(), "LENGTH on numeric column must error");
    }

    #[test]
    fn test_eq_string() {
        let (_, store) = make_store();
        let pred = CompiledPredicate::EqString {
            col: 3, // desk
            value: CompactString::new("RATES"),
        };
        assert!(pred.matches(&store, 0));
        assert!(!pred.matches(&store, 1));
        assert!(pred.matches(&store, 2));
    }

    #[test]
    fn test_gt_double() {
        let (_, store) = make_store();
        let pred = CompiledPredicate::GtDouble {
            col: 1, // price
            value: 200.0,
        };
        assert!(!pred.matches(&store, 0)); // 150
        assert!(pred.matches(&store, 1));  // 300
        assert!(pred.matches(&store, 2));  // 2800
    }

    #[test]
    fn test_and_or() {
        let (_, store) = make_store();
        // desk = 'RATES' AND price > 200
        let pred = CompiledPredicate::And(
            Box::new(CompiledPredicate::EqString {
                col: 3,
                value: CompactString::new("RATES"),
            }),
            Box::new(CompiledPredicate::GtDouble {
                col: 1,
                value: 200.0,
            }),
        );
        assert!(!pred.matches(&store, 0)); // RATES but 150
        assert!(!pred.matches(&store, 1)); // EQUITIES
        assert!(pred.matches(&store, 2));  // RATES and 2800
    }

    #[test]
    fn test_like() {
        let (_, store) = make_store();
        let regex_str = like_to_regex("A%");
        let regex = regex::Regex::new(&regex_str).unwrap();
        let pred = CompiledPredicate::Like {
            col: 0, // symbol
            pattern: regex,
        };
        assert!(pred.matches(&store, 0));  // AAPL
        assert!(!pred.matches(&store, 1)); // MSFT
        assert!(!pred.matches(&store, 2)); // GOOGL
    }

    #[test]
    fn like_to_regex_handles_escape() {
        // Literal percent / underscore via backslash escape.
        let re = regex::Regex::new(&like_to_regex(r"50\%")).unwrap();
        assert!(re.is_match("50%"));
        assert!(!re.is_match("5050"));

        let re = regex::Regex::new(&like_to_regex(r"foo\_bar")).unwrap();
        assert!(re.is_match("foo_bar"));
        assert!(!re.is_match("fooXbar"));

        // Escaped backslash → literal backslash.
        let re = regex::Regex::new(&like_to_regex(r"a\\b")).unwrap();
        assert!(re.is_match(r"a\b"));

        // Unescaped wildcards still wildcard.
        let re = regex::Regex::new(&like_to_regex("foo%")).unwrap();
        assert!(re.is_match("foobar"));
        assert!(re.is_match("foo"));

        // Regex metacharacters get auto-escaped.
        let re = regex::Regex::new(&like_to_regex("a.b")).unwrap();
        assert!(re.is_match("a.b"));
        assert!(!re.is_match("aXb"));
    }

    #[test]
    fn test_in_string() {
        let (_, store) = make_store();
        let values: HashSet<CompactString> = ["AAPL", "GOOGL"]
            .iter()
            .map(|s| CompactString::new(s))
            .collect();
        let pred = CompiledPredicate::InString { col: 0, values };
        assert!(pred.matches(&store, 0));
        assert!(!pred.matches(&store, 1));
        assert!(pred.matches(&store, 2));
    }

    #[test]
    fn test_between_double() {
        let (_, store) = make_store();
        let pred = CompiledPredicate::BetweenDouble {
            col: 1,
            low: 100.0,
            high: 500.0,
        };
        assert!(pred.matches(&store, 0));  // 150
        assert!(pred.matches(&store, 1));  // 300
        assert!(!pred.matches(&store, 2)); // 2800
    }
}

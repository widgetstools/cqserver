//! Authentication + per-user entitlements.
//!
//! Each user has a bcrypt-hashed password and a list of entitlements of
//! the form `op:pattern` (e.g. `publish:/orders`, `subscribe:/market-*`,
//! `*:*` for full access). When `AuthStore::required` is true, the
//! router gates every command except `Logon` and `Heartbeat` on a
//! successful logon and the user's entitlements.

use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Op {
    Publish,
    Subscribe,
    Sow,
    Delete,
    Admin,
}

impl Op {
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "publish" => Some(Op::Publish),
            "subscribe" => Some(Op::Subscribe),
            "sow" => Some(Op::Sow),
            "delete" => Some(Op::Delete),
            "admin" => Some(Op::Admin),
            _ => None,
        }
    }
}

/// One `op:pattern` rule. `pattern == "*"` matches every topic;
/// otherwise it's an exact match unless the pattern ends with `*`, in
/// which case it's a prefix match.
#[derive(Debug, Clone)]
pub struct Entitlement {
    pub op: Op,
    pub pattern: String,
    is_wildcard_op: bool,
}

impl Entitlement {
    /// Parse one entitlement string. `*:*` and `*` both grant everything;
    /// `op:pattern` grants `op` on topics matching `pattern`.
    pub fn parse(s: &str) -> Option<Self> {
        if s == "*" || s == "*:*" {
            return Some(Entitlement {
                op: Op::Admin, // unused when is_wildcard_op
                pattern: "*".into(),
                is_wildcard_op: true,
            });
        }
        let (op_str, pattern) = s.split_once(':')?;
        let op = Op::parse(op_str)?;
        Some(Entitlement {
            op,
            pattern: pattern.to_string(),
            is_wildcard_op: false,
        })
    }

    pub fn matches(&self, op: Op, topic: &str) -> bool {
        if !self.is_wildcard_op && self.op != op {
            return false;
        }
        if self.pattern == "*" {
            return true;
        }
        match self.pattern.strip_suffix('*') {
            Some(prefix) => topic.starts_with(prefix),
            None => self.pattern == topic,
        }
    }
}

#[derive(Debug, Clone)]
pub struct User {
    pub username: String,
    pub password_hash: String,
    pub entitlements: Vec<Entitlement>,
    /// Optional row-level entitlement: a SQL WHERE-clause fragment
    /// that's AND'd into every subscribe/sow query this user issues.
    /// Lets the server enforce restrictions like
    /// "alice can only see desk='RATES' rows" without trusting the
    /// client to filter correctly.
    pub row_filter: Option<String>,
}

impl User {
    /// Build a user from raw config strings. Returns `None` if any
    /// entitlement string fails to parse.
    pub fn from_parts(
        username: String,
        password_hash: String,
        entitlements: &[String],
    ) -> Option<Self> {
        Self::from_parts_with_row_filter(username, password_hash, entitlements, None)
    }

    /// Same as `from_parts` but also carries a row-level filter that
    /// will be AND'd into every subscribe/sow this user issues.
    pub fn from_parts_with_row_filter(
        username: String,
        password_hash: String,
        entitlements: &[String],
        row_filter: Option<String>,
    ) -> Option<Self> {
        let mut parsed = Vec::with_capacity(entitlements.len());
        for e in entitlements {
            parsed.push(Entitlement::parse(e)?);
        }
        Some(User {
            username,
            password_hash,
            entitlements: parsed,
            row_filter,
        })
    }

    /// True iff any of this user's entitlements grants `op` on `topic`.
    pub fn can(&self, op: Op, topic: &str) -> bool {
        self.entitlements.iter().any(|e| e.matches(op, topic))
    }
}

/// Shared, immutable view of the auth configuration. Cloned (cheaply,
/// via `Arc`) into every transport for per-connection gating.
pub struct AuthStore {
    pub required: bool,
    users: HashMap<String, User>,
}

impl AuthStore {
    pub fn new(required: bool, users: Vec<User>) -> Self {
        let map = users
            .into_iter()
            .map(|u| (u.username.clone(), u))
            .collect();
        AuthStore {
            required,
            users: map,
        }
    }

    pub fn disabled() -> Self {
        AuthStore {
            required: false,
            users: HashMap::new(),
        }
    }

    pub fn user_count(&self) -> usize {
        self.users.len()
    }

    /// Look up the row-level entitlement filter for `username`.
    /// Returns `None` for unauthenticated sessions (auth disabled) or
    /// users with no row_filter configured.
    pub fn row_filter_for(&self, username: &str) -> Option<String> {
        self.users.get(username).and_then(|u| u.row_filter.clone())
    }

    /// Verify `(username, password)`. Returns the matching user on
    /// success. Constant-time-ish: the bcrypt verify call dominates
    /// timing for both hit and miss paths.
    pub fn verify(&self, username: &str, password: &str) -> Option<User> {
        let user = self.users.get(username)?;
        match bcrypt::verify(password, &user.password_hash) {
            Ok(true) => Some(user.clone()),
            _ => None,
        }
    }
}

pub type SharedAuth = Arc<AuthStore>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entitlement_exact_match() {
        let e = Entitlement::parse("publish:/orders").unwrap();
        assert!(e.matches(Op::Publish, "/orders"));
        assert!(!e.matches(Op::Publish, "/orders-archive"));
        assert!(!e.matches(Op::Subscribe, "/orders"));
    }

    #[test]
    fn entitlement_prefix_wildcard() {
        let e = Entitlement::parse("subscribe:/market-*").unwrap();
        assert!(e.matches(Op::Subscribe, "/market-data"));
        assert!(e.matches(Op::Subscribe, "/market-data-bars"));
        assert!(!e.matches(Op::Subscribe, "/orders"));
        assert!(!e.matches(Op::Publish, "/market-data"));
    }

    #[test]
    fn entitlement_op_wildcard() {
        let e = Entitlement::parse("*:*").unwrap();
        assert!(e.matches(Op::Publish, "/anything"));
        assert!(e.matches(Op::Subscribe, "/whatever"));
        assert!(e.matches(Op::Admin, "/x"));
    }

    #[test]
    fn user_can_aggregates_entitlements() {
        let u = User::from_parts(
            "alice".into(),
            "_".into(),
            &[
                "publish:/orders".into(),
                "subscribe:/market-*".into(),
            ],
        )
        .unwrap();
        assert!(u.can(Op::Publish, "/orders"));
        assert!(!u.can(Op::Publish, "/market-data"));
        assert!(u.can(Op::Subscribe, "/market-data"));
        assert!(!u.can(Op::Subscribe, "/orders"));
    }

    #[test]
    fn row_filter_round_trips_through_store() {
        let u = User::from_parts_with_row_filter(
            "alice".into(),
            "_".into(),
            &["subscribe:/*".into()],
            Some("desk = 'RATES'".into()),
        )
        .unwrap();
        let store = AuthStore::new(true, vec![u]);
        assert_eq!(
            store.row_filter_for("alice").as_deref(),
            Some("desk = 'RATES'")
        );
        assert!(store.row_filter_for("nobody").is_none());
    }

    #[test]
    fn verify_with_bcrypt() {
        let hash = bcrypt::hash("hunter2", 4).unwrap();
        let u = User::from_parts("bob".into(), hash, &["*:*".into()]).unwrap();
        let store = AuthStore::new(true, vec![u]);
        assert!(store.verify("bob", "hunter2").is_some());
        assert!(store.verify("bob", "wrong").is_none());
        assert!(store.verify("nobody", "hunter2").is_none());
    }
}

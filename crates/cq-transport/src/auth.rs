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
    /// S16 JWT validator. When `Some`, the `Logon` handler can also
    /// accept a `data.token` field instead of `user`/`password`;
    /// the token is verified via `verify_jwt` and the matched user
    /// is built from its claims. Static `users` and JWT validation
    /// coexist — operators can use either path on a per-Logon
    /// basis. `None` disables the JWT route.
    jwt: Option<JwtValidator>,
}

/// S16 — JWT validator. Holds the decoding key + the claim names the
/// server should consult for username and entitlements. Constructed
/// once at startup from `[auth.jwt]` config.
pub struct JwtValidator {
    decoding_key: jsonwebtoken::DecodingKey,
    validation: jsonwebtoken::Validation,
    pub username_claim: String,
    pub entitlements_claim: String,
}

impl std::fmt::Debug for JwtValidator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JwtValidator")
            .field("username_claim", &self.username_claim)
            .field("entitlements_claim", &self.entitlements_claim)
            .finish_non_exhaustive()
    }
}

impl JwtValidator {
    /// Build an HS256 validator. `issuer` and `audience` are optional;
    /// when set, the corresponding `iss`/`aud` claims are also
    /// validated.
    pub fn new_hs256(
        secret: &[u8],
        issuer: Option<&str>,
        audience: Option<&str>,
        username_claim: &str,
        entitlements_claim: &str,
    ) -> Self {
        let mut validation = jsonwebtoken::Validation::new(jsonwebtoken::Algorithm::HS256);
        if let Some(iss) = issuer {
            validation.set_issuer(&[iss]);
        } else {
            // No iss claim required by default — clear the
            // requirement that `iss` matches (jsonwebtoken's default).
            validation.iss = None;
        }
        if let Some(aud) = audience {
            validation.set_audience(&[aud]);
        } else {
            // Don't require the audience claim either.
            validation.validate_aud = false;
        }
        Self {
            decoding_key: jsonwebtoken::DecodingKey::from_secret(secret),
            validation,
            username_claim: username_claim.to_string(),
            entitlements_claim: entitlements_claim.to_string(),
        }
    }

    /// Verify a JWT and extract a `User`. The username is read from
    /// `username_claim` (default `"sub"`); the entitlement list is
    /// read from `entitlements_claim` (default `"entitlements"`).
    /// Returns `None` on signature failure, expired tokens, claim
    /// missing, or malformed entitlement strings.
    pub fn verify(&self, token: &str) -> Option<User> {
        let claims: serde_json::Value = jsonwebtoken::decode::<serde_json::Value>(
            token,
            &self.decoding_key,
            &self.validation,
        )
        .map(|t| t.claims)
        .ok()?;
        let username = claims
            .get(&self.username_claim)
            .and_then(|v| v.as_str())?
            .to_string();
        let entitlement_strs: Vec<String> = match claims.get(&self.entitlements_claim) {
            Some(serde_json::Value::Array(arr)) => arr
                .iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect(),
            None => Vec::new(),
            _ => return None,
        };
        let mut parsed = Vec::with_capacity(entitlement_strs.len());
        for e in &entitlement_strs {
            parsed.push(Entitlement::parse(e)?);
        }
        Some(User {
            username,
            password_hash: String::new(), // unused for JWT-authenticated sessions
            entitlements: parsed,
            row_filter: None,
        })
    }
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
            jwt: None,
        }
    }

    /// Attach a JWT validator. Builder-style for ergonomic
    /// construction from the server's startup path.
    pub fn with_jwt(mut self, jwt: JwtValidator) -> Self {
        self.jwt = Some(jwt);
        self
    }

    pub fn disabled() -> Self {
        AuthStore {
            required: false,
            users: HashMap::new(),
            jwt: None,
        }
    }

    /// True iff a JWT validator is configured. The Logon handler
    /// consults this to decide whether to accept a `data.token`
    /// payload in addition to (or instead of) `user`/`password`.
    pub fn has_jwt(&self) -> bool {
        self.jwt.is_some()
    }

    /// Verify a JWT and produce the matched user. Returns `None` if
    /// no validator is configured or the token is invalid.
    pub fn verify_jwt(&self, token: &str) -> Option<User> {
        self.jwt.as_ref().and_then(|j| j.verify(token))
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

    fn issue_jwt(secret: &[u8], claims: serde_json::Value) -> String {
        let header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::HS256);
        jsonwebtoken::encode(
            &header,
            &claims,
            &jsonwebtoken::EncodingKey::from_secret(secret),
        )
        .expect("encode test JWT")
    }

    #[test]
    fn jwt_valid_token_extracts_user_and_entitlements() {
        let secret = b"shhh-very-secret";
        let validator = JwtValidator::new_hs256(
            secret,
            None,
            None,
            "sub",
            "entitlements",
        );
        let claims = serde_json::json!({
            "sub": "alice",
            "entitlements": ["publish:/orders", "subscribe:/market-*"],
            "exp": (chrono_unix_now() + 3600) as i64,
        });
        let token = issue_jwt(secret, claims);
        let u = validator.verify(&token).expect("valid token should pass");
        assert_eq!(u.username, "alice");
        assert!(u.can(Op::Publish, "/orders"));
        assert!(u.can(Op::Subscribe, "/market-data"));
        assert!(!u.can(Op::Publish, "/market-data"));
    }

    #[test]
    fn jwt_invalid_signature_rejected() {
        let secret = b"shhh";
        let validator = JwtValidator::new_hs256(secret, None, None, "sub", "entitlements");
        let token = issue_jwt(b"wrong-secret", serde_json::json!({
            "sub": "alice",
            "entitlements": [],
            "exp": (chrono_unix_now() + 3600) as i64,
        }));
        assert!(validator.verify(&token).is_none());
    }

    #[test]
    fn jwt_expired_token_rejected() {
        let secret = b"shhh";
        let validator = JwtValidator::new_hs256(secret, None, None, "sub", "entitlements");
        // 2 hours past expiry — well outside jsonwebtoken's default
        // 60-second `leeway` so the clock-skew tolerance can't mask the
        // expiry.
        let token = issue_jwt(secret, serde_json::json!({
            "sub": "alice",
            "entitlements": [],
            "exp": (chrono_unix_now() - 7200) as i64,
        }));
        assert!(validator.verify(&token).is_none());
    }

    #[test]
    fn jwt_issuer_mismatch_rejected() {
        let secret = b"shhh";
        let validator = JwtValidator::new_hs256(
            secret,
            Some("trusted-issuer"),
            None,
            "sub",
            "entitlements",
        );
        let token = issue_jwt(secret, serde_json::json!({
            "iss": "other-issuer",
            "sub": "alice",
            "entitlements": [],
            "exp": (chrono_unix_now() + 3600) as i64,
        }));
        assert!(validator.verify(&token).is_none());
    }

    #[test]
    fn jwt_store_routes_verify_through_validator() {
        let secret = b"abc";
        let validator = JwtValidator::new_hs256(secret, None, None, "sub", "entitlements");
        let store = AuthStore::new(true, vec![]).with_jwt(validator);
        assert!(store.has_jwt());
        let token = issue_jwt(secret, serde_json::json!({
            "sub": "carol",
            "entitlements": ["*:*"],
            "exp": (chrono_unix_now() + 3600) as i64,
        }));
        let u = store.verify_jwt(&token).expect("token should validate");
        assert_eq!(u.username, "carol");
        assert!(u.can(Op::Publish, "/anything"));
    }

    /// Tiny helper — returns current unix epoch seconds. We avoid
    /// pulling chrono just for this; use std::time directly.
    fn chrono_unix_now() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }
}

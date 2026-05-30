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
    /// Query Guardrails G5: per-user override of the server-wide
    /// query limits. `None` means "use the server's
    /// [query_limits] defaults." `Some(_)` tightens specific fields
    /// — `merge_with(server_limits)` takes the **tighter** value
    /// per field, so an override can only be more restrictive than
    /// the global setting (never more permissive).
    pub query_budget: Option<QueryBudget>,
}

/// Query Guardrails G5: per-user query-cost override. All fields are
/// optional so an admin can tighten a single dimension without
/// having to restate the entire QueryLimits. `merge_with` produces
/// the effective limits for one subscribe by picking the tighter
/// (smaller) value when both are set.
#[derive(Debug, Clone, Copy, Default)]
pub struct QueryBudget {
    pub max_sow_estimated_rows: Option<u64>,
    pub max_sow_estimated_bytes: Option<u64>,
    pub max_join_estimated_fanout: Option<u64>,
    pub max_group_estimated_cardinality: Option<u64>,
    pub hard_max_sow_result_rows: Option<u64>,
    pub hard_max_sow_result_bytes: Option<u64>,
}

impl QueryBudget {
    /// Merge this budget over `server_limits`, returning a new
    /// QueryLimits with each tunable field set to whichever side is
    /// tighter (smaller non-zero, treating 0 as "disabled / no cap").
    /// Structural fields (PIVOT IN-list cap, view chain depth, etc.)
    /// stay at the server default — per-user overrides only cover
    /// quantitative caps where "tighter" is unambiguous.
    pub fn merge_with(
        &self,
        server: &cq_core::query::QueryLimits,
    ) -> cq_core::query::QueryLimits {
        fn tighter(server_val: u64, user_val: Option<u64>) -> u64 {
            match user_val {
                None => server_val,
                Some(0) => server_val, // user explicitly disabled — ignore
                Some(u) if server_val == 0 => u, // server disabled, user tightens
                Some(u) => server_val.min(u),
            }
        }
        cq_core::query::QueryLimits {
            max_sow_estimated_rows: tighter(
                server.max_sow_estimated_rows,
                self.max_sow_estimated_rows,
            ),
            max_sow_estimated_bytes: tighter(
                server.max_sow_estimated_bytes,
                self.max_sow_estimated_bytes,
            ),
            max_join_estimated_fanout: tighter(
                server.max_join_estimated_fanout,
                self.max_join_estimated_fanout,
            ),
            max_group_estimated_cardinality: tighter(
                server.max_group_estimated_cardinality,
                self.max_group_estimated_cardinality,
            ),
            hard_max_sow_result_rows: tighter(
                server.hard_max_sow_result_rows,
                self.hard_max_sow_result_rows,
            ),
            hard_max_sow_result_bytes: tighter(
                server.hard_max_sow_result_bytes,
                self.hard_max_sow_result_bytes,
            ),
            ..*server
        }
    }
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
        Self::from_parts_full(username, password_hash, entitlements, row_filter, None)
    }

    /// G5 variant that also accepts a per-user query budget. `None`
    /// keeps the server-wide [query_limits] in force; `Some(_)` can
    /// only TIGHTEN limits (the merge picks the smaller of server +
    /// user per field).
    pub fn from_parts_full(
        username: String,
        password_hash: String,
        entitlements: &[String],
        row_filter: Option<String>,
        query_budget: Option<QueryBudget>,
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
            query_budget,
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
    /// Build a `Validation` for `algorithm` with optional `iss`/`aud`
    /// requirements. Shared by the HS256 and RS256 constructors.
    fn build_validation(
        algorithm: jsonwebtoken::Algorithm,
        issuer: Option<&str>,
        audience: Option<&str>,
    ) -> jsonwebtoken::Validation {
        let mut validation = jsonwebtoken::Validation::new(algorithm);
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
        validation
    }

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
        Self {
            decoding_key: jsonwebtoken::DecodingKey::from_secret(secret),
            validation: Self::build_validation(
                jsonwebtoken::Algorithm::HS256,
                issuer,
                audience,
            ),
            username_claim: username_claim.to_string(),
            entitlements_claim: entitlements_claim.to_string(),
        }
    }

    /// Build an RS256 validator from a PEM-encoded RSA public key.
    /// Tokens are verified against the asymmetric public key — the
    /// matching private key lives with the issuer (e.g. an identity
    /// provider), so the server never holds signing material.
    /// `issuer` and `audience` behave as in [`new_hs256`].
    pub fn new_rs256(
        public_key_pem: &[u8],
        issuer: Option<&str>,
        audience: Option<&str>,
        username_claim: &str,
        entitlements_claim: &str,
    ) -> Result<Self, jsonwebtoken::errors::Error> {
        Ok(Self {
            decoding_key: jsonwebtoken::DecodingKey::from_rsa_pem(public_key_pem)?,
            validation: Self::build_validation(
                jsonwebtoken::Algorithm::RS256,
                issuer,
                audience,
            ),
            username_claim: username_claim.to_string(),
            entitlements_claim: entitlements_claim.to_string(),
        })
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
            // JWT-authenticated users don't carry per-user budgets in
            // the token today; ops can add a `budget` claim and
            // populate this from it in a follow-up.
            query_budget: None,
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

    /// G5: look up the per-user query budget for `username`. Returns
    /// `None` for unauthenticated sessions, unknown users, or users
    /// without an override (in which case the server's
    /// `[query_limits]` defaults apply unchanged).
    pub fn query_budget_for(&self, username: &str) -> Option<QueryBudget> {
        self.users.get(username).and_then(|u| u.query_budget)
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

    // Throwaway 2048-bit RSA keypair generated solely for these tests
    // (never used to sign anything real).
    const TEST_RSA_PRIV_PEM: &str = "-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC3n6HOSbrrYORQ
6YXOqRf60PLQ6jM8ypDxDvgUXe/QwIbjOxs+t8/wmwXUutUjcQPCy6yQQOkHSsw4
qiGFS3TJrUhaf3hGCN2psyNYIHA5R/JO5u9lXVfMASTWvsEtIprODGyPwWJ1xy8i
4OdTyo4u2RX6HujZoRnP+h1a6nIB7EyXLc8814M7Ei7iDP2wLgBhLt7DCpATTieI
idSs5tIw1taYvX6h2U+6JNlwDOkeFMkqVJwWMD2GpJv03vEhQkYybWz1/BRY28Cj
DeGCiqMdcWyEn1rbI/QHCnVH+myVePzfwuEUJ4zcbxbNLbsieMx5+GerJoVMHSON
bQUpk8HpAgMBAAECggEAPN8E1ytW9R9+IJqBWpRwmBt7WylASgdGzDqvn6TSUqv4
K0zVR9HMc5EYekBjVqfo3MMDFiEGfv3bPG+dxB/S++ZfRWzbVLAst0xky5qZSUvh
9ikVNE+gwsagTTYYONuvYN36gR9VAgFBTXksBnlv7/TUFcH4Y+jFc04RPCnbGGqK
O7DKzAASzZUQTtLJw3JCUuFoPTseJCFMmCEbQ5R/k9uVMM1M9p8zR73VM/BVkMy9
SAHIt6aUdVlXG+kmdi2wM96OYLtyJ8LAr8h/3+gTKkzXqEaZ0//pk4BIzblosU2r
W75E9iMoQJr0Bhwd/ZZ79injKa7vQX/Ovl0AxY2ntwKBgQDvE8ufbCszXDpRA86Q
T9zTPXNUyoY4iWeL435PXg/VMO5huIwwRfqi2A5/BsJTdSwvKxBJHgyX3kCOadsg
6e+eH/zAAgEbCaIyaIkK+7YZdca3hE186Xv7vPuS+KWPnbqkmfPleutPu3AH3gLY
bwMGpvaooAVYKaCnTRorbo9TuwKBgQDEnvxfYBWXzmUrIJPKfeAeD6OhCP3oIrbh
832UTNViRg781vjiVGWwxztM9pdmhM59FWy+juv/rwb59kOlPPunwNHHkv/P8QMH
UHOUUSJInjXnmoakvwgoIwJ3dUD6y34eF96WZeZKw9ul0d3Qq4Y0QCJyfGh16+Yj
ZAY+v7U8qwKBgFhtSfM9Xv0wL6Gnds+JunOnVvEVt29R4yqqih1w/Qotfv5F9BQm
zf1NTI9PQLD9tcn8c5mXs7C4U8hY/uO9oxMpYaLjGuWVOpjKcWXOlBv2o/lcxgxd
j64cyDAkJ5hnDpGzH7LRNBfZjCZcx1CmPshHGRRlm5RwUSuQKQ3HZtvhAoGBAI9B
31OGaHUw9llT5RqWWCLO9kOwj38BPAqpJAhXaumtbeIepzwQjf8dSkGrMWiKvwA4
CgFVlPG4Dvc0zNip9BmnzbEBk81oJvK/VVbtPnN2goP6/LswTLshtvxevDd+6Kb4
cT9Xg1FaHsFUha8yKhgL2o1bw6iXdhi3Gi3B9ET9AoGAewoHOVwydKtZujX+Pwg7
sZBvvSudl5J1VZJaG5UBV+D6VHaRvZMysDUh8ZcbadYVmewChtmoQfm/uRd8wHY4
tP+uTizPtPDU+ACu+3wD/clI/cmi80QIeB9tuXSGZLXCMslnUU/wZOiKxyL2M/Gs
IlVX9kUfhE6EfAHJUGsFAqg=
-----END PRIVATE KEY-----";
    const TEST_RSA_PUB_PEM: &str = "-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAt5+hzkm662DkUOmFzqkX
+tDy0OozPMqQ8Q74FF3v0MCG4zsbPrfP8JsF1LrVI3EDwsuskEDpB0rMOKohhUt0
ya1IWn94RgjdqbMjWCBwOUfyTubvZV1XzAEk1r7BLSKazgxsj8FidccvIuDnU8qO
LtkV+h7o2aEZz/odWupyAexMly3PPNeDOxIu4gz9sC4AYS7ewwqQE04niInUrObS
MNbWmL1+odlPuiTZcAzpHhTJKlScFjA9hqSb9N7xIUJGMm1s9fwUWNvAow3hgoqj
HXFshJ9a2yP0Bwp1R/pslXj838LhFCeM3G8WzS27InjMefhnqyaFTB0jjW0FKZPB
6QIDAQAB
-----END PUBLIC KEY-----";

    fn issue_jwt_rs256(priv_pem: &[u8], claims: serde_json::Value) -> String {
        let header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256);
        let key = jsonwebtoken::EncodingKey::from_rsa_pem(priv_pem)
            .expect("load test RSA private key");
        jsonwebtoken::encode(&header, &claims, &key).expect("encode RS256 test JWT")
    }

    #[test]
    fn jwt_rs256_valid_token_extracts_user() {
        let validator = JwtValidator::new_rs256(
            TEST_RSA_PUB_PEM.as_bytes(),
            None,
            None,
            "sub",
            "entitlements",
        )
        .expect("public key parses");
        let token = issue_jwt_rs256(
            TEST_RSA_PRIV_PEM.as_bytes(),
            serde_json::json!({
                "sub": "dave",
                "entitlements": ["subscribe:/feeds-*"],
                "exp": (chrono_unix_now() + 3600) as i64,
            }),
        );
        let u = validator.verify(&token).expect("valid RS256 token should pass");
        assert_eq!(u.username, "dave");
        assert!(u.can(Op::Subscribe, "/feeds-eu"));
        assert!(!u.can(Op::Publish, "/feeds-eu"));
    }

    #[test]
    fn jwt_rs256_rejects_hs256_token_with_pubkey_as_secret() {
        // "alg confusion" downgrade: an attacker signs an HS256 token
        // using the public key bytes as the HMAC secret, hoping the
        // server validates it with the same public key. Pinning the
        // validator to RS256 must reject it.
        let validator = JwtValidator::new_rs256(
            TEST_RSA_PUB_PEM.as_bytes(),
            None,
            None,
            "sub",
            "entitlements",
        )
        .expect("public key parses");
        let forged = issue_jwt(
            TEST_RSA_PUB_PEM.as_bytes(),
            serde_json::json!({
                "sub": "attacker",
                "entitlements": ["*:*"],
                "exp": (chrono_unix_now() + 3600) as i64,
            }),
        );
        assert!(validator.verify(&forged).is_none());
    }

    #[test]
    fn jwt_rs256_malformed_public_key_errors() {
        let err = JwtValidator::new_rs256(
            b"not a pem key",
            None,
            None,
            "sub",
            "entitlements",
        );
        assert!(err.is_err());
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

    // ─── Query Guardrails G5 — per-user budget merge ───────────────

    #[test]
    fn g5_budget_none_returns_server_limits_unchanged() {
        let server = cq_core::query::QueryLimits::default();
        let budget = QueryBudget::default(); // all None
        let effective = budget.merge_with(&server);
        assert_eq!(effective.max_sow_estimated_rows, server.max_sow_estimated_rows);
        assert_eq!(effective.hard_max_sow_result_rows, server.hard_max_sow_result_rows);
    }

    #[test]
    fn g5_user_can_tighten_but_not_loosen() {
        let server = cq_core::query::QueryLimits {
            max_sow_estimated_rows: 1_000_000,
            ..cq_core::query::QueryLimits::default()
        };
        let budget = QueryBudget {
            max_sow_estimated_rows: Some(10_000), // tighter
            ..QueryBudget::default()
        };
        let effective = budget.merge_with(&server);
        assert_eq!(effective.max_sow_estimated_rows, 10_000);

        let budget_loose = QueryBudget {
            max_sow_estimated_rows: Some(10_000_000), // user tries to loosen
            ..QueryBudget::default()
        };
        let effective = budget_loose.merge_with(&server);
        // Tighter (server) wins.
        assert_eq!(effective.max_sow_estimated_rows, 1_000_000);
    }

    #[test]
    fn g5_user_zero_is_treated_as_disabled_not_unlimited() {
        // Operator intent: user wrote `max_sow_estimated_rows = 0`
        // meaning "I don't want a per-user override on this field."
        // Server limit must remain in force.
        let server = cq_core::query::QueryLimits {
            max_sow_estimated_rows: 500,
            ..cq_core::query::QueryLimits::default()
        };
        let budget = QueryBudget {
            max_sow_estimated_rows: Some(0),
            ..QueryBudget::default()
        };
        assert_eq!(budget.merge_with(&server).max_sow_estimated_rows, 500);
    }

    #[test]
    fn g5_user_tightens_when_server_disabled() {
        // Server has the cap turned off (0); user wants to enforce
        // one for themselves. Per-user cap should win.
        let server = cq_core::query::QueryLimits {
            max_sow_estimated_rows: 0, // server disabled
            ..cq_core::query::QueryLimits::default()
        };
        let budget = QueryBudget {
            max_sow_estimated_rows: Some(10_000),
            ..QueryBudget::default()
        };
        assert_eq!(budget.merge_with(&server).max_sow_estimated_rows, 10_000);
    }

    #[test]
    fn g5_user_budget_round_trips_through_user_struct() {
        let budget = QueryBudget {
            max_sow_estimated_rows: Some(5_000),
            ..QueryBudget::default()
        };
        let u = User::from_parts_full(
            "viewer-bob".into(),
            "pw".into(),
            &["subscribe:*".into()],
            None,
            Some(budget),
        )
        .unwrap();
        assert_eq!(
            u.query_budget.and_then(|b| b.max_sow_estimated_rows),
            Some(5_000)
        );
    }
}

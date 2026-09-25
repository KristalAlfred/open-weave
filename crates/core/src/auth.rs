//! Bearer-token authentication for the control-plane HTTP surfaces.
//!
//! Operators and the CLI present one shared secret, [`NORTHBOUND_TOKEN_VAR`], to
//! northbound, which re-presents it to the controller. Each node presents a
//! token of its own, [`SOUTHBOUND_TOKEN_VAR`], to southbound, which forwards it
//! to the controller. A node token is
//! `<node id>.<epoch>.<hex HMAC-SHA256(key, "<node id>.<epoch>")>`, where the key
//! is [`SOUTHBOUND_KEY_VAR`]: southbound and the controller hold the key, derive
//! the node id from the token, and refuse a node acting under another node's id.
//! [`SOUTHBOUND_MIN_EPOCHS_VAR`] revokes one node's older tokens.
//!
//! [`Guard::from_env`] and [`NodeGuard::from_env`] error when their variable is
//! unset or blank, and [`NodeGuard::from_env`] also when the key is shorter than
//! [`MIN_NODE_KEY_LEN`], so a service with no secret or a weak key does not start.
//! [`AUTH_DISABLED_VAR`] switches authentication off for local development.

use std::collections::BTreeMap;
use std::fmt;

use hmac::{Hmac, Mac};
use sha2::Sha256;

use crate::validate_resource_id;

/// Environment variable holding the token for the northbound (operator) surface.
pub const NORTHBOUND_TOKEN_VAR: &str = "WEAVE_NORTHBOUND_TOKEN";
/// Environment variable holding a node's own southbound token.
pub const SOUTHBOUND_TOKEN_VAR: &str = "WEAVE_SOUTHBOUND_TOKEN";
/// Environment variable holding the key southbound and the controller derive
/// node tokens from.
pub const SOUTHBOUND_KEY_VAR: &str = "WEAVE_SOUTHBOUND_KEY";
/// The fewest characters a [`NodeKey`] may have.
pub const MIN_NODE_KEY_LEN: usize = 32;
/// Environment variable holding the lowest token epoch southbound and the
/// controller accept for a node, as comma-separated `<node id>=<epoch>` pairs.
pub const SOUTHBOUND_MIN_EPOCHS_VAR: &str = "WEAVE_SOUTHBOUND_MIN_EPOCHS";
/// Set to `1` or `true` to serve and call without authentication. Local
/// development only.
pub const AUTH_DISABLED_VAR: &str = "WEAVE_AUTH_DISABLED";

/// A bearer token.
///
/// [`fmt::Debug`] redacts the value, no [`fmt::Display`] is implemented, and
/// [`Token::header_value`] is the only accessor.
#[derive(Clone, PartialEq, Eq)]
pub struct Token(String);

impl Token {
    /// Wrap a token value, trimming surrounding whitespace.
    ///
    /// Returns `None` when the value is blank, so an env var set to the empty
    /// string is treated as unset rather than as a token that matches nothing.
    #[must_use]
    pub fn new(value: &str) -> Option<Self> {
        let value = value.trim();
        (!value.is_empty()).then(|| Self(value.to_string()))
    }

    /// Read a token from environment variable `var`, `None` when unset or blank.
    #[must_use]
    pub fn from_env(var: &str) -> Option<Self> {
        std::env::var(var).ok().as_deref().and_then(Token::new)
    }

    /// The `Authorization` header value presenting this token.
    #[must_use]
    pub fn header_value(&self) -> String {
        format!("Bearer {}", self.0)
    }

    /// Whether `header` is an `Authorization` value presenting this token.
    ///
    /// The scheme is matched normally — it carries no secret — and the token
    /// itself is compared in constant time. Only the token's *length* can leak,
    /// which is inherent to comparing variable-length secrets.
    #[must_use]
    pub fn matches_header(&self, header: &str) -> bool {
        bearer_value(header)
            .is_some_and(|presented| constant_time_eq(presented.as_bytes(), self.0.as_bytes()))
    }
}

impl fmt::Debug for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Token(<redacted>)")
    }
}

/// The token presented by an `Authorization: Bearer <token>` header value.
fn bearer_value(header: &str) -> Option<&str> {
    let (scheme, value) = header.split_once(' ')?;
    scheme
        .eq_ignore_ascii_case("bearer")
        .then(|| value.trim_start())
}

/// Compare two byte strings without an early exit on the first differing byte.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |diff, (x, y)| diff | (x ^ y)) == 0
}

/// Whether a surface authenticates requests, and with which token.
#[derive(Clone, Debug)]
pub enum Guard {
    /// Every request must present this token.
    Required(Token),
    /// Authentication is switched off via [`AUTH_DISABLED_VAR`]; every request
    /// passes.
    Disabled,
}

impl Guard {
    /// Resolve the guard for the surface whose token lives in `var`.
    ///
    /// # Errors
    /// Returns [`AuthError::MissingToken`] when `var` is unset or blank and the
    /// [`AUTH_DISABLED_VAR`] escape hatch is not engaged.
    pub fn from_env(var: &str) -> Result<Self, AuthError> {
        if auth_disabled() {
            return Ok(Self::Disabled);
        }
        Token::from_env(var)
            .map(Self::Required)
            .ok_or_else(|| AuthError::MissingToken {
                var: var.to_string(),
            })
    }

    /// The token this guard requires, `None` when authentication is disabled.
    ///
    /// A stateless proxy re-presents its own surface token to the controller, so
    /// this doubles as the outbound client credential.
    #[must_use]
    pub fn token(&self) -> Option<&Token> {
        match self {
            Self::Required(token) => Some(token),
            Self::Disabled => None,
        }
    }

    /// Whether authentication is switched off.
    #[must_use]
    pub fn is_disabled(&self) -> bool {
        matches!(self, Self::Disabled)
    }
}

/// The key node tokens are derived from, and the lowest epoch accepted for each
/// node. [`fmt::Debug`] redacts the key.
#[derive(Clone)]
pub struct NodeKey {
    key: Vec<u8>,
    min_epochs: MinEpochs,
}

/// The lowest token epoch accepted for each node it names. A node it does not
/// name accepts every epoch.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MinEpochs(BTreeMap<String, u64>);

impl MinEpochs {
    /// Parse comma-separated `<node id>=<epoch>` pairs. Blank means none.
    ///
    /// # Errors
    /// Returns what is wrong with the first pair that is not a valid node id and
    /// a non-negative integer, or that names a node again.
    pub fn parse(value: &str) -> Result<Self, String> {
        let mut epochs = BTreeMap::new();
        for pair in value
            .split(',')
            .map(str::trim)
            .filter(|pair| !pair.is_empty())
        {
            let (node_id, epoch) = pair
                .split_once('=')
                .map(|(node_id, epoch)| (node_id.trim(), epoch.trim()))
                .ok_or_else(|| format!("`{pair}` is not `<node id>=<epoch>`"))?;
            validate_resource_id(node_id).map_err(|error| format!("`{pair}`: {error}"))?;
            let epoch = epoch
                .parse()
                .map_err(|_| format!("`{pair}`: the epoch is not a non-negative integer"))?;
            if epochs.insert(node_id.to_string(), epoch).is_some() {
                return Err(format!("node {node_id} is named twice"));
            }
        }
        Ok(Self(epochs))
    }

    /// Read [`SOUTHBOUND_MIN_EPOCHS_VAR`]; unset means none.
    ///
    /// # Errors
    /// Returns [`AuthError::InvalidMinEpochs`] when it does not parse.
    pub fn from_env() -> Result<Self, AuthError> {
        Self::parse(&std::env::var(SOUTHBOUND_MIN_EPOCHS_VAR).unwrap_or_default()).map_err(
            |reason| AuthError::InvalidMinEpochs {
                var: SOUTHBOUND_MIN_EPOCHS_VAR.to_string(),
                reason,
            },
        )
    }

    fn of(&self, node_id: &str) -> u64 {
        self.0.get(node_id).copied().unwrap_or(0)
    }
}

/// Why a value is not a [`NodeKey`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum NodeKeyError {
    #[error("the key is blank")]
    Blank,
    #[error("the key must be at least {MIN_NODE_KEY_LEN} characters")]
    TooShort,
}

impl NodeKey {
    /// Wrap a key, trimming surrounding whitespace.
    ///
    /// # Errors
    /// Returns [`NodeKeyError`] when the key is blank or shorter than
    /// [`MIN_NODE_KEY_LEN`].
    pub fn new(value: &str) -> Result<Self, NodeKeyError> {
        match value.trim() {
            "" => Err(NodeKeyError::Blank),
            value if value.len() < MIN_NODE_KEY_LEN => Err(NodeKeyError::TooShort),
            value => Ok(Self {
                key: value.as_bytes().to_vec(),
                min_epochs: MinEpochs::default(),
            }),
        }
    }

    /// Read a key from environment variable `var`.
    ///
    /// # Errors
    /// Returns [`AuthError::MissingToken`] when `var` is unset or blank and
    /// [`AuthError::ShortKey`] when it is shorter than [`MIN_NODE_KEY_LEN`].
    pub fn from_env(var: &str) -> Result<Self, AuthError> {
        Self::new(&std::env::var(var).unwrap_or_default()).map_err(|error| match error {
            NodeKeyError::Blank => AuthError::MissingToken {
                var: var.to_string(),
            },
            NodeKeyError::TooShort => AuthError::ShortKey {
                var: var.to_string(),
            },
        })
    }

    /// This key, accepting for each node only tokens at or above its epoch in
    /// `min_epochs`.
    #[must_use]
    pub fn with_min_epochs(self, min_epochs: MinEpochs) -> Self {
        Self { min_epochs, ..self }
    }

    /// The token that authenticates as `node_id` at `epoch`: the id, the epoch,
    /// and the lowercase hex HMAC-SHA256 of `<id>.<epoch>` under this key,
    /// joined by `.`.
    #[must_use]
    pub fn token_for(&self, node_id: &str, epoch: u64) -> String {
        format!("{node_id}.{epoch}.{}", self.mac_hex(node_id, epoch))
    }

    /// The node id an `Authorization` header value authenticates as, `None`
    /// unless it presents a token this key issued at an epoch no lower than the
    /// node's minimum. The MAC is compared in constant time.
    #[must_use]
    pub fn verify_header(&self, header: &str) -> Option<String> {
        let (node_id, rest) = bearer_value(header)?.split_once('.')?;
        let (epoch_text, mac) = rest.split_once('.')?;
        validate_resource_id(node_id).ok()?;
        let epoch: u64 = epoch_text.parse().ok()?;
        if epoch.to_string() != epoch_text
            || !constant_time_eq(mac.as_bytes(), self.mac_hex(node_id, epoch).as_bytes())
        {
            return None;
        }
        (epoch >= self.min_epochs.of(node_id)).then(|| node_id.to_string())
    }

    fn mac_hex(&self, node_id: &str, epoch: u64) -> String {
        let mut mac =
            Hmac::<Sha256>::new_from_slice(&self.key).expect("HMAC accepts a key of any length");
        mac.update(format!("{node_id}.{epoch}").as_bytes());
        mac.finalize()
            .into_bytes()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect()
    }
}

impl fmt::Debug for NodeKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("NodeKey(<redacted>)")
    }
}

/// Whether the node surface authenticates requests, and with which key.
#[derive(Clone, Debug)]
pub enum NodeGuard {
    /// Every request must present a token derived from this key.
    Required(NodeKey),
    /// Authentication is switched off via [`AUTH_DISABLED_VAR`]; every request
    /// passes and may act as any node.
    Disabled,
}

impl NodeGuard {
    /// Resolve the guard from the key in `var` and the minimum epochs in
    /// [`SOUTHBOUND_MIN_EPOCHS_VAR`].
    ///
    /// # Errors
    /// Returns the [`NodeKey::from_env`] or [`MinEpochs::from_env`] error when
    /// the [`AUTH_DISABLED_VAR`] escape hatch is not engaged.
    pub fn from_env(var: &str) -> Result<Self, AuthError> {
        if auth_disabled() {
            return Ok(Self::Disabled);
        }
        let key = NodeKey::from_env(var)?;
        Ok(Self::Required(key.with_min_epochs(MinEpochs::from_env()?)))
    }

    /// Who a request with this `Authorization` header value is from, `None`
    /// when the request is refused.
    #[must_use]
    pub fn caller(&self, header: Option<&str>) -> Option<NodeCaller> {
        match self {
            Self::Required(key) => header
                .and_then(|header| key.verify_header(header))
                .map(NodeCaller::Node),
            Self::Disabled => Some(NodeCaller::Anyone),
        }
    }

    /// Whether authentication is switched off.
    #[must_use]
    pub fn is_disabled(&self) -> bool {
        matches!(self, Self::Disabled)
    }
}

/// The node a southbound request authenticated as.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NodeCaller {
    /// The request presented this node's token.
    Node(String),
    /// Authentication is disabled, so the caller may act as any node.
    Anyone,
}

impl NodeCaller {
    /// Whether this caller may register, heartbeat or read desired hops as
    /// `node_id`.
    #[must_use]
    pub fn may_act_as(&self, node_id: &str) -> bool {
        match self {
            Self::Node(id) => id == node_id,
            Self::Anyone => true,
        }
    }
}

/// Whether the [`AUTH_DISABLED_VAR`] escape hatch is engaged.
///
/// Only an explicit `1` or `true` counts; `WEAVE_AUTH_DISABLED=0` leaves
/// authentication on.
#[must_use]
pub fn auth_disabled() -> bool {
    std::env::var(AUTH_DISABLED_VAR).is_ok_and(|value| {
        let value = value.trim();
        value == "1" || value.eq_ignore_ascii_case("true")
    })
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AuthError {
    #[error(
        "{var} is unset or empty: set it, or set WEAVE_AUTH_DISABLED=1 to run without authentication (local development only)"
    )]
    MissingToken { var: String },
    #[error(
        "{var} must be at least {MIN_NODE_KEY_LEN} characters, for example `openssl rand -hex 32`"
    )]
    ShortKey { var: String },
    #[error("{var} is invalid: {reason}")]
    InvalidMinEpochs { var: String, reason: String },
}

#[cfg(feature = "server")]
mod middleware {
    use super::{Guard, NodeCaller, NodeGuard};
    use crate::{ApiError, ApiErrorCode};
    use axum::{
        extract::{Request, State},
        http::{HeaderValue, StatusCode, header},
        middleware::Next,
        response::{IntoResponse, Response},
    };

    /// axum middleware enforcing a [`Guard`] over the routes it is layered onto.
    ///
    /// Wire it with `middleware::from_fn_with_state(guard, require_bearer)` on a
    /// sub-router holding only the routes that need authentication, so `/health`
    /// and any browser-loaded surface stay reachable.
    pub async fn require_bearer(
        State(guard): State<Guard>,
        request: Request,
        next: Next,
    ) -> Response {
        let Some(token) = guard.token() else {
            return next.run(request).await;
        };
        let authorized = request
            .headers()
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|header| token.matches_header(header));

        if !authorized {
            // Method and path only: the presented credential never reaches a log.
            tracing::warn!(
                method = %request.method(),
                path = %request.uri().path(),
                "rejected request with missing or invalid bearer token"
            );
            return unauthorized();
        }
        next.run(request).await
    }

    /// axum middleware enforcing a [`NodeGuard`]: a request without a valid node
    /// token gets `401`, and an accepted one carries its [`NodeCaller`] as a
    /// request extension for [`refuse_other_node`].
    pub async fn require_node_token(
        State(guard): State<NodeGuard>,
        mut request: Request,
        next: Next,
    ) -> Response {
        let caller = guard.caller(
            request
                .headers()
                .get(header::AUTHORIZATION)
                .and_then(|value| value.to_str().ok()),
        );
        let Some(caller) = caller else {
            tracing::warn!(
                method = %request.method(),
                path = %request.uri().path(),
                "rejected request with missing or invalid node token"
            );
            return unauthorized();
        };
        request.extensions_mut().insert(caller);
        next.run(request).await
    }

    /// The `403 forbidden` to send when `caller` may not act as `node_id`,
    /// `None` when it may.
    #[must_use]
    pub fn refuse_other_node(caller: &NodeCaller, node_id: &str) -> Option<Response> {
        if caller.may_act_as(node_id) {
            return None;
        }
        tracing::warn!(%node_id, ?caller, "refused a node token presented for another node");
        Some(
            ApiError::new(
                ApiErrorCode::Forbidden,
                format!("token does not belong to node {node_id}"),
            )
            .response(StatusCode::FORBIDDEN),
        )
    }

    /// `401` with a `WWW-Authenticate: Bearer` challenge.
    #[must_use]
    pub fn unauthorized() -> Response {
        (
            StatusCode::UNAUTHORIZED,
            [(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"))],
            axum::Json(ApiError::new(
                ApiErrorCode::Unauthorized,
                "missing or invalid bearer token",
            )),
        )
            .into_response()
    }
}

#[cfg(feature = "server")]
pub use middleware::{refuse_other_node, require_bearer, require_node_token, unauthorized};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blank_token_values_are_treated_as_unset() {
        assert!(Token::new("").is_none());
        assert!(Token::new("   ").is_none());
        assert!(
            Token::new(" secret ").is_some(),
            "surrounding space trimmed"
        );
    }

    #[test]
    fn header_round_trips_and_rejects_mismatches() {
        let token = Token::new("s3cret").unwrap();
        assert_eq!(token.header_value(), "Bearer s3cret");
        assert!(token.matches_header(&token.header_value()));
        assert!(
            token.matches_header("bearer s3cret"),
            "scheme is case-insensitive"
        );
        assert!(!token.matches_header("Bearer wrong"));
        assert!(
            !token.matches_header("Bearer s3cret-and-more"),
            "a prefix match is not a match"
        );
        assert!(!token.matches_header("Basic s3cret"), "wrong scheme");
        assert!(!token.matches_header("s3cret"), "no scheme");
        assert!(!token.matches_header(""));
    }

    #[test]
    fn debug_never_reveals_the_value() {
        let rendered = format!("{:?}", Token::new("s3cret").unwrap());
        assert!(!rendered.contains("s3cret"), "{rendered}");

        // Also covers the guard, which is logged at startup.
        let rendered = format!("{:?}", Guard::Required(Token::new("s3cret").unwrap()));
        assert!(!rendered.contains("s3cret"), "{rendered}");
    }

    #[test]
    fn constant_time_eq_matches_plain_equality() {
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"ab"));
        assert!(constant_time_eq(b"", b""));
    }

    #[test]
    fn disabled_guard_requires_no_token_and_reports_itself() {
        let guard = Guard::Disabled;
        assert!(guard.is_disabled());
        assert!(guard.token().is_none());

        let guard = Guard::Required(Token::new("s3cret").unwrap());
        assert!(!guard.is_disabled());
        assert!(guard.token().is_some());
    }

    const KEY: &str = "bench-southbound-key-for-local-use-only";

    fn key() -> NodeKey {
        NodeKey::new(KEY).unwrap()
    }

    fn bearer(token: &str) -> String {
        format!("Bearer {token}")
    }

    /// The MAC `printf %s strom-node-1.0 | openssl dgst -sha256 -hmac
    /// bench-southbound-key-for-local-use-only -r` prints, which the README gives
    /// as the way to mint a token without this crate.
    #[test]
    fn node_token_matches_the_openssl_one_liner() {
        assert_eq!(
            key().token_for("strom-node-1", 0),
            "strom-node-1.0.dc18eb10e0f080536c55f9ff6569abb31d2da3ce4f782b294dd0d32fcbd8dbaf"
        );
    }

    #[test]
    fn node_token_verifies_as_its_own_node() {
        let token = key().token_for("strom-node-1", 0);
        assert_eq!(
            key().verify_header(&bearer(&token)).as_deref(),
            Some("strom-node-1")
        );
        assert_eq!(
            key().verify_header(&format!("bearer {token}")).as_deref(),
            Some("strom-node-1"),
            "scheme is case-insensitive"
        );
    }

    #[test]
    fn node_token_rejects_forgeries() {
        let token = key().token_for("strom-node-1", 0);
        let mac = token.rsplit_once('.').unwrap().1;
        let other_key = NodeKey::new("another-key-0123456789abcdef0123456789")
            .unwrap()
            .token_for("strom-node-1", 0);
        let mut tampered = token.clone();
        tampered.replace_range(
            token.len() - 1..,
            if token.ends_with('0') { "1" } else { "0" },
        );

        for (presented, why) in [
            (other_key, "issued under another key"),
            (format!("strom-node-2.0.{mac}"), "another node's MAC"),
            (format!("strom-node-1.1.{mac}"), "another epoch's MAC"),
            (
                format!("strom-node-1.00.{mac}"),
                "an epoch with a leading zero",
            ),
            (format!("strom-node-1.+0.{mac}"), "an epoch with a sign"),
            (format!("strom-node-1.-1.{mac}"), "a negative epoch"),
            (format!("strom-node-1.{mac}"), "no epoch"),
            (tampered, "tampered MAC"),
            (token.to_uppercase(), "uppercase"),
            (
                format!("strom-node-1.0.{}", mac.to_uppercase()),
                "uppercase hex",
            ),
            ("strom-node-1".to_string(), "no MAC"),
            ("strom-node-1.0.".to_string(), "empty MAC"),
            (format!("{token}0"), "MAC too long"),
            (format!("{token}.0"), "a fourth part"),
            (format!("Strom_Node.0.{mac}"), "invalid node id"),
            (KEY.to_string(), "the key itself"),
            ("bench-southbound-token".to_string(), "an old shared token"),
            (String::new(), "nothing"),
        ] {
            assert_eq!(key().verify_header(&bearer(&presented)), None, "{why}");
        }
        assert_eq!(key().verify_header(&format!("Basic {token}")), None);
        assert_eq!(key().verify_header(&token), None, "no scheme");
    }

    #[test]
    fn node_guard_names_the_caller_or_refuses() {
        let guard = NodeGuard::Required(key());
        let token = key().token_for("strom-node-1", 0);
        assert_eq!(
            guard.caller(Some(&bearer(&token))),
            Some(NodeCaller::Node("strom-node-1".to_string()))
        );
        assert_eq!(guard.caller(Some("Bearer wrong")), None);
        assert_eq!(guard.caller(None), None);

        let guard = NodeGuard::Disabled;
        assert!(guard.is_disabled());
        assert_eq!(guard.caller(None), Some(NodeCaller::Anyone));
    }

    #[test]
    fn a_token_below_its_nodes_minimum_epoch_is_refused() {
        let revoking = key().with_min_epochs(MinEpochs::parse("strom-node-1=2").unwrap());
        let verify = |node_id: &str, epoch| {
            revoking.verify_header(&bearer(&revoking.token_for(node_id, epoch)))
        };
        assert_eq!(verify("strom-node-1", 0), None);
        assert_eq!(verify("strom-node-1", 1), None);
        assert_eq!(verify("strom-node-1", 2).as_deref(), Some("strom-node-1"));
        assert_eq!(verify("strom-node-1", 3).as_deref(), Some("strom-node-1"));
        assert_eq!(
            verify("strom-node-2", 0).as_deref(),
            Some("strom-node-2"),
            "another node is unaffected"
        );
        assert_eq!(
            revoking.token_for("strom-node-1", 2),
            key().token_for("strom-node-1", 2),
            "minimum epochs do not change the tokens a key makes"
        );
    }

    #[test]
    fn min_epochs_parse_pairs_and_refuse_anything_else() {
        let epochs = MinEpochs::parse(" strom-node-1=2, browser-guest = 1 ,").unwrap();
        assert_eq!(epochs.of("strom-node-1"), 2);
        assert_eq!(epochs.of("browser-guest"), 1);
        assert_eq!(epochs.of("strom-node-2"), 0);
        assert_eq!(MinEpochs::parse("").unwrap(), MinEpochs::default());
        for (value, why) in [
            ("strom-node-1", "no epoch"),
            ("strom-node-1=", "an empty epoch"),
            ("strom-node-1=-1", "a negative epoch"),
            ("strom-node-1=two", "a word"),
            ("Strom_Node=1", "an invalid node id"),
            ("strom-node-1=1,strom-node-1=2", "a node named twice"),
        ] {
            assert!(MinEpochs::parse(value).is_err(), "{why}");
        }
        let message = AuthError::InvalidMinEpochs {
            var: SOUTHBOUND_MIN_EPOCHS_VAR.to_string(),
            reason: MinEpochs::parse("strom-node-1").unwrap_err(),
        }
        .to_string();
        assert!(message.contains(SOUTHBOUND_MIN_EPOCHS_VAR), "{message}");
        assert!(message.contains("strom-node-1"), "{message}");
    }

    #[test]
    fn a_node_may_act_only_as_itself() {
        let caller = NodeCaller::Node("strom-node-1".to_string());
        assert!(caller.may_act_as("strom-node-1"));
        assert!(!caller.may_act_as("strom-node-2"));
        assert!(!caller.may_act_as("strom-node-10"));
        assert!(NodeCaller::Anyone.may_act_as("strom-node-2"));
    }

    #[test]
    fn blank_or_short_keys_are_refused() {
        assert_eq!(NodeKey::new("").unwrap_err(), NodeKeyError::Blank);
        assert_eq!(NodeKey::new("  ").unwrap_err(), NodeKeyError::Blank);
        let short = "k".repeat(MIN_NODE_KEY_LEN - 1);
        assert_eq!(NodeKey::new(&short).unwrap_err(), NodeKeyError::TooShort);
        assert_eq!(
            NodeKey::new(&format!("  {short}  ")).unwrap_err(),
            NodeKeyError::TooShort,
            "surrounding space does not count"
        );
        assert!(NodeKey::new(&"k".repeat(MIN_NODE_KEY_LEN)).is_ok());
        assert_eq!(
            NodeKey::new(&format!(" {KEY} ")).unwrap().token_for("a", 0),
            key().token_for("a", 0),
            "surrounding space trimmed"
        );
    }

    #[test]
    fn debug_never_reveals_the_key() {
        let rendered = format!("{:?}", NodeGuard::Required(key()));
        assert!(!rendered.contains(KEY), "{rendered}");
    }

    #[test]
    fn missing_token_error_names_the_variable_and_the_escape_hatch() {
        let message = AuthError::MissingToken {
            var: NORTHBOUND_TOKEN_VAR.to_string(),
        }
        .to_string();
        assert!(message.contains(NORTHBOUND_TOKEN_VAR), "{message}");
        assert!(message.contains(AUTH_DISABLED_VAR), "{message}");
    }

    #[test]
    fn short_key_error_names_the_variable_and_the_length() {
        let message = AuthError::ShortKey {
            var: SOUTHBOUND_KEY_VAR.to_string(),
        }
        .to_string();
        assert!(message.contains(SOUTHBOUND_KEY_VAR), "{message}");
        assert!(message.contains("32 characters"), "{message}");
    }
}

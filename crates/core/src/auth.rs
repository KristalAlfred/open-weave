//! Bearer-token authentication for the control-plane HTTP surfaces.
//!
//! Operators and the CLI present one shared secret, [`NORTHBOUND_TOKEN_VAR`], to
//! northbound, which re-presents it to the controller. Each node presents a
//! token of its own, [`SOUTHBOUND_TOKEN_VAR`], to southbound, which forwards it
//! to the controller. A node token is `<node id>.<hex HMAC-SHA256(key, node id)>`,
//! where the key is [`SOUTHBOUND_KEY_VAR`]: southbound and the controller hold
//! the key, derive the node id from the token, and refuse a node acting under
//! another node's id.
//!
//! [`Guard::from_env`] and [`NodeGuard::from_env`] error when their variable is
//! unset or blank, so a service with no secret does not start.
//! [`AUTH_DISABLED_VAR`] switches authentication off for local development.

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

/// The key node tokens are derived from. [`fmt::Debug`] redacts it.
#[derive(Clone)]
pub struct NodeKey(Vec<u8>);

impl NodeKey {
    /// Wrap a key, trimming surrounding whitespace. `None` when blank.
    #[must_use]
    pub fn new(value: &str) -> Option<Self> {
        let value = value.trim();
        (!value.is_empty()).then(|| Self(value.as_bytes().to_vec()))
    }

    /// Read a key from environment variable `var`, `None` when unset or blank.
    #[must_use]
    pub fn from_env(var: &str) -> Option<Self> {
        std::env::var(var).ok().as_deref().and_then(NodeKey::new)
    }

    /// The token that authenticates as `node_id`: the id, a `.`, and the
    /// lowercase hex HMAC-SHA256 of the id under this key.
    #[must_use]
    pub fn token_for(&self, node_id: &str) -> String {
        format!("{node_id}.{}", self.mac_hex(node_id))
    }

    /// The node id an `Authorization` header value authenticates as, `None`
    /// unless it presents a token this key issued. The MAC is compared in
    /// constant time.
    #[must_use]
    pub fn verify_header(&self, header: &str) -> Option<String> {
        let (node_id, mac) = bearer_value(header)?.split_once('.')?;
        validate_resource_id(node_id).ok()?;
        constant_time_eq(mac.as_bytes(), self.mac_hex(node_id).as_bytes())
            .then(|| node_id.to_string())
    }

    fn mac_hex(&self, node_id: &str) -> String {
        let mut mac =
            Hmac::<Sha256>::new_from_slice(&self.0).expect("HMAC accepts a key of any length");
        mac.update(node_id.as_bytes());
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
    /// Resolve the guard from the key in `var`.
    ///
    /// # Errors
    /// Returns [`AuthError::MissingToken`] when `var` is unset or blank and the
    /// [`AUTH_DISABLED_VAR`] escape hatch is not engaged.
    pub fn from_env(var: &str) -> Result<Self, AuthError> {
        if auth_disabled() {
            return Ok(Self::Disabled);
        }
        NodeKey::from_env(var)
            .map(Self::Required)
            .ok_or_else(|| AuthError::MissingToken {
                var: var.to_string(),
            })
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

    const KEY: &str = "bench-southbound-key";

    fn key() -> NodeKey {
        NodeKey::new(KEY).unwrap()
    }

    fn bearer(token: &str) -> String {
        format!("Bearer {token}")
    }

    /// The value `printf %s strom-node-1 | openssl dgst -sha256 -hmac
    /// bench-southbound-key -r` prints, which the README gives as the way to
    /// mint a token without this crate.
    #[test]
    fn node_token_matches_the_openssl_one_liner() {
        assert_eq!(
            key().token_for("strom-node-1"),
            "strom-node-1.35e6718c0cdacc8a30175b4036cb8ebf271c7674e59f9b99f2db900b1c8532f7"
        );
    }

    #[test]
    fn node_token_verifies_as_its_own_node() {
        let token = key().token_for("strom-node-1");
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
        let token = key().token_for("strom-node-1");
        let (_, mac) = token.split_once('.').unwrap();
        let other_key = NodeKey::new("another-key")
            .unwrap()
            .token_for("strom-node-1");
        let mut tampered = token.clone();
        tampered.replace_range(
            token.len() - 1..,
            if token.ends_with('0') { "1" } else { "0" },
        );

        for (presented, why) in [
            (other_key, "issued under another key"),
            (format!("strom-node-2.{mac}"), "another node's MAC"),
            (tampered, "tampered MAC"),
            (token.to_uppercase(), "uppercase"),
            (
                format!("strom-node-1.{}", mac.to_uppercase()),
                "uppercase hex",
            ),
            ("strom-node-1".to_string(), "no MAC"),
            ("strom-node-1.".to_string(), "empty MAC"),
            (format!("{token}0"), "MAC too long"),
            (format!("Strom_Node.{mac}"), "invalid node id"),
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
        let token = key().token_for("strom-node-1");
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
    fn a_node_may_act_only_as_itself() {
        let caller = NodeCaller::Node("strom-node-1".to_string());
        assert!(caller.may_act_as("strom-node-1"));
        assert!(!caller.may_act_as("strom-node-2"));
        assert!(!caller.may_act_as("strom-node-10"));
        assert!(NodeCaller::Anyone.may_act_as("strom-node-2"));
    }

    #[test]
    fn blank_keys_are_treated_as_unset() {
        assert!(NodeKey::new("").is_none());
        assert!(NodeKey::new("  ").is_none());
        assert_eq!(
            NodeKey::new(&format!(" {KEY} ")).unwrap().token_for("a"),
            key().token_for("a"),
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
}

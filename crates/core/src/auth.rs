//! Shared bearer-token authentication for the control-plane HTTP surfaces.
//!
//! One shared secret per surface, supplied through the environment. Operators and
//! the CLI present [`NORTHBOUND_TOKEN_VAR`] to northbound; adapters and media
//! nodes present [`SOUTHBOUND_TOKEN_VAR`] to southbound. Northbound and
//! southbound are stateless proxies, so each re-presents its own surface token to
//! the controller, which validates both — one token per surface all the way down.
//!
//! Resolution is **fail closed**: [`Guard::from_env`] errors when the surface's
//! token is unset or blank, so a service refuses to start rather than silently
//! serving unauthenticated traffic. [`AUTH_DISABLED_VAR`] is the explicit
//! local-development escape hatch.

use std::fmt;

/// Environment variable holding the token for the northbound (operator) surface.
pub const NORTHBOUND_TOKEN_VAR: &str = "WEAVE_NORTHBOUND_TOKEN";
/// Environment variable holding the token for the southbound (node) surface.
pub const SOUTHBOUND_TOKEN_VAR: &str = "WEAVE_SOUTHBOUND_TOKEN";
/// Set to `1` or `true` to serve and call without authentication. Local
/// development only — it disables the fail-closed default.
pub const AUTH_DISABLED_VAR: &str = "WEAVE_AUTH_DISABLED";

/// A shared bearer token.
///
/// The value is deliberately hard to leak: [`fmt::Debug`] redacts it, no
/// [`fmt::Display`] is implemented, and the only way out is
/// [`Token::header_value`], which is named for the one place it belongs.
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
    /// passes. Never the default — only an explicit opt-out reaches this.
    Disabled,
}

impl Guard {
    /// Resolve the guard for the surface whose token lives in `var`.
    ///
    /// # Errors
    /// Returns [`AuthError::MissingToken`] when `var` is unset or blank and the
    /// [`AUTH_DISABLED_VAR`] escape hatch is not engaged. Callers are expected to
    /// propagate this and exit: serving the surface unauthenticated is not a
    /// fallback.
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

    /// Whether authentication is switched off. Worth logging loudly at startup.
    #[must_use]
    pub fn is_disabled(&self) -> bool {
        matches!(self, Self::Disabled)
    }
}

/// Whether the [`AUTH_DISABLED_VAR`] escape hatch is engaged.
///
/// Only an explicit `1` or `true` counts, so `WEAVE_AUTH_DISABLED=0` leaves
/// authentication on rather than disabling it by mere presence.
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
        "{var} is unset or empty: set it to a shared secret, or set WEAVE_AUTH_DISABLED=1 to run without authentication (local development only)"
    )]
    MissingToken { var: String },
}

#[cfg(feature = "server")]
mod middleware {
    use axum::{
        extract::{Request, State},
        http::{HeaderValue, StatusCode, header},
        middleware::Next,
        response::{IntoResponse, Response},
    };
    use serde_json::json;

    use super::Guard;

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

    fn unauthorized() -> Response {
        (
            StatusCode::UNAUTHORIZED,
            [(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"))],
            axum::Json(json!({ "error": "missing or invalid bearer token" })),
        )
            .into_response()
    }
}

#[cfg(feature = "server")]
pub use middleware::require_bearer;

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

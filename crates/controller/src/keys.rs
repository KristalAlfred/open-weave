//! Keys for SRT links between nodes. Each key is derived from one controller
//! secret, the id of the hop the link feeds and the two nodes it joins, so both
//! ends get the same key on every tick and nothing is stored.

use std::fmt::{self, Write};

use hmac::{Hmac, Mac};
use sha2::Sha256;
use weave_core::Passphrase;

pub(crate) const SECRET_VAR: &str = "WEAVE_SRT_KEY_SECRET";
const MIN_SECRET_LEN: usize = 32;
const LINK_LABEL: &[u8] = b"open-weave srt link\0";

#[derive(Clone)]
pub(crate) struct LinkKeys {
    secret: Vec<u8>,
}

impl fmt::Debug for LinkKeys {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("LinkKeys(<redacted>)")
    }
}

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub(crate) enum SecretError {
    #[error(
        "{SECRET_VAR} is unset or empty: set it to a random value of at least {MIN_SECRET_LEN} characters, or set {}=1 to generate one per run (local development only)",
        weave_core::auth::AUTH_DISABLED_VAR
    )]
    Missing,
    #[error("{SECRET_VAR} must be at least {MIN_SECRET_LEN} characters")]
    TooShort,
    #[error("no random source for a generated {SECRET_VAR}: {0}")]
    Random(String),
}

/// Where the secret came from, so the caller can warn about a generated one.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum SecretSource {
    Configured,
    Generated,
}

impl LinkKeys {
    pub(crate) fn new(secret: impl Into<Vec<u8>>) -> Self {
        Self {
            secret: secret.into(),
        }
    }

    /// Read [`SECRET_VAR`]. When it is unset or blank and authentication is
    /// disabled, a random secret is generated instead.
    pub(crate) fn from_env() -> Result<(Self, SecretSource), SecretError> {
        Self::resolve(
            std::env::var(SECRET_VAR).ok().as_deref(),
            weave_core::auth::auth_disabled(),
        )
    }

    fn resolve(
        configured: Option<&str>,
        auth_disabled: bool,
    ) -> Result<(Self, SecretSource), SecretError> {
        match configured.map(str::trim).filter(|value| !value.is_empty()) {
            Some(value) if value.len() < MIN_SECRET_LEN => Err(SecretError::TooShort),
            Some(value) => Ok((Self::new(value), SecretSource::Configured)),
            None if auth_disabled => {
                let mut secret = [0u8; 32];
                getrandom::fill(&mut secret)
                    .map_err(|error| SecretError::Random(error.to_string()))?;
                Ok((Self::new(secret), SecretSource::Generated))
            }
            None => Err(SecretError::Missing),
        }
    }

    #[cfg(test)]
    pub(crate) fn for_tests() -> Self {
        Self::new("0123456789abcdef0123456789abcdef")
    }

    /// The key for the link feeding hop `hop_id` between the nodes `ends`, in
    /// either order: HMAC-SHA256 of the secret over the hop id and both node
    /// ids, as 64 lowercase hex characters.
    pub(crate) fn link(&self, hop_id: &str, mut ends: [&str; 2]) -> Passphrase {
        ends.sort_unstable();
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.secret)
            .expect("HMAC-SHA256 accepts a key of any length");
        mac.update(LINK_LABEL);
        for part in [hop_id, ends[0], ends[1]] {
            mac.update(part.as_bytes());
            mac.update(b"\0");
        }
        let mut key = String::with_capacity(64);
        for byte in mac.finalize().into_bytes() {
            write!(key, "{byte:02x}").expect("writing to a string cannot fail");
        }
        Passphrase::new(key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECRET: &str = "0123456789abcdef0123456789abcdef";

    const HOP: &str = "weave-feed-receiver-studio";
    const ENDS: [&str; 2] = ["source", "studio-node"];

    #[test]
    fn a_link_key_is_stable_per_hop_and_distinct_across_hops_and_secrets() {
        let keys = LinkKeys::new(SECRET);
        let key = keys.link(HOP, ENDS);
        assert_eq!(key, keys.link(HOP, ENDS));
        assert_eq!(key.expose().len(), 64);
        assert!(key.expose().bytes().all(|byte| byte.is_ascii_hexdigit()));
        assert_ne!(key, keys.link("weave-feed-receiver-preview", ENDS));
        assert_ne!(key, LinkKeys::new(format!("{SECRET}x")).link(HOP, ENDS));
    }

    #[test]
    fn a_link_key_changes_with_either_end_node_but_not_their_order() {
        let keys = LinkKeys::new(SECRET);
        let key = keys.link(HOP, ENDS);
        assert_eq!(key, keys.link(HOP, ["studio-node", "source"]));
        assert_ne!(key, keys.link(HOP, ["relay", "studio-node"]));
        assert_ne!(key, keys.link(HOP, ["source", "relay"]));
        assert_ne!(
            keys.link("hop", ["a", "bc"]),
            keys.link("hop", ["ab", "c"]),
            "the ids are separated"
        );
    }

    #[test]
    fn a_missing_secret_refuses_to_start_unless_auth_is_disabled() {
        assert_eq!(
            LinkKeys::resolve(None, false).unwrap_err(),
            SecretError::Missing
        );
        assert_eq!(
            LinkKeys::resolve(Some("  "), false).unwrap_err(),
            SecretError::Missing
        );
        let (first, source) = LinkKeys::resolve(None, true).unwrap();
        assert_eq!(source, SecretSource::Generated);
        let (second, _) = LinkKeys::resolve(None, true).unwrap();
        assert_ne!(first.link(HOP, ENDS), second.link(HOP, ENDS));
    }

    #[test]
    fn a_short_secret_is_refused_even_with_auth_disabled() {
        assert_eq!(
            LinkKeys::resolve(Some("too-short"), true).unwrap_err(),
            SecretError::TooShort
        );
        let (keys, source) = LinkKeys::resolve(Some(SECRET), false).unwrap();
        assert_eq!(source, SecretSource::Configured);
        assert_eq!(keys.link(HOP, ENDS), LinkKeys::new(SECRET).link(HOP, ENDS));
    }

    #[test]
    fn debug_never_prints_the_secret() {
        assert!(!format!("{:?}", LinkKeys::new(SECRET)).contains(SECRET));
    }
}

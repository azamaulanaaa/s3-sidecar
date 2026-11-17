//! Secret values with redaction and zeroization.

use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use zeroize::{Zeroize, ZeroizeOnDrop};

/// A secret string. The value is redacted in `Debug` output and erased on drop.
#[derive(Clone, Default, Zeroize, ZeroizeOnDrop)]
pub struct SecretString(String);

impl SecretString {
    /// Build a secret from a string.
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Return the secret value. Call this only at the point of use.
    #[must_use]
    pub fn expose_secret(&self) -> &str {
        &self.0
    }

    /// Return true when the secret is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Return the redacted form for display.
    #[must_use]
    pub fn redacted(&self) -> &'static str {
        if self.0.is_empty() { "" } else { "****" }
    }
}

impl fmt::Debug for SecretString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.redacted())
    }
}

impl Serialize for SecretString {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.redacted())
    }
}

impl<'de> Deserialize<'de> for SecretString {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer).map(Self)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;

    #[test]
    fn serialization_hides_the_secret_value() {
        let secret = SecretString::new("super-secret-value");
        let json = serde_json::to_string(&secret).unwrap();
        assert!(!json.contains("super-secret-value"));
        assert_eq!(json, "\"****\"");
    }
}

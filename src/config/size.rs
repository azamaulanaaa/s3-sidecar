//! Byte sizes with a short binary-unit syntax.

use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// A byte size that accepts `1024`, `64MiB`, or `5GiB`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Size(u64);

impl Size {
    /// Build a size from a byte count.
    #[must_use]
    pub fn new(bytes: u64) -> Self {
        Self(bytes)
    }

    /// Return the size in bytes.
    #[must_use]
    pub fn as_bytes(self) -> u64 {
        self.0
    }
}

impl fmt::Display for Size {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let bytes = self.0;
        if bytes == 0 {
            return f.write_str("0");
        }
        for (unit, multiplier) in [
            ("TiB", 1_099_511_627_776_u64),
            ("GiB", 1_073_741_824),
            ("MiB", 1_048_576),
            ("KiB", 1_024),
        ] {
            if bytes.is_multiple_of(multiplier) {
                return write!(f, "{}{unit}", bytes / multiplier);
            }
        }
        f.write_str(&bytes.to_string())
    }
}

impl Serialize for Size {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for Size {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct Visitor;

        impl serde::de::Visitor<'_> for Visitor {
            type Value = Size;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a byte size such as 1024, 64MiB, or 5GiB")
            }

            fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Size(value))
            }

            fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                let value =
                    u64::try_from(value).map_err(|_| E::custom("size must not be negative"))?;
                Ok(Size(value))
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                parse_size(value).map(Size).map_err(E::custom)
            }
        }

        deserializer.deserialize_any(Visitor)
    }
}

fn parse_size(value: &str) -> Result<u64, String> {
    let value = value.trim();
    if value.is_empty() {
        return Err("size must not be empty".to_owned());
    }
    let split = value
        .find(|character: char| !(character.is_ascii_digit() || character == '_'))
        .unwrap_or(value.len());
    let (number, suffix) = value.split_at(split);
    if number.is_empty() {
        return Err(format!("size has no number: {value}"));
    }
    let digits: String = number
        .chars()
        .filter(|character| *character != '_')
        .collect();
    let amount: u64 = digits
        .parse()
        .map_err(|_| format!("size number is too large: {number}"))?;
    let multiplier = match suffix.trim().to_ascii_lowercase().as_str() {
        "" | "b" => 1,
        "k" | "kb" | "kib" => 1_024,
        "m" | "mb" | "mib" => 1_048_576,
        "g" | "gb" | "gib" => 1_073_741_824,
        "t" | "tb" | "tib" => 1_099_511_627_776,
        _ => return Err(format!("size suffix is not valid: {suffix}")),
    };
    amount
        .checked_mul(multiplier)
        .ok_or_else(|| format!("size is too large: {value}"))
}

#[cfg(test)]
mod tests {
    #![expect(clippy::unwrap_used, reason = "test code uses unwrap for fixtures")]

    use super::*;

    #[test]
    fn parses_plain_byte_counts() {
        assert_eq!(parse_size("1024").unwrap(), 1_024);
        assert_eq!(parse_size("1_024").unwrap(), 1_024);
        assert_eq!(parse_size("0").unwrap(), 0);
    }

    #[test]
    fn parses_binary_units() {
        assert_eq!(parse_size("64MiB").unwrap(), 67_108_864);
        assert_eq!(parse_size("5GiB").unwrap(), 5_368_709_120);
        assert_eq!(parse_size("2kib").unwrap(), 2_048);
        assert_eq!(parse_size("1 TB").unwrap(), 1_099_511_627_776);
    }

    #[test]
    fn rejects_invalid_sizes() {
        assert!(parse_size("").is_err());
        assert!(parse_size("abc").is_err());
        assert!(parse_size("64XiB").is_err());
        assert!(parse_size("18446744073709551616").is_err());
    }

    #[test]
    fn displays_the_largest_exact_unit() {
        assert_eq!(Size::new(0).to_string(), "0");
        assert_eq!(Size::new(1_024).to_string(), "1KiB");
        assert_eq!(Size::new(67_108_864).to_string(), "64MiB");
        assert_eq!(Size::new(5_368_709_120).to_string(), "5GiB");
        assert_eq!(Size::new(1_500).to_string(), "1500");
    }
}

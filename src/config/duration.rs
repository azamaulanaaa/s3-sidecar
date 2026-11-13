//! Duration values with a short human-readable syntax.

use std::fmt;
use std::time::Duration;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// A duration that accepts `30s`, `5m`, `1h`, `7d`, or a number of seconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct HumanDuration(Duration);

impl HumanDuration {
    /// Build a duration from a standard duration.
    #[must_use]
    pub fn new(value: Duration) -> Self {
        Self(value)
    }

    /// Return the inner duration.
    #[must_use]
    pub fn as_duration(self) -> Duration {
        self.0
    }
}

impl fmt::Display for HumanDuration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let seconds = self.0.as_secs();
        if seconds.is_multiple_of(86_400) && seconds > 0 {
            write!(f, "{}d", seconds / 86_400)
        } else if seconds.is_multiple_of(3_600) && seconds > 0 {
            write!(f, "{}h", seconds / 3_600)
        } else if seconds.is_multiple_of(60) && seconds > 0 {
            write!(f, "{}m", seconds / 60)
        } else {
            write!(f, "{seconds}s")
        }
    }
}

impl Serialize for HumanDuration {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for HumanDuration {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct Visitor;

        impl serde::de::Visitor<'_> for Visitor {
            type Value = HumanDuration;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a duration such as 30s, 5m, 1h, 7d, or a number of seconds")
            }

            fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(HumanDuration(Duration::from_secs(value)))
            }

            fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                let value =
                    u64::try_from(value).map_err(|_| E::custom("duration must not be negative"))?;
                Ok(HumanDuration(Duration::from_secs(value)))
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                parse_duration(value).map(HumanDuration).map_err(E::custom)
            }
        }

        deserializer.deserialize_any(Visitor)
    }
}

fn parse_duration(value: &str) -> Result<Duration, String> {
    let value = value.trim();
    if value.is_empty() {
        return Err("duration must not be empty".to_owned());
    }
    let split = value
        .find(|character: char| !character.is_ascii_digit())
        .unwrap_or(value.len());
    let (number, suffix) = value.split_at(split);
    if number.is_empty() {
        return Err(format!("duration has no number: {value}"));
    }
    let amount: u64 = number
        .parse()
        .map_err(|_| format!("duration number is too large: {number}"))?;
    let multiplier = match suffix {
        "" | "s" => 1,
        "m" => 60,
        "h" => 3_600,
        "d" => 86_400,
        _ => return Err(format!("duration suffix is not valid: {suffix}")),
    };
    amount
        .checked_mul(multiplier)
        .map(Duration::from_secs)
        .ok_or_else(|| format!("duration is too large: {value}"))
}

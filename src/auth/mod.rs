//! `SigV4` authentication and upstream request signing.

pub mod body;
pub mod chunked;
pub mod sign;
pub mod verify;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use time::{Date, Month, OffsetDateTime, PrimitiveDateTime, Time};

use crate::config::ClientConfig;
use crate::config::secret::SecretString;

/// `SigV4` algorithm name.
pub const ALGORITHM: &str = "AWS4-HMAC-SHA256";
/// SHA-256 hash of an empty payload.
pub const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
/// Allowed clock difference in seconds.
pub const MAX_CLOCK_SKEW_SECONDS: i64 = 900;
/// Maximum presigned URL lifetime in seconds.
pub const MAX_PRESIGN_SECONDS: u64 = 604_800;

/// A client credential and its bucket allow-list.
pub struct ClientCredential {
    access_key_id: String,
    secret_access_key: SecretString,
    session_token: Option<SecretString>,
    allowed_buckets: HashSet<String>,
}

impl ClientCredential {
    /// Build a credential from its parts.
    #[must_use]
    pub fn new(
        access_key_id: impl Into<String>,
        secret_access_key: SecretString,
        session_token: Option<SecretString>,
        allowed_buckets: impl IntoIterator<Item = String>,
    ) -> Self {
        Self {
            access_key_id: access_key_id.into(),
            secret_access_key,
            session_token,
            allowed_buckets: allowed_buckets.into_iter().collect(),
        }
    }

    /// Return the access key ID.
    #[must_use]
    pub fn access_key_id(&self) -> &str {
        &self.access_key_id
    }

    /// Return the secret access key.
    #[must_use]
    pub fn secret_access_key(&self) -> &str {
        self.secret_access_key.expose_secret()
    }

    /// Return the session token.
    #[must_use]
    pub fn session_token(&self) -> Option<&str> {
        self.session_token.as_ref().map(SecretString::expose_secret)
    }

    /// Return true when the client may use the bucket.
    #[must_use]
    pub fn allows_bucket(&self, bucket: &str) -> bool {
        self.allowed_buckets.contains(bucket)
    }
}

/// A set of client credentials.
pub struct CredentialStore {
    clients: HashMap<String, Arc<ClientCredential>>,
}

impl CredentialStore {
    /// Build a credential store from configuration.
    #[must_use]
    pub fn new(clients: &[ClientConfig]) -> Self {
        let mut store = HashMap::with_capacity(clients.len());
        for client in clients {
            let entry = Arc::new(ClientCredential {
                access_key_id: client.access_key_id.clone(),
                secret_access_key: client.secret_access_key.clone(),
                session_token: client.session_token.clone(),
                allowed_buckets: client.buckets.iter().cloned().collect(),
            });
            store.insert(client.access_key_id.clone(), entry);
        }
        Self { clients: store }
    }

    /// Look up a credential by access key ID.
    #[must_use]
    pub fn lookup(&self, access_key_id: &str) -> Option<Arc<ClientCredential>> {
        self.clients.get(access_key_id).cloned()
    }

    /// Return the number of configured clients.
    #[must_use]
    pub fn len(&self) -> usize {
        self.clients.len()
    }

    /// Return true when no client is configured.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.clients.is_empty()
    }
}

/// Verified authentication data.
pub struct AuthContext {
    credential: Arc<ClientCredential>,
    payload: PayloadKind,
}

impl AuthContext {
    pub(crate) fn new(credential: Arc<ClientCredential>, payload: PayloadKind) -> Self {
        Self {
            credential,
            payload,
        }
    }

    /// Return the authenticated credential.
    #[must_use]
    pub fn credential(&self) -> &Arc<ClientCredential> {
        &self.credential
    }

    /// Return the payload verification mode.
    #[must_use]
    pub fn payload(&self) -> &PayloadKind {
        &self.payload
    }

    /// Split the context into the credential and the payload mode.
    #[must_use]
    pub fn into_parts(self) -> (Arc<ClientCredential>, PayloadKind) {
        (self.credential, self.payload)
    }

    /// Return true when the client may use the bucket.
    #[must_use]
    pub fn allows_bucket(&self, bucket: &str) -> bool {
        self.credential.allows_bucket(bucket)
    }
}

/// Payload verification mode.
pub enum PayloadKind {
    /// The payload is not signed.
    Unsigned,
    /// The payload must hash to this value.
    Hash([u8; 32]),
    /// The payload uses signed `aws-chunked` framing.
    Streaming(StreamingContext),
    /// The payload uses unsigned `aws-chunked` framing with trailers.
    StreamingUnsignedTrailer,
}

/// State needed to verify signed `aws-chunked` chunks.
pub struct StreamingContext {
    pub(crate) signing_key: SigningKey,
    pub(crate) seed_signature: String,
    pub(crate) amz_date: String,
    pub(crate) scope: String,
    pub(crate) decoded_length: Option<u64>,
    pub(crate) trailer: bool,
}

/// A secret HMAC key that is erased on drop.
pub struct SigningKey([u8; 32]);

impl SigningKey {
    pub(crate) fn new(value: [u8; 32]) -> Self {
        Self(value)
    }

    pub(crate) fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl Drop for SigningKey {
    fn drop(&mut self) {
        use zeroize::Zeroize;
        self.0.zeroize();
    }
}

/// Return the current UTC time.
#[must_use]
pub fn now() -> OffsetDateTime {
    OffsetDateTime::now_utc()
}

/// Format a timestamp as `YYYYMMDDTHHMMSSZ`.
#[must_use]
pub fn format_amz_date(value: OffsetDateTime) -> String {
    let date = value.date();
    let time = value.time();
    format!(
        "{:04}{:02}{:02}T{:02}{:02}{:02}Z",
        date.year(),
        u8::from(date.month()),
        date.day(),
        time.hour(),
        time.minute(),
        time.second()
    )
}

/// Format a timestamp as `YYYYMMDD`.
#[must_use]
pub fn format_scope_date(value: OffsetDateTime) -> String {
    let date = value.date();
    format!(
        "{:04}{:02}{:02}",
        date.year(),
        u8::from(date.month()),
        date.day()
    )
}

/// Parse a timestamp in `YYYYMMDDTHHMMSSZ` form.
#[must_use]
pub fn parse_amz_date(value: &str) -> Option<OffsetDateTime> {
    if value.len() != 16 || !value.ends_with('Z') || value.as_bytes().get(8) != Some(&b'T') {
        return None;
    }
    let year = value.get(0..4)?.parse::<i32>().ok()?;
    let month = value.get(4..6)?.parse::<u8>().ok()?;
    let day = value.get(6..8)?.parse::<u8>().ok()?;
    let hour = value.get(9..11)?.parse::<u8>().ok()?;
    let minute = value.get(11..13)?.parse::<u8>().ok()?;
    let second = value.get(13..15)?.parse::<u8>().ok()?;
    let month = Month::try_from(month).ok()?;
    let date = Date::from_calendar_date(year, month, day).ok()?;
    let time = Time::from_hms(hour, minute, second).ok()?;
    Some(PrimitiveDateTime::new(date, time).assume_utc())
}

/// Build a `SigV4` credential scope.
#[must_use]
pub fn credential_scope(date: &str, region: &str, service: &str) -> String {
    format!("{date}/{region}/{service}/aws4_request")
}

/// Compute the `SigV4` signing key.
#[must_use]
pub fn signing_key(secret: &str, date: &str, region: &str, service: &str) -> [u8; 32] {
    let date_key = hmac_sha256(format!("AWS4{secret}").as_bytes(), date.as_bytes());
    let region_key = hmac_sha256(&date_key, region.as_bytes());
    let service_key = hmac_sha256(&region_key, service.as_bytes());
    hmac_sha256(&service_key, b"aws4_request")
}

/// Compute HMAC-SHA256.
#[must_use]
pub fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
    let mac = Hmac::<Sha256>::new_from_slice(key);
    let Ok(mut mac) = mac else {
        return [0_u8; 32];
    };
    mac.update(data);
    let bytes = mac.finalize().into_bytes();
    let mut output = [0_u8; 32];
    output.copy_from_slice(&bytes);
    output
}

/// Compute a lowercase SHA-256 hex digest.
#[must_use]
pub fn sha256_hex(data: &[u8]) -> String {
    hex::encode(Sha256::digest(data))
}

/// Compare two strings in constant time.
#[must_use]
pub fn constant_time_eq(left: &str, right: &str) -> bool {
    left.as_bytes().ct_eq(right.as_bytes()).into()
}

/// Compile a canonical URI for a `SigV4` request.
#[must_use]
pub fn canonical_uri(path: &str) -> String {
    if path.is_empty() {
        return "/".to_owned();
    }
    let bytes = path.as_bytes();
    let mut output = String::with_capacity(path.len());
    let mut index = 0;
    while index < bytes.len() {
        let byte = bytes[index];
        if byte == b'/' {
            output.push('/');
            index += 1;
        } else if byte == b'%'
            && index + 2 < bytes.len()
            && bytes[index + 1].is_ascii_hexdigit()
            && bytes[index + 2].is_ascii_hexdigit()
        {
            output.push('%');
            output.push(bytes[index + 1].to_ascii_uppercase() as char);
            output.push(bytes[index + 2].to_ascii_uppercase() as char);
            index += 3;
        } else if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            output.push(byte as char);
            index += 1;
        } else {
            push_percent(&mut output, byte);
            index += 1;
        }
    }
    output
}

/// Compile a canonical query string for a `SigV4` request.
#[must_use]
pub fn canonical_query(raw: Option<&str>, exclude: Option<&str>) -> String {
    let mut pairs: Vec<(String, String)> = Vec::new();
    if let Some(raw) = raw {
        for pair in raw.split('&') {
            if pair.is_empty() {
                continue;
            }
            let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
            let name = decode_component(name);
            if exclude.is_some_and(|excluded| name == excluded) {
                continue;
            }
            let value = decode_component(value);
            pairs.push((uri_encode(&name), uri_encode(&value)));
        }
    }
    pairs.sort();
    pairs
        .into_iter()
        .map(|(name, value)| format!("{name}={value}"))
        .collect::<Vec<_>>()
        .join("&")
}

/// Percent-encode one path segment or query component for `SigV4`.
#[must_use]
pub fn uri_encode(value: &str) -> String {
    let mut output = String::with_capacity(value.len());
    for &byte in value.as_bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            output.push(byte as char);
        } else {
            push_percent(&mut output, byte);
        }
    }
    output
}

fn push_percent(output: &mut String, byte: u8) {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    output.push('%');
    output.push(HEX[(byte >> 4) as usize] as char);
    output.push(HEX[(byte & 0x0f) as usize] as char);
}

fn decode_component(value: &str) -> String {
    percent_encoding::percent_decode_str(value)
        .decode_utf8_lossy()
        .into_owned()
}

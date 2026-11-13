//! Verify `SigV4` header and presigned query authentication.

use axum::http::{HeaderMap, Method, Uri, header::HOST};
use time::OffsetDateTime;

use super::{
    ALGORITHM, AuthContext, CredentialStore, MAX_CLOCK_SKEW_SECONDS, MAX_PRESIGN_SECONDS,
    PayloadKind, SigningKey, StreamingContext, canonical_query, canonical_uri, constant_time_eq,
    credential_scope, format_scope_date, hmac_sha256, parse_amz_date, sha256_hex, signing_key,
};
use crate::errors::S3Error;

/// Verify the request signature.
///
/// # Errors
///
/// Returns an S3 error when the request has no credentials, unknown
/// credentials, a malformed signature, a stale time, or a wrong signature.
pub fn authenticate(
    method: &Method,
    uri: &Uri,
    headers: &HeaderMap,
    store: &CredentialStore,
    expected_region: &str,
    now: OffsetDateTime,
) -> Result<AuthContext, S3Error> {
    if headers.contains_key("authorization") {
        verify_header(method, uri, headers, store, expected_region, now)
    } else if uri
        .query()
        .is_some_and(|query| has_query_parameter(query, "X-Amz-Algorithm"))
    {
        verify_presigned(method, uri, headers, store, expected_region, now)
    } else {
        Err(S3Error::access_denied())
    }
}

fn verify_header(
    method: &Method,
    uri: &Uri,
    headers: &HeaderMap,
    store: &CredentialStore,
    expected_region: &str,
    now: OffsetDateTime,
) -> Result<AuthContext, S3Error> {
    let authorization = header_value(headers, "authorization")?;
    let parsed = parse_authorization(authorization)?;

    let credential = store
        .lookup(&parsed.scope.access_key_id)
        .ok_or_else(S3Error::invalid_access_key)?;

    if !expected_region.is_empty() && parsed.scope.region != expected_region {
        return Err(S3Error::malformed(
            "The authorization header has the wrong region.",
        ));
    }

    let amz_date = header_value(headers, "x-amz-date")?;
    let timestamp = parse_amz_date(amz_date)
        .ok_or_else(|| S3Error::malformed("The x-amz-date header is not valid."))?;
    check_clock_skew(timestamp, now)?;

    if parsed.scope.date != format_scope_date(timestamp) {
        return Err(S3Error::malformed(
            "The credential date does not match the x-amz-date header.",
        ));
    }

    let payload_hash = header_value(headers, "x-amz-content-sha256")?;
    let canonical_headers = build_canonical_headers(headers, uri, parsed.signed_headers)?;
    let canonical_request = build_canonical_request(
        method,
        uri,
        &canonical_headers,
        parsed.signed_headers,
        payload_hash,
        None,
    );
    let scope = credential_scope(&parsed.scope.date, &parsed.scope.region, "s3");
    let to_sign = string_to_sign(amz_date, &scope, &canonical_request);
    let key = signing_key(
        credential.secret_access_key(),
        &parsed.scope.date,
        &parsed.scope.region,
        "s3",
    );
    let expected = hex::encode(hmac_sha256(&key, to_sign.as_bytes()));
    if !constant_time_eq(&expected, parsed.signature) {
        return Err(S3Error::signature_mismatch());
    }

    let payload = payload_kind(
        payload_hash,
        headers,
        key,
        parsed.signature.to_owned(),
        amz_date.to_owned(),
        scope,
    )?;

    Ok(AuthContext::new(credential, payload))
}

fn verify_presigned(
    method: &Method,
    uri: &Uri,
    headers: &HeaderMap,
    store: &CredentialStore,
    expected_region: &str,
    now: OffsetDateTime,
) -> Result<AuthContext, S3Error> {
    let query = uri.query().unwrap_or_default();
    let parameters = parse_query_parameters(query);

    let algorithm = required_parameter(&parameters, "X-Amz-Algorithm")?;
    if algorithm != ALGORITHM {
        return Err(S3Error::new(
            axum::http::StatusCode::BAD_REQUEST,
            "AuthorizationQueryParametersError",
            "X-Amz-Algorithm must be AWS4-HMAC-SHA256.",
        ));
    }
    let credential_value = required_parameter(&parameters, "X-Amz-Credential")?;
    let parsed_scope = parse_credential(credential_value)?;
    let credential = store
        .lookup(&parsed_scope.access_key_id)
        .ok_or_else(S3Error::invalid_access_key)?;

    if !expected_region.is_empty() && parsed_scope.region != expected_region {
        return Err(S3Error::malformed(
            "The presigned request has the wrong region.",
        ));
    }

    let amz_date = required_parameter(&parameters, "X-Amz-Date")?;
    let timestamp =
        parse_amz_date(amz_date).ok_or_else(|| S3Error::malformed("X-Amz-Date is not valid."))?;
    if timestamp - now > time::Duration::seconds(MAX_CLOCK_SKEW_SECONDS) {
        return Err(S3Error::time_skewed());
    }

    let expires = required_parameter(&parameters, "X-Amz-Expires")?
        .parse::<u64>()
        .map_err(|_| {
            S3Error::new(
                axum::http::StatusCode::BAD_REQUEST,
                "AuthorizationQueryParametersError",
                "X-Amz-Expires is not a number.",
            )
        })?;
    if expires == 0 || expires > MAX_PRESIGN_SECONDS {
        return Err(S3Error::new(
            axum::http::StatusCode::BAD_REQUEST,
            "AuthorizationQueryParametersError",
            "X-Amz-Expires must be between 1 and 604800 seconds.",
        ));
    }
    let lifetime = i64::try_from(expires).map_err(|_| S3Error::expired())?;
    if now > timestamp + time::Duration::seconds(lifetime) {
        return Err(S3Error::expired());
    }

    let signed_headers = required_parameter(&parameters, "X-Amz-SignedHeaders")?;
    let signature = required_parameter(&parameters, "X-Amz-Signature")?;
    if signature.len() != 64 || !signature.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(S3Error::signature_mismatch());
    }

    let canonical_headers = build_canonical_headers(headers, uri, signed_headers)?;
    let canonical_request = build_canonical_request(
        method,
        uri,
        &canonical_headers,
        signed_headers,
        "UNSIGNED-PAYLOAD",
        Some("X-Amz-Signature"),
    );
    let scope = credential_scope(&parsed_scope.date, &parsed_scope.region, "s3");
    let to_sign = string_to_sign(amz_date, &scope, &canonical_request);
    let key = signing_key(
        credential.secret_access_key(),
        &parsed_scope.date,
        &parsed_scope.region,
        "s3",
    );
    let expected = hex::encode(hmac_sha256(&key, to_sign.as_bytes()));
    if !constant_time_eq(&expected, signature) {
        return Err(S3Error::signature_mismatch());
    }

    Ok(AuthContext::new(credential, PayloadKind::Unsigned))
}

fn check_clock_skew(timestamp: OffsetDateTime, now: OffsetDateTime) -> Result<(), S3Error> {
    let difference = (now - timestamp).whole_seconds().abs();
    if difference > MAX_CLOCK_SKEW_SECONDS {
        Err(S3Error::time_skewed())
    } else {
        Ok(())
    }
}

struct ParsedAuthorization<'a> {
    scope: CredentialScope,
    signed_headers: &'a str,
    signature: &'a str,
}

fn parse_authorization(value: &str) -> Result<ParsedAuthorization<'_>, S3Error> {
    let rest = value
        .strip_prefix(ALGORITHM)
        .and_then(|rest| rest.strip_prefix(' '))
        .ok_or_else(|| S3Error::malformed("The authorization algorithm is not valid."))?;

    let mut credential = None;
    let mut signed_headers = None;
    let mut signature = None;
    for part in rest.split(',') {
        let part = part.trim();
        let Some((name, value)) = part.split_once('=') else {
            continue;
        };
        match name {
            "Credential" => credential = Some(value),
            "SignedHeaders" => signed_headers = Some(value),
            "Signature" => signature = Some(value),
            _ => {}
        }
    }

    let credential = credential.ok_or_else(|| S3Error::malformed("Credential is missing."))?;
    let signed_headers =
        signed_headers.ok_or_else(|| S3Error::malformed("SignedHeaders is missing."))?;
    let signature = signature.ok_or_else(|| S3Error::malformed("Signature is missing."))?;
    if signed_headers.is_empty() {
        return Err(S3Error::malformed("SignedHeaders is empty."));
    }
    if signature.len() != 64 || !signature.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(S3Error::signature_mismatch());
    }

    Ok(ParsedAuthorization {
        scope: parse_credential(credential)?,
        signed_headers,
        signature,
    })
}

struct CredentialScope {
    access_key_id: String,
    date: String,
    region: String,
}

fn parse_credential(value: &str) -> Result<CredentialScope, S3Error> {
    let parts: Vec<&str> = value.split('/').collect();
    if parts.len() != 5 {
        return Err(S3Error::malformed("The Credential value is not valid."));
    }
    let access_key_id = parts[0];
    let date = parts[1];
    let region = parts[2];
    let service = parts[3];
    let terminator = parts[4];
    if access_key_id.is_empty() {
        return Err(S3Error::malformed("The Credential access key is empty."));
    }
    if date.len() != 8 || !date.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(S3Error::malformed("The Credential date is not valid."));
    }
    if region.is_empty() {
        return Err(S3Error::malformed("The Credential region is empty."));
    }
    if service != "s3" {
        return Err(S3Error::malformed("The Credential service must be s3."));
    }
    if terminator != "aws4_request" {
        return Err(S3Error::malformed(
            "The Credential terminator must be aws4_request.",
        ));
    }
    Ok(CredentialScope {
        access_key_id: access_key_id.to_owned(),
        date: date.to_owned(),
        region: region.to_owned(),
    })
}

fn header_value<'a>(headers: &'a HeaderMap, name: &str) -> Result<&'a str, S3Error> {
    headers
        .get(name)
        .ok_or_else(|| S3Error::malformed(&format!("The {name} header is required.")))?
        .to_str()
        .map_err(|_| S3Error::malformed(&format!("The {name} header is not valid.")))
}

fn build_canonical_headers(
    headers: &HeaderMap,
    uri: &Uri,
    signed_headers: &str,
) -> Result<String, S3Error> {
    let names: Vec<&str> = signed_headers.split(';').collect();
    let mut previous: Option<&str> = None;
    for name in &names {
        if name.is_empty()
            || *name != name.to_ascii_lowercase()
            || previous.is_some_and(|previous| previous >= *name)
        {
            return Err(S3Error::malformed(
                "SignedHeaders must be lowercase and sorted.",
            ));
        }
        previous = Some(name);
    }

    let mut output = String::new();
    for name in names {
        let values: Vec<String> = if name == "host" {
            let host = headers
                .get(HOST)
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned)
                .or_else(|| uri.authority().map(ToString::to_string))
                .ok_or_else(|| S3Error::malformed("The Host header is required."))?;
            vec![host]
        } else {
            let header_name = axum::http::HeaderName::from_bytes(name.as_bytes())
                .map_err(|_| S3Error::malformed("A signed header name is not valid."))?;
            let mut collected = Vec::new();
            for value in headers.get_all(&header_name) {
                let value = value
                    .to_str()
                    .map_err(|_| S3Error::malformed("A signed header value is not valid."))?;
                collected.push(normalize_header_value(value));
            }
            if collected.is_empty() {
                return Err(S3Error::malformed(&format!(
                    "The signed header {name} is missing."
                )));
            }
            collected
        };
        output.push_str(name);
        output.push(':');
        output.push_str(&values.join(","));
        output.push('\n');
    }
    Ok(output)
}

fn build_canonical_request(
    method: &Method,
    uri: &Uri,
    canonical_headers: &str,
    signed_headers: &str,
    payload_hash: &str,
    exclude_query: Option<&str>,
) -> String {
    format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        method.as_str(),
        canonical_uri(uri.path()),
        canonical_query(uri.query(), exclude_query),
        canonical_headers,
        signed_headers,
        payload_hash
    )
}

fn string_to_sign(amz_date: &str, scope: &str, canonical_request: &str) -> String {
    let request_hash = sha256_hex(canonical_request.as_bytes());
    format!("{ALGORITHM}\n{amz_date}\n{scope}\n{request_hash}")
}

fn normalize_header_value(value: &str) -> String {
    let mut output = String::with_capacity(value.len());
    let mut in_space = false;
    for character in value.trim().chars() {
        if character == ' ' || character == '\t' {
            if !in_space {
                output.push(' ');
                in_space = true;
            }
        } else {
            output.push(character);
            in_space = false;
        }
    }
    output
}

fn payload_kind(
    payload_hash: &str,
    headers: &HeaderMap,
    key: [u8; 32],
    seed_signature: String,
    amz_date: String,
    scope: String,
) -> Result<PayloadKind, S3Error> {
    match payload_hash {
        "UNSIGNED-PAYLOAD" => Ok(PayloadKind::Unsigned),
        "STREAMING-AWS4-HMAC-SHA256-PAYLOAD" => Ok(PayloadKind::Streaming(StreamingContext {
            signing_key: SigningKey::new(key),
            seed_signature,
            amz_date,
            scope,
            decoded_length: decoded_length(headers)?,
            trailer: false,
        })),
        "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER" => {
            Ok(PayloadKind::Streaming(StreamingContext {
                signing_key: SigningKey::new(key),
                seed_signature,
                amz_date,
                scope,
                decoded_length: decoded_length(headers)?,
                trailer: true,
            }))
        }
        "STREAMING-UNSIGNED-PAYLOAD-TRAILER" => Ok(PayloadKind::StreamingUnsignedTrailer),
        value if value.starts_with("STREAMING-") => Err(S3Error::not_implemented(
            "The streaming payload mode is not supported.",
        )),
        value => parse_hash(value).map(PayloadKind::Hash).ok_or_else(|| {
            S3Error::invalid_request("The x-amz-content-sha256 header is not valid.")
        }),
    }
}

fn parse_hash(value: &str) -> Option<[u8; 32]> {
    if value.len() != 64 {
        return None;
    }
    let bytes = hex::decode(value).ok()?;
    let hash: [u8; 32] = bytes.try_into().ok()?;
    Some(hash)
}

fn decoded_length(headers: &HeaderMap) -> Result<Option<u64>, S3Error> {
    let Some(value) = headers.get("x-amz-decoded-content-length") else {
        return Ok(None);
    };
    let value = value
        .to_str()
        .map_err(|_| S3Error::malformed("x-amz-decoded-content-length is not valid."))?;
    value
        .parse::<u64>()
        .map(Some)
        .map_err(|_| S3Error::malformed("x-amz-decoded-content-length is not a number."))
}

fn parse_query_parameters(query: &str) -> Vec<(String, String)> {
    let mut parameters = Vec::new();
    for pair in query.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
        parameters.push((decode(name), decode(value)));
    }
    parameters
}

fn required_parameter<'a>(
    parameters: &'a [(String, String)],
    name: &str,
) -> Result<&'a str, S3Error> {
    parameters
        .iter()
        .find(|(key, _)| key == name)
        .map(|(_, value)| value.as_str())
        .ok_or_else(|| {
            S3Error::new(
                axum::http::StatusCode::BAD_REQUEST,
                "AuthorizationQueryParametersError",
                &format!("{name} is required."),
            )
        })
}

fn has_query_parameter(query: &str, name: &str) -> bool {
    query.split('&').any(|pair| {
        let key = pair.split_once('=').map_or(pair, |(key, _)| key);
        key == name
    })
}

fn decode(value: &str) -> String {
    percent_encoding::percent_decode_str(value)
        .decode_utf8_lossy()
        .into_owned()
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use axum::http::{HeaderMap, HeaderValue, Method, Uri, header::HOST};

    use super::authenticate;
    use crate::auth::{CredentialStore, EMPTY_SHA256, parse_amz_date};
    use crate::config::ClientConfig;
    use crate::config::secret::SecretString;

    const ACCESS_KEY: &str = "AKIAIOSFODNN7EXAMPLE";
    const SECRET_KEY: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";

    fn store() -> CredentialStore {
        CredentialStore::new(&[ClientConfig {
            access_key_id: ACCESS_KEY.to_owned(),
            secret_access_key: SecretString::new(SECRET_KEY),
            session_token: None,
            buckets: vec!["media".to_owned()],
        }])
    }

    fn header_request() -> (Method, Uri, HeaderMap) {
        let uri: Uri = "/test.txt".parse().unwrap();
        let mut headers = HeaderMap::new();
        headers.insert(
            HOST,
            HeaderValue::from_static("examplebucket.s3.amazonaws.com"),
        );
        headers.insert("range", HeaderValue::from_static("bytes=0-9"));
        headers.insert(
            "x-amz-content-sha256",
            HeaderValue::from_static(EMPTY_SHA256),
        );
        headers.insert("x-amz-date", HeaderValue::from_static("20130524T000000Z"));
        headers.insert(
            "authorization",
            HeaderValue::from_static(
                "AWS4-HMAC-SHA256 \
                 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, \
                 SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, \
                 Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41",
            ),
        );
        (Method::GET, uri, headers)
    }

    fn now() -> time::OffsetDateTime {
        parse_amz_date("20130524T000000Z").unwrap()
    }

    #[test]
    fn aws_header_vector_verifies() {
        let (method, uri, headers) = header_request();
        let auth = authenticate(&method, &uri, &headers, &store(), "us-east-1", now()).unwrap();
        assert_eq!(auth.credential().access_key_id(), ACCESS_KEY);
        assert!(auth.allows_bucket("media"));
    }

    #[test]
    fn wrong_signature_is_rejected() {
        let (method, uri, mut headers) = header_request();
        headers.insert(
            "authorization",
            HeaderValue::from_static(
                "AWS4-HMAC-SHA256 \
                 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, \
                 SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, \
                 Signature=0000000000000000000000000000000000000000000000000000000000000000",
            ),
        );
        let error = authenticate(&method, &uri, &headers, &store(), "us-east-1", now())
            .err()
            .unwrap();
        assert_eq!(error.code, "SignatureDoesNotMatch");
    }

    #[test]
    fn unknown_access_key_is_rejected() {
        let (method, uri, mut headers) = header_request();
        headers.insert(
            "authorization",
            HeaderValue::from_static(
                "AWS4-HMAC-SHA256 \
                 Credential=UNKNOWN/20130524/us-east-1/s3/aws4_request, \
                 SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, \
                 Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41",
            ),
        );
        let error = authenticate(&method, &uri, &headers, &store(), "us-east-1", now())
            .err()
            .unwrap();
        assert_eq!(error.code, "InvalidAccessKeyId");
    }

    #[test]
    fn stale_time_is_rejected() {
        let (method, uri, headers) = header_request();
        let late = now() + time::Duration::minutes(20);
        let error = authenticate(&method, &uri, &headers, &store(), "us-east-1", late)
            .err()
            .unwrap();
        assert_eq!(error.code, "RequestTimeTooSkewed");
    }

    fn presigned_uri() -> Uri {
        "/test.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20130524%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20130524T000000Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host&X-Amz-Signature=aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404".parse().unwrap()
    }

    fn presigned_headers() -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            HOST,
            HeaderValue::from_static("examplebucket.s3.amazonaws.com"),
        );
        headers
    }

    #[test]
    fn aws_presigned_vector_verifies() {
        let uri = presigned_uri();
        let headers = presigned_headers();
        let auth =
            authenticate(&Method::GET, &uri, &headers, &store(), "us-east-1", now()).unwrap();
        assert_eq!(auth.credential().access_key_id(), ACCESS_KEY);
    }

    #[test]
    fn expired_presigned_request_is_rejected() {
        let uri = presigned_uri();
        let headers = presigned_headers();
        let later = now() + time::Duration::days(2);
        let error = authenticate(&Method::GET, &uri, &headers, &store(), "us-east-1", later)
            .err()
            .unwrap();
        assert_eq!(error.code, "ExpiredToken");
    }

    #[test]
    fn tampered_presigned_signature_is_rejected() {
        let uri: Uri = "/test.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20130524%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20130524T000000Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host&X-Amz-Signature=0000000000000000000000000000000000000000000000000000000000000000".parse().unwrap();
        let headers = presigned_headers();
        let error = authenticate(&Method::GET, &uri, &headers, &store(), "us-east-1", now())
            .err()
            .unwrap();
        assert_eq!(error.code, "SignatureDoesNotMatch");
    }
}

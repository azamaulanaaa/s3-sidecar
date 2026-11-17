//! Sign upstream requests with a client credential.

use axum::http::{HeaderMap, HeaderValue, Method, header};
use reqwest::Url;
use time::OffsetDateTime;

use super::{
    ALGORITHM, ClientCredential, canonical_query, canonical_uri, credential_scope, format_amz_date,
    format_scope_date, hmac_sha256, sha256_hex, signing_key,
};
use crate::errors::S3Error;

/// Sign a request with `SigV4`.
///
/// # Errors
///
/// Returns an internal error when a header value cannot be built.
// The signer needs the request, the credential, and the signing context.
#[expect(
    clippy::too_many_arguments,
    reason = "the signer needs the request, the credential, and the signing context"
)]
pub fn sign_request(
    method: &Method,
    url: &Url,
    headers: &mut HeaderMap,
    payload_hash: &str,
    credential: &ClientCredential,
    region: &str,
    now: OffsetDateTime,
) -> Result<(), S3Error> {
    headers.remove(header::AUTHORIZATION);
    headers.remove("x-amz-date");
    headers.remove("x-amz-content-sha256");
    headers.remove("x-amz-security-token");

    let host = host_header(url)?;
    set_header(headers, header::HOST.as_str(), &host)?;
    let amz_date = format_amz_date(now);
    set_header(headers, "x-amz-date", &amz_date)?;
    set_header(headers, "x-amz-content-sha256", payload_hash)?;
    if let Some(token) = credential.session_token() {
        set_header(headers, "x-amz-security-token", token)?;
    }

    let (canonical_headers, signed_headers) = canonical_headers(headers)?;
    let canonical_request = format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        method.as_str(),
        canonical_uri(url.path()),
        canonical_query(url.query(), None),
        canonical_headers,
        signed_headers,
        payload_hash
    );

    let date = format_scope_date(now);
    let scope = credential_scope(&date, region, "s3");
    let request_hash = sha256_hex(canonical_request.as_bytes());
    let to_sign = format!("{ALGORITHM}\n{amz_date}\n{scope}\n{request_hash}");
    let key = signing_key(credential.secret_access_key(), &date, region, "s3");
    let signature = hex::encode(hmac_sha256(&key, to_sign.as_bytes()));

    let authorization = format!(
        "{ALGORITHM} Credential={}/{}/{}/s3/aws4_request, SignedHeaders={}, Signature={}",
        credential.access_key_id(),
        date,
        region,
        signed_headers,
        signature
    );
    set_header(headers, header::AUTHORIZATION.as_str(), &authorization)?;
    Ok(())
}

fn host_header(url: &Url) -> Result<String, S3Error> {
    let host = url.host_str().ok_or_else(S3Error::internal)?.to_owned();
    match url.port() {
        Some(port) => Ok(format!("{host}:{port}")),
        None => Ok(host),
    }
}

fn set_header(headers: &mut HeaderMap, name: &str, value: &str) -> Result<(), S3Error> {
    let name =
        axum::http::HeaderName::from_bytes(name.as_bytes()).map_err(|_| S3Error::internal())?;
    let value = HeaderValue::from_str(value).map_err(|_| S3Error::internal())?;
    headers.insert(name, value);
    Ok(())
}

fn canonical_headers(headers: &HeaderMap) -> Result<(String, String), S3Error> {
    let mut entries: Vec<(String, String)> = Vec::new();
    for (name, value) in headers {
        let name = name.as_str();
        if is_unsigned(name) {
            continue;
        }
        let value = value.to_str().map_err(|_| S3Error::internal())?;
        entries.push((name.to_owned(), normalize(value)));
    }
    entries.sort_by(|left, right| left.0.cmp(&right.0));

    let mut canonical = String::new();
    let mut signed = Vec::with_capacity(entries.len());
    let mut index = 0;
    while index < entries.len() {
        let name = entries[index].0.clone();
        let mut values = Vec::new();
        while index < entries.len() && entries[index].0 == name {
            values.push(entries[index].1.clone());
            index += 1;
        }
        canonical.push_str(&name);
        canonical.push(':');
        canonical.push_str(&values.join(","));
        canonical.push('\n');
        signed.push(name);
    }
    Ok((canonical, signed.join(";")))
}

fn is_unsigned(name: &str) -> bool {
    matches!(
        name,
        "authorization"
            | "content-length"
            | "user-agent"
            | "accept-encoding"
            | "connection"
            | "expect"
            | "transfer-encoding"
            | "trailer"
            | "te"
            | "upgrade"
            | "cookie"
    )
}

fn normalize(value: &str) -> String {
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

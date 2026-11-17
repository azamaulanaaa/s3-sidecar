//! Merge write-back cache state into S3 list responses.

use std::collections::{BTreeMap, HashSet};

use axum::body::Body;
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, header};
use axum::response::Response;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use super::AppState;
use crate::auth::ClientCredential;
use crate::cache::meta::CacheMetadata;
use crate::errors::S3Error;
use crate::storage::ForwardRequest;

/// The S3 default and maximum page size.
const MAX_KEYS_LIMIT: usize = 1_000;

/// The upper bound for origin items that one request may examine.
const MAX_LIST_SCAN: usize = 10_000;

/// The upper bound for origin pages that one request may read.
const MAX_ORIGIN_PAGES: usize = 16;

/// The token format version.
const TOKEN_VERSION: u8 = 1;

/// Characters that a `ListObjects` response encodes for `encoding-type=url`.
const URL_ENCODE_SET: &percent_encoding::AsciiSet = &percent_encoding::NON_ALPHANUMERIC
    .remove(b'/')
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'~');

/// The stateless resume state for one merged listing.
#[derive(serde::Deserialize, serde::Serialize)]
struct ResumeToken {
    /// The token format version.
    version: u8,
    /// The last returned item key or common prefix.
    last: String,
    /// The origin continuation token for the next page.
    #[serde(skip_serializing_if = "Option::is_none")]
    origin: Option<String>,
}

/// One item in a merged listing.
#[derive(Clone)]
enum Item {
    /// One object record.
    Contents { key: String, block: String },
    /// One common prefix.
    CommonPrefix { key: String },
}

impl Item {
    fn key(&self) -> &str {
        match self {
            Self::Contents { key, .. } | Self::CommonPrefix { key } => key,
        }
    }
}

/// One origin listing page.
struct OriginPage {
    items: Vec<Item>,
    next_token: Option<String>,
}

/// Merge write-back state into one list response.
///
/// The function returns `None` when the request is not a `ListObjects` V1 or V2
/// request, or when a V2 continuation token is not a sidecar token.
///
/// # Errors
///
/// Returns an error when the origin listing fails or `encoding-type` is not
/// valid.
#[expect(
    clippy::too_many_lines,
    reason = "the listing merge handles each page item in order"
)]
pub(crate) async fn merged_response(
    state: &AppState,
    write_state: (Vec<CacheMetadata>, Vec<String>),
    bucket: &str,
    query: &str,
    credential: &std::sync::Arc<ClientCredential>,
) -> Result<Option<Response>, S3Error> {
    if super::bucket_subresource(query) {
        return Ok(None);
    }
    let pairs = query_pairs(query);
    let version2 = query_value(&pairs, "list-type") == Some("2");
    if !version2 && query_value(&pairs, "list-type").is_some() {
        return Ok(None);
    }
    let encode = match query_value(&pairs, "encoding-type") {
        Some(value) if value.eq_ignore_ascii_case("url") => true,
        Some(_) => return Err(invalid_encoding_type()),
        None => false,
    };
    let prefix = query_value(&pairs, "prefix").unwrap_or_default();
    let delimiter = query_value(&pairs, "delimiter").filter(|value| !value.is_empty());
    let max_keys = query_value(&pairs, "max-keys")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(MAX_KEYS_LIMIT)
        .min(MAX_KEYS_LIMIT);
    let marker = query_value(&pairs, "marker").unwrap_or_default();
    let start_after_param = if version2 && query_value(&pairs, "continuation-token").is_none() {
        query_value(&pairs, "start-after").map(str::to_owned)
    } else {
        None
    };
    let (start_after, initial_origin) = if version2 {
        match query_value(&pairs, "continuation-token") {
            Some(token) => match decode_token(token) {
                Some(resume) => (Some(resume.last), resume.origin),
                None => return Ok(None),
            },
            None => (start_after_param.clone(), None),
        }
    } else if marker.is_empty() {
        (None, None)
    } else {
        match decode_token(marker) {
            Some(resume) => (Some(resume.last), resume.origin),
            None => (Some(marker.to_owned()), None),
        }
    };

    let (pending, tombstones) = write_state;
    let tombstones: HashSet<String> = tombstones.into_iter().collect();
    let pending_len = pending.len();
    let tombstone_len = tombstones.len();
    let pending = pending_items(
        &pending,
        &tombstones,
        prefix,
        delimiter,
        start_after.as_deref(),
        encode,
    );

    let mut contents = Vec::new();
    let mut prefixes = Vec::new();
    let mut count = 0;
    let mut after = start_after;
    let mut last_returned: Option<String> = None;
    let mut truncated = false;
    let mut token_origin: Option<String> = None;
    let mut pending_index = 0;

    if max_keys > 0 {
        let scan_limit = max_keys
            .saturating_add(pending_len)
            .saturating_add(tombstone_len)
            .saturating_add(1)
            .clamp(1, MAX_LIST_SCAN);
        let mut scanned = 0_usize;
        let mut page = fetch_origin_page(
            state,
            credential,
            bucket,
            prefix,
            delimiter,
            max_keys.max(1),
            initial_origin.as_deref(),
            if initial_origin.is_none() {
                after.as_deref()
            } else {
                None
            },
        )
        .await?;
        scanned = scanned.saturating_add(page.items.len());
        let mut page_input = initial_origin;
        let mut origin_index = 0;
        let mut pages = 1;

        loop {
            let origin_next = next_origin_item(
                &page.items,
                &mut origin_index,
                after.as_deref(),
                &tombstones,
            );
            let pending_next = next_pending_item(&pending, &mut pending_index, after.as_deref());

            if count >= max_keys {
                truncated =
                    origin_next.is_some() || pending_next.is_some() || page.next_token.is_some();
                token_origin = if origin_next.is_some() {
                    page_input.clone()
                } else {
                    page.next_token.clone()
                };
                break;
            }

            match (origin_next, pending_next) {
                (None, None) => {
                    let Some(next) = page.next_token.clone() else {
                        break;
                    };
                    if pages >= MAX_ORIGIN_PAGES || scanned >= scan_limit {
                        truncated = true;
                        token_origin = Some(next);
                        break;
                    }
                    page = fetch_origin_page(
                        state,
                        credential,
                        bucket,
                        prefix,
                        delimiter,
                        max_keys.saturating_sub(count).max(1),
                        Some(&next),
                        None,
                    )
                    .await?;
                    page_input = Some(next);
                    origin_index = 0;
                    scanned = scanned.saturating_add(page.items.len());
                    pages += 1;
                }
                (Some(origin), pending) => {
                    let key = match pending {
                        Some(pending) if pending.key() < origin.key() => {
                            emit_item(pending, &mut contents, &mut prefixes, encode);
                            pending_index += 1;
                            pending.key().to_owned()
                        }
                        Some(pending) if pending.key() == origin.key() => {
                            // A pending Contents item wins over the origin item.
                            emit_item(pending, &mut contents, &mut prefixes, encode);
                            origin_index += 1;
                            pending_index += 1;
                            pending.key().to_owned()
                        }
                        _ => {
                            emit_item(origin, &mut contents, &mut prefixes, encode);
                            origin_index += 1;
                            origin.key().to_owned()
                        }
                    };
                    count += 1;
                    after = Some(key.clone());
                    last_returned = Some(key);
                }
                (None, Some(pending)) => {
                    emit_item(pending, &mut contents, &mut prefixes, encode);
                    let key = pending.key().to_owned();
                    pending_index += 1;
                    count += 1;
                    after = Some(key.clone());
                    last_returned = Some(key);
                }
            }
        }
    }

    let token = if truncated {
        let last = last_returned
            .clone()
            .unwrap_or_else(|| after.clone().unwrap_or_default());
        Some(encode_token(&last, token_origin.as_deref())?)
    } else {
        None
    };
    Ok(Some(list_response(&ListPage {
        bucket,
        prefix,
        marker,
        delimiter,
        start_after: start_after_param.as_deref(),
        max_keys,
        truncated,
        token: token.as_deref(),
        contents: &contents,
        prefixes: &prefixes,
        version2,
        encode,
    })))
}

/// One merged list page.
struct ListPage<'a> {
    bucket: &'a str,
    prefix: &'a str,
    marker: &'a str,
    delimiter: Option<&'a str>,
    start_after: Option<&'a str>,
    max_keys: usize,
    truncated: bool,
    token: Option<&'a str>,
    contents: &'a [String],
    prefixes: &'a [String],
    version2: bool,
    encode: bool,
}

#[expect(
    clippy::format_push_string,
    reason = "the listing XML is built with push_str"
)]
fn list_response(page: &ListPage<'_>) -> Response {
    let mut xml = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
    xml.push_str("<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">");
    push_tag(&mut xml, "Name", page.bucket);
    push_value(&mut xml, "Prefix", page.prefix, page.encode);
    if page.version2 {
        if let Some(start_after) = page.start_after {
            push_value(&mut xml, "StartAfter", start_after, page.encode);
        }
        xml.push_str(&format!(
            "<KeyCount>{}</KeyCount>",
            page.contents.len() + page.prefixes.len()
        ));
    } else {
        push_value(&mut xml, "Marker", page.marker, page.encode);
    }
    if let Some(delimiter) = page.delimiter {
        push_value(&mut xml, "Delimiter", delimiter, page.encode);
    }
    if page.encode {
        push_tag(&mut xml, "EncodingType", "url");
    }
    xml.push_str(&format!("<MaxKeys>{}</MaxKeys>", page.max_keys));
    xml.push_str(&format!("<IsTruncated>{}</IsTruncated>", page.truncated));
    if page.truncated
        && let Some(token) = page.token
    {
        if page.version2 {
            push_tag(&mut xml, "NextContinuationToken", token);
        } else {
            push_value(&mut xml, "NextMarker", token, page.encode);
        }
    }
    for block in page.contents {
        xml.push_str(block);
    }
    for prefix in page.prefixes {
        xml.push_str("<CommonPrefixes>");
        push_value(&mut xml, "Prefix", prefix, page.encode);
        xml.push_str("</CommonPrefixes>");
    }
    xml.push_str("</ListBucketResult>");

    let mut response = Response::new(Body::from(xml));
    *response.status_mut() = StatusCode::OK;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/xml"),
    );
    response
}

/// Build the local items for the pending writes.
fn pending_items(
    pending: &[CacheMetadata],
    tombstones: &HashSet<String>,
    prefix: &str,
    delimiter: Option<&str>,
    start_after: Option<&str>,
    encode: bool,
) -> Vec<Item> {
    let mut items: BTreeMap<String, Item> = BTreeMap::new();
    for meta in pending {
        if !meta.origin_key.starts_with(prefix) || tombstones.contains(&meta.origin_key) {
            continue;
        }
        let rest = meta.origin_key.get(prefix.len()..).unwrap_or_default();
        if let Some(delimiter) = delimiter
            && let Some(index) = rest.find(delimiter)
        {
            let common = format!("{prefix}{}", &rest[..index + delimiter.len()]);
            if start_after.is_some_and(|after| common.as_str() <= after) {
                continue;
            }
            items
                .entry(common.clone())
                .or_insert(Item::CommonPrefix { key: common });
            continue;
        }
        if start_after.is_some_and(|after| meta.origin_key.as_str() <= after) {
            continue;
        }
        items.insert(
            meta.origin_key.clone(),
            Item::Contents {
                key: meta.origin_key.clone(),
                block: pending_block(meta, encode),
            },
        );
    }
    items.into_values().collect()
}

/// Return the next origin item that is not hidden or already returned.
fn next_origin_item<'a>(
    items: &'a [Item],
    index: &mut usize,
    after: Option<&str>,
    tombstones: &HashSet<String>,
) -> Option<&'a Item> {
    loop {
        let item = items.get(*index)?;
        if after.is_some_and(|value| item.key() <= value) {
            *index += 1;
            continue;
        }
        if let Item::Contents { key, .. } = item
            && tombstones.contains(key)
        {
            *index += 1;
            continue;
        }
        return Some(item);
    }
}

/// Return the next pending item that is not already returned.
fn next_pending_item<'a>(
    items: &'a [Item],
    index: &mut usize,
    after: Option<&str>,
) -> Option<&'a Item> {
    loop {
        let item = items.get(*index)?;
        if after.is_some_and(|value| item.key() <= value) {
            *index += 1;
            continue;
        }
        return Some(item);
    }
}

fn emit_item(item: &Item, contents: &mut Vec<String>, prefixes: &mut Vec<String>, encode: bool) {
    match item {
        Item::Contents { key, block } => {
            contents.push(if encode {
                encode_contents_key(block, key)
            } else {
                block.clone()
            });
        }
        Item::CommonPrefix { key } => prefixes.push(key.clone()),
    }
}

/// Read one origin listing page.
#[expect(
    clippy::too_many_arguments,
    reason = "the origin page request needs the listing context"
)]
async fn fetch_origin_page(
    state: &AppState,
    credential: &std::sync::Arc<ClientCredential>,
    bucket: &str,
    prefix: &str,
    delimiter: Option<&str>,
    max_keys: usize,
    token: Option<&str>,
    start_after: Option<&str>,
) -> Result<OriginPage, S3Error> {
    let mut path = format!("/{bucket}?list-type=2&max-keys={max_keys}");
    if !prefix.is_empty() {
        path.push_str("&prefix=");
        path.push_str(&encode_query(prefix));
    }
    if let Some(delimiter) = delimiter {
        path.push_str("&delimiter=");
        path.push_str(&encode_query(delimiter));
    }
    if let Some(token) = token {
        path.push_str("&continuation-token=");
        path.push_str(&encode_query(token));
    } else if let Some(start_after) = start_after {
        path.push_str("&start-after=");
        path.push_str(&encode_query(start_after));
    }
    let request = ForwardRequest {
        method: Method::GET,
        path_and_query: path,
        headers: HeaderMap::new(),
        body: None,
        content_length: None,
    };
    let response = state
        .client
        .execute(credential, request)
        .await
        .map_err(|error| super::map_storage_error(error, None))?;
    if !response.status().is_success() {
        return Err(S3Error::from_status(response.status()));
    }
    let body = response
        .bytes()
        .await
        .map_err(|_| S3Error::service_unavailable())?;
    let text = String::from_utf8_lossy(&body);
    let truncated =
        extract_tag(&text, "IsTruncated").is_some_and(|value| value.eq_ignore_ascii_case("true"));
    let next_token = if truncated {
        extract_tag(&text, "NextContinuationToken")
    } else {
        None
    };
    let mut items: Vec<Item> = parse_contents(&text)
        .into_iter()
        .map(|(key, block)| Item::Contents { key, block })
        .collect();
    items.extend(
        parse_common_prefixes(&text)
            .into_iter()
            .map(|key| Item::CommonPrefix { key }),
    );
    items.sort_by(|left, right| left.key().cmp(right.key()));
    Ok(OriginPage { items, next_token })
}

/// Return the `<Contents>` block and key for every listed object.
fn parse_contents(xml: &str) -> Vec<(String, String)> {
    const OPEN: &str = "<Contents>";
    const CLOSE: &str = "</Contents>";
    let mut output = Vec::new();
    let mut rest = xml;
    while let Some(start) = rest.find(OPEN) {
        let after = &rest[start..];
        let Some(end) = after.find(CLOSE) else {
            break;
        };
        let block = &after[..end + CLOSE.len()];
        if let Some(key) = extract_tag(block, "Key") {
            output.push((key, block.to_owned()));
        }
        rest = &after[end + CLOSE.len()..];
    }
    output
}

/// Return the `<Prefix>` value for every listed common prefix.
fn parse_common_prefixes(xml: &str) -> Vec<String> {
    const OPEN: &str = "<CommonPrefixes>";
    const CLOSE: &str = "</CommonPrefixes>";
    let mut output = Vec::new();
    let mut rest = xml;
    while let Some(start) = rest.find(OPEN) {
        let after = &rest[start..];
        let Some(end) = after.find(CLOSE) else {
            break;
        };
        let block = &after[..end + CLOSE.len()];
        if let Some(prefix) = extract_tag(block, "Prefix") {
            output.push(prefix);
        }
        rest = &after[end + CLOSE.len()..];
    }
    output
}

/// Build one `<Contents>` block from a pending write.
#[expect(
    clippy::format_push_string,
    reason = "the contents block is built with push_str"
)]
fn pending_block(meta: &CacheMetadata, encode: bool) -> String {
    let mut xml = String::from("<Contents>");
    if encode {
        push_tag(&mut xml, "Key", &encode_url(&meta.origin_key));
    } else {
        push_tag(&mut xml, "Key", &meta.origin_key);
    }
    if let Ok(value) = OffsetDateTime::from_unix_timestamp(meta.created_at)
        && let Ok(value) = value.format(&Rfc3339)
    {
        push_tag(&mut xml, "LastModified", &value);
    }
    push_tag(&mut xml, "ETag", &meta.etag);
    xml.push_str(&format!("<Size>{}</Size>", meta.size));
    push_tag(&mut xml, "StorageClass", "STANDARD");
    xml.push_str("</Contents>");
    xml
}

/// Encode the key in one origin `<Contents>` block.
fn encode_contents_key(block: &str, key: &str) -> String {
    let encoded = encode_url(key);
    let open = "<Key>";
    let close = "</Key>";
    let Some(start) = block.find(open) else {
        return block.to_owned();
    };
    let value_start = start + open.len();
    let Some(offset) = block[value_start..].find(close) else {
        return block.to_owned();
    };
    let value_end = value_start + offset;
    let mut output = String::with_capacity(block.len() + encoded.len());
    output.push_str(&block[..value_start]);
    output.push_str(&encoded);
    output.push_str(&block[value_end..]);
    output
}

fn encode_url(value: &str) -> String {
    percent_encoding::utf8_percent_encode(value, URL_ENCODE_SET).to_string()
}

fn push_value(xml: &mut String, name: &str, value: &str, encode: bool) {
    if encode {
        push_tag(xml, name, &encode_url(value));
    } else {
        push_tag(xml, name, value);
    }
}

/// Return one XML element value.
fn extract_tag(xml: &str, name: &str) -> Option<String> {
    let open = format!("<{name}>");
    let close = format!("</{name}>");
    let start = xml.find(&open)? + open.len();
    let rest = &xml[start..];
    let end = rest.find(&close)?;
    Some(unescape_xml(&rest[..end]))
}

fn push_tag(xml: &mut String, name: &str, value: &str) {
    xml.push('<');
    xml.push_str(name);
    xml.push('>');
    push_escaped(xml, value);
    xml.push_str("</");
    xml.push_str(name);
    xml.push('>');
}

fn push_escaped(xml: &mut String, value: &str) {
    for character in value.chars() {
        match character {
            '&' => xml.push_str("&amp;"),
            '<' => xml.push_str("&lt;"),
            '>' => xml.push_str("&gt;"),
            '"' => xml.push_str("&quot;"),
            '\'' => xml.push_str("&apos;"),
            other => xml.push(other),
        }
    }
}

fn unescape_xml(value: &str) -> String {
    value
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&amp;", "&")
}

/// Decode one query string into name-value pairs.
fn query_pairs(query: &str) -> Vec<(String, String)> {
    query
        .split('&')
        .filter(|pair| !pair.is_empty())
        .map(|pair| {
            let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
            (decode_query(key), decode_query(value))
        })
        .collect()
}

fn query_value<'a>(pairs: &'a [(String, String)], name: &str) -> Option<&'a str> {
    pairs
        .iter()
        .find(|(key, _)| key == name)
        .map(|(_, value)| value.as_str())
}

fn decode_query(value: &str) -> String {
    percent_encoding::percent_decode_str(value)
        .decode_utf8_lossy()
        .into_owned()
}

fn encode_query(value: &str) -> String {
    percent_encoding::utf8_percent_encode(value, percent_encoding::NON_ALPHANUMERIC).to_string()
}

/// Encode the stateless resume state.
fn encode_token(last: &str, origin: Option<&str>) -> Result<String, S3Error> {
    let token = ResumeToken {
        version: TOKEN_VERSION,
        last: last.to_owned(),
        origin: origin.map(str::to_owned),
    };
    let bytes = serde_json::to_vec(&token).map_err(|_| S3Error::internal())?;
    Ok(hex::encode(bytes))
}

/// Decode the stateless resume state.
fn decode_token(value: &str) -> Option<ResumeToken> {
    let bytes = hex::decode(value).ok()?;
    let token: ResumeToken = serde_json::from_slice(&bytes).ok()?;
    (token.version == TOKEN_VERSION).then_some(token)
}

fn invalid_encoding_type() -> S3Error {
    S3Error::new(
        StatusCode::BAD_REQUEST,
        "InvalidArgument",
        "The encoding-type value is not valid. Use url.",
    )
}

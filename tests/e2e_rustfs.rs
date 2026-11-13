#![allow(clippy::unwrap_used, clippy::expect_used)]

//! End-to-end checks against a real S3 origin and a running sidecar.
//!
//! The test is ignored by default. Start rustfs or `MinIO` and the sidecar, then
//! set `S3_SIDECAR_E2E_PROXY`, `S3_SIDECAR_E2E_BUCKET`,
//! `S3_SIDECAR_E2E_ACCESS_KEY`, and `S3_SIDECAR_E2E_SECRET_KEY`.
//! The script `scripts/e2e-rustfs.sh` does this setup.

use std::sync::Arc;

use axum::http::{HeaderMap, HeaderValue, Method, header};
use reqwest::Url;
use s3_sidecar::auth::sign::sign_request;
use s3_sidecar::auth::{ClientCredential, CredentialStore, EMPTY_SHA256, now};
use s3_sidecar::config::ClientConfig;
use s3_sidecar::config::secret::SecretString;
use time::OffsetDateTime;

struct Settings {
    proxy: String,
    bucket: String,
    region: String,
    access_key: String,
    credential: Arc<ClientCredential>,
}

fn settings() -> Settings {
    let proxy = std::env::var("S3_SIDECAR_E2E_PROXY")
        .expect("S3_SIDECAR_E2E_PROXY is not set. Run scripts/e2e-rustfs.sh.");
    let bucket = std::env::var("S3_SIDECAR_E2E_BUCKET").expect("S3_SIDECAR_E2E_BUCKET is not set.");
    let region = std::env::var("S3_SIDECAR_E2E_REGION").unwrap_or_else(|_| "us-east-1".to_owned());
    let access_key =
        std::env::var("S3_SIDECAR_E2E_ACCESS_KEY").expect("S3_SIDECAR_E2E_ACCESS_KEY is not set.");
    let secret_key =
        std::env::var("S3_SIDECAR_E2E_SECRET_KEY").expect("S3_SIDECAR_E2E_SECRET_KEY is not set.");
    let store = CredentialStore::new(&[ClientConfig {
        access_key_id: access_key.clone(),
        secret_access_key: SecretString::new(secret_key),
        session_token: None,
        buckets: vec![bucket.clone()],
    }]);
    let credential = store.lookup(&access_key).unwrap();
    Settings {
        proxy,
        bucket,
        region,
        access_key,
        credential,
    }
}

fn request(
    client: &reqwest::Client,
    settings: &Settings,
    method: Method,
    path: &str,
    body: Option<Vec<u8>>,
    extra: &[(&str, &str)],
) -> reqwest::RequestBuilder {
    let url = Url::parse(&format!("{}{path}", settings.proxy)).unwrap();
    let mut headers = HeaderMap::new();
    for (name, value) in extra {
        headers.insert(
            axum::http::HeaderName::from_bytes(name.as_bytes()).unwrap(),
            HeaderValue::from_str(value).unwrap(),
        );
    }
    let payload_hash = if body.is_some() {
        "UNSIGNED-PAYLOAD"
    } else {
        EMPTY_SHA256
    };
    sign_request(
        &method,
        &url,
        &mut headers,
        payload_hash,
        &settings.credential,
        &settings.region,
        OffsetDateTime::now_utc(),
    )
    .unwrap();
    let mut builder = client.request(method, url);
    for (name, value) in &headers {
        builder = builder.header(name, value);
    }
    if let Some(body) = body {
        let length = body.len();
        builder = builder.header(header::CONTENT_LENGTH, length).body(body);
    }
    builder
}

#[tokio::test]
#[ignore = "requires a running sidecar and rustfs or MinIO"]
// The test walks the S3 operations in order.
#[allow(clippy::too_many_lines)]
async fn object_list_copy_multipart_and_auth() {
    let settings = settings();
    let client = reqwest::Client::new();
    let suffix = now().unix_timestamp_nanos();
    let key = format!("e2e/object-{suffix}.txt");
    let copy_key = format!("e2e/copy-{suffix}.txt");
    let multipart_key = format!("e2e/multipart-{suffix}.txt");
    let bucket = &settings.bucket;

    let put = request(
        &client,
        &settings,
        Method::PUT,
        &format!("/{bucket}/{key}"),
        Some(b"hello e2e".to_vec()),
        &[],
    )
    .send()
    .await
    .unwrap();
    assert!(put.status().is_success(), "PUT failed: {}", put.status());

    let get = request(
        &client,
        &settings,
        Method::GET,
        &format!("/{bucket}/{key}"),
        None,
        &[],
    )
    .send()
    .await
    .unwrap();
    assert_eq!(get.status(), reqwest::StatusCode::OK);
    assert_eq!(get.text().await.unwrap(), "hello e2e");

    let head = request(
        &client,
        &settings,
        Method::HEAD,
        &format!("/{bucket}/{key}"),
        None,
        &[],
    )
    .send()
    .await
    .unwrap();
    assert_eq!(head.status(), reqwest::StatusCode::OK);
    assert_eq!(
        head.headers()
            .get(header::CONTENT_LENGTH)
            .unwrap()
            .to_str()
            .unwrap(),
        "9"
    );

    let range = request(
        &client,
        &settings,
        Method::GET,
        &format!("/{bucket}/{key}"),
        None,
        &[("range", "bytes=0-4")],
    )
    .send()
    .await
    .unwrap();
    assert_eq!(range.status(), reqwest::StatusCode::PARTIAL_CONTENT);
    assert_eq!(range.text().await.unwrap(), "hello");

    let list = request(
        &client,
        &settings,
        Method::GET,
        &format!("/{bucket}?list-type=2&prefix=e2e%2F"),
        None,
        &[],
    )
    .send()
    .await
    .unwrap();
    assert_eq!(list.status(), reqwest::StatusCode::OK);
    let list_body = list.text().await.unwrap();
    assert!(list_body.contains(&key), "list body: {list_body}");

    let copy = request(
        &client,
        &settings,
        Method::PUT,
        &format!("/{bucket}/{copy_key}"),
        None,
        &[("x-amz-copy-source", &format!("/{bucket}/{key}"))],
    )
    .send()
    .await
    .unwrap();
    assert!(copy.status().is_success(), "COPY failed: {}", copy.status());

    let create = request(
        &client,
        &settings,
        Method::POST,
        &format!("/{bucket}/{multipart_key}?uploads"),
        Some(Vec::new()),
        &[],
    )
    .send()
    .await
    .unwrap();
    assert!(
        create.status().is_success(),
        "CREATE failed: {}",
        create.status()
    );
    let create_body = create.text().await.unwrap();
    let upload_id = between(&create_body, "<UploadId>", "</UploadId>").unwrap();

    let part = request(
        &client,
        &settings,
        Method::PUT,
        &format!("/{bucket}/{multipart_key}?partNumber=1&uploadId={upload_id}"),
        Some(b"multipart body".to_vec()),
        &[],
    )
    .send()
    .await
    .unwrap();
    assert!(part.status().is_success(), "PART failed: {}", part.status());
    let etag = part
        .headers()
        .get(header::ETAG)
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();

    let complete_xml = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag}</ETag></Part></CompleteMultipartUpload>"
    );
    let complete = request(
        &client,
        &settings,
        Method::POST,
        &format!("/{bucket}/{multipart_key}?uploadId={upload_id}"),
        Some(complete_xml.into_bytes()),
        &[],
    )
    .send()
    .await
    .unwrap();
    assert!(
        complete.status().is_success(),
        "COMPLETE failed: {}",
        complete.status()
    );

    for key in [&key, &copy_key, &multipart_key] {
        let delete = request(
            &client,
            &settings,
            Method::DELETE,
            &format!("/{bucket}/{key}"),
            None,
            &[],
        )
        .send()
        .await
        .unwrap();
        assert!(
            delete.status().is_success(),
            "DELETE failed: {}",
            delete.status()
        );
    }

    let wrong_store = CredentialStore::new(&[ClientConfig {
        access_key_id: settings.access_key.clone(),
        secret_access_key: SecretString::new("wrong-secret"),
        session_token: None,
        buckets: vec![bucket.clone()],
    }]);
    let wrong = wrong_store.lookup(&settings.access_key).unwrap();
    let url = Url::parse(&format!("{}/{bucket}", settings.proxy)).unwrap();
    let mut headers = HeaderMap::new();
    sign_request(
        &Method::GET,
        &url,
        &mut headers,
        EMPTY_SHA256,
        &wrong,
        &settings.region,
        OffsetDateTime::now_utc(),
    )
    .unwrap();
    let mut builder = client.get(url);
    for (name, value) in &headers {
        builder = builder.header(name, value);
    }
    let denied = builder.send().await.unwrap();
    assert_eq!(denied.status(), reqwest::StatusCode::FORBIDDEN);
    let body = denied.text().await.unwrap();
    assert!(body.contains("SignatureDoesNotMatch"), "body: {body}");
}

fn between<'a>(text: &'a str, start: &str, end: &str) -> Option<&'a str> {
    let rest = text.split_once(start)?.1;
    rest.split_once(end).map(|(value, _)| value)
}

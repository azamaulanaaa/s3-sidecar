//! S3 error responses in XML.

use axum::body::Body;
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::Response;

/// An S3 error with an XML body.
#[derive(Debug, Clone)]
pub struct S3Error {
    /// HTTP status code.
    pub status: StatusCode,
    /// S3 error code.
    pub code: String,
    /// Error message.
    pub message: String,
    /// Resource path.
    pub resource: Option<String>,
    /// Extra response headers.
    pub extra_headers: Vec<(String, String)>,
}

impl S3Error {
    /// Build an error.
    #[must_use]
    pub fn new(status: StatusCode, code: &str, message: &str) -> Self {
        Self {
            status,
            code: code.to_owned(),
            message: message.to_owned(),
            resource: None,
            extra_headers: Vec::new(),
        }
    }

    /// Add one response header to the error.
    #[must_use]
    pub fn with_header(mut self, name: &str, value: &str) -> Self {
        self.extra_headers.push((name.to_owned(), value.to_owned()));
        self
    }

    /// Attach the request resource.
    #[must_use]
    pub fn with_resource(mut self, resource: &str) -> Self {
        self.resource = Some(resource.to_owned());
        self
    }

    /// Anonymous access is not allowed.
    #[must_use]
    pub fn access_denied() -> Self {
        Self::new(
            StatusCode::FORBIDDEN,
            "AccessDenied",
            "Access denied. Use valid credentials.",
        )
    }

    /// The access key ID is not known.
    #[must_use]
    pub fn invalid_access_key() -> Self {
        Self::new(
            StatusCode::FORBIDDEN,
            "InvalidAccessKeyId",
            "The access key ID is not known.",
        )
    }

    /// The signature does not match.
    #[must_use]
    pub fn signature_mismatch() -> Self {
        Self::new(
            StatusCode::FORBIDDEN,
            "SignatureDoesNotMatch",
            "The request signature does not match.",
        )
    }

    /// The request time is too far from the server time.
    #[must_use]
    pub fn time_skewed() -> Self {
        Self::new(
            StatusCode::FORBIDDEN,
            "RequestTimeTooSkewed",
            "The request time is too far from the server time.",
        )
    }

    /// The presigned request has expired.
    #[must_use]
    pub fn expired() -> Self {
        Self::new(
            StatusCode::FORBIDDEN,
            "ExpiredToken",
            "The presigned request has expired.",
        )
    }

    /// The authorization header is malformed.
    #[must_use]
    pub fn malformed(message: &str) -> Self {
        Self::new(
            StatusCode::BAD_REQUEST,
            "AuthorizationHeaderMalformed",
            message,
        )
    }

    /// The request is not valid.
    #[must_use]
    pub fn invalid_request(message: &str) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "InvalidRequest", message)
    }

    /// The payload hash does not match.
    #[must_use]
    pub fn payload_mismatch() -> Self {
        Self::new(
            StatusCode::BAD_REQUEST,
            "XAmzContentSHA256Mismatch",
            "The payload hash does not match.",
        )
    }

    /// The operation is not supported.
    #[must_use]
    pub fn not_implemented(message: &str) -> Self {
        Self::new(StatusCode::NOT_IMPLEMENTED, "NotImplemented", message)
    }

    /// The upstream service is not available.
    #[must_use]
    pub fn service_unavailable() -> Self {
        Self::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "ServiceUnavailable",
            "The upstream S3 service is not available.",
        )
    }

    /// No leader or metadata quorum is available. The client can retry.
    #[must_use]
    pub fn no_leader() -> Self {
        Self::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "ServiceUnavailable",
            "No leader or metadata quorum is available. Retry the request.",
        )
        .with_header("Retry-After", "1")
    }

    /// The request is too large.
    #[must_use]
    pub fn entity_too_large() -> Self {
        Self::new(
            StatusCode::PAYLOAD_TOO_LARGE,
            "EntityTooLarge",
            "The request body is larger than the configured limit.",
        )
    }

    /// The object is too large for the cache.
    #[must_use]
    pub fn cache_object_too_large() -> Self {
        Self::new(
            StatusCode::BAD_REQUEST,
            "EntityTooLarge",
            "The object is larger than the cache object size limit.",
        )
    }

    /// The upstream did not respond in time.
    #[must_use]
    pub fn request_timeout() -> Self {
        Self::new(
            StatusCode::GATEWAY_TIMEOUT,
            "RequestTimeout",
            "The upstream did not respond in time.",
        )
    }

    /// Too many concurrent requests are active.
    #[must_use]
    pub fn slow_down() -> Self {
        Self::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "SlowDown",
            "Too many requests are active. Retry the request.",
        )
        .with_header("Retry-After", "1")
    }

    /// An internal error occurred.
    #[must_use]
    pub fn internal() -> Self {
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalError",
            "An internal error occurred.",
        )
    }

    /// Map a status code to a generic S3 error.
    #[must_use]
    pub fn from_status(status: StatusCode) -> Self {
        let (code, message) = match status {
            StatusCode::BAD_REQUEST => ("InvalidRequest", "The request is not valid."),
            StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => ("AccessDenied", "Access denied."),
            StatusCode::NOT_FOUND => ("NoSuchKey", "The key does not exist."),
            StatusCode::METHOD_NOT_ALLOWED => ("MethodNotAllowed", "The method is not allowed."),
            StatusCode::CONFLICT => ("Conflict", "The request conflicts with the bucket state."),
            StatusCode::PRECONDITION_FAILED => {
                ("PreconditionFailed", "A condition in the request failed.")
            }
            StatusCode::RANGE_NOT_SATISFIABLE => ("InvalidRange", "The range is not valid."),
            StatusCode::PAYLOAD_TOO_LARGE => (
                "EntityTooLarge",
                "The request body is larger than the limit.",
            ),
            StatusCode::TOO_MANY_REQUESTS => ("SlowDown", "Too many requests are active."),
            StatusCode::NOT_IMPLEMENTED => ("NotImplemented", "The operation is not supported."),
            StatusCode::SERVICE_UNAVAILABLE => {
                ("ServiceUnavailable", "The service is not available.")
            }
            StatusCode::GATEWAY_TIMEOUT => ("RequestTimeout", "The upstream did not respond."),
            status if status.is_server_error() => {
                ("InternalError", "The service reported an internal error.")
            }
            _ => ("InvalidRequest", "The request is not valid."),
        };
        Self::new(status, code, message)
    }

    /// Render the error as an XML response.
    #[must_use]
    pub fn into_response(self, request_id: &str) -> Response {
        let mut xml = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error>");
        push_element(&mut xml, "Code", &self.code);
        push_element(&mut xml, "Message", &self.message);
        if let Some(resource) = &self.resource {
            push_element(&mut xml, "Resource", resource);
        }
        push_element(&mut xml, "RequestId", request_id);
        xml.push_str("</Error>");

        let mut response = Response::new(Body::from(xml));
        *response.status_mut() = self.status;
        let headers = response.headers_mut();
        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/xml"),
        );
        if let Ok(value) = HeaderValue::from_str(request_id) {
            headers.insert("x-amz-request-id", value.clone());
            headers.insert("x-s3-sidecar-request-id", value);
        }
        for (name, value) in &self.extra_headers {
            if let (Ok(name), Ok(value)) = (
                axum::http::HeaderName::from_bytes(name.as_bytes()),
                HeaderValue::from_str(value),
            ) {
                headers.insert(name, value);
            }
        }
        response
    }
}

impl std::fmt::Display for S3Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for S3Error {}

fn push_element(xml: &mut String, name: &str, value: &str) {
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

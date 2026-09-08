//! Exact-Environment S3-compatible Product data-plane transport.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write as _,
    ops::Range,
    str::FromStr,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use axum::{
    Router,
    body::{Body, Bytes, to_bytes},
    extract::{DefaultBodyLimit, Path, Request, State},
    http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode, Uri, header},
    response::Response,
    routing::any,
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use hmac::{Hmac, KeyInit as _, Mac};
use runku_core::{EnvironmentScope, OperationId, RequestId};
use runku_file_storage::{FileObjectStore, FileStorageError, LogicalObjectPart};
use runku_object_storage::{
    AccessKeyId, AccessKeyMetadata, AccessKeyOperation, Bucket, BucketName, BucketPolicy,
    BucketState, CorsMethod, DeleteObjectCommand, MultipartPart, MultipartUpload,
    MultipartUploadId, MultipartUploadState, ObjectMetadata, ObjectPageRequest, ObjectStorageActor,
    ObjectStorageError, ObjectStorageService, ObjectVersionId, ObjectVersionPageRequest,
    PutObjectCommand,
};
use runku_runtime::CancellationToken;
use runku_value::TimestampMicros;
use sha2::{Digest as _, Sha256};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use ulid::Ulid;
use url::form_urlencoded;
use zeroize::{Zeroize, Zeroizing};

const MAX_HEADER_BYTES: usize = 16 * 1024;
const MAX_HEADER_VALUES: usize = 96;
const MAX_PRESIGN_SECONDS: i64 = 604_800;
const CLOCK_SKEW_MICROS: i64 = 900_000_000;
const MAX_MULTIPART_COMPLETION_BODY_BYTES: usize = 4 * 1024 * 1024;
const MAX_S3_OBJECT_BYTES: u64 = 5 * 1024 * 1024 * 1024 * 1024;
const OBJECT_STREAM_DEADLINE: Duration = Duration::from_hours(24);

/// Exact Product authority and physical byte adapter exposed through the S3 protocol.
#[derive(Clone)]
pub struct S3ProductConfig {
    /// Exact Project and Environment served by this listener.
    pub scope: EnvironmentScope,
    /// Shared logical bucket/object/access-key authority.
    pub service: ObjectStorageService,
    /// Shared content-addressed physical byte adapter.
    pub bytes: FileObjectStore,
    /// Logical customer-visible signing region.
    pub logical_region: String,
    /// Maximum bytes buffered and accepted by one PUT or `UploadPart` request.
    pub max_single_put_bytes: u64,
    /// Maximum bytes accepted for one completed object, including multipart composition.
    pub max_object_bytes: u64,
}

impl std::fmt::Debug for S3ProductConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("S3ProductConfig")
            .field("scope", &self.scope)
            .field("service", &self.service)
            .field("bytes", &self.bytes)
            .field("logical_region", &self.logical_region)
            .field("max_single_put_bytes", &self.max_single_put_bytes)
            .field("max_object_bytes", &self.max_object_bytes)
            .finish()
    }
}

impl S3ProductConfig {
    fn validate(&self) -> Result<(), ObjectStorageError> {
        if self.logical_region.is_empty()
            || self.logical_region.len() > 32
            || !self
                .logical_region
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
            || self.max_single_put_bytes == 0
            || self.max_single_put_bytes > 5 * 1024 * 1024 * 1024
            || self.max_object_bytes < self.max_single_put_bytes
            || self.max_object_bytes > MAX_S3_OBJECT_BYTES
        {
            Err(ObjectStorageError::InvalidInput)
        } else {
            Ok(())
        }
    }
}

#[derive(Clone)]
struct S3State(S3ProductConfig);

/// Builds the strict path-style S3 Product router for one exact Environment.
///
/// # Errors
///
/// Returns a stable validation error when the Product configuration is invalid.
pub fn build_s3_router(config: S3ProductConfig) -> Result<Router, ObjectStorageError> {
    config.validate()?;
    let state = S3State(config);
    Ok(Router::new()
        .route("/s3/{bucket}", any(s3_bucket))
        .route("/s3/{bucket}/", any(s3_bucket))
        .route("/s3/{bucket}/{*key}", any(s3_object))
        .layer(DefaultBodyLimit::disable())
        .with_state(state))
}

async fn s3_bucket(
    State(state): State<S3State>,
    Path(bucket): Path<String>,
    request: Request,
) -> Response {
    handle(state, bucket, None, request).await
}

async fn s3_object(
    State(state): State<S3State>,
    Path((bucket, key)): Path<(String, String)>,
    request: Request,
) -> Response {
    handle(state, bucket, Some(key), request).await
}

#[allow(clippy::too_many_lines)]
async fn handle(
    state: S3State,
    bucket_name: String,
    key: Option<String>,
    request: Request,
) -> Response {
    let request_id = RequestId::generate();
    if !valid_headers(request.headers()) {
        return s3_error(request_id, StatusCode::BAD_REQUEST, "InvalidRequest");
    }
    let Ok(bucket_name) = BucketName::from_str(&bucket_name) else {
        return s3_error(request_id, StatusCode::BAD_REQUEST, "InvalidBucketName");
    };
    let bucket = match state
        .0
        .service
        .get_bucket_by_name(state.0.scope, &bucket_name)
        .await
    {
        Ok(Some(bucket)) if bucket.state == BucketState::Active => bucket,
        Ok(_) => return s3_error(request_id, StatusCode::NOT_FOUND, "NoSuchBucket"),
        Err(error) => return storage_error(request_id, error),
    };
    if request.method() == Method::OPTIONS {
        return preflight(request_id, &bucket, request.headers());
    }

    let method = request.method().clone();
    let uri = request.uri().clone();
    let headers = request.headers().clone();
    let query = query_pairs(&uri);
    let multipart_requested = query.iter().any(|(name, _)| name == "uploadId");
    let maximum = if method == Method::PUT {
        usize::try_from(state.0.max_single_put_bytes).unwrap_or(usize::MAX)
    } else if method == Method::POST && multipart_requested {
        MAX_MULTIPART_COMPLETION_BODY_BYTES
    } else {
        0
    };
    let body = if matches!(method, Method::PUT | Method::POST) {
        match to_bytes(request.into_body(), maximum).await {
            Ok(bytes) => bytes,
            Err(_) => return s3_error(request_id, StatusCode::PAYLOAD_TOO_LARGE, "EntityTooLarge"),
        }
    } else {
        Bytes::new()
    };
    let now = now_micros();
    let authentication = match authenticate(&state.0, &method, &uri, &headers, &body, now).await {
        Ok(value) => value,
        Err(error) => return s3_error(request_id, error.status, error.code),
    };
    let key = key.as_deref();
    let operation = match (&method, key, headers.get("x-amz-copy-source")) {
        (&Method::GET, None, _) => AccessKeyOperation::List,
        (&Method::GET, Some(_), _) if multipart_requested => AccessKeyOperation::List,
        (&Method::GET | &Method::HEAD, Some(_), _) => AccessKeyOperation::Read,
        (&Method::POST | &Method::PUT, Some(_), _) => AccessKeyOperation::Write,
        (&Method::DELETE, Some(_), _) => AccessKeyOperation::Delete,
        _ => {
            return s3_error(
                request_id,
                StatusCode::METHOD_NOT_ALLOWED,
                "MethodNotAllowed",
            );
        }
    };
    let public_read = authentication.is_none()
        && bucket.configuration.policy == BucketPolicy::PublicRead
        && operation == AccessKeyOperation::Read;
    let auth = match authentication {
        Some(value)
            if value.metadata.bucket_id == bucket.id
                && value.metadata.configuration.operations.contains(&operation) =>
        {
            Some(value)
        }
        None if public_read => None,
        _ => return s3_error(request_id, StatusCode::FORBIDDEN, "AccessDenied"),
    };
    if let (Some(auth), Some(key)) = (&auth, key)
        && !key.starts_with(&auth.metadata.configuration.prefix)
    {
        return s3_error(request_id, StatusCode::FORBIDDEN, "AccessDenied");
    }
    if let Err(error) = state
        .0
        .service
        .apply_lifecycle(state.0.scope, bucket.id, now, 100)
        .await
    {
        return storage_error(request_id, error);
    }
    let response = match (method.clone(), key) {
        (Method::GET, None) if has_bare_query(&query, "versions") => {
            list_object_versions(request_id, &state.0, &bucket, auth.as_ref(), &uri).await
        }
        (Method::GET, None) if has_bare_query(&query, "uploads") => {
            list_multipart_uploads(request_id, &state.0, &bucket, auth.as_ref(), &uri).await
        }
        (Method::GET, None) => {
            list_objects(request_id, &state.0, &bucket, auth.as_ref(), &uri).await
        }
        (Method::GET, Some(key)) => {
            if multipart_requested {
                list_multipart_parts(request_id, &state.0, &bucket, key, &uri).await
            } else {
                get_object(request_id, &state.0, &bucket, key, false, &uri, &headers).await
            }
        }
        (Method::HEAD, Some(key)) => {
            get_object(request_id, &state.0, &bucket, key, true, &uri, &headers).await
        }
        (Method::POST, Some(key)) if has_bare_query(&query, "uploads") => {
            create_multipart_upload(
                request_id,
                &state.0,
                &bucket,
                key,
                auth.as_ref(),
                &headers,
                now,
            )
            .await
        }
        (Method::POST, Some(key)) if multipart_requested => {
            complete_multipart_upload(
                request_id,
                &state.0,
                &bucket,
                key,
                auth.as_ref(),
                &uri,
                &body,
                now,
            )
            .await
        }
        (Method::PUT, Some(key)) if multipart_requested => {
            put_multipart_part(request_id, &state.0, &bucket, key, &uri, &body, now).await
        }
        (Method::PUT, Some(key)) if headers.contains_key("x-amz-copy-source") => {
            copy_object(
                request_id,
                &state.0,
                &bucket,
                key,
                auth.as_ref(),
                &headers,
                now,
            )
            .await
        }
        (Method::PUT, Some(key)) => {
            put_object(
                request_id,
                &state.0,
                &bucket,
                key,
                auth.as_ref(),
                &headers,
                body,
                now,
            )
            .await
        }
        (Method::DELETE, Some(key)) => {
            if multipart_requested {
                abort_multipart_upload(request_id, &state.0, &bucket, key, &uri).await
            } else {
                delete_object(request_id, &state.0, &bucket, key, auth.as_ref(), &uri, now).await
            }
        }
        _ => s3_error(
            request_id,
            StatusCode::METHOD_NOT_ALLOWED,
            "MethodNotAllowed",
        ),
    };
    apply_cors(response, &bucket, &headers, &method)
}

#[derive(Debug)]
struct Authentication {
    metadata: AccessKeyMetadata,
    operation_id: OperationId,
}

#[derive(Clone, Copy)]
struct AuthFailure {
    status: StatusCode,
    code: &'static str,
}

async fn authenticate(
    config: &S3ProductConfig,
    method: &Method,
    uri: &Uri,
    headers: &HeaderMap,
    body: &[u8],
    now: TimestampMicros,
) -> Result<Option<Authentication>, AuthFailure> {
    let Some(parsed) = ParsedSignature::parse(uri, headers, now, &config.logical_region)? else {
        return Ok(None);
    };
    let actual_payload = sha256_hex(body);
    if parsed.payload_hash != "UNSIGNED-PAYLOAD" && parsed.payload_hash != actual_payload {
        return Err(AuthFailure {
            status: StatusCode::BAD_REQUEST,
            code: "XAmzContentSHA256Mismatch",
        });
    }
    let material = config
        .service
        .s3_access_key_material(config.scope, parsed.access_key_id, now)
        .await
        .map_err(|_| AuthFailure {
            status: StatusCode::SERVICE_UNAVAILABLE,
            code: "ServiceUnavailable",
        })?
        .ok_or(AuthFailure {
            status: StatusCode::FORBIDDEN,
            code: "InvalidAccessKeyId",
        })?;
    let canonical = parsed.canonical_request(method, uri, headers)?;
    let canonical_hash = sha256_hex(canonical.as_bytes());
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{}\n{}\n{}",
        parsed.amz_date, parsed.credential_scope, canonical_hash
    );
    let signature = decode_hex_32(&parsed.signature).ok_or(AuthFailure {
        status: StatusCode::BAD_REQUEST,
        code: "AuthorizationHeaderMalformed",
    })?;
    let valid = material.secrets.iter().any(|secret| {
        let mut signing_key = derive_signing_key(
            secret.expose(),
            &parsed.date,
            &parsed.region,
            &parsed.service,
        );
        let Ok(mut mac) = Hmac::<Sha256>::new_from_slice(&signing_key) else {
            signing_key.zeroize();
            return false;
        };
        mac.update(string_to_sign.as_bytes());
        let valid = mac.verify_slice(&signature).is_ok();
        signing_key.zeroize();
        valid
    });
    if !valid {
        return Err(AuthFailure {
            status: StatusCode::FORBIDDEN,
            code: "SignatureDoesNotMatch",
        });
    }
    let operation_id = signature_operation_id(method, uri, &signature, &actual_payload);
    Ok(Some(Authentication {
        metadata: material.metadata,
        operation_id,
    }))
}

struct ParsedSignature {
    access_key_id: AccessKeyId,
    amz_date: String,
    date: String,
    region: String,
    service: String,
    credential_scope: String,
    signed_headers: Vec<String>,
    signature: String,
    payload_hash: String,
    presigned: bool,
}

impl ParsedSignature {
    fn parse(
        uri: &Uri,
        headers: &HeaderMap,
        now: TimestampMicros,
        logical_region: &str,
    ) -> Result<Option<Self>, AuthFailure> {
        if let Some(value) = headers.get(header::AUTHORIZATION) {
            return Self::header(value, headers, now, logical_region).map(Some);
        }
        let query = query_pairs(uri);
        if query.iter().any(|(name, _)| name == "X-Amz-Algorithm") {
            return Self::presigned(&query, headers, now, logical_region).map(Some);
        }
        Ok(None)
    }

    fn header(
        authorization: &HeaderValue,
        headers: &HeaderMap,
        now: TimestampMicros,
        logical_region: &str,
    ) -> Result<Self, AuthFailure> {
        let authorization = authorization.to_str().map_err(|_| malformed_auth())?;
        let values = authorization
            .strip_prefix("AWS4-HMAC-SHA256 ")
            .ok_or_else(malformed_auth)?;
        let fields = comma_fields(values)?;
        let credential = one_field(&fields, "Credential")?;
        let signed_headers = one_field(&fields, "SignedHeaders")?;
        let signature = one_field(&fields, "Signature")?.to_owned();
        let amz_date = single_header(headers, "x-amz-date")?;
        let payload_hash = single_header(headers, "x-amz-content-sha256")?;
        Self::finish(
            credential,
            signed_headers,
            signature,
            amz_date,
            payload_hash,
            false,
            now,
            logical_region,
            None,
        )
    }

    fn presigned(
        query: &[(String, String)],
        headers: &HeaderMap,
        now: TimestampMicros,
        logical_region: &str,
    ) -> Result<Self, AuthFailure> {
        if one_query(query, "X-Amz-Algorithm")? != "AWS4-HMAC-SHA256" {
            return Err(malformed_auth());
        }
        let expires = one_query(query, "X-Amz-Expires")?
            .parse::<i64>()
            .map_err(|_| malformed_auth())?;
        if !(1..=MAX_PRESIGN_SECONDS).contains(&expires) {
            return Err(malformed_auth());
        }
        Self::finish(
            one_query(query, "X-Amz-Credential")?,
            one_query(query, "X-Amz-SignedHeaders")?,
            one_query(query, "X-Amz-Signature")?.to_owned(),
            one_query(query, "X-Amz-Date")?.to_owned(),
            headers
                .get("x-amz-content-sha256")
                .and_then(|value| value.to_str().ok())
                .unwrap_or("UNSIGNED-PAYLOAD")
                .to_owned(),
            true,
            now,
            logical_region,
            Some(expires),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn finish(
        credential: &str,
        signed_headers: &str,
        signature: String,
        amz_date: String,
        payload_hash: String,
        presigned: bool,
        now: TimestampMicros,
        logical_region: &str,
        expires: Option<i64>,
    ) -> Result<Self, AuthFailure> {
        let mut credential = credential.split('/');
        let access_key_id = credential
            .next()
            .ok_or_else(malformed_auth)?
            .parse::<AccessKeyId>()
            .map_err(|_| malformed_auth())?;
        let date = credential.next().ok_or_else(malformed_auth)?.to_owned();
        let region = credential.next().ok_or_else(malformed_auth)?.to_owned();
        let service = credential.next().ok_or_else(malformed_auth)?.to_owned();
        if credential.next() != Some("aws4_request")
            || credential.next().is_some()
            || region != logical_region
            || service != "s3"
            || signature.len() != 64
            || !signature.bytes().all(|byte| byte.is_ascii_hexdigit())
            || signature.bytes().any(|byte| byte.is_ascii_uppercase())
            || payload_hash != "UNSIGNED-PAYLOAD"
                && (payload_hash.len() != 64
                    || !payload_hash
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f')))
        {
            return Err(malformed_auth());
        }
        let signed_headers = parse_signed_headers(signed_headers)?;
        let signed_at = parse_amz_date(&amz_date).ok_or_else(malformed_auth)?;
        if date != amz_date[..8] {
            return Err(malformed_auth());
        }
        let earliest = signed_at.saturating_sub(CLOCK_SKEW_MICROS);
        let latest = signed_at.saturating_add(
            expires
                .unwrap_or(0)
                .saturating_mul(1_000_000)
                .saturating_add(CLOCK_SKEW_MICROS),
        );
        if now.get() < earliest || now.get() > latest {
            return Err(AuthFailure {
                status: StatusCode::FORBIDDEN,
                code: "RequestTimeTooSkewed",
            });
        }
        let credential_scope = format!("{date}/{logical_region}/s3/aws4_request");
        Ok(Self {
            access_key_id,
            amz_date,
            date,
            region,
            service,
            credential_scope,
            signed_headers,
            signature,
            payload_hash,
            presigned,
        })
    }

    fn canonical_request(
        &self,
        method: &Method,
        uri: &Uri,
        headers: &HeaderMap,
    ) -> Result<String, AuthFailure> {
        if headers.keys().any(|name| {
            semantic_signed_header(name.as_str())
                && !self
                    .signed_headers
                    .iter()
                    .any(|signed| signed == name.as_str())
        }) {
            return Err(malformed_auth());
        }
        let canonical_uri = canonical_uri(uri.path()).ok_or_else(malformed_auth)?;
        let canonical_query = canonical_query(uri, self.presigned);
        let canonical_headers = canonical_headers(headers, &self.signed_headers)?;
        Ok(format!(
            "{}\n{}\n{}\n{}\n{}\n{}",
            method.as_str(),
            canonical_uri,
            canonical_query,
            canonical_headers,
            self.signed_headers.join(";"),
            self.payload_hash,
        ))
    }
}

fn semantic_signed_header(name: &str) -> bool {
    matches!(
        name,
        "content-type"
            | "x-amz-content-sha256"
            | "x-amz-copy-source"
            | "x-amz-date"
            | "x-amz-metadata-directive"
            | "range"
            | "if-match"
            | "if-none-match"
            | "if-modified-since"
            | "if-unmodified-since"
            | "if-range"
    ) || name.starts_with("x-amz-meta-")
        || name.starts_with("x-amz-checksum-")
}

fn malformed_auth() -> AuthFailure {
    AuthFailure {
        status: StatusCode::BAD_REQUEST,
        code: "AuthorizationHeaderMalformed",
    }
}

fn comma_fields(value: &str) -> Result<BTreeMap<String, String>, AuthFailure> {
    let mut output = BTreeMap::new();
    for field in value.split(',') {
        let (name, value) = field.trim().split_once('=').ok_or_else(malformed_auth)?;
        if output.insert(name.to_owned(), value.to_owned()).is_some() {
            return Err(malformed_auth());
        }
    }
    Ok(output)
}

fn one_field<'a>(fields: &'a BTreeMap<String, String>, name: &str) -> Result<&'a str, AuthFailure> {
    fields
        .get(name)
        .map(String::as_str)
        .ok_or_else(malformed_auth)
}

fn single_header(headers: &HeaderMap, name: &'static str) -> Result<String, AuthFailure> {
    let mut values = headers.get_all(name).iter();
    let value = values
        .next()
        .ok_or_else(malformed_auth)?
        .to_str()
        .map_err(|_| malformed_auth())?;
    if values.next().is_some() {
        return Err(malformed_auth());
    }
    Ok(value.to_owned())
}

fn query_pairs(uri: &Uri) -> Vec<(String, String)> {
    form_urlencoded::parse(uri.query().unwrap_or_default().as_bytes())
        .map(|(name, value)| (name.into_owned(), value.into_owned()))
        .collect()
}

fn has_bare_query(pairs: &[(String, String)], name: &str) -> bool {
    pairs
        .iter()
        .any(|(candidate, value)| candidate == name && value.is_empty())
}

fn one_query<'a>(pairs: &'a [(String, String)], name: &str) -> Result<&'a str, AuthFailure> {
    let mut values = pairs.iter().filter(|(candidate, _)| candidate == name);
    let value = values.next().ok_or_else(malformed_auth)?;
    if values.next().is_some() {
        return Err(malformed_auth());
    }
    Ok(&value.1)
}

fn parse_signed_headers(value: &str) -> Result<Vec<String>, AuthFailure> {
    let values = value.split(';').map(str::to_owned).collect::<Vec<_>>();
    if values.is_empty()
        || !values.iter().any(|value| value == "host")
        || !values.windows(2).all(|pair| pair[0] < pair[1])
        || values.iter().any(|name| {
            name.is_empty()
                || !name
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        })
    {
        return Err(malformed_auth());
    }
    Ok(values)
}

fn canonical_headers(headers: &HeaderMap, names: &[String]) -> Result<String, AuthFailure> {
    let mut output = String::new();
    for name in names {
        let header_name = HeaderName::from_str(name).map_err(|_| malformed_auth())?;
        let values = headers.get_all(header_name).iter().collect::<Vec<_>>();
        if values.is_empty() {
            return Err(malformed_auth());
        }
        let mut canonical_values = Vec::with_capacity(values.len());
        for value in values {
            let value = value.to_str().map_err(|_| malformed_auth())?;
            canonical_values.push(collapse_spaces(value));
        }
        output.push_str(name);
        output.push(':');
        output.push_str(&canonical_values.join(","));
        output.push('\n');
    }
    Ok(output)
}

fn collapse_spaces(value: &str) -> String {
    value.split_ascii_whitespace().collect::<Vec<_>>().join(" ")
}

fn canonical_query(uri: &Uri, presigned: bool) -> String {
    let mut pairs = query_pairs(uri)
        .into_iter()
        .filter(|(name, _)| !(presigned && name == "X-Amz-Signature"))
        .map(|(name, value)| {
            (
                aws_encode(name.as_bytes(), false),
                aws_encode(value.as_bytes(), false),
            )
        })
        .collect::<Vec<_>>();
    pairs.sort();
    pairs
        .into_iter()
        .map(|(name, value)| format!("{name}={value}"))
        .collect::<Vec<_>>()
        .join("&")
}

fn canonical_uri(value: &str) -> Option<String> {
    let decoded = percent_decode(value)?;
    Some(aws_encode(&decoded, true))
}

fn percent_decode(value: &str) -> Option<Vec<u8>> {
    let bytes = value.as_bytes();
    let mut output = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            let high = hex_value(*bytes.get(index + 1)?)?;
            let low = hex_value(*bytes.get(index + 2)?)?;
            output.push((high << 4) | low);
            index += 3;
        } else {
            output.push(bytes[index]);
            index += 1;
        }
    }
    Some(output)
}

fn aws_encode(bytes: &[u8], preserve_slash: bool) -> String {
    let mut output = String::new();
    for byte in bytes {
        if byte.is_ascii_alphanumeric()
            || matches!(byte, b'-' | b'_' | b'.' | b'~')
            || preserve_slash && *byte == b'/'
        {
            output.push(char::from(*byte));
        } else {
            output.push('%');
            output.push(hex_digit(byte >> 4));
            output.push(hex_digit(byte & 0x0f));
        }
    }
    output
}

fn hex_digit(value: u8) -> char {
    char::from(if value < 10 {
        b'0' + value
    } else {
        b'A' + value - 10
    })
}

fn hex_value(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

fn derive_signing_key(secret: &str, date: &str, region: &str, service: &str) -> [u8; 32] {
    let mut root = Zeroizing::new(Vec::with_capacity(4 + secret.len()));
    root.extend_from_slice(b"AWS4");
    root.extend_from_slice(secret.as_bytes());
    let mut date_key = hmac_sha256(&root, date.as_bytes());
    let mut region_key = hmac_sha256(&date_key, region.as_bytes());
    date_key.zeroize();
    let mut service_key = hmac_sha256(&region_key, service.as_bytes());
    region_key.zeroize();
    let signing_key = hmac_sha256(&service_key, b"aws4_request");
    service_key.zeroize();
    signing_key
}

#[allow(clippy::expect_used)]
fn hmac_sha256(key: &[u8], value: &[u8]) -> [u8; 32] {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("SHA-256 accepts every HMAC key size");
    mac.update(value);
    mac.finalize().into_bytes().into()
}

fn signature_operation_id(
    method: &Method,
    uri: &Uri,
    signature: &[u8; 32],
    payload_hash: &str,
) -> OperationId {
    let mut hash = Sha256::new();
    hash.update(b"RUNKU_S3_OPERATION_V1\0");
    hash.update(method.as_str());
    hash.update(b"\0");
    hash.update(uri.path_and_query().map_or("", |value| value.as_str()));
    hash.update(b"\0");
    hash.update(signature);
    hash.update(payload_hash);
    let digest: [u8; 32] = hash.finalize().into();
    let mut id = [0_u8; 16];
    id.copy_from_slice(&digest[..16]);
    OperationId::from_ulid(Ulid::from(u128::from_be_bytes(id)))
}

fn multipart_completion_operation_id(
    upload_id: MultipartUploadId,
    completion_digest: &[u8; 32],
) -> OperationId {
    let mut hash = Sha256::new();
    hash.update(b"RUNKU_S3_MULTIPART_COMPLETION_OPERATION_V1\0");
    hash.update(upload_id.to_string());
    hash.update(b"\0");
    hash.update(completion_digest);
    let digest: [u8; 32] = hash.finalize().into();
    let mut id = [0_u8; 16];
    id.copy_from_slice(&digest[..16]);
    OperationId::from_ulid(Ulid::from(u128::from_be_bytes(id)))
}

#[allow(clippy::format_push_string)]
async fn list_objects(
    request_id: RequestId,
    config: &S3ProductConfig,
    bucket: &Bucket,
    auth: Option<&Authentication>,
    uri: &Uri,
) -> Response {
    let query = query_pairs(uri);
    if one_optional_query(&query, "list-type").is_some_and(|value| value != "2") {
        return s3_error(request_id, StatusCode::BAD_REQUEST, "InvalidArgument");
    }
    let prefix = one_optional_query(&query, "prefix").unwrap_or_default();
    if auth.is_some_and(|value| !prefix.starts_with(&value.metadata.configuration.prefix)) {
        return s3_error(request_id, StatusCode::FORBIDDEN, "AccessDenied");
    }
    let delimiter = match one_optional_query(&query, "delimiter") {
        None => None,
        Some("/") => Some('/'),
        Some(_) => return s3_error(request_id, StatusCode::BAD_REQUEST, "InvalidArgument"),
    };
    let after = one_optional_query(&query, "continuation-token").map(str::to_owned);
    let limit = match one_optional_query(&query, "max-keys") {
        None => 100,
        Some(value) => match value.parse::<u16>() {
            Ok(value) if (1..=100).contains(&value) => value,
            _ => return s3_error(request_id, StatusCode::BAD_REQUEST, "InvalidArgument"),
        },
    };
    let request = ObjectPageRequest {
        prefix: prefix.to_owned(),
        delimiter,
        after,
        limit,
    };
    let page = match config
        .service
        .list_objects(config.scope, bucket.id, &request)
        .await
    {
        Ok(value) => value,
        Err(error) => return storage_error(request_id, error),
    };
    let mut xml = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Name>{}</Name><Prefix>{}</Prefix><KeyCount>{}</KeyCount><MaxKeys>{}</MaxKeys><IsTruncated>{}</IsTruncated>",
        xml_escape(bucket.configuration.name.as_str()),
        xml_escape(prefix),
        page.objects.len() + page.common_prefixes.len(),
        limit,
        page.next.is_some(),
    );
    if let Some(delimiter) = delimiter {
        xml.push_str(&format!("<Delimiter>{delimiter}</Delimiter>"));
    }
    for object in &page.objects {
        xml.push_str(&format!(
            "<Contents><Key>{}</Key><LastModified>{}</LastModified><ETag>{}</ETag><Size>{}</Size><StorageClass>STANDARD</StorageClass></Contents>",
            xml_escape(&object.key),
            format_micros(object.created_at),
            xml_escape(&object.etag),
            object.size,
        ));
    }
    for prefix in &page.common_prefixes {
        xml.push_str(&format!(
            "<CommonPrefixes><Prefix>{}</Prefix></CommonPrefixes>",
            xml_escape(prefix)
        ));
    }
    if let Some(next) = &page.next {
        xml.push_str(&format!(
            "<NextContinuationToken>{}</NextContinuationToken>",
            xml_escape(next)
        ));
    }
    xml.push_str("</ListBucketResult>");
    xml_response(request_id, StatusCode::OK, xml)
}

async fn list_object_versions(
    request_id: RequestId,
    config: &S3ProductConfig,
    bucket: &Bucket,
    auth: Option<&Authentication>,
    uri: &Uri,
) -> Response {
    let query = query_pairs(uri);
    let prefix = one_optional_query(&query, "prefix").unwrap_or_default();
    if auth.is_some_and(|value| !prefix.starts_with(&value.metadata.configuration.prefix)) {
        return s3_error(request_id, StatusCode::FORBIDDEN, "AccessDenied");
    }
    let after_key = one_optional_query(&query, "key-marker").map(str::to_owned);
    let after_version = match one_optional_query(&query, "version-id-marker") {
        Some(value) => match value.parse() {
            Ok(value) => Some(value),
            Err(_) => return s3_error(request_id, StatusCode::BAD_REQUEST, "InvalidArgument"),
        },
        None => None,
    };
    let limit = match one_optional_query(&query, "max-keys") {
        None => 100,
        Some(value) => match value.parse::<u16>() {
            Ok(value) if (1..=100).contains(&value) => value,
            _ => return s3_error(request_id, StatusCode::BAD_REQUEST, "InvalidArgument"),
        },
    };
    let page = match config
        .service
        .list_object_versions(
            config.scope,
            bucket.id,
            &ObjectVersionPageRequest {
                prefix: prefix.to_owned(),
                after_key,
                after_version,
                limit,
            },
        )
        .await
    {
        Ok(value) => value,
        Err(error) => return storage_error(request_id, error),
    };
    let mut xml = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListVersionsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Name>{}</Name><Prefix>{}</Prefix><MaxKeys>{}</MaxKeys><IsTruncated>{}</IsTruncated>",
        xml_escape(bucket.configuration.name.as_str()),
        xml_escape(prefix),
        limit,
        page.next.is_some(),
    );
    for version in &page.versions {
        let latest = config
            .service
            .get_object(config.scope, bucket.id, &version.key)
            .await
            .ok()
            .flatten()
            .is_some_and(|current| current.version_id == version.version_id);
        let _ = write!(
            xml,
            "<Version><Key>{}</Key><VersionId>{}</VersionId><IsLatest>{}</IsLatest><LastModified>{}</LastModified><ETag>{}</ETag><Size>{}</Size><StorageClass>STANDARD</StorageClass></Version>",
            xml_escape(&version.key),
            version.version_id,
            latest,
            format_micros(version.created_at),
            xml_escape(&version.etag),
            version.size,
        );
    }
    if let Some((key, version)) = &page.next {
        let _ = write!(
            xml,
            "<NextKeyMarker>{}</NextKeyMarker><NextVersionIdMarker>{}</NextVersionIdMarker>",
            xml_escape(key),
            version
        );
    }
    xml.push_str("</ListVersionsResult>");
    xml_response(request_id, StatusCode::OK, xml)
}

async fn create_multipart_upload(
    request_id: RequestId,
    config: &S3ProductConfig,
    bucket: &Bucket,
    key: &str,
    auth: Option<&Authentication>,
    headers: &HeaderMap,
    now: TimestampMicros,
) -> Response {
    let Some(auth) = auth else {
        return s3_error(request_id, StatusCode::FORBIDDEN, "AccessDenied");
    };
    let Ok(metadata) = object_user_metadata(headers) else {
        return s3_error(request_id, StatusCode::BAD_REQUEST, "InvalidArgument");
    };
    let upload_id = MultipartUploadId::generate();
    let upload = MultipartUpload {
        scope: config.scope,
        bucket_id: bucket.id,
        upload_id,
        key: key.to_owned(),
        content_type: headers
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or("application/octet-stream")
            .to_owned(),
        metadata,
        actor: match format!("storage-key:{}", auth.metadata.id).parse() {
            Ok(value) => value,
            Err(error) => return storage_error(request_id, error),
        },
        state: MultipartUploadState::Active,
        created_at: now,
        completed_at: None,
        completed_version_id: None,
    };
    if let Err(error) = config.service.create_multipart_upload(&upload).await {
        return storage_error(request_id, error);
    }
    xml_response(
        request_id,
        StatusCode::OK,
        format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?><InitiateMultipartUploadResult><Bucket>{}</Bucket><Key>{}</Key><UploadId>{}</UploadId></InitiateMultipartUploadResult>",
            xml_escape(bucket.configuration.name.as_str()),
            xml_escape(key),
            upload_id,
        ),
    )
}

fn multipart_upload_id(uri: &Uri) -> Result<MultipartUploadId, ()> {
    unique_optional_query(&query_pairs(uri), "uploadId")?
        .ok_or(())?
        .parse()
        .map_err(|_| ())
}

async fn put_multipart_part(
    request_id: RequestId,
    config: &S3ProductConfig,
    bucket: &Bucket,
    key: &str,
    uri: &Uri,
    body: &Bytes,
    now: TimestampMicros,
) -> Response {
    let Ok(upload_id) = multipart_upload_id(uri) else {
        return s3_error(request_id, StatusCode::BAD_REQUEST, "InvalidArgument");
    };
    let part_number = match unique_optional_query(&query_pairs(uri), "partNumber") {
        Ok(Some(value)) => match value.parse::<u16>() {
            Ok(value) if (1..=10_000).contains(&value) => value,
            _ => return s3_error(request_id, StatusCode::BAD_REQUEST, "InvalidArgument"),
        },
        _ => return s3_error(request_id, StatusCode::BAD_REQUEST, "InvalidArgument"),
    };
    let upload = match config
        .service
        .get_multipart_upload(config.scope, bucket.id, upload_id)
        .await
    {
        Ok(Some(value)) if value.key == key && value.state == MultipartUploadState::Active => value,
        Ok(_) => return s3_error(request_id, StatusCode::NOT_FOUND, "NoSuchUpload"),
        Err(error) => return storage_error(request_id, error),
    };
    let digest: [u8; 32] = Sha256::digest(body).into();
    if let Err(error) = config
        .bytes
        .put_logical_object(
            config.scope,
            &bucket.id.to_string(),
            &hex_bytes(&digest),
            body.clone(),
            config.max_object_bytes,
        )
        .await
    {
        return file_error(request_id, error);
    }
    let part = MultipartPart {
        number: part_number,
        size: body.len() as u64,
        sha256: digest,
        etag: runku_object_storage::object_etag(&digest),
        created_at: now,
    };
    let _ = upload;
    if let Err(error) = config
        .service
        .put_multipart_part(config.scope, bucket.id, upload_id, &part)
        .await
    {
        return storage_error(request_id, error);
    }
    let mut response = empty_response(request_id, StatusCode::OK);
    if let Ok(value) = HeaderValue::from_str(&part.etag) {
        response.headers_mut().insert(header::ETAG, value);
    }
    response
}

async fn list_multipart_parts(
    request_id: RequestId,
    config: &S3ProductConfig,
    bucket: &Bucket,
    key: &str,
    uri: &Uri,
) -> Response {
    let Ok(upload_id) = multipart_upload_id(uri) else {
        return s3_error(request_id, StatusCode::BAD_REQUEST, "InvalidArgument");
    };
    let upload = match config
        .service
        .get_multipart_upload(config.scope, bucket.id, upload_id)
        .await
    {
        Ok(Some(value)) if value.key == key && value.state == MultipartUploadState::Active => value,
        Ok(_) => return s3_error(request_id, StatusCode::NOT_FOUND, "NoSuchUpload"),
        Err(error) => return storage_error(request_id, error),
    };
    let parts = match config
        .service
        .list_multipart_parts(config.scope, bucket.id, upload_id)
        .await
    {
        Ok(value) => value,
        Err(error) => return storage_error(request_id, error),
    };
    let mut xml = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListPartsResult><Bucket>{}</Bucket><Key>{}</Key><UploadId>{}</UploadId><IsTruncated>false</IsTruncated>",
        xml_escape(bucket.configuration.name.as_str()),
        xml_escape(&upload.key),
        upload_id
    );
    for part in parts {
        let _ = write!(
            xml,
            "<Part><PartNumber>{}</PartNumber><LastModified>{}</LastModified><ETag>{}</ETag><Size>{}</Size></Part>",
            part.number,
            format_micros(part.created_at),
            xml_escape(&part.etag),
            part.size
        );
    }
    xml.push_str("</ListPartsResult>");
    xml_response(request_id, StatusCode::OK, xml)
}

async fn list_multipart_uploads(
    request_id: RequestId,
    config: &S3ProductConfig,
    bucket: &Bucket,
    auth: Option<&Authentication>,
    uri: &Uri,
) -> Response {
    let query = query_pairs(uri);
    let prefix = one_optional_query(&query, "prefix").unwrap_or_default();
    if auth.is_some_and(|value| !prefix.starts_with(&value.metadata.configuration.prefix)) {
        return s3_error(request_id, StatusCode::FORBIDDEN, "AccessDenied");
    }
    let after = match (
        one_optional_query(&query, "key-marker"),
        one_optional_query(&query, "upload-id-marker"),
    ) {
        (None, None) => None,
        (Some(key), Some(id)) => match id.parse() {
            Ok(id) => Some((key, id)),
            Err(_) => return s3_error(request_id, StatusCode::BAD_REQUEST, "InvalidArgument"),
        },
        _ => return s3_error(request_id, StatusCode::BAD_REQUEST, "InvalidArgument"),
    };
    let limit = one_optional_query(&query, "max-uploads")
        .map_or(Ok(100), str::parse::<u16>)
        .ok()
        .filter(|value| (1..=100).contains(value));
    let Some(limit) = limit else {
        return s3_error(request_id, StatusCode::BAD_REQUEST, "InvalidArgument");
    };
    let page = match config
        .service
        .list_multipart_uploads(config.scope, bucket.id, prefix, after, limit)
        .await
    {
        Ok(value) => value,
        Err(error) => return storage_error(request_id, error),
    };
    let mut xml = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListMultipartUploadsResult><Bucket>{}</Bucket><Prefix>{}</Prefix><MaxUploads>{}</MaxUploads><IsTruncated>{}</IsTruncated>",
        xml_escape(bucket.configuration.name.as_str()),
        xml_escape(prefix),
        limit,
        page.next.is_some()
    );
    for upload in page.uploads {
        let _ = write!(
            xml,
            "<Upload><Key>{}</Key><UploadId>{}</UploadId><Initiated>{}</Initiated></Upload>",
            xml_escape(&upload.key),
            upload.upload_id,
            format_micros(upload.created_at)
        );
    }
    if let Some((key, id)) = page.next {
        let _ = write!(
            xml,
            "<NextKeyMarker>{}</NextKeyMarker><NextUploadIdMarker>{}</NextUploadIdMarker>",
            xml_escape(&key),
            id
        );
    }
    xml.push_str("</ListMultipartUploadsResult>");
    xml_response(request_id, StatusCode::OK, xml)
}

async fn get_object(
    request_id: RequestId,
    config: &S3ProductConfig,
    bucket: &Bucket,
    key: &str,
    head_only: bool,
    uri: &Uri,
    headers: &HeaderMap,
) -> Response {
    let Ok(version) = requested_version(uri) else {
        return s3_error(request_id, StatusCode::BAD_REQUEST, "InvalidArgument");
    };
    if version.is_some()
        && bucket.configuration.versioning != runku_object_storage::Versioning::Enabled
    {
        return s3_error(request_id, StatusCode::NOT_FOUND, "NoSuchVersion");
    }
    let loaded = if let Some(version_id) = version {
        config
            .service
            .get_object_version(config.scope, bucket.id, key, version_id)
            .await
    } else {
        config
            .service
            .get_object(config.scope, bucket.id, key)
            .await
    };
    let object = match loaded {
        Ok(Some(value)) => value,
        Ok(None) => {
            return s3_error(
                request_id,
                StatusCode::NOT_FOUND,
                if version.is_some() {
                    "NoSuchVersion"
                } else {
                    "NoSuchKey"
                },
            );
        }
        Err(error) => return storage_error(request_id, error),
    };
    if let Some(status) = conditional_status(headers, &object) {
        let mut response = empty_response(request_id, status);
        object_headers(response.headers_mut(), &object);
        return response;
    }
    let Ok(range) = requested_range(headers, object.size, &object) else {
        let mut response = s3_error(
            request_id,
            StatusCode::RANGE_NOT_SATISFIABLE,
            "InvalidRange",
        );
        if let Ok(value) = HeaderValue::from_str(&format!("bytes */{}", object.size)) {
            response.headers_mut().insert(header::CONTENT_RANGE, value);
        }
        return response;
    };
    let stream = if head_only {
        None
    } else {
        match config
            .bytes
            .get_logical_object_stream(
                config.scope,
                &bucket.id.to_string(),
                &hex_bytes(&object.sha256),
                object.size,
                range.clone(),
                config.max_object_bytes,
                Instant::now() + OBJECT_STREAM_DEADLINE,
                CancellationToken::new(),
            )
            .await
        {
            Ok(download) => Some(download.stream),
            Err(error) => return file_error(request_id, error),
        }
    };
    let mut response = Response::new(match stream {
        Some(stream) => Body::from_stream(stream),
        None => Body::empty(),
    });
    *response.status_mut() = if range.is_some() {
        StatusCode::PARTIAL_CONTENT
    } else {
        StatusCode::OK
    };
    object_headers(response.headers_mut(), &object);
    if let Some(range) = range {
        response.headers_mut().remove("x-amz-checksum-sha256");
        if let Ok(value) = HeaderValue::from_str(&(range.end - range.start).to_string()) {
            response.headers_mut().insert(header::CONTENT_LENGTH, value);
        }
        if let Ok(value) = HeaderValue::from_str(&format!(
            "bytes {}-{}/{}",
            range.start,
            range.end.saturating_sub(1),
            object.size
        )) {
            response.headers_mut().insert(header::CONTENT_RANGE, value);
        }
    }
    response_headers(response.headers_mut(), request_id);
    response
}

fn requested_version(uri: &Uri) -> Result<Option<ObjectVersionId>, ()> {
    let query = query_pairs(uri);
    let value = unique_optional_query(&query, "versionId")?;
    value.map(str::parse).transpose().map_err(|_| ())
}

fn conditional_status(headers: &HeaderMap, object: &ObjectMetadata) -> Option<StatusCode> {
    let if_match = headers
        .get(header::IF_MATCH)
        .and_then(|value| value.to_str().ok());
    if if_match.is_some_and(|value| !etag_condition(value, &object.etag, false)) {
        return Some(StatusCode::PRECONDITION_FAILED);
    }
    if if_match.is_none()
        && headers
            .get(header::IF_UNMODIFIED_SINCE)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| httpdate::parse_http_date(value).ok())
            .is_some_and(|at| object_system_time(object) > at)
    {
        return Some(StatusCode::PRECONDITION_FAILED);
    }
    let if_none_match = headers
        .get(header::IF_NONE_MATCH)
        .and_then(|value| value.to_str().ok());
    if if_none_match.is_some_and(|value| etag_condition(value, &object.etag, true)) {
        return Some(StatusCode::NOT_MODIFIED);
    }
    if if_none_match.is_none()
        && headers
            .get(header::IF_MODIFIED_SINCE)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| httpdate::parse_http_date(value).ok())
            .is_some_and(|at| object_system_time(object) <= at)
    {
        return Some(StatusCode::NOT_MODIFIED);
    }
    None
}

fn etag_condition(value: &str, etag: &str, weak: bool) -> bool {
    value.split(',').map(str::trim).any(|candidate| {
        candidate == "*" || candidate == etag || weak && candidate.strip_prefix("W/") == Some(etag)
    })
}

fn requested_range(
    headers: &HeaderMap,
    size: u64,
    object: &ObjectMetadata,
) -> Result<Option<Range<u64>>, ()> {
    let Some(value) = headers
        .get(header::RANGE)
        .and_then(|value| value.to_str().ok())
    else {
        return Ok(None);
    };
    if let Some(if_range) = headers
        .get(header::IF_RANGE)
        .and_then(|value| value.to_str().ok())
    {
        let matches = if if_range.starts_with('"') {
            if_range == object.etag
        } else {
            httpdate::parse_http_date(if_range).is_ok_and(|at| object_system_time(object) <= at)
        };
        if !matches {
            return Ok(None);
        }
    }
    let value = value.strip_prefix("bytes=").ok_or(())?;
    if value.contains(',') || size == 0 {
        return Err(());
    }
    let (start, end) = value.split_once('-').ok_or(())?;
    let (start, end_exclusive) = if start.is_empty() {
        let suffix = end.parse::<u64>().map_err(|_| ())?;
        if suffix == 0 {
            return Err(());
        }
        (size.saturating_sub(suffix), size)
    } else {
        let start = start.parse::<u64>().map_err(|_| ())?;
        if start >= size {
            return Err(());
        }
        let end = if end.is_empty() {
            size - 1
        } else {
            end.parse::<u64>().map_err(|_| ())?.min(size - 1)
        };
        if end < start {
            return Err(());
        }
        (start, end.checked_add(1).ok_or(())?)
    };
    Ok(Some(start..end_exclusive))
}

fn object_system_time(object: &ObjectMetadata) -> SystemTime {
    let micros = u64::try_from(object.created_at.get()).unwrap_or(0);
    UNIX_EPOCH + Duration::from_micros(micros)
}

#[allow(clippy::too_many_arguments)]
async fn put_object(
    request_id: RequestId,
    config: &S3ProductConfig,
    bucket: &Bucket,
    key: &str,
    auth: Option<&Authentication>,
    headers: &HeaderMap,
    body: Bytes,
    now: TimestampMicros,
) -> Response {
    let Some(auth) = auth else {
        return s3_error(request_id, StatusCode::FORBIDDEN, "AccessDenied");
    };
    let Ok(metadata) = object_user_metadata(headers) else {
        return s3_error(request_id, StatusCode::BAD_REQUEST, "InvalidArgument");
    };
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("application/octet-stream")
        .to_owned();
    write_object(
        request_id,
        config,
        bucket,
        key,
        auth,
        content_type,
        metadata,
        body,
        now,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn write_object(
    request_id: RequestId,
    config: &S3ProductConfig,
    bucket: &Bucket,
    key: &str,
    auth: &Authentication,
    content_type: String,
    metadata: BTreeMap<String, String>,
    body: Bytes,
    now: TimestampMicros,
) -> Response {
    let digest: [u8; 32] = Sha256::digest(&body).into();
    if let Err(error) = config
        .bytes
        .put_logical_object(
            config.scope,
            &bucket.id.to_string(),
            &sha256_hex(&body),
            body.clone(),
            config.max_object_bytes,
        )
        .await
    {
        return file_error(request_id, error);
    }
    commit_written_object(
        request_id,
        config,
        bucket,
        key,
        auth,
        content_type,
        metadata,
        body.len() as u64,
        digest,
        now,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn commit_written_object(
    request_id: RequestId,
    config: &S3ProductConfig,
    bucket: &Bucket,
    key: &str,
    auth: &Authentication,
    content_type: String,
    metadata: BTreeMap<String, String>,
    size: u64,
    digest: [u8; 32],
    now: TimestampMicros,
) -> Response {
    let actor = match format!("storage-key:{}", auth.metadata.id).parse::<ObjectStorageActor>() {
        Ok(value) => value,
        Err(error) => return storage_error(request_id, error),
    };
    let result = config
        .service
        .put_object(
            config.scope,
            bucket.id,
            auth.operation_id,
            &PutObjectCommand {
                version_id: ObjectVersionId::generate(),
                key: key.to_owned(),
                size,
                sha256: digest,
                content_type,
                metadata,
                actor,
                at: now,
            },
        )
        .await;
    let object = match result {
        Ok(value) => match value.object {
            Some(object) => object,
            None => match config
                .service
                .get_object(config.scope, bucket.id, key)
                .await
            {
                Ok(Some(object)) => object,
                _ => {
                    return s3_error(
                        request_id,
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "InternalError",
                    );
                }
            },
        },
        Err(error) => return storage_error(request_id, error),
    };
    let mut response = Response::new(Body::empty());
    *response.status_mut() = StatusCode::OK;
    object_headers(response.headers_mut(), &object);
    response_headers(response.headers_mut(), request_id);
    response
}

#[allow(clippy::too_many_lines)]
async fn copy_object(
    request_id: RequestId,
    config: &S3ProductConfig,
    bucket: &Bucket,
    destination: &str,
    auth: Option<&Authentication>,
    headers: &HeaderMap,
    now: TimestampMicros,
) -> Response {
    let Some(auth) = auth else {
        return s3_error(request_id, StatusCode::FORBIDDEN, "AccessDenied");
    };
    if !auth
        .metadata
        .configuration
        .operations
        .contains(&AccessKeyOperation::Read)
    {
        return s3_error(request_id, StatusCode::FORBIDDEN, "AccessDenied");
    }
    let Some(source) = headers
        .get("x-amz-copy-source")
        .and_then(|value| value.to_str().ok())
        .and_then(percent_decode)
        .and_then(|value| String::from_utf8(value).ok())
    else {
        return s3_error(request_id, StatusCode::BAD_REQUEST, "InvalidArgument");
    };
    let Some((source_bucket, source_key)) = source.trim_start_matches('/').split_once('/') else {
        return s3_error(request_id, StatusCode::BAD_REQUEST, "InvalidArgument");
    };
    if source_bucket != bucket.configuration.name.as_str()
        || !source_key.starts_with(&auth.metadata.configuration.prefix)
    {
        return s3_error(request_id, StatusCode::FORBIDDEN, "AccessDenied");
    }
    let source_object = match config
        .service
        .get_object(config.scope, bucket.id, source_key)
        .await
    {
        Ok(Some(value)) => value,
        Ok(None) => return s3_error(request_id, StatusCode::NOT_FOUND, "NoSuchKey"),
        Err(error) => return storage_error(request_id, error),
    };
    if let Err(error) = config
        .bytes
        .verify_logical_object(
            config.scope,
            &bucket.id.to_string(),
            &hex_bytes(&source_object.sha256),
            source_object.size,
            config.max_object_bytes,
        )
        .await
    {
        return file_error(request_id, error);
    }
    let replace = headers
        .get("x-amz-metadata-directive")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value == "REPLACE");
    let (content_type, metadata) = if replace {
        let Ok(metadata) = object_user_metadata(headers) else {
            return s3_error(request_id, StatusCode::BAD_REQUEST, "InvalidArgument");
        };
        let content_type = headers
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or("application/octet-stream")
            .to_owned();
        (content_type, metadata)
    } else {
        (
            source_object.content_type.clone(),
            source_object.metadata.clone(),
        )
    };
    let response = commit_written_object(
        request_id,
        config,
        bucket,
        destination,
        auth,
        content_type,
        metadata,
        source_object.size,
        source_object.sha256,
        now,
    )
    .await;
    if !response.status().is_success() {
        return response;
    }
    let Some(etag) = response
        .headers()
        .get(header::ETAG)
        .and_then(|value| value.to_str().ok())
    else {
        return s3_error(
            request_id,
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalError",
        );
    };
    xml_response(
        request_id,
        StatusCode::OK,
        format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?><CopyObjectResult><ETag>{}</ETag><LastModified>{}</LastModified></CopyObjectResult>",
            xml_escape(etag),
            format_micros(now),
        ),
    )
}

fn complete_part_list(body: &[u8]) -> Result<Vec<(u16, String)>, ()> {
    let text = std::str::from_utf8(body).map_err(|_| ())?;
    if !text.contains("<CompleteMultipartUpload") || !text.contains("</CompleteMultipartUpload>") {
        return Err(());
    }
    let mut parts = Vec::new();
    let mut rest = text;
    while let Some(start) = rest.find("<Part>") {
        rest = &rest[start + "<Part>".len()..];
        let end = rest.find("</Part>").ok_or(())?;
        let part = &rest[..end];
        let number = xml_text(part, "PartNumber")?
            .parse::<u16>()
            .map_err(|_| ())?;
        let etag = xml_text(part, "ETag")?;
        if !(1..=10_000).contains(&number)
            || !etag.starts_with('"')
            || !etag.ends_with('"')
            || etag.len() > 130
            || parts
                .last()
                .is_some_and(|(previous, _)| *previous >= number)
        {
            return Err(());
        }
        parts.push((number, etag.to_owned()));
        rest = &rest[end + "</Part>".len()..];
    }
    if parts.is_empty() || parts.len() > 10_000 {
        return Err(());
    }
    Ok(parts)
}

fn xml_text<'a>(value: &'a str, tag: &str) -> Result<&'a str, ()> {
    let start_tag = format!("<{tag}>");
    let end_tag = format!("</{tag}>");
    let start = value.find(&start_tag).ok_or(())? + start_tag.len();
    let end = value[start..].find(&end_tag).ok_or(())? + start;
    let text = &value[start..end];
    if text.is_empty() || text.contains(['<', '>', '&']) {
        return Err(());
    }
    Ok(text)
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn complete_multipart_upload(
    request_id: RequestId,
    config: &S3ProductConfig,
    bucket: &Bucket,
    key: &str,
    auth: Option<&Authentication>,
    uri: &Uri,
    body: &[u8],
    now: TimestampMicros,
) -> Response {
    let Some(auth) = auth else {
        return s3_error(request_id, StatusCode::FORBIDDEN, "AccessDenied");
    };
    let Ok(upload_id) = multipart_upload_id(uri) else {
        return s3_error(request_id, StatusCode::BAD_REQUEST, "InvalidArgument");
    };
    let upload = match config
        .service
        .get_multipart_upload(config.scope, bucket.id, upload_id)
        .await
    {
        Ok(Some(value)) if value.key == key => value,
        Ok(_) => return s3_error(request_id, StatusCode::NOT_FOUND, "NoSuchUpload"),
        Err(error) => return storage_error(request_id, error),
    };
    if let Some(version_id) = upload.completed_version_id {
        let Some(object) = config
            .service
            .get_object_version(config.scope, bucket.id, key, version_id)
            .await
            .ok()
            .flatten()
        else {
            return s3_error(
                request_id,
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalError",
            );
        };
        return complete_multipart_response(request_id, bucket, &object);
    }
    let Ok(requested) = complete_part_list(body) else {
        return s3_error(request_id, StatusCode::BAD_REQUEST, "MalformedXML");
    };
    let available = match config
        .service
        .list_multipart_parts(config.scope, bucket.id, upload_id)
        .await
    {
        Ok(value) => value,
        Err(error) => return storage_error(request_id, error),
    };
    for (index, (number, etag)) in requested.iter().enumerate() {
        let Some(part) = available.iter().find(|part| part.number == *number) else {
            return s3_error(request_id, StatusCode::BAD_REQUEST, "InvalidPart");
        };
        if &part.etag != etag {
            return s3_error(request_id, StatusCode::BAD_REQUEST, "InvalidPart");
        }
        if index + 1 != requested.len() && part.size < 5 * 1024 * 1024 {
            return s3_error(request_id, StatusCode::BAD_REQUEST, "EntityTooSmall");
        }
    }
    let mut total_size = 0_u64;
    let mut composition_parts = Vec::with_capacity(requested.len());
    for (number, etag) in &requested {
        let Some(part) = available.iter().find(|part| part.number == *number) else {
            return s3_error(request_id, StatusCode::BAD_REQUEST, "InvalidPart");
        };
        if &part.etag != etag {
            return s3_error(request_id, StatusCode::BAD_REQUEST, "InvalidPart");
        }
        total_size = match total_size.checked_add(part.size) {
            Some(value)
                if value <= bucket.configuration.quota.max_object_bytes
                    && value <= config.max_object_bytes =>
            {
                value
            }
            _ => return s3_error(request_id, StatusCode::PAYLOAD_TOO_LARGE, "EntityTooLarge"),
        };
        composition_parts.push(LogicalObjectPart {
            digest_hex: hex_bytes(&part.sha256),
            size: part.size,
        });
    }
    let completion_digest: [u8; 32] = Sha256::digest(body).into();
    if let Err(error) = config
        .service
        .claim_multipart_completion(config.scope, bucket.id, upload_id, completion_digest)
        .await
    {
        return match error {
            ObjectStorageError::Conflict => {
                s3_error(request_id, StatusCode::CONFLICT, "InvalidRequest")
            }
            _ => storage_error(request_id, error),
        };
    }
    let composed = match config
        .bytes
        .compose_logical_object(
            config.scope,
            &bucket.id.to_string(),
            &hex_bytes(&completion_digest),
            &composition_parts,
            total_size,
            Instant::now() + OBJECT_STREAM_DEADLINE,
            CancellationToken::new(),
        )
        .await
    {
        Ok(value) => value,
        Err(error) => return file_error(request_id, error),
    };
    let completion_auth = Authentication {
        metadata: auth.metadata.clone(),
        operation_id: multipart_completion_operation_id(upload_id, &completion_digest),
    };
    let response = commit_written_object(
        request_id,
        config,
        bucket,
        key,
        &completion_auth,
        upload.content_type,
        upload.metadata,
        composed.size,
        composed.sha256,
        now,
    )
    .await;
    if !response.status().is_success() {
        return response;
    }
    let version_id = response
        .headers()
        .get("x-amz-version-id")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse().ok());
    let Some(version_id) = version_id else {
        return s3_error(
            request_id,
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalError",
        );
    };
    if let Err(error) = config
        .service
        .complete_multipart_upload(config.scope, bucket.id, upload_id, version_id, now)
        .await
    {
        return storage_error(request_id, error);
    }
    let object = match config
        .service
        .get_object_version(config.scope, bucket.id, key, version_id)
        .await
    {
        Ok(Some(value)) => value,
        Ok(None) => {
            return s3_error(
                request_id,
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalError",
            );
        }
        Err(error) => return storage_error(request_id, error),
    };
    complete_multipart_response(request_id, bucket, &object)
}

fn complete_multipart_response(
    request_id: RequestId,
    bucket: &Bucket,
    object: &ObjectMetadata,
) -> Response {
    let mut response = xml_response(
        request_id,
        StatusCode::OK,
        format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?><CompleteMultipartUploadResult><Bucket>{}</Bucket><Key>{}</Key><ETag>{}</ETag></CompleteMultipartUploadResult>",
            xml_escape(bucket.configuration.name.as_str()),
            xml_escape(&object.key),
            xml_escape(&object.etag),
        ),
    );
    if let Ok(value) = HeaderValue::from_str(&object.etag) {
        response.headers_mut().insert(header::ETAG, value);
    }
    if let Ok(value) = HeaderValue::from_str(&object.version_id.to_string()) {
        response.headers_mut().insert("x-amz-version-id", value);
    }
    response
}

async fn abort_multipart_upload(
    request_id: RequestId,
    config: &S3ProductConfig,
    bucket: &Bucket,
    key: &str,
    uri: &Uri,
) -> Response {
    let Ok(upload_id) = multipart_upload_id(uri) else {
        return s3_error(request_id, StatusCode::BAD_REQUEST, "InvalidArgument");
    };
    match config
        .service
        .get_multipart_upload(config.scope, bucket.id, upload_id)
        .await
    {
        Ok(Some(value)) if value.key != key => {
            return s3_error(request_id, StatusCode::NOT_FOUND, "NoSuchUpload");
        }
        Ok(_) => {}
        Err(error) => return storage_error(request_id, error),
    }
    match config
        .service
        .abort_multipart_upload(config.scope, bucket.id, upload_id)
        .await
    {
        Ok(()) => empty_response(request_id, StatusCode::NO_CONTENT),
        Err(ObjectStorageError::Conflict) => {
            s3_error(request_id, StatusCode::CONFLICT, "InvalidRequest")
        }
        Err(error) => storage_error(request_id, error),
    }
}

async fn delete_object(
    request_id: RequestId,
    config: &S3ProductConfig,
    bucket: &Bucket,
    key: &str,
    auth: Option<&Authentication>,
    uri: &Uri,
    now: TimestampMicros,
) -> Response {
    let Some(auth) = auth else {
        return s3_error(request_id, StatusCode::FORBIDDEN, "AccessDenied");
    };
    let Ok(version) = requested_version(uri) else {
        return s3_error(request_id, StatusCode::BAD_REQUEST, "InvalidArgument");
    };
    if let Some(version_id) = version {
        return match config
            .service
            .delete_object_version(config.scope, bucket.id, key, version_id)
            .await
        {
            Ok(true) => {
                let mut response = empty_response(request_id, StatusCode::NO_CONTENT);
                if let Ok(value) = HeaderValue::from_str(&version_id.to_string()) {
                    response.headers_mut().insert("x-amz-version-id", value);
                }
                response
            }
            Ok(false) => empty_response(request_id, StatusCode::NO_CONTENT),
            Err(error) => storage_error(request_id, error),
        };
    }
    let current = match config
        .service
        .get_object(config.scope, bucket.id, key)
        .await
    {
        Ok(Some(value)) => value,
        Ok(None) => return empty_response(request_id, StatusCode::NO_CONTENT),
        Err(error) => return storage_error(request_id, error),
    };
    let actor = match format!("storage-key:{}", auth.metadata.id).parse::<ObjectStorageActor>() {
        Ok(value) => value,
        Err(error) => return storage_error(request_id, error),
    };
    match config
        .service
        .delete_object(
            config.scope,
            bucket.id,
            auth.operation_id,
            &DeleteObjectCommand {
                key: key.to_owned(),
                expected_version_id: current.version_id,
                actor,
                at: now,
            },
        )
        .await
    {
        Ok(_) => {
            let mut response = empty_response(request_id, StatusCode::NO_CONTENT);
            if let Ok(value) = HeaderValue::from_str(&current.version_id.to_string()) {
                response.headers_mut().insert("x-amz-version-id", value);
            }
            response
        }
        Err(error) => storage_error(request_id, error),
    }
}

fn one_optional_query<'a>(pairs: &'a [(String, String)], name: &str) -> Option<&'a str> {
    let mut values = pairs.iter().filter(|(candidate, _)| candidate == name);
    let value = values.next()?;
    if values.next().is_some() {
        return None;
    }
    Some(&value.1)
}

fn unique_optional_query<'a>(
    pairs: &'a [(String, String)],
    name: &str,
) -> Result<Option<&'a str>, ()> {
    let mut values = pairs.iter().filter(|(candidate, _)| candidate == name);
    let value = values.next().map(|value| value.1.as_str());
    if values.next().is_some() {
        return Err(());
    }
    Ok(value)
}

fn object_user_metadata(headers: &HeaderMap) -> Result<BTreeMap<String, String>, ()> {
    let mut metadata = BTreeMap::new();
    for (name, value) in headers {
        let Some(name) = name.as_str().strip_prefix("x-amz-meta-") else {
            continue;
        };
        let value = value.to_str().map_err(|_| ())?;
        if metadata.insert(name.to_owned(), value.to_owned()).is_some() || metadata.len() > 32 {
            return Err(());
        }
    }
    Ok(metadata)
}

fn object_headers(headers: &mut HeaderMap, object: &ObjectMetadata) {
    headers.insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    if let Ok(value) = HeaderValue::from_str(&object.content_type) {
        headers.insert(header::CONTENT_TYPE, value);
    }
    if let Ok(value) = HeaderValue::from_str(&object.size.to_string()) {
        headers.insert(header::CONTENT_LENGTH, value);
    }
    if let Ok(value) = HeaderValue::from_str(&object.etag) {
        headers.insert(header::ETAG, value);
    }
    if let Ok(value) = HeaderValue::from_str(&object.version_id.to_string()) {
        headers.insert("x-amz-version-id", value);
    }
    if let Ok(value) = HeaderValue::from_str(&httpdate::fmt_http_date(object_system_time(object))) {
        headers.insert(header::LAST_MODIFIED, value);
    }
    if let Ok(value) = HeaderValue::from_str(&STANDARD.encode(object.sha256)) {
        headers.insert("x-amz-checksum-sha256", value);
    }
    for (name, value) in &object.metadata {
        if let (Ok(name), Ok(value)) = (
            HeaderName::from_str(&format!("x-amz-meta-{name}")),
            HeaderValue::from_str(value),
        ) {
            headers.insert(name, value);
        }
    }
}

fn valid_headers(headers: &HeaderMap) -> bool {
    headers.len() <= MAX_HEADER_VALUES
        && headers
            .iter()
            .try_fold(0_usize, |total, (name, value)| {
                total
                    .checked_add(name.as_str().len())?
                    .checked_add(value.as_bytes().len())
            })
            .is_some_and(|total| total <= MAX_HEADER_BYTES)
}

fn preflight(request_id: RequestId, bucket: &Bucket, headers: &HeaderMap) -> Response {
    let Some(origin) = headers
        .get(header::ORIGIN)
        .and_then(|value| value.to_str().ok())
    else {
        return s3_error(request_id, StatusCode::BAD_REQUEST, "InvalidRequest");
    };
    let Some(method) = headers
        .get("access-control-request-method")
        .and_then(|value| value.to_str().ok())
        .and_then(cors_method)
    else {
        return s3_error(request_id, StatusCode::FORBIDDEN, "AccessDenied");
    };
    let requested = headers
        .get("access-control-request-headers")
        .and_then(|value| value.to_str().ok())
        .map(|value| {
            value
                .split(',')
                .map(|header| header.trim().to_ascii_lowercase())
                .collect::<BTreeSet<_>>()
        })
        .unwrap_or_default();
    let Some(rule) = bucket.configuration.cors.iter().find(|rule| {
        (rule.origins.iter().any(|value| value == "*")
            || rule.origins.iter().any(|value| value == origin))
            && rule.methods.contains(&method)
            && requested.iter().all(|header| {
                rule.allowed_headers.iter().any(|value| value == "*")
                    || rule.allowed_headers.iter().any(|value| value == header)
            })
    }) else {
        return s3_error(request_id, StatusCode::FORBIDDEN, "AccessDenied");
    };
    let mut response = empty_response(request_id, StatusCode::NO_CONTENT);
    cors_headers(response.headers_mut(), rule, origin);
    response.headers_mut().insert(
        header::ACCESS_CONTROL_ALLOW_METHODS,
        HeaderValue::from_static("GET, HEAD, PUT, DELETE, OPTIONS"),
    );
    if !requested.is_empty()
        && let Ok(value) =
            HeaderValue::from_str(&requested.into_iter().collect::<Vec<_>>().join(", "))
    {
        response
            .headers_mut()
            .insert(header::ACCESS_CONTROL_ALLOW_HEADERS, value);
    }
    response
}

fn apply_cors(
    mut response: Response,
    bucket: &Bucket,
    request_headers: &HeaderMap,
    method: &Method,
) -> Response {
    let Some(origin) = request_headers
        .get(header::ORIGIN)
        .and_then(|value| value.to_str().ok())
    else {
        return response;
    };
    let method = match *method {
        Method::GET => CorsMethod::Get,
        Method::HEAD => CorsMethod::Head,
        Method::PUT => CorsMethod::Put,
        Method::POST => CorsMethod::Post,
        Method::DELETE => CorsMethod::Delete,
        _ => return response,
    };
    if let Some(rule) = bucket.configuration.cors.iter().find(|rule| {
        (rule.origins.iter().any(|value| value == "*")
            || rule.origins.iter().any(|value| value == origin))
            && rule.methods.contains(&method)
    }) {
        cors_headers(response.headers_mut(), rule, origin);
    }
    response
}

fn cors_headers(headers: &mut HeaderMap, rule: &runku_object_storage::CorsRule, origin: &str) {
    let allowed_origin = if rule.origins.iter().any(|value| value == "*") {
        "*"
    } else {
        origin
    };
    if let Ok(value) = HeaderValue::from_str(allowed_origin) {
        headers.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, value);
    }
    if let Ok(value) = HeaderValue::from_str(&rule.exposed_headers.join(", "))
        && !rule.exposed_headers.is_empty()
    {
        headers.insert(header::ACCESS_CONTROL_EXPOSE_HEADERS, value);
    }
    if let Ok(value) = HeaderValue::from_str(&rule.max_age_seconds.to_string()) {
        headers.insert(header::ACCESS_CONTROL_MAX_AGE, value);
    }
    headers.append(header::VARY, HeaderValue::from_static("Origin"));
}

fn cors_method(value: &str) -> Option<CorsMethod> {
    match value {
        "GET" => Some(CorsMethod::Get),
        "HEAD" => Some(CorsMethod::Head),
        "PUT" => Some(CorsMethod::Put),
        "POST" => Some(CorsMethod::Post),
        "DELETE" => Some(CorsMethod::Delete),
        _ => None,
    }
}

fn response_headers(headers: &mut HeaderMap, request_id: RequestId) {
    if let Ok(value) = HeaderValue::from_str(&request_id.to_string()) {
        headers.insert("x-amz-request-id", value.clone());
        headers.insert("x-runku-request-id", value);
    }
    headers.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("private, no-store"),
    );
    headers.insert(
        "x-content-type-options",
        HeaderValue::from_static("nosniff"),
    );
}

fn empty_response(request_id: RequestId, status: StatusCode) -> Response {
    let mut response = Response::new(Body::empty());
    *response.status_mut() = status;
    response_headers(response.headers_mut(), request_id);
    response
}

fn xml_response(request_id: RequestId, status: StatusCode, body: String) -> Response {
    let mut response = Response::new(Body::from(body));
    *response.status_mut() = status;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/xml"),
    );
    response_headers(response.headers_mut(), request_id);
    response
}

fn s3_error(request_id: RequestId, status: StatusCode, code: &'static str) -> Response {
    xml_response(
        request_id,
        status,
        format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>{code}</Code><Message>Request rejected</Message><RequestId>{request_id}</RequestId></Error>"
        ),
    )
}

fn storage_error(request_id: RequestId, error: ObjectStorageError) -> Response {
    match error {
        ObjectStorageError::InvalidInput => {
            s3_error(request_id, StatusCode::BAD_REQUEST, "InvalidArgument")
        }
        ObjectStorageError::LimitExceeded => {
            s3_error(request_id, StatusCode::PAYLOAD_TOO_LARGE, "EntityTooLarge")
        }
        ObjectStorageError::NotFound => s3_error(request_id, StatusCode::NOT_FOUND, "NoSuchKey"),
        ObjectStorageError::Conflict | ObjectStorageError::OperationIdReused => {
            s3_error(request_id, StatusCode::CONFLICT, "OperationAborted")
        }
        ObjectStorageError::Busy
        | ObjectStorageError::Unavailable
        | ObjectStorageError::ResultUncertain => s3_error(
            request_id,
            StatusCode::SERVICE_UNAVAILABLE,
            "ServiceUnavailable",
        ),
        ObjectStorageError::Corruption
        | ObjectStorageError::Unsupported
        | ObjectStorageError::ProductionBackendUnsupported
        | ObjectStorageError::Internal => s3_error(
            request_id,
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalError",
        ),
    }
}

fn file_error(request_id: RequestId, error: FileStorageError) -> Response {
    match error {
        FileStorageError::LimitExceeded => {
            s3_error(request_id, StatusCode::PAYLOAD_TOO_LARGE, "EntityTooLarge")
        }
        FileStorageError::InvalidRequest => {
            s3_error(request_id, StatusCode::BAD_REQUEST, "InvalidArgument")
        }
        FileStorageError::Conflict => {
            s3_error(request_id, StatusCode::CONFLICT, "OperationAborted")
        }
        FileStorageError::Corruption => s3_error(
            request_id,
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalError",
        ),
        _ => s3_error(
            request_id,
            StatusCode::SERVICE_UNAVAILABLE,
            "ServiceUnavailable",
        ),
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    hex_bytes(&digest)
}

fn hex_bytes(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(64);
    for byte in bytes {
        output.push(hex_digit(byte >> 4).to_ascii_lowercase());
        output.push(hex_digit(byte & 0x0f).to_ascii_lowercase());
    }
    output
}

fn decode_hex_32(value: &str) -> Option<[u8; 32]> {
    if value.len() != 64 {
        return None;
    }
    let mut output = [0_u8; 32];
    for (index, chunk) in value.as_bytes().as_chunks::<2>().0.iter().enumerate() {
        output[index] = (hex_value(chunk[0])? << 4) | hex_value(chunk[1])?;
    }
    Some(output)
}

fn parse_amz_date(value: &str) -> Option<i64> {
    if value.len() != 16 || &value[8..9] != "T" || &value[15..] != "Z" {
        return None;
    }
    let year = value[0..4].parse::<i32>().ok()?;
    let month = value[4..6].parse::<u8>().ok()?;
    let day = value[6..8].parse::<u8>().ok()?;
    let hour = value[9..11].parse::<u8>().ok()?;
    let minute = value[11..13].parse::<u8>().ok()?;
    let second = value[13..15].parse::<u8>().ok()?;
    let month = time::Month::try_from(month).ok()?;
    let date = time::Date::from_calendar_date(year, month, day).ok()?;
    let time = time::Time::from_hms(hour, minute, second).ok()?;
    let seconds = date.with_time(time).assume_utc().unix_timestamp();
    seconds.checked_mul(1_000_000)
}

fn now_micros() -> TimestampMicros {
    let micros = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|value| i64::try_from(value.as_micros()).ok())
        .unwrap_or(i64::MAX);
    TimestampMicros::new(micros)
}

fn format_micros(value: TimestampMicros) -> String {
    OffsetDateTime::from_unix_timestamp_nanos(i128::from(value.get()) * 1_000)
        .ok()
        .and_then(|value| value.format(&Rfc3339).ok())
        .unwrap_or_else(|| "1970-01-01T00:00:00Z".to_owned())
}

fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, error::Error, process::Command, sync::Arc};

    use axum::{
        body::{Body, to_bytes},
        http::{HeaderMap, HeaderName, HeaderValue, Method, Request, StatusCode},
    };
    use runku_core::{EnvironmentId, EnvironmentScope, OperationId, ProjectId};
    use runku_file_storage::FileObjectStore;
    use runku_object_storage::{
        AccessKeyConfiguration, AccessKeyOperation, BucketConfiguration, BucketLifecycle,
        BucketPolicy, BucketQuota, CorsMethod, CorsRule, ObjectStorageActor, ObjectStorageService,
        SecretDigestKey, Versioning,
    };
    use runku_object_storage_repository::{
        ObjectStorageRepositoryConfig, SqlObjectStorageRepository,
    };
    use runku_value::TimestampMicros;
    use tempfile::tempdir;
    use time::OffsetDateTime;
    use tower::ServiceExt as _;

    use super::{
        S3ProductConfig, aws_encode, build_s3_router, canonical_headers, canonical_query,
        canonical_uri, derive_signing_key, hmac_sha256, multipart_completion_operation_id,
        parse_amz_date, sha256_hex,
    };

    #[test]
    fn canonical_encoding_and_dates_match_sigv4_examples() {
        assert_eq!(aws_encode(b"a b/+", false), "a%20b%2F%2B");
        assert_eq!(
            canonical_uri("/s3/media/a%20b/+"),
            Some("/s3/media/a%20b/%2B".to_owned())
        );
        assert_eq!(parse_amz_date("19700101T000000Z"), Some(0));
        assert_eq!(
            sha256_hex(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert!(parse_amz_date("20260230T000000Z").is_none());
    }

    #[test]
    fn multipart_completion_identity_binds_upload_and_exact_body() {
        let upload = runku_object_storage::MultipartUploadId::generate();
        assert_eq!(
            multipart_completion_operation_id(upload, &[1; 32]),
            multipart_completion_operation_id(upload, &[1; 32])
        );
        assert_ne!(
            multipart_completion_operation_id(upload, &[1; 32]),
            multipart_completion_operation_id(upload, &[2; 32])
        );
        assert_ne!(
            multipart_completion_operation_id(upload, &[1; 32]),
            multipart_completion_operation_id(
                runku_object_storage::MultipartUploadId::generate(),
                &[1; 32],
            )
        );
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn s3_and_admin_share_exact_object_authority() -> Result<(), Box<dyn Error>> {
        let directory = tempdir()?;
        let database_url = format!(
            "sqlite://{}?mode=rwc",
            directory.path().join("storage.sqlite3").display()
        );
        let repository = SqlObjectStorageRepository::connect_sqlite(
            &database_url,
            ObjectStorageRepositoryConfig::LOCAL,
        )
        .await?;
        let scope = EnvironmentScope::new(ProjectId::generate(), EnvironmentId::generate());
        let service =
            ObjectStorageService::new(Arc::new(repository), SecretDigestKey::new([31; 32]));
        let now = super::now_micros();
        let actor: ObjectStorageActor = "operator:s3-test".parse()?;
        let bucket = service
            .create_bucket(
                scope,
                OperationId::generate(),
                BucketConfiguration {
                    name: "media".parse()?,
                    policy: BucketPolicy::PublicRead,
                    cors: vec![CorsRule {
                        origins: vec!["https://console.example".to_owned()],
                        methods: BTreeSet::from([
                            CorsMethod::Get,
                            CorsMethod::Head,
                            CorsMethod::Put,
                            CorsMethod::Delete,
                        ]),
                        allowed_headers: vec!["*".to_owned()],
                        exposed_headers: vec!["etag".to_owned(), "x-amz-version-id".to_owned()],
                        max_age_seconds: 300,
                    }],
                    versioning: Versioning::Enabled,
                    lifecycle: BucketLifecycle {
                        expire_current_after_days: None,
                        expire_noncurrent_after_days: None,
                        abort_incomplete_after_days: None,
                    },
                    quota: BucketQuota {
                        max_object_bytes: 8 * 1_048_576,
                        max_total_bytes: 32 * 1_048_576,
                        max_objects: 10,
                    },
                },
                actor.clone(),
                TimestampMicros::new(now.get() - 2_000_000),
            )
            .await
            .map_err(|error| format!("bucket create failed: {error:?}"))?
            .operation
            .bucket_id;
        let issued = service
            .issue_access_key(
                scope,
                bucket,
                OperationId::generate(),
                AccessKeyConfiguration {
                    label: "sdk".to_owned(),
                    prefix: "uploads/".to_owned(),
                    operations: BTreeSet::from([
                        AccessKeyOperation::List,
                        AccessKeyOperation::Read,
                        AccessKeyOperation::Write,
                        AccessKeyOperation::Delete,
                    ]),
                },
                actor,
                TimestampMicros::new(now.get() - 1_000_000),
            )
            .await
            .map_err(|error| format!("key issue failed: {error:?}"))?;
        let secret = issued.secret.ok_or("issued secret missing")?;
        let secret = secret
            .expose()
            .rsplit_once('.')
            .ok_or("malformed secret")?
            .1
            .to_owned();
        let router = build_s3_router(S3ProductConfig {
            scope,
            service: service.clone(),
            bytes: FileObjectStore::filesystem(&directory.path().join("objects")).await?,
            logical_region: "runku".to_owned(),
            max_single_put_bytes: 5 * 1_048_576,
            max_object_bytes: 8 * 1_048_576,
        })?;

        let put = router
            .clone()
            .oneshot(signed_request(
                Method::PUT,
                "/s3/media/uploads/hello.txt",
                b"hello".as_slice(),
                issued.metadata.id,
                &secret,
                &[("content-type", "text/plain"), ("x-amz-meta-owner", "test")],
            )?)
            .await?;
        if put.status() != StatusCode::OK {
            let status = put.status();
            let failure = String::from_utf8(to_bytes(put.into_body(), 8_192).await?.to_vec())?;
            return Err(format!("S3 PUT failed with {status}: {failure}").into());
        }
        assert!(put.headers().contains_key("x-amz-version-id"));
        let first_version = put
            .headers()
            .get("x-amz-version-id")
            .and_then(|value| value.to_str().ok())
            .ok_or("first version missing")?
            .to_owned();
        let admin_object = service
            .get_object(scope, bucket, "uploads/hello.txt")
            .await
            .map_err(|error| format!("admin object read failed: {error:?}"))?
            .ok_or("admin object missing")?;
        assert_eq!(
            admin_object.metadata.get("owner").map(String::as_str),
            Some("test")
        );

        let public = router
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/s3/media/uploads/hello.txt")
                    .header("host", "objects.example")
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(public.status(), StatusCode::OK);
        assert_eq!(to_bytes(public.into_body(), 16).await?, "hello");

        let replaced = router
            .clone()
            .oneshot(signed_request(
                Method::PUT,
                "/s3/media/uploads/hello.txt",
                b"hello-new",
                issued.metadata.id,
                &secret,
                &[("content-type", "text/plain")],
            )?)
            .await?;
        assert_eq!(replaced.status(), StatusCode::OK);
        let current_etag = replaced
            .headers()
            .get("etag")
            .and_then(|value| value.to_str().ok())
            .ok_or("current etag missing")?
            .to_owned();

        let old_version_uri = format!("/s3/media/uploads/hello.txt?versionId={first_version}");
        let old_version = router
            .clone()
            .oneshot(signed_request(
                Method::GET,
                &old_version_uri,
                &[],
                issued.metadata.id,
                &secret,
                &[],
            )?)
            .await?;
        assert_eq!(old_version.status(), StatusCode::OK);
        assert_eq!(to_bytes(old_version.into_body(), 16).await?, "hello");

        let range = router
            .clone()
            .oneshot(signed_request(
                Method::GET,
                "/s3/media/uploads/hello.txt",
                &[],
                issued.metadata.id,
                &secret,
                &[("range", "bytes=1-4")],
            )?)
            .await?;
        assert_eq!(range.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(
            range
                .headers()
                .get("content-range")
                .and_then(|value| value.to_str().ok()),
            Some("bytes 1-4/9")
        );
        assert_eq!(to_bytes(range.into_body(), 16).await?, "ello");

        let not_modified = router
            .clone()
            .oneshot(signed_request(
                Method::GET,
                "/s3/media/uploads/hello.txt",
                &[],
                issued.metadata.id,
                &secret,
                &[("if-none-match", &current_etag)],
            )?)
            .await?;
        assert_eq!(not_modified.status(), StatusCode::NOT_MODIFIED);
        assert_eq!(to_bytes(not_modified.into_body(), 16).await?.len(), 0);

        let list = router
            .clone()
            .oneshot(signed_request(
                Method::GET,
                "/s3/media?list-type=2&prefix=uploads%2F",
                &[],
                issued.metadata.id,
                &secret,
                &[],
            )?)
            .await?;
        assert_eq!(list.status(), StatusCode::OK);
        let list = String::from_utf8(to_bytes(list.into_body(), 8_192).await?.to_vec())?;
        assert!(list.contains("<Key>uploads/hello.txt</Key>"));

        let versions = router
            .clone()
            .oneshot(signed_request(
                Method::GET,
                "/s3/media?versions&prefix=uploads%2F",
                &[],
                issued.metadata.id,
                &secret,
                &[],
            )?)
            .await?;
        assert_eq!(versions.status(), StatusCode::OK);
        let versions = String::from_utf8(to_bytes(versions.into_body(), 8_192).await?.to_vec())?;
        assert!(versions.contains(&format!("<VersionId>{first_version}</VersionId>")));

        let delete_old_uri = format!("/s3/media/uploads/hello.txt?versionId={first_version}");
        let deleted_old = router
            .clone()
            .oneshot(signed_request(
                Method::DELETE,
                &delete_old_uri,
                &[],
                issued.metadata.id,
                &secret,
                &[],
            )?)
            .await?;
        assert_eq!(deleted_old.status(), StatusCode::NO_CONTENT);
        assert!(
            service
                .get_object(scope, bucket, "uploads/hello.txt")
                .await?
                .is_some()
        );

        let initiated = router
            .clone()
            .oneshot(signed_request(
                Method::POST,
                "/s3/media/uploads/multipart.txt?uploads",
                &[],
                issued.metadata.id,
                &secret,
                &[("content-type", "text/plain")],
            )?)
            .await?;
        assert_eq!(initiated.status(), StatusCode::OK);
        let initiated = String::from_utf8(to_bytes(initiated.into_body(), 8_192).await?.to_vec())?;
        let upload_id = super::xml_text(&initiated, "UploadId")
            .map_err(|()| "multipart response omitted UploadId")?
            .to_owned();
        let first_part_uri =
            format!("/s3/media/uploads/multipart.txt?partNumber=1&uploadId={upload_id}");
        let first_part_bytes = vec![b'm'; 5 * 1_048_576];
        let first_part = router
            .clone()
            .oneshot(signed_request(
                Method::PUT,
                &first_part_uri,
                &first_part_bytes,
                issued.metadata.id,
                &secret,
                &[],
            )?)
            .await?;
        assert_eq!(first_part.status(), StatusCode::OK);
        let first_part_etag = first_part
            .headers()
            .get("etag")
            .and_then(|value| value.to_str().ok())
            .ok_or("multipart part etag missing")?
            .to_owned();
        let second_part_uri =
            format!("/s3/media/uploads/multipart.txt?partNumber=2&uploadId={upload_id}");
        let second_part = router
            .clone()
            .oneshot(signed_request(
                Method::PUT,
                &second_part_uri,
                b"tail",
                issued.metadata.id,
                &secret,
                &[],
            )?)
            .await?;
        assert_eq!(second_part.status(), StatusCode::OK);
        let second_part_etag = second_part
            .headers()
            .get("etag")
            .and_then(|value| value.to_str().ok())
            .ok_or("multipart part etag missing")?
            .to_owned();
        let complete_uri = format!("/s3/media/uploads/multipart.txt?uploadId={upload_id}");
        let complete_body = format!(
            "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{first_part_etag}</ETag></Part><Part><PartNumber>2</PartNumber><ETag>{second_part_etag}</ETag></Part></CompleteMultipartUpload>"
        );
        let completed = router
            .clone()
            .oneshot(signed_request(
                Method::POST,
                &complete_uri,
                complete_body.as_bytes(),
                issued.metadata.id,
                &secret,
                &[("content-type", "application/xml")],
            )?)
            .await?;
        assert_eq!(completed.status(), StatusCode::OK);
        let completed_retry = router
            .clone()
            .oneshot(signed_request(
                Method::POST,
                &complete_uri,
                complete_body.as_bytes(),
                issued.metadata.id,
                &secret,
                &[("content-type", "application/xml")],
            )?)
            .await?;
        assert_eq!(completed_retry.status(), StatusCode::OK);
        let multipart = router
            .clone()
            .oneshot(signed_request(
                Method::GET,
                "/s3/media/uploads/multipart.txt",
                &[],
                issued.metadata.id,
                &secret,
                &[],
            )?)
            .await?;
        assert_eq!(multipart.status(), StatusCode::OK);
        let multipart = to_bytes(multipart.into_body(), 6 * 1_048_576).await?;
        assert_eq!(multipart.len(), 5 * 1_048_576 + 4);
        assert!(multipart[..5 * 1_048_576].iter().all(|byte| *byte == b'm'));
        assert_eq!(&multipart[5 * 1_048_576..], b"tail");
        let multipart_range = router
            .clone()
            .oneshot(signed_request(
                Method::GET,
                "/s3/media/uploads/multipart.txt",
                &[],
                issued.metadata.id,
                &secret,
                &[("range", "bytes=5242878-5242881")],
            )?)
            .await?;
        assert_eq!(multipart_range.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(
            to_bytes(multipart_range.into_body(), 8).await?,
            b"mmta".as_slice()
        );

        let presigned_uri =
            presigned_get_uri("/s3/media/uploads/hello.txt", issued.metadata.id, &secret)?;
        let presigned = router
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri(&presigned_uri)
                    .header("host", "objects.example")
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(presigned.status(), StatusCode::OK);
        let mut tampered_uri = presigned_uri.clone();
        let signature_last = tampered_uri.pop().ok_or("presigned URL empty")?;
        tampered_uri.push(if signature_last == '0' { '1' } else { '0' });
        let tampered = router
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri(tampered_uri)
                    .header("host", "objects.example")
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(tampered.status(), StatusCode::FORBIDDEN);

        let denied = router
            .clone()
            .oneshot(signed_request(
                Method::PUT,
                "/s3/media/private/denied.txt",
                b"denied",
                issued.metadata.id,
                &secret,
                &[("content-type", "text/plain")],
            )?)
            .await?;
        assert_eq!(denied.status(), StatusCode::FORBIDDEN);

        let preflight = router
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::OPTIONS)
                    .uri("/s3/media/uploads/hello.txt")
                    .header("origin", "https://console.example")
                    .header("access-control-request-method", "PUT")
                    .header("access-control-request-headers", "content-type, x-amz-date")
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(preflight.status(), StatusCode::NO_CONTENT);
        assert_eq!(
            preflight
                .headers()
                .get("access-control-allow-origin")
                .and_then(|value| value.to_str().ok()),
            Some("https://console.example")
        );

        let deleted = router
            .clone()
            .oneshot(signed_request(
                Method::DELETE,
                "/s3/media/uploads/hello.txt",
                &[],
                issued.metadata.id,
                &secret,
                &[],
            )?)
            .await?;
        assert_eq!(deleted.status(), StatusCode::NO_CONTENT);
        assert!(
            service
                .get_object(scope, bucket, "uploads/hello.txt")
                .await?
                .is_none()
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[allow(clippy::too_many_lines)]
    async fn official_aws_cli_conformance_when_enabled() -> Result<(), Box<dyn Error>> {
        if std::env::var("RUNKU_TEST_AWS_CLI").ok().as_deref() != Some("1") {
            return Ok(());
        }
        let directory = tempdir()?;
        let database_url = format!(
            "sqlite://{}?mode=rwc",
            directory.path().join("aws-cli.sqlite3").display()
        );
        let repository = SqlObjectStorageRepository::connect_sqlite(
            &database_url,
            ObjectStorageRepositoryConfig::LOCAL,
        )
        .await?;
        let scope = EnvironmentScope::new(ProjectId::generate(), EnvironmentId::generate());
        let service =
            ObjectStorageService::new(Arc::new(repository), SecretDigestKey::new([47; 32]));
        let now = super::now_micros();
        let actor: ObjectStorageActor = "operator:aws-cli".parse()?;
        let bucket = service
            .create_bucket(
                scope,
                OperationId::generate(),
                BucketConfiguration {
                    name: "aws-cli".parse()?,
                    policy: BucketPolicy::Private,
                    cors: Vec::new(),
                    versioning: Versioning::Enabled,
                    lifecycle: BucketLifecycle {
                        expire_current_after_days: None,
                        expire_noncurrent_after_days: None,
                        abort_incomplete_after_days: None,
                    },
                    quota: BucketQuota {
                        max_object_bytes: 16 * 1_048_576,
                        max_total_bytes: 64 * 1_048_576,
                        max_objects: 10,
                    },
                },
                actor.clone(),
                TimestampMicros::new(now.get() - 2_000_000),
            )
            .await?
            .operation
            .bucket_id;
        let issued = service
            .issue_access_key(
                scope,
                bucket,
                OperationId::generate(),
                AccessKeyConfiguration {
                    label: "official-aws-cli".to_owned(),
                    prefix: "uploads/".to_owned(),
                    operations: BTreeSet::from([
                        AccessKeyOperation::List,
                        AccessKeyOperation::Read,
                        AccessKeyOperation::Write,
                        AccessKeyOperation::Delete,
                    ]),
                },
                actor,
                TimestampMicros::new(now.get() - 1_000_000),
            )
            .await?;
        let access_key_id = issued.metadata.id.to_string();
        let secret = issued
            .secret
            .ok_or("issued AWS CLI secret missing")?
            .expose()
            .rsplit_once('.')
            .ok_or("malformed AWS CLI secret")?
            .1
            .to_owned();
        let router = build_s3_router(S3ProductConfig {
            scope,
            service: service.clone(),
            bytes: FileObjectStore::filesystem(&directory.path().join("aws-cli-objects")).await?,
            logical_region: "runku".to_owned(),
            max_single_put_bytes: 5 * 1_048_576,
            max_object_bytes: 16 * 1_048_576,
        })?;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}/s3", listener.local_addr()?);
        let server = tokio::spawn(async move { axum::serve(listener, router).await });
        let input = directory.path().join("input.txt");
        std::fs::write(&input, b"official aws cli")?;
        let first_put = aws_cli(
            &endpoint,
            &access_key_id,
            &secret,
            &[
                "put-object",
                "--bucket",
                "aws-cli",
                "--key",
                "uploads/source.txt",
                "--body",
                input.to_str().ok_or("input path")?,
            ],
        )?;
        let first_version = serde_json::from_str::<serde_json::Value>(&first_put)?["VersionId"]
            .as_str()
            .ok_or("AWS CLI put response omitted VersionId")?
            .to_owned();
        aws_cli(
            &endpoint,
            &access_key_id,
            &secret,
            &[
                "head-object",
                "--bucket",
                "aws-cli",
                "--key",
                "uploads/source.txt",
            ],
        )?;
        let listed = aws_cli(
            &endpoint,
            &access_key_id,
            &secret,
            &[
                "list-objects-v2",
                "--bucket",
                "aws-cli",
                "--prefix",
                "uploads/",
            ],
        )?;
        assert!(listed.contains("uploads/source.txt"));
        std::fs::write(&input, b"official aws cli replaced")?;
        aws_cli(
            &endpoint,
            &access_key_id,
            &secret,
            &[
                "put-object",
                "--bucket",
                "aws-cli",
                "--key",
                "uploads/source.txt",
                "--body",
                input.to_str().ok_or("input path")?,
            ],
        )?;
        let old_output = directory.path().join("old-output.txt");
        aws_cli(
            &endpoint,
            &access_key_id,
            &secret,
            &[
                "get-object",
                "--bucket",
                "aws-cli",
                "--key",
                "uploads/source.txt",
                "--version-id",
                &first_version,
                old_output.to_str().ok_or("old output path")?,
            ],
        )?;
        assert_eq!(std::fs::read(old_output)?, b"official aws cli");
        let range_output = directory.path().join("range-output.txt");
        aws_cli(
            &endpoint,
            &access_key_id,
            &secret,
            &[
                "get-object",
                "--bucket",
                "aws-cli",
                "--key",
                "uploads/source.txt",
                "--range",
                "bytes=9-15",
                range_output.to_str().ok_or("range output path")?,
            ],
        )?;
        assert_eq!(std::fs::read(range_output)?, b"aws cli");
        aws_cli(
            &endpoint,
            &access_key_id,
            &secret,
            &[
                "copy-object",
                "--bucket",
                "aws-cli",
                "--key",
                "uploads/copy.txt",
                "--copy-source",
                "aws-cli/uploads/source.txt",
            ],
        )?;
        let output = directory.path().join("output.txt");
        aws_cli(
            &endpoint,
            &access_key_id,
            &secret,
            &[
                "get-object",
                "--bucket",
                "aws-cli",
                "--key",
                "uploads/copy.txt",
                output.to_str().ok_or("output path")?,
            ],
        )?;
        assert_eq!(std::fs::read(output)?, b"official aws cli replaced");
        let multipart = aws_cli(
            &endpoint,
            &access_key_id,
            &secret,
            &[
                "create-multipart-upload",
                "--bucket",
                "aws-cli",
                "--key",
                "uploads/multipart.bin",
                "--content-type",
                "application/octet-stream",
            ],
        )?;
        let upload_id = serde_json::from_str::<serde_json::Value>(&multipart)?["UploadId"]
            .as_str()
            .ok_or("AWS CLI multipart response omitted UploadId")?
            .to_owned();
        let active_uploads = aws_cli(
            &endpoint,
            &access_key_id,
            &secret,
            &[
                "list-multipart-uploads",
                "--bucket",
                "aws-cli",
                "--prefix",
                "uploads/",
            ],
        )?;
        assert!(active_uploads.contains(&upload_id));
        let first_part = directory.path().join("part-one.bin");
        std::fs::write(&first_part, vec![b'a'; 5 * 1_048_576])?;
        let second_part = directory.path().join("part-two.bin");
        std::fs::write(&second_part, b"final-part")?;
        let first = aws_cli(
            &endpoint,
            &access_key_id,
            &secret,
            &[
                "upload-part",
                "--bucket",
                "aws-cli",
                "--key",
                "uploads/multipart.bin",
                "--upload-id",
                &upload_id,
                "--part-number",
                "1",
                "--body",
                first_part.to_str().ok_or("first part path")?,
            ],
        )?;
        let first_etag = serde_json::from_str::<serde_json::Value>(&first)?["ETag"]
            .as_str()
            .ok_or("AWS CLI first part omitted ETag")?
            .to_owned();
        let listed_parts = aws_cli(
            &endpoint,
            &access_key_id,
            &secret,
            &[
                "list-parts",
                "--bucket",
                "aws-cli",
                "--key",
                "uploads/multipart.bin",
                "--upload-id",
                &upload_id,
            ],
        )?;
        let listed_parts = serde_json::from_str::<serde_json::Value>(&listed_parts)?;
        assert_eq!(listed_parts["Parts"][0]["ETag"], first_etag);
        let second = aws_cli(
            &endpoint,
            &access_key_id,
            &secret,
            &[
                "upload-part",
                "--bucket",
                "aws-cli",
                "--key",
                "uploads/multipart.bin",
                "--upload-id",
                &upload_id,
                "--part-number",
                "2",
                "--body",
                second_part.to_str().ok_or("second part path")?,
            ],
        )?;
        let second_etag = serde_json::from_str::<serde_json::Value>(&second)?["ETag"]
            .as_str()
            .ok_or("AWS CLI second part omitted ETag")?
            .to_owned();
        let completion = directory.path().join("multipart.json");
        std::fs::write(
            &completion,
            serde_json::to_vec(&serde_json::json!({
                "Parts": [
                    {"ETag": first_etag, "PartNumber": 1},
                    {"ETag": second_etag, "PartNumber": 2}
                ]
            }))?,
        )?;
        aws_cli(
            &endpoint,
            &access_key_id,
            &secret,
            &[
                "complete-multipart-upload",
                "--bucket",
                "aws-cli",
                "--key",
                "uploads/multipart.bin",
                "--upload-id",
                &upload_id,
                "--multipart-upload",
                &format!("file://{}", completion.display()),
            ],
        )?;
        let multipart_output = directory.path().join("multipart-output.bin");
        aws_cli(
            &endpoint,
            &access_key_id,
            &secret,
            &[
                "get-object",
                "--bucket",
                "aws-cli",
                "--key",
                "uploads/multipart.bin",
                multipart_output.to_str().ok_or("multipart output path")?,
            ],
        )?;
        let multipart_bytes = std::fs::read(multipart_output)?;
        assert_eq!(multipart_bytes.len(), 5 * 1_048_576 + 10);
        assert_eq!(
            &multipart_bytes[multipart_bytes.len() - 10..],
            b"final-part"
        );
        let abortable = aws_cli(
            &endpoint,
            &access_key_id,
            &secret,
            &[
                "create-multipart-upload",
                "--bucket",
                "aws-cli",
                "--key",
                "uploads/abort.bin",
            ],
        )?;
        let abort_id = serde_json::from_str::<serde_json::Value>(&abortable)?["UploadId"]
            .as_str()
            .ok_or("AWS CLI abort upload omitted UploadId")?
            .to_owned();
        aws_cli(
            &endpoint,
            &access_key_id,
            &secret,
            &[
                "abort-multipart-upload",
                "--bucket",
                "aws-cli",
                "--key",
                "uploads/abort.bin",
                "--upload-id",
                &abort_id,
            ],
        )?;
        let versions = aws_cli(
            &endpoint,
            &access_key_id,
            &secret,
            &[
                "list-object-versions",
                "--bucket",
                "aws-cli",
                "--prefix",
                "uploads/source.txt",
            ],
        )?;
        assert!(versions.contains(&first_version));
        aws_cli(
            &endpoint,
            &access_key_id,
            &secret,
            &[
                "delete-object",
                "--bucket",
                "aws-cli",
                "--key",
                "uploads/source.txt",
                "--version-id",
                &first_version,
            ],
        )?;
        aws_cli(
            &endpoint,
            &access_key_id,
            &secret,
            &[
                "delete-object",
                "--bucket",
                "aws-cli",
                "--key",
                "uploads/source.txt",
            ],
        )?;
        server.abort();
        Ok(())
    }

    fn aws_cli(
        endpoint: &str,
        access_key_id: &str,
        secret: &str,
        arguments: &[&str],
    ) -> Result<String, Box<dyn Error>> {
        let output = Command::new("aws")
            .env("AWS_ACCESS_KEY_ID", access_key_id)
            .env("AWS_SECRET_ACCESS_KEY", secret)
            .env("AWS_DEFAULT_REGION", "runku")
            .env("AWS_EC2_METADATA_DISABLED", "true")
            .arg("--endpoint-url")
            .arg(endpoint)
            .arg("s3api")
            .args(arguments)
            .output()?;
        if !output.status.success() {
            return Err(format!(
                "AWS CLI failed: {}",
                String::from_utf8_lossy(&output.stderr)
            )
            .into());
        }
        Ok(String::from_utf8(output.stdout)?)
    }

    fn signed_request(
        method: Method,
        uri: &str,
        body: &[u8],
        access_key_id: runku_object_storage::AccessKeyId,
        secret: &str,
        extra_headers: &[(&str, &str)],
    ) -> Result<Request<Body>, Box<dyn Error>> {
        let now = OffsetDateTime::now_utc();
        let amz_date = format!(
            "{:04}{:02}{:02}T{:02}{:02}{:02}Z",
            now.year(),
            u8::from(now.month()),
            now.day(),
            now.hour(),
            now.minute(),
            now.second()
        );
        let date = &amz_date[..8];
        let payload_hash = sha256_hex(body);
        let mut headers = HeaderMap::new();
        headers.insert("host", HeaderValue::from_static("objects.example"));
        headers.insert("x-amz-date", HeaderValue::from_str(&amz_date)?);
        headers.insert(
            "x-amz-content-sha256",
            HeaderValue::from_str(&payload_hash)?,
        );
        for (name, value) in extra_headers {
            headers.insert(
                HeaderName::from_bytes(name.as_bytes())?,
                HeaderValue::from_str(value)?,
            );
        }
        let mut signed_headers = headers
            .keys()
            .map(|name| name.as_str().to_owned())
            .collect::<Vec<_>>();
        signed_headers.sort();
        let parsed_uri = uri.parse::<axum::http::Uri>()?;
        let canonical = format!(
            "{}\n{}\n{}\n{}\n{}\n{}",
            method.as_str(),
            canonical_uri(parsed_uri.path()).ok_or("canonical URI")?,
            canonical_query(&parsed_uri, false),
            canonical_headers(&headers, &signed_headers).map_err(|_| "canonical headers")?,
            signed_headers.join(";"),
            payload_hash,
        );
        let scope = format!("{date}/runku/s3/aws4_request");
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
            sha256_hex(canonical.as_bytes())
        );
        let signing_key = derive_signing_key(secret, date, "runku", "s3");
        let signature = hmac_sha256(&signing_key, string_to_sign.as_bytes());
        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={access_key_id}/{scope}, SignedHeaders={}, Signature={}",
            signed_headers.join(";"),
            super::hex_bytes(&signature)
        );
        headers.insert("authorization", HeaderValue::from_str(&authorization)?);
        let mut request = Request::builder()
            .method(method)
            .uri(uri)
            .body(Body::from(body.to_vec()))?;
        *request.headers_mut() = headers;
        Ok(request)
    }

    fn presigned_get_uri(
        path: &str,
        access_key_id: runku_object_storage::AccessKeyId,
        secret: &str,
    ) -> Result<String, Box<dyn Error>> {
        let now = OffsetDateTime::now_utc();
        let amz_date = format!(
            "{:04}{:02}{:02}T{:02}{:02}{:02}Z",
            now.year(),
            u8::from(now.month()),
            now.day(),
            now.hour(),
            now.minute(),
            now.second()
        );
        let date = &amz_date[..8];
        let scope = format!("{date}/runku/s3/aws4_request");
        let credential = format!("{access_key_id}/{scope}");
        let query = format!(
            "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential={}&X-Amz-Date={amz_date}&X-Amz-Expires=300&X-Amz-SignedHeaders=host",
            aws_encode(credential.as_bytes(), false)
        );
        let unsigned_uri = format!("{path}?{query}").parse::<axum::http::Uri>()?;
        let canonical = format!(
            "GET\n{}\n{}\nhost:objects.example\n\nhost\nUNSIGNED-PAYLOAD",
            canonical_uri(path).ok_or("canonical URI")?,
            canonical_query(&unsigned_uri, true),
        );
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
            sha256_hex(canonical.as_bytes())
        );
        let signing_key = derive_signing_key(secret, date, "runku", "s3");
        let signature = hmac_sha256(&signing_key, string_to_sign.as_bytes());
        Ok(format!(
            "{path}?{query}&X-Amz-Signature={}",
            super::hex_bytes(&signature)
        ))
    }
}

//! Real-socket OTLP/HTTP response, bound, header, and secret-leakage conformance.

use std::{collections::BTreeMap, error::Error, str::FromStr, sync::Arc, time::Duration};

use axum::{
    Router,
    body::{Body, Bytes},
    extract::State,
    http::{HeaderMap, Response, StatusCode},
    routing::post,
};
use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsPartialSuccess, ExportLogsServiceResponse,
};
use prost::Message as _;
use runku_otel::{
    OtlpEndpoint, OtlpHeaders, OtlpHttpTransport, OtlpTransportConfig, OtlpTransportError,
    OtlpTransportOutcome,
};
use tokio::{net::TcpListener, sync::Mutex};

type TestResult = Result<(), Box<dyn Error>>;

#[derive(Clone, Debug)]
struct Reply {
    status: StatusCode,
    body: Vec<u8>,
    retry_after: Option<&'static str>,
    received: Arc<Mutex<Vec<(HeaderMap, Bytes)>>>,
}

async fn handler(State(reply): State<Reply>, headers: HeaderMap, body: Bytes) -> Response<Body> {
    reply.received.lock().await.push((headers, body));
    let mut response = Response::builder().status(reply.status);
    if let Some(value) = reply.retry_after {
        response = response.header("retry-after", value);
    }
    response
        .body(Body::from(reply.body))
        .unwrap_or_else(|_| Response::new(Body::empty()))
}

async fn transport(
    status: StatusCode,
    body: Vec<u8>,
    retry_after: Option<&'static str>,
    maximum_response_bytes: usize,
) -> Result<(OtlpHttpTransport, Arc<Mutex<Vec<(HeaderMap, Bytes)>>>), Box<dyn Error>> {
    let received = Arc::new(Mutex::new(Vec::new()));
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let reply = Reply {
        status,
        body,
        retry_after,
        received: Arc::clone(&received),
    };
    tokio::spawn(async move {
        drop(
            axum::serve(
                listener,
                Router::new()
                    .route("/v1/logs", post(handler))
                    .with_state(reply),
            )
            .await,
        );
    });
    let headers = OtlpHeaders::new(BTreeMap::from([(
        "authorization".to_owned(),
        "Bearer must-never-leak".to_owned(),
    )]))?;
    let client = OtlpHttpTransport::new(OtlpTransportConfig {
        endpoint: format!("http://{address}/v1/logs").parse()?,
        headers,
        request_timeout: Duration::from_secs(2),
        maximum_response_bytes,
    })?;
    Ok((client, received))
}

#[tokio::test]
async fn full_success_posts_protobuf_and_sensitive_headers_are_never_debugged() -> TestResult {
    let response = ExportLogsServiceResponse {
        partial_success: None,
    }
    .encode_to_vec();
    let (client, received) = transport(StatusCode::OK, response, None, 1024).await?;
    assert!(!format!("{client:?}").contains("must-never-leak"));
    assert_eq!(
        client.send(vec![1, 2, 3]).await?,
        OtlpTransportOutcome::Accepted
    );
    let received = received.lock().await;
    assert_eq!(received.len(), 1);
    assert_eq!(received[0].1.as_ref(), &[1, 2, 3]);
    assert_eq!(
        received[0]
            .0
            .get("content-type")
            .and_then(|value| value.to_str().ok()),
        Some("application/x-protobuf")
    );
    assert_eq!(
        received[0]
            .0
            .get("authorization")
            .and_then(|value| value.to_str().ok()),
        Some("Bearer must-never-leak")
    );
    Ok(())
}

#[tokio::test]
async fn partial_retryable_terminal_malformed_and_oversized_responses_are_exact() -> TestResult {
    let partial = ExportLogsServiceResponse {
        partial_success: Some(ExportLogsPartialSuccess {
            rejected_log_records: 1,
            error_message: "rejected".to_owned(),
        }),
    }
    .encode_to_vec();
    let (client, _) = transport(StatusCode::OK, partial, None, 1024).await?;
    assert_eq!(client.send(vec![1]).await?, OtlpTransportOutcome::Terminal);

    for status in [
        StatusCode::TOO_MANY_REQUESTS,
        StatusCode::BAD_GATEWAY,
        StatusCode::SERVICE_UNAVAILABLE,
        StatusCode::GATEWAY_TIMEOUT,
    ] {
        let (client, _) = transport(status, vec![], Some("73"), 1024).await?;
        assert_eq!(
            client.send(vec![1]).await?,
            OtlpTransportOutcome::Retryable {
                retry_after: Some(Duration::from_secs(73)),
            }
        );
    }
    let (client, _) = transport(StatusCode::TOO_MANY_REQUESTS, vec![], Some("999"), 1024).await?;
    assert_eq!(
        client.send(vec![1]).await?,
        OtlpTransportOutcome::Retryable {
            retry_after: Some(Duration::from_mins(2)),
        }
    );
    let (client, _) = transport(StatusCode::TOO_MANY_REQUESTS, vec![], Some("date"), 1024).await?;
    assert_eq!(
        client.send(vec![1]).await?,
        OtlpTransportOutcome::Retryable { retry_after: None }
    );
    let (client, _) = transport(StatusCode::BAD_REQUEST, vec![], None, 1024).await?;
    assert_eq!(client.send(vec![1]).await?, OtlpTransportOutcome::Terminal);
    let (client, _) = transport(StatusCode::INTERNAL_SERVER_ERROR, vec![], None, 1024).await?;
    assert_eq!(client.send(vec![1]).await?, OtlpTransportOutcome::Terminal);
    let (client, _) = transport(StatusCode::OK, vec![255], None, 1024).await?;
    assert_eq!(
        client.send(vec![1]).await,
        Err(OtlpTransportError::InvalidResponse)
    );
    let (client, _) = transport(StatusCode::OK, vec![0; 33], None, 32).await?;
    assert_eq!(
        client.send(vec![1]).await,
        Err(OtlpTransportError::LimitExceeded)
    );
    Ok(())
}

#[tokio::test]
async fn complete_request_timeout_is_sanitized_as_unavailable() -> TestResult {
    async fn slow() -> (StatusCode, Vec<u8>) {
        tokio::time::sleep(Duration::from_millis(250)).await;
        (
            StatusCode::OK,
            ExportLogsServiceResponse {
                partial_success: None,
            }
            .encode_to_vec(),
        )
    }

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    tokio::spawn(async move {
        drop(axum::serve(listener, Router::new().route("/v1/logs", post(slow))).await);
    });
    let client = OtlpHttpTransport::new(OtlpTransportConfig {
        endpoint: format!("http://{address}/v1/logs").parse()?,
        headers: OtlpHeaders::default(),
        request_timeout: Duration::from_millis(100),
        maximum_response_bytes: 1024,
    })?;
    assert_eq!(
        client.send(vec![1]).await,
        Err(OtlpTransportError::Unavailable)
    );
    Ok(())
}

#[test]
fn endpoints_headers_and_bounds_fail_closed() {
    for invalid in [
        "http://collector.example/v1/logs",
        "https://user:password@example.com/v1/logs",
        "https://example.com/v1/traces",
        "https://example.com/v1/logs?token=secret",
        "ftp://example.com/v1/logs",
    ] {
        assert!(OtlpEndpoint::from_str(invalid).is_err());
    }
    assert!(OtlpEndpoint::from_str("https://collector.example/v1/logs").is_ok());
    assert!(OtlpEndpoint::from_str("http://127.0.0.1:4318/v1/logs").is_ok());
    assert!(OtlpEndpoint::from_str("http://[::1]:4318/v1/logs").is_ok());
    assert!(
        OtlpHeaders::new(BTreeMap::from([(
            "content-type".to_owned(),
            "text/plain".to_owned(),
        )]))
        .is_err()
    );
    assert!(
        OtlpHeaders::new(BTreeMap::from([(
            "authorization".to_owned(),
            "\n".to_owned(),
        )]))
        .is_err()
    );
}

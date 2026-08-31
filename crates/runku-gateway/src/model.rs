//! Framework-independent semantic invocation boundary.

use std::{fmt, str::FromStr};

use async_trait::async_trait;
use runku_core::{CodeTarget, FunctionName, ReleaseId, RequestId, SubscriptionId};
use runku_execution::QueryOutcome;
use runku_protocol::{ActionCallV1, MutationCallV1, PublicErrorV1, QueryCallV1, SuccessMetadataV1};
use runku_realtime::SubscriptionSpec;
use runku_runtime::CancellationToken;
use runku_value::CanonicalValue;
use url::Url;
use zeroize::Zeroizing;

/// Exact normalized browser origin allowed by the HTTP CORS policy.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CorsOrigin(String);

impl CorsOrigin {
    /// Canonical serialized origin without a trailing slash.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for CorsOrigin {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for CorsOrigin {
    type Err = runku_protocol::ProtocolError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let url = Url::parse(value).map_err(|_| runku_protocol::ProtocolError::InvalidRequest)?;
        if !matches!(url.scheme(), "http" | "https")
            || url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.path() != "/"
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(runku_protocol::ProtocolError::InvalidRequest);
        }
        let host = url
            .host_str()
            .ok_or(runku_protocol::ProtocolError::InvalidRequest)?;
        let mut canonical = format!("{}://{host}", url.scheme());
        if let Some(port) = url.port() {
            canonical.push(':');
            canonical.push_str(&port.to_string());
        }
        if canonical != value {
            return Err(runku_protocol::ProtocolError::InvalidRequest);
        }
        Ok(Self(canonical))
    }
}

/// Redacted, zeroizing application and functional bearer evidence from HTTP headers.
pub struct PresentedCredentials {
    application_key: Option<Zeroizing<String>>,
    bearer: Option<Zeroizing<String>>,
}

impl PresentedCredentials {
    pub(crate) fn new(application_key: Option<String>, bearer: Option<String>) -> Self {
        Self {
            application_key: application_key.map(Zeroizing::new),
            bearer: bearer.map(Zeroizing::new),
        }
    }

    pub(crate) fn duplicate(&self) -> Self {
        Self::new(
            self.application_key.as_deref().cloned(),
            self.bearer.as_deref().cloned(),
        )
    }

    /// Optional exact `X-Runku-Key` value for the identity gateway.
    #[must_use]
    pub fn application_key(&self) -> Option<&str> {
        self.application_key.as_deref().map(String::as_str)
    }

    /// Optional exact bearer token without the `Bearer ` scheme prefix.
    #[must_use]
    pub fn bearer(&self) -> Option<&str> {
        self.bearer.as_deref().map(String::as_str)
    }
}

impl fmt::Debug for PresentedCredentials {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PresentedCredentials")
            .field(
                "application_key",
                &self.application_key.as_ref().map(|_| "[REDACTED]"),
            )
            .field("bearer", &self.bearer.as_ref().map(|_| "[REDACTED]"))
            .finish()
    }
}

/// Transport-generated context propagated to semantic gateway composition.
#[derive(Debug)]
pub struct InvocationContext {
    /// Server-generated request/correlation identity.
    pub request_id: RequestId,
    /// Redacted zeroizing credentials.
    pub credentials: PresentedCredentials,
    /// Cancellation signalled on deadline or dropped/disconnected HTTP future.
    pub cancellation: CancellationToken,
}

/// Transport-generated context for one initial Realtime Query authorization/execution.
#[derive(Debug)]
pub struct RealtimeSubscribeContext {
    /// Client request correlation identity.
    pub request_id: RequestId,
    /// Server-generated registry identity.
    pub subscription_id: SubscriptionId,
    /// Redacted zeroizing credentials from the authenticate frame.
    pub credentials: PresentedCredentials,
    /// Maximum authorization duration imposed by the WebSocket transport.
    pub maximum_authorization_duration: std::time::Duration,
    /// Cancellation signalled on deadline or socket disconnect.
    pub cancellation: CancellationToken,
}

/// Successful initial Query plus its immutable rerun descriptor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RealtimePreparedSubscription {
    /// Token-free immutable descriptor retained by Realtime Core.
    pub spec: SubscriptionSpec,
    /// Initial authoritative Query state and dependencies.
    pub outcome: QueryOutcome,
}

/// Strict decoded call union selected by the HTTP endpoint path.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum InvokeCallV1 {
    /// Read-only Query.
    Query(QueryCallV1),
    /// Transactional Mutation.
    Mutation(MutationCallV1),
    /// Non-transactional Action.
    Action(ActionCallV1),
}

/// Semantic success returned to the transport after auth/routing/execution.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GatewaySuccess {
    /// Exact resolved immutable Release.
    pub release_id: ReleaseId,
    /// Canonical Function result.
    pub value: CanonicalValue,
    /// Kind-specific public execution metadata.
    pub metadata: SuccessMetadataV1,
}

/// Sanitized semantic failure returned to the HTTP transport.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GatewayFailure {
    /// Validated public classification/code/retryability.
    pub error: PublicErrorV1,
}

/// Semantic Product Base gateway injected into the framework-specific HTTP boundary.
#[async_trait]
pub trait InvocationService: fmt::Debug + Send + Sync {
    /// Resolves identity/target/function, executes, and returns only public success/failure data.
    async fn invoke(
        &self,
        context: InvocationContext,
        call: InvokeCallV1,
    ) -> Result<GatewaySuccess, GatewayFailure>;
}

/// Semantic Product Base boundary used by the public WebSocket adapter.
#[async_trait]
pub trait RealtimeQueryService: fmt::Debug + Send + Sync {
    /// Resolves a target once, authenticates, executes a Query, and returns its rerun descriptor.
    async fn prepare_subscription(
        &self,
        context: RealtimeSubscribeContext,
        target: CodeTarget,
        function: FunctionName,
        arguments: CanonicalValue,
    ) -> Result<RealtimePreparedSubscription, GatewayFailure>;
}

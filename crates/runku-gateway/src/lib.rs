//! Bounded application HTTP transport independent from serving/storage/SaaS implementations.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod model;
mod router;
mod service;
mod websocket;

pub use model::{
    CorsOrigin, GatewayFailure, GatewaySuccess, InvocationContext, InvocationService, InvokeCallV1,
    PresentedCredentials, RealtimePreparedSubscription, RealtimeQueryService,
    RealtimeSubscribeContext,
};
pub use router::{GatewayHttpConfig, build_router, build_router_with_realtime, serve};
pub use service::{
    ArtifactCacheTelemetrySnapshot, DevelopmentCatalog, GatewayClock, PrincipalVerificationError,
    PrincipalVerifier, ProductInvocationConfig, ProductInvocationService, ServingCatalog,
    ServingRefresh, SystemGatewayClock,
};
pub use websocket::{
    REALTIME_SUBPROTOCOL, RealtimeGateway, RealtimeGatewayConfig, RealtimeGatewayTelemetrySnapshot,
};

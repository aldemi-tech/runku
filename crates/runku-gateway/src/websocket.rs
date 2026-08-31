//! Bounded public WebSocket transport over Realtime Core.

use std::{
    collections::BTreeMap,
    fmt,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use axum::{
    extract::{
        WebSocketUpgrade,
        ws::{CloseFrame, Message, WebSocket},
    },
    http::{HeaderMap, header},
    response::Response,
};
use runku_core::{RequestId, SubscriptionId};
use runku_protocol::{
    REALTIME_MESSAGE_MAX_BYTES, RealtimeClientMessageV1, RealtimeServerMessageV1,
    decode_realtime_client_v1, encode_realtime_server_v1,
};
use runku_realtime::{DeliveryEvent, RealtimeError, SubscriptionHandle, SubscriptionRegistry};
use runku_runtime::CancellationToken;
use runku_value::TimestampMicros;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, broadcast, mpsc};

use crate::{GatewayFailure, PresentedCredentials, RealtimeQueryService, RealtimeSubscribeContext};

/// Required WebSocket subprotocol for the strict v1 contract.
pub const REALTIME_SUBPROTOCOL: &str = "runku.realtime.v1";

/// Validated connection/session limits for the public Realtime transport.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RealtimeGatewayConfig {
    /// Maximum upgraded sockets owned by this process.
    pub max_connections: usize,
    /// Time allowed for the first `authenticate` message.
    pub authentication_timeout: Duration,
    /// Maximum silence between client messages/control frames.
    pub idle_timeout: Duration,
    /// Maximum authorization lifetime before fresh credentials are mandatory.
    pub reauthentication_interval: Duration,
    /// Maximum time allowed for one initial Query preparation.
    pub command_timeout: Duration,
    /// Maximum simultaneous subscriptions owned by one socket.
    pub max_subscriptions_per_connection: usize,
    /// Bounded internal delivery queue for one socket.
    pub outbound_buffer: usize,
}

impl RealtimeGatewayConfig {
    /// Conservative Product Base defaults.
    pub const PRODUCTION: Self = Self {
        max_connections: 10_000,
        authentication_timeout: Duration::from_secs(10),
        idle_timeout: Duration::from_secs(90),
        reauthentication_interval: Duration::from_mins(15),
        command_timeout: Duration::from_secs(30),
        max_subscriptions_per_connection: 64,
        outbound_buffer: 128,
    };

    fn validate(self) -> Result<Self, RealtimeError> {
        if !(1..=100_000).contains(&self.max_connections)
            || !(Duration::from_millis(100)..=Duration::from_mins(1))
                .contains(&self.authentication_timeout)
            || !(Duration::from_secs(1)..=Duration::from_hours(1)).contains(&self.idle_timeout)
            || !(Duration::from_secs(1)..=Duration::from_hours(24))
                .contains(&self.reauthentication_interval)
            || !(Duration::from_millis(1)..=Duration::from_mins(5)).contains(&self.command_timeout)
            || !(1..=1_024).contains(&self.max_subscriptions_per_connection)
            || !(1..=4_096).contains(&self.outbound_buffer)
        {
            return Err(RealtimeError::InvalidConfiguration);
        }
        Ok(self)
    }
}

/// Aggregate bounded transport telemetry without tenant/key/subscription labels.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RealtimeGatewayTelemetrySnapshot {
    /// Successful WebSocket upgrades admitted.
    pub connections: u64,
    /// Upgrade attempts rejected by connection admission.
    pub admission_rejections: u64,
    /// Structurally accepted authenticate commands.
    pub authentications: u64,
    /// Initial subscriptions registered successfully.
    pub subscriptions: u64,
    /// Explicit/implicit subscription removals.
    pub removals: u64,
    /// Protocol/state-machine failures.
    pub protocol_failures: u64,
    /// Delivery receivers that lagged.
    pub lagged_deliveries: u64,
    /// Socket outbound queues that exhausted capacity.
    pub outbound_overloads: u64,
    /// Sessions closed by auth/idle deadline.
    pub deadline_closures: u64,
}

#[derive(Debug, Default)]
struct RealtimeGatewayTelemetry {
    connections: AtomicU64,
    admission_rejections: AtomicU64,
    authentications: AtomicU64,
    subscriptions: AtomicU64,
    removals: AtomicU64,
    protocol_failures: AtomicU64,
    lagged_deliveries: AtomicU64,
    outbound_overloads: AtomicU64,
    deadline_closures: AtomicU64,
}

/// Cloneable WebSocket composition sharing a semantic Query service and Realtime registry.
#[derive(Clone)]
pub struct RealtimeGateway {
    config: RealtimeGatewayConfig,
    service: Arc<dyn RealtimeQueryService>,
    registry: SubscriptionRegistry,
    admission: Arc<Semaphore>,
    telemetry: Arc<RealtimeGatewayTelemetry>,
}

impl fmt::Debug for RealtimeGateway {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RealtimeGateway")
            .field("config", &self.config)
            .field("registry", &self.registry)
            .field("telemetry", &self.telemetry())
            .finish_non_exhaustive()
    }
}

impl RealtimeGateway {
    /// Creates a transport from validated limits and initialized Product Base components.
    ///
    /// # Errors
    ///
    /// Rejects zero/excessive durations, capacities, connections, or subscriptions.
    pub fn new(
        config: RealtimeGatewayConfig,
        service: Arc<dyn RealtimeQueryService>,
        registry: SubscriptionRegistry,
    ) -> Result<Self, RealtimeError> {
        let config = config.validate()?;
        Ok(Self {
            admission: Arc::new(Semaphore::new(config.max_connections)),
            config,
            service,
            registry,
            telemetry: Arc::new(RealtimeGatewayTelemetry::default()),
        })
    }

    /// Returns bounded transport counters.
    #[must_use]
    pub fn telemetry(&self) -> RealtimeGatewayTelemetrySnapshot {
        RealtimeGatewayTelemetrySnapshot {
            connections: self.telemetry.connections.load(Ordering::Relaxed),
            admission_rejections: self.telemetry.admission_rejections.load(Ordering::Relaxed),
            authentications: self.telemetry.authentications.load(Ordering::Relaxed),
            subscriptions: self.telemetry.subscriptions.load(Ordering::Relaxed),
            removals: self.telemetry.removals.load(Ordering::Relaxed),
            protocol_failures: self.telemetry.protocol_failures.load(Ordering::Relaxed),
            lagged_deliveries: self.telemetry.lagged_deliveries.load(Ordering::Relaxed),
            outbound_overloads: self.telemetry.outbound_overloads.load(Ordering::Relaxed),
            deadline_closures: self.telemetry.deadline_closures.load(Ordering::Relaxed),
        }
    }

    pub(crate) fn validate_upgrade(
        websocket: &WebSocketUpgrade,
        headers: &HeaderMap,
    ) -> Result<(), UpgradeFailure> {
        if headers.contains_key(header::AUTHORIZATION)
            || headers.contains_key("x-runku-key")
            || websocket
                .requested_protocols()
                .map(axum::http::HeaderValue::as_bytes)
                .collect::<Vec<_>>()
                != [REALTIME_SUBPROTOCOL.as_bytes()]
        {
            return Err(UpgradeFailure::Invalid);
        }
        Ok(())
    }

    pub(crate) fn upgrade(&self, websocket: WebSocketUpgrade) -> Result<Response, UpgradeFailure> {
        let permit = Arc::clone(&self.admission)
            .try_acquire_owned()
            .map_err(|_| {
                self.telemetry
                    .admission_rejections
                    .fetch_add(1, Ordering::Relaxed);
                UpgradeFailure::Busy
            })?;
        self.telemetry.connections.fetch_add(1, Ordering::Relaxed);
        let gateway = self.clone();
        Ok(websocket
            .max_message_size(REALTIME_MESSAGE_MAX_BYTES)
            .max_frame_size(REALTIME_MESSAGE_MAX_BYTES)
            .max_write_buffer_size(REALTIME_MESSAGE_MAX_BYTES * 4)
            .protocols([REALTIME_SUBPROTOCOL])
            .on_upgrade(move |socket| session(socket, gateway, permit)))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum UpgradeFailure {
    Invalid,
    Busy,
}

struct ActiveSubscription {
    authorized_until: TimestampMicros,
    cancellation: CancellationToken,
}

struct OutboundDelivery {
    message: RealtimeServerMessageV1,
    remove: Option<SubscriptionId>,
}

#[allow(clippy::too_many_lines)]
async fn session(mut socket: WebSocket, gateway: RealtimeGateway, _permit: OwnedSemaphorePermit) {
    let connected_at = Instant::now();
    let auth_deadline = connected_at + gateway.config.authentication_timeout;
    let mut last_activity = connected_at;
    let mut session_deadline = auth_deadline;
    let mut credentials: Option<PresentedCredentials> = None;
    let mut active = BTreeMap::<SubscriptionId, ActiveSubscription>::new();
    let (outbound_tx, mut outbound_rx) =
        mpsc::channel::<OutboundDelivery>(gateway.config.outbound_buffer);
    let connection_cancel = CancellationToken::new();
    let mut close_code = 1000;
    let mut close_reason = "session ended";

    loop {
        let idle_deadline = last_activity + gateway.config.idle_timeout;
        let base_deadline = if credentials.is_some() {
            session_deadline.min(idle_deadline)
        } else {
            auth_deadline.min(idle_deadline)
        };
        let next_deadline = active
            .values()
            .filter_map(|entry| instant_for_timestamp(entry.authorized_until))
            .fold(base_deadline, Instant::min);
        let deadline = tokio::time::sleep_until(tokio::time::Instant::from_std(next_deadline));
        tokio::pin!(deadline);
        tokio::select! {
            () = connection_cancel.cancelled() => {
                close_code = 1013;
                close_reason = "resync required";
                break;
            }
            () = &mut deadline => {
                gateway.telemetry.deadline_closures.fetch_add(1, Ordering::Relaxed);
                let wall_now = unix_micros();
                let authorization_expired = wall_now.is_none_or(|now| {
                    active.values().any(|entry| entry.authorized_until <= now)
                });
                let code = if authorization_expired {
                    "AUTHORIZATION_EXPIRED"
                } else if credentials.is_none() {
                    "REALTIME_AUTH_TIMEOUT"
                } else if Instant::now() >= session_deadline {
                    "AUTHORIZATION_EXPIRED"
                } else {
                    "REALTIME_IDLE_TIMEOUT"
                };
                let _ = send_server(&mut socket, RealtimeServerMessageV1::Error {
                    request_id: None,
                    subscription_id: None,
                    delivery_revision: None,
                    code: code.to_owned(),
                    retryable: false,
                }).await;
                close_code = 1008;
                close_reason = "session deadline";
                break;
            }
            outbound = outbound_rx.recv() => {
                let Some(outbound) = outbound else {
                    close_code = 1011;
                    close_reason = "delivery channel closed";
                    break;
                };
                if let Some(id) = outbound.remove
                    && let Some(removed) = active.remove(&id)
                {
                    removed.cancellation.cancel();
                    let _ = gateway.registry.remove(id);
                    gateway.telemetry.removals.fetch_add(1, Ordering::Relaxed);
                }
                if send_server(&mut socket, outbound.message).await.is_err() {
                    break;
                }
            }
            incoming = socket.recv() => {
                let Some(incoming) = incoming else { break; };
                let Ok(message) = incoming else {
                    close_code = 1009;
                    close_reason = "invalid or oversized message";
                    break;
                };
                last_activity = Instant::now();
                let text = match message {
                    Message::Text(text) => text,
                    Message::Ping(_) | Message::Pong(_) => continue,
                    Message::Close(_) => break,
                    Message::Binary(_) => {
                        gateway.telemetry.protocol_failures.fetch_add(1, Ordering::Relaxed);
                        let _ = send_server(&mut socket, protocol_error(None, "REALTIME_TEXT_REQUIRED")).await;
                        close_code = 1003;
                        close_reason = "text required";
                        break;
                    }
                };
                let command = match decode_realtime_client_v1(text.as_bytes()) {
                    Ok(command) => command,
                    Err(error) => {
                        gateway.telemetry.protocol_failures.fetch_add(1, Ordering::Relaxed);
                        let _ = send_server(&mut socket, protocol_error(None, error.code())).await;
                        close_code = 1008;
                        close_reason = "invalid realtime message";
                        break;
                    }
                };
                match command {
                    RealtimeClientMessageV1::Authenticate { request_id, credentials: supplied } => {
                        if !active.is_empty() {
                            let _ = send_server(&mut socket, protocol_error(Some(request_id), "REALTIME_REAUTH_REQUIRES_UNSUBSCRIBE")).await;
                            continue;
                        }
                        session_deadline = Instant::now() + gateway.config.reauthentication_interval;
                        credentials = Some(PresentedCredentials::new(supplied.application_key, supplied.bearer));
                        gateway.telemetry.authentications.fetch_add(1, Ordering::Relaxed);
                        if send_server(&mut socket, RealtimeServerMessageV1::AuthenticationAccepted { request_id }).await.is_err() {
                            break;
                        }
                    }
                    RealtimeClientMessageV1::Subscribe { request_id, target, function, arguments } => {
                        let Some(presented) = credentials.as_ref() else {
                            let _ = send_server(&mut socket, protocol_error(Some(request_id), "REALTIME_AUTH_REQUIRED")).await;
                            close_code = 1008;
                            close_reason = "authenticate first";
                            break;
                        };
                        if active.len() >= gateway.config.max_subscriptions_per_connection {
                            let _ = send_server(&mut socket, protocol_error(Some(request_id), "REALTIME_SUBSCRIPTION_LIMIT_EXCEEDED")).await;
                            continue;
                        }
                        let subscription_id = SubscriptionId::generate();
                        let invocation_cancel = CancellationToken::new();
                        let context = RealtimeSubscribeContext {
                            request_id,
                            subscription_id,
                            credentials: presented.duplicate(),
                            maximum_authorization_duration: gateway.config.reauthentication_interval,
                            cancellation: invocation_cancel.clone(),
                        };
                        let prepared = match tokio::time::timeout(
                            gateway.config.command_timeout,
                            gateway.service.prepare_subscription(context, target, function, arguments),
                        ).await {
                            Ok(Ok(prepared)) => prepared,
                            Ok(Err(error)) => {
                                let _ = send_server(&mut socket, gateway_error(Some(request_id), error)).await;
                                continue;
                            }
                            Err(_) => {
                                invocation_cancel.cancel();
                                let _ = send_server(&mut socket, protocol_error(Some(request_id), "REALTIME_COMMAND_TIMEOUT")).await;
                                continue;
                            }
                        };
                        let authorized_until = prepared.spec.authorized_until;
                        let handle = match gateway.registry.register(prepared.spec, prepared.outcome) {
                            Ok(handle) => handle,
                            Err(error) => {
                                let _ = send_server(&mut socket, realtime_error(Some(request_id), error)).await;
                                continue;
                            }
                        };
                        let subscription_cancel = CancellationToken::new();
                        active.insert(subscription_id, ActiveSubscription {
                            authorized_until,
                            cancellation: subscription_cancel.clone(),
                        });
                        gateway.telemetry.subscriptions.fetch_add(1, Ordering::Relaxed);
                        spawn_forwarder(
                            gateway.clone(),
                            handle,
                            request_id,
                            outbound_tx.clone(),
                            subscription_cancel,
                            connection_cancel.clone(),
                        );
                    }
                    RealtimeClientMessageV1::Unsubscribe { request_id, subscription_id } => {
                        let Some(removed) = active.remove(&subscription_id) else {
                            let _ = send_server(&mut socket, protocol_error(Some(request_id), "REALTIME_SUBSCRIPTION_NOT_OWNED")).await;
                            continue;
                        };
                        removed.cancellation.cancel();
                        match gateway.registry.remove(subscription_id) {
                            Ok(()) => {
                                gateway.telemetry.removals.fetch_add(1, Ordering::Relaxed);
                                if send_server(&mut socket, RealtimeServerMessageV1::Unsubscribed { request_id, subscription_id }).await.is_err() {
                                    break;
                                }
                            }
                            Err(error) => {
                                let _ = send_server(&mut socket, realtime_error(Some(request_id), error)).await;
                            }
                        }
                    }
                    RealtimeClientMessageV1::Ping { request_id } => {
                        if credentials.is_none() {
                            let _ = send_server(&mut socket, protocol_error(Some(request_id), "REALTIME_AUTH_REQUIRED")).await;
                            close_code = 1008;
                            close_reason = "authenticate first";
                            break;
                        }
                        if send_server(&mut socket, RealtimeServerMessageV1::Pong { request_id }).await.is_err() {
                            break;
                        }
                    }
                }
            }
        }
        if credentials.is_some() && Instant::now() >= session_deadline {
            break;
        }
        if active.values().any(|entry| {
            entry.authorized_until <= unix_micros().unwrap_or(TimestampMicros::new(i64::MAX))
        }) {
            let _ = send_server(&mut socket, protocol_error(None, "AUTHORIZATION_EXPIRED")).await;
            close_code = 1008;
            close_reason = "authorization expired";
            break;
        }
    }

    connection_cancel.cancel();
    for (id, subscription) in active {
        subscription.cancellation.cancel();
        if gateway.registry.remove(id).is_ok() {
            gateway.telemetry.removals.fetch_add(1, Ordering::Relaxed);
        }
    }
    let _ = socket
        .send(Message::Close(Some(CloseFrame {
            code: close_code,
            reason: close_reason.into(),
        })))
        .await;
}

fn spawn_forwarder(
    gateway: RealtimeGateway,
    mut handle: SubscriptionHandle,
    initial_request_id: RequestId,
    outbound: mpsc::Sender<OutboundDelivery>,
    subscription_cancel: CancellationToken,
    connection_cancel: CancellationToken,
) {
    tokio::spawn(async move {
        let release_id = handle.snapshot.spec.release_id;
        let authorized_until = handle.snapshot.spec.authorized_until;
        let subscription_id = handle.snapshot.spec.id;
        let mut initial = Some(initial_request_id);
        loop {
            let delivery = tokio::select! {
                () = subscription_cancel.cancelled() => break,
                () = connection_cancel.cancelled() => break,
                delivery = handle.receiver.recv() => delivery,
            };
            let outbound_delivery = match delivery {
                Ok(DeliveryEvent::State {
                    subscription_id,
                    delivery_revision,
                    value,
                    result_hash,
                    snapshot_sequence,
                }) => OutboundDelivery {
                    message: RealtimeServerMessageV1::State {
                        request_id: initial.take(),
                        subscription_id,
                        release_id,
                        delivery_revision,
                        value,
                        result_hash,
                        snapshot_sequence,
                        authorized_until,
                    },
                    remove: None,
                },
                Ok(DeliveryEvent::Error {
                    subscription_id,
                    delivery_revision,
                    code,
                    retryable,
                    suspended,
                }) => OutboundDelivery {
                    message: RealtimeServerMessageV1::Error {
                        request_id: None,
                        subscription_id: Some(subscription_id),
                        delivery_revision: Some(delivery_revision),
                        code: code.to_owned(),
                        retryable,
                    },
                    remove: suspended.then_some(subscription_id),
                },
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    gateway
                        .telemetry
                        .lagged_deliveries
                        .fetch_add(1, Ordering::Relaxed);
                    OutboundDelivery {
                        message: RealtimeServerMessageV1::ResyncRequired {
                            subscription_id,
                            code: "REALTIME_DELIVERY_LAGGED".to_owned(),
                        },
                        remove: Some(subscription_id),
                    }
                }
                Err(broadcast::error::RecvError::Closed) => break,
            };
            let terminal = outbound_delivery.remove.is_some();
            if outbound.try_send(outbound_delivery).is_err() {
                gateway
                    .telemetry
                    .outbound_overloads
                    .fetch_add(1, Ordering::Relaxed);
                connection_cancel.cancel();
                break;
            }
            if terminal {
                break;
            }
        }
    });
}

async fn send_server(socket: &mut WebSocket, message: RealtimeServerMessageV1) -> Result<(), ()> {
    let bytes = encode_realtime_server_v1(&message).map_err(|_| ())?;
    let text = String::from_utf8(bytes).map_err(|_| ())?;
    socket
        .send(Message::Text(text.into()))
        .await
        .map_err(|_| ())
}

fn protocol_error(request_id: Option<RequestId>, code: &'static str) -> RealtimeServerMessageV1 {
    RealtimeServerMessageV1::Error {
        request_id,
        subscription_id: None,
        delivery_revision: None,
        code: code.to_owned(),
        retryable: false,
    }
}

fn gateway_error(
    request_id: Option<RequestId>,
    failure: GatewayFailure,
) -> RealtimeServerMessageV1 {
    RealtimeServerMessageV1::Error {
        request_id,
        subscription_id: None,
        delivery_revision: None,
        code: failure.error.code().to_owned(),
        retryable: failure.error.retryable(),
    }
}

fn realtime_error(request_id: Option<RequestId>, error: RealtimeError) -> RealtimeServerMessageV1 {
    RealtimeServerMessageV1::Error {
        request_id,
        subscription_id: None,
        delivery_revision: None,
        code: error.code().to_owned(),
        retryable: error.retryable(),
    }
}

fn unix_micros() -> Option<TimestampMicros> {
    let micros = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()?
        .as_micros();
    i64::try_from(micros).ok().map(TimestampMicros::new)
}

fn instant_for_timestamp(deadline: TimestampMicros) -> Option<Instant> {
    let now = unix_micros()?;
    if deadline <= now {
        return Some(Instant::now());
    }
    let delta = u64::try_from(deadline.get().checked_sub(now.get())?).ok()?;
    Instant::now().checked_add(Duration::from_micros(delta))
}

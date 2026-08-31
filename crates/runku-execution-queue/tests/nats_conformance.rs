//! Opt-in `JetStream` conformance suite executed by the infrastructure evidence script.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use runku_core::{EnvironmentId, InvocationId, ProjectId, ReleaseId, RequestId};
use runku_execution_queue::{
    EXECUTION_JOB_FORMAT_VERSION, EXECUTION_JOB_PAYLOAD_MAX_BYTES, ExecutionClass,
    ExecutionCompletion, ExecutionControlPlane, ExecutionJobV1, ExecutionQueue, ExecutionState,
    NatsExecutionControlConfig, NatsExecutionControlPlane, NatsExecutionQueue,
    NatsExecutionQueueConfig,
};
use tokio::task::JoinSet;

fn job(payload: u8) -> ExecutionJobV1 {
    ExecutionJobV1 {
        format_version: EXECUTION_JOB_FORMAT_VERSION,
        invocation_id: InvocationId::generate(),
        request_id: RequestId::generate(),
        project_id: ProjectId::generate(),
        environment_id: EnvironmentId::generate(),
        release_id: ReleaseId::generate(),
        deadline_unix_ms: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(1, |duration| {
                u64::try_from((duration + Duration::from_secs(60)).as_millis()).unwrap_or(u64::MAX)
            }),
        payload: vec![payload],
    }
}

async fn queue()
-> Result<Option<(async_nats::Client, NatsExecutionQueue, String)>, Box<dyn std::error::Error>> {
    let Ok(url) = std::env::var("RUNKU_TEST_NATS_URL") else {
        return Ok(None);
    };
    let client = async_nats::connect(url).await?;
    let unique = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let name = format!("RUNKU_TEST_{}_{}", std::process::id(), unique);
    let config = NatsExecutionQueueConfig {
        stream_name: name.clone(),
        subject_prefix: format!("runku.test.{}.{}", std::process::id(), unique),
        max_messages: 100,
        max_bytes: 10_485_760,
        max_age: Duration::from_secs(60),
        replicas: 1,
        ack_wait: Duration::from_secs(3),
        max_deliver: 3,
        max_waiting: 32,
    };
    let queue = NatsExecutionQueue::open(client.clone(), config).await?;
    Ok(Some((client, queue, name)))
}

#[tokio::test]
async fn waiting_runner_gets_immediate_delivery_and_idle_jobs_persist()
-> Result<(), Box<dyn std::error::Error>> {
    let Some((client, queue, stream_name)) = queue().await? else {
        eprintln!("skipped: RUNKU_TEST_NATS_URL is not configured");
        return Ok(());
    };
    let class = ExecutionClass::new("node_oci_v1")?;

    let receiver = queue.clone();
    let receiver_class = class.clone();
    let outstanding_pull =
        tokio::spawn(async move { receiver.pull(&receiver_class, Duration::from_secs(2)).await });
    tokio::time::sleep(Duration::from_millis(100)).await;
    let immediate = job(1);
    queue.enqueue(&class, &immediate).await?;
    let delivery = outstanding_pull
        .await??
        .ok_or("immediate delivery absent")?;
    assert_eq!(delivery.job(), &immediate);
    delivery.ack().await?;

    let persisted = job(2);
    queue.enqueue(&class, &persisted).await?;
    queue.enqueue(&class, &persisted).await?;
    tokio::time::sleep(Duration::from_millis(100)).await;
    let delivery = queue
        .pull(&class, Duration::from_secs(1))
        .await?
        .ok_or("persisted delivery absent")?;
    assert_eq!(delivery.job(), &persisted);
    delivery.retry(Some(Duration::from_millis(50))).await?;
    let redelivery = queue
        .pull(&class, Duration::from_secs(1))
        .await?
        .ok_or("retry delivery absent")?;
    assert_eq!(redelivery.job(), &persisted);
    redelivery.ack().await?;
    assert!(
        queue
            .pull(&class, Duration::from_millis(200))
            .await?
            .is_none(),
        "the idempotent enqueue retry must not create a duplicate execution"
    );

    let mut maximum = job(3);
    maximum.payload = vec![0xff; EXECUTION_JOB_PAYLOAD_MAX_BYTES];
    queue.enqueue(&class, &maximum).await?;
    let maximum_delivery = queue
        .pull(&class, Duration::from_secs(1))
        .await?
        .ok_or("maximum payload delivery absent")?;
    assert_eq!(maximum_delivery.job(), &maximum);
    maximum_delivery.ack().await?;

    async_nats::jetstream::new(client)
        .delete_stream(stream_name)
        .await?;
    Ok(())
}

#[tokio::test]
async fn durable_control_plane_survives_reopen_and_delivers_result_and_cancellation()
-> Result<(), Box<dyn std::error::Error>> {
    let Ok(url) = std::env::var("RUNKU_TEST_NATS_URL") else {
        eprintln!("skipped: RUNKU_TEST_NATS_URL is not configured");
        return Ok(());
    };
    let client = async_nats::connect(url).await?;
    let unique = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let bucket = format!("RUNKU_CTL_{}_{}", std::process::id(), unique);
    let config = NatsExecutionControlConfig {
        bucket: bucket.clone(),
        max_bytes: 10_485_760,
        max_age: Duration::from_secs(60),
        replicas: 1,
    };
    let control = NatsExecutionControlPlane::open(client.clone(), config.clone()).await?;
    let id = InvocationId::generate();
    control.register(id, 100).await?;
    control.begin_preparing(id).await?;
    let running = control.begin_running(id).await?;
    let waiter = control.clone();
    let wait = tokio::spawn(async move {
        waiter
            .wait_changed(id, running.revision, Duration::from_secs(2))
            .await
    });
    control.request_cancel(id).await?;
    assert_eq!(
        wait.await??.ok_or("missing cancellation")?.record.state,
        ExecutionState::CancelRequested
    );
    control.complete(id, ExecutionCompletion::Cancelled).await?;
    let reopened = NatsExecutionControlPlane::open(client.clone(), config).await?;
    assert_eq!(
        reopened.get(id).await?.record.state,
        ExecutionState::Cancelled
    );

    let result_id = InvocationId::generate();
    reopened.register(result_id, 100).await?;
    let payload = vec![0x5a; 512 * 1024];
    reopened
        .complete(result_id, ExecutionCompletion::Succeeded(payload.clone()))
        .await?;
    assert_eq!(reopened.get(result_id).await?.record.result, Some(payload));
    async_nats::jetstream::new(client)
        .delete_key_value(bucket)
        .await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn shared_change_bus_correlates_five_hundred_concurrent_waiters()
-> Result<(), Box<dyn std::error::Error>> {
    let Ok(url) = std::env::var("RUNKU_TEST_NATS_URL") else {
        eprintln!("skipped: RUNKU_TEST_NATS_URL is not configured");
        return Ok(());
    };
    let client = async_nats::connect(url).await?;
    let unique = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let bucket = format!("RUNKU_CTL_SCALE_{}_{}", std::process::id(), unique);
    let control = NatsExecutionControlPlane::open(
        client.clone(),
        NatsExecutionControlConfig {
            bucket: bucket.clone(),
            max_bytes: 67_108_864,
            max_age: Duration::from_secs(60),
            replicas: 1,
        },
    )
    .await?;
    tokio::time::sleep(Duration::from_millis(100)).await;
    let mut registered = Vec::with_capacity(500);
    for _ in 0..500 {
        let invocation_id = InvocationId::generate();
        let record = control.register(invocation_id, u64::MAX).await?;
        registered.push((invocation_id, record.revision));
    }
    let mut waiters = JoinSet::new();
    for (invocation_id, revision) in &registered {
        let control = control.clone();
        let invocation_id = *invocation_id;
        let revision = *revision;
        waiters.spawn(async move {
            let changed = control
                .wait_changed(invocation_id, revision, Duration::from_secs(10))
                .await?;
            Ok::<_, runku_execution_queue::ExecutionControlError>((invocation_id, changed))
        });
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    let mut transitions = JoinSet::new();
    for (invocation_id, _) in &registered {
        let control = control.clone();
        let invocation_id = *invocation_id;
        transitions.spawn(async move { control.begin_preparing(invocation_id).await });
    }
    while let Some(transition) = transitions.join_next().await {
        assert_eq!(transition??.record.state, ExecutionState::Preparing);
    }
    let mut observed = 0;
    while let Some(waiter) = waiters.join_next().await {
        let (expected_id, changed) = waiter??;
        let changed = changed.ok_or("shared change bus timed out")?;
        assert_eq!(changed.record.invocation_id, expected_id);
        assert_eq!(changed.record.state, ExecutionState::Preparing);
        observed += 1;
    }
    assert_eq!(observed, 500);
    async_nats::jetstream::new(client)
        .delete_key_value(bucket)
        .await?;
    Ok(())
}

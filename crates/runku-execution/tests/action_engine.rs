//! Action coordinator scheduling behavior over durable `SQLite`.

use std::{error::Error, sync::Arc, time::Duration};

use runku_core::{
    BuildId, DevRevisionId, DocumentId, EnvironmentId, EnvironmentScope, FunctionId, InvocationId,
    ProjectId, ReleaseId, RequestId, ScheduledInvocationId, TableId,
};
use runku_data::{LogicalStore, PinnedCode};
use runku_data_sqlite::{SqliteRole, SqliteStore, SqliteStoreConfig};
use runku_execution::{ActionExecutionError, ActionExecutor, MutationExecutor, QueryExecutor};
use runku_releases::{
    AuthPolicy, Capability, FunctionManifest, FunctionType, FunctionVisibility, ReleaseManifestV1,
    RuntimeClass, SafeEsmBundleV1, Sha256Digest, encode_safe_esm_bundle,
};
use runku_runtime::{
    CancellationToken, InvocationRequest, RuntimeError, RuntimeLimits, RuntimeSupervisor,
};
use runku_value::{CanonicalValue, TimestampMicros};
use tempfile::TempDir;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn action_run_after_is_durable_idempotent_and_release_pinned() -> Result<(), Box<dyn Error>> {
    let directory = TempDir::new()?;
    let store = Arc::new(
        SqliteStore::open(
            directory.path().join("action-scheduling.sqlite3"),
            SqliteStoreConfig {
                role: SqliteRole::Test,
                ..SqliteStoreConfig::TEST
            },
        )
        .await?,
    );
    let scope = EnvironmentScope::new(ProjectId::generate(), EnvironmentId::generate());
    let source = r#"
        export default async (ctx, value) =>
          await ctx.scheduler.runAfter(0, "jobs.send", value, { idempotencyKey: "delivery-42" });
    "#;
    let request = action_request(scope, source, FunctionVisibility::Internal)?;
    let release = request.release_id();
    let executor = ActionExecutor::new(
        RuntimeSupervisor::start(RuntimeLimits::builder(1, 8).build()?)?,
        store.clone(),
    );
    let first = executor
        .execute(request.clone())
        .await
        .map_err(|error| format!("first Action failed: {error:?}"))?;
    let second = executor
        .execute(request)
        .await
        .map_err(|error| format!("second Action failed: {error:?}"))?;
    assert_eq!(first.value, second.value);
    assert_eq!(first.schedules_created, 1);
    assert_eq!(second.schedules_created, 0);
    let CanonicalValue::String(id) = first.value else {
        return Err("expected scheduled ID".into());
    };
    let id: ScheduledInvocationId = id.parse()?;
    let mut snapshot = store
        .begin_read(scope)
        .await
        .map_err(|error| format!("snapshot begin failed: {error:?}"))?;
    let record = snapshot
        .get_scheduled(id)
        .await?
        .ok_or("scheduled Action missing")?;
    snapshot.close().await?;
    assert_eq!(record.pinned_code, PinnedCode::Release(release));
    assert_eq!(record.function.as_str(), "jobs.send");
    assert_eq!(record.args, CanonicalValue::String("payload".to_owned()));
    assert_eq!(record.idempotency_key.as_deref(), Some("delivery-42"));
    let telemetry = executor.telemetry();
    assert_eq!(telemetry.schedules_created, 1);
    assert_eq!(telemetry.schedule_replays, 1);

    let denied = executor
        .execute(action_request(scope, source, FunctionVisibility::Public)?)
        .await;
    assert_eq!(
        denied,
        Err(ActionExecutionError::Runtime(RuntimeError::JavaScript))
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn action_nested_coordinators_are_independent_pinned_and_mutation_is_idempotent()
-> Result<(), Box<dyn Error>> {
    let directory = TempDir::new()?;
    let store = Arc::new(
        SqliteStore::open(
            directory.path().join("action-nested.sqlite3"),
            SqliteStoreConfig::TEST,
        )
        .await?,
    );
    let scope = EnvironmentScope::new(ProjectId::generate(), EnvironmentId::generate());
    let table = TableId::generate();
    let document = DocumentId::generate();
    let runtime = RuntimeSupervisor::start(RuntimeLimits::builder(1, 8).build()?)?;
    let query = QueryExecutor::new(runtime.clone(), store.clone());
    let mutation = MutationExecutor::new(runtime.clone(), store.clone());
    let executor =
        ActionExecutor::new(runtime, store.clone()).with_nested_executors(query, mutation);
    let request = nested_action_request(scope, table, document)?;
    let first = executor.execute(request.clone()).await?;
    let replay = executor.execute(request).await?;
    let expected = CanonicalValue::Array(vec![
        CanonicalValue::String("written".to_owned()),
        CanonicalValue::String("child-action".to_owned()),
        CanonicalValue::String("written".to_owned()),
    ]);
    assert_eq!(first.value, expected);
    assert_eq!(replay.value, expected);
    let export = store.export_environment(scope).await?;
    assert_eq!(export.documents.len(), 1);
    assert_eq!(export.documents[0].table_id, table);
    assert_eq!(export.documents[0].document_id, document);
    assert_eq!(
        export.documents[0].value,
        CanonicalValue::String("written".to_owned())
    );
    assert_eq!(export.outbox.len(), 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nested_action_preserves_dev_revision_pin_for_durable_scheduling()
-> Result<(), Box<dyn Error>> {
    let directory = TempDir::new()?;
    let store = Arc::new(
        SqliteStore::open(
            directory.path().join("action-nested-dev-pin.sqlite3"),
            SqliteStoreConfig::TEST,
        )
        .await?,
    );
    let scope = EnvironmentScope::new(ProjectId::generate(), EnvironmentId::generate());
    let revision = DevRevisionId::generate();
    let request = nested_scheduling_action_request(scope)?
        .with_pinned_code(PinnedCode::DevRevision(revision))?;
    let executor = ActionExecutor::new(
        RuntimeSupervisor::start(RuntimeLimits::builder(1, 4).build()?)?,
        store.clone(),
    );
    let outcome = executor.execute(request).await?;
    let CanonicalValue::String(id) = outcome.value else {
        return Err("expected nested schedule ID".into());
    };
    let id: ScheduledInvocationId = id.parse()?;
    let mut snapshot = store.begin_read(scope).await?;
    let scheduled = snapshot
        .get_scheduled(id)
        .await?
        .ok_or("nested schedule missing")?;
    snapshot.close().await?;
    assert_eq!(scheduled.pinned_code, PinnedCode::DevRevision(revision));
    assert_eq!(scheduled.function.as_str(), "jobs.target");
    Ok(())
}

fn action_request(
    scope: EnvironmentScope,
    source: &str,
    target_visibility: FunctionVisibility,
) -> Result<InvocationRequest, Box<dyn Error>> {
    let bundle = SafeEsmBundleV1::from_sources([source])?;
    let artifact: Arc<[u8]> = encode_safe_esm_bundle(&bundle)?.into();
    let release_id = ReleaseId::generate();
    let action_id = FunctionId::generate();
    let implementation_hash = Sha256Digest::of(source.as_bytes());
    let function = |id, name, function_type, visibility, capabilities| FunctionManifest {
        id,
        name,
        function_type,
        visibility,
        auth_policy: AuthPolicy::None,
        runtime_class: RuntimeClass::SafeV8,
        implementation_hash,
        arguments_contract_hash: Sha256Digest::from_bytes([4; 32]),
        result_contract_hash: Sha256Digest::from_bytes([5; 32]),
        capabilities,
    };
    let manifest = ReleaseManifestV1 {
        release_id,
        project_id: scope.project_id(),
        build_id: BuildId::generate(),
        created_at: TimestampMicros::new(1_700_000_000_000_000),
        runtime_version: "platform-js-1".parse()?,
        artifact: bundle.descriptor()?,
        function_contract_hash: Sha256Digest::from_bytes([1; 32]),
        schema_contract_hash: Sha256Digest::from_bytes([2; 32]),
        index_contract_hash: Sha256Digest::from_bytes([3; 32]),
        functions: vec![
            function(
                FunctionId::generate(),
                "jobs.send".parse()?,
                FunctionType::Action,
                target_visibility,
                Vec::new(),
            ),
            function(
                action_id,
                "tests.action".parse()?,
                FunctionType::Action,
                FunctionVisibility::Public,
                vec![Capability::SchedulerCreate],
            ),
        ],
        cron_definitions: Vec::new(),
    };
    Ok(InvocationRequest::new(
        scope,
        release_id,
        RequestId::generate(),
        InvocationId::generate(),
        action_id,
        Arc::new(manifest),
        artifact,
        CanonicalValue::String("payload".to_owned()),
        Duration::from_secs(2),
        CancellationToken::new(),
    )?)
}

#[allow(clippy::too_many_lines)]
fn nested_action_request(
    scope: EnvironmentScope,
    table: TableId,
    document: DocumentId,
) -> Result<InvocationRequest, Box<dyn Error>> {
    let child_action = "export default (_ctx, value) => value;\n";
    let mutation = format!(
        "export default async (ctx, value) => {{ await ctx.db.insert(\"{table}\", \"{document}\", value); return value; }};\n"
    );
    let query = format!(
        "export default async (ctx) => (await ctx.db.get(\"{table}\", \"{document}\")).value;\n"
    );
    let parent = r#"
      export default async (ctx) => {
        const written = await ctx.runMutation("mutations.write", "written");
        const child = await ctx.runAction("actions.child", "child-action");
        const observed = await ctx.runQuery("queries.read", null);
        return [written, child, observed];
      };
    "#;
    let bundle =
        SafeEsmBundleV1::from_sources([child_action, mutation.as_str(), query.as_str(), parent])?;
    let artifact: Arc<[u8]> = encode_safe_esm_bundle(&bundle)?.into();
    let release_id = ReleaseId::generate();
    let child_id = FunctionId::generate();
    let mutation_id = FunctionId::generate();
    let query_id = FunctionId::generate();
    let parent_id = FunctionId::generate();
    let function = |id,
                    name: &str,
                    source: &str,
                    function_type,
                    visibility,
                    capabilities|
     -> Result<_, Box<dyn Error>> {
        Ok(FunctionManifest {
            id,
            name: name.parse()?,
            function_type,
            visibility,
            auth_policy: AuthPolicy::None,
            runtime_class: RuntimeClass::SafeV8,
            implementation_hash: Sha256Digest::of(source.as_bytes()),
            arguments_contract_hash: Sha256Digest::from_bytes([4; 32]),
            result_contract_hash: Sha256Digest::from_bytes([5; 32]),
            capabilities,
        })
    };
    let manifest = ReleaseManifestV1 {
        release_id,
        project_id: scope.project_id(),
        build_id: BuildId::generate(),
        created_at: TimestampMicros::new(1_700_000_000_000_000),
        runtime_version: "platform-js-1".parse()?,
        artifact: bundle.descriptor()?,
        function_contract_hash: Sha256Digest::from_bytes([1; 32]),
        schema_contract_hash: Sha256Digest::from_bytes([2; 32]),
        index_contract_hash: Sha256Digest::from_bytes([3; 32]),
        functions: vec![
            function(
                child_id,
                "actions.child",
                child_action,
                FunctionType::Action,
                FunctionVisibility::Internal,
                Vec::new(),
            )?,
            function(
                mutation_id,
                "mutations.write",
                &mutation,
                FunctionType::Mutation,
                FunctionVisibility::Internal,
                vec![Capability::DbWrite],
            )?,
            function(
                query_id,
                "queries.read",
                &query,
                FunctionType::Query,
                FunctionVisibility::Internal,
                vec![Capability::DbRead],
            )?,
            function(
                parent_id,
                "tests.action",
                parent,
                FunctionType::Action,
                FunctionVisibility::Public,
                vec![
                    Capability::FunctionQuery,
                    Capability::FunctionMutation,
                    Capability::FunctionAction,
                ],
            )?,
        ],
        cron_definitions: Vec::new(),
    };
    Ok(InvocationRequest::new(
        scope,
        release_id,
        RequestId::generate(),
        InvocationId::generate(),
        parent_id,
        Arc::new(manifest),
        artifact,
        CanonicalValue::Null,
        Duration::from_secs(2),
        CancellationToken::new(),
    )?)
}

fn nested_scheduling_action_request(
    scope: EnvironmentScope,
) -> Result<InvocationRequest, Box<dyn Error>> {
    let child = r#"
      export default async (ctx) =>
        ctx.scheduler.runAfter(0n, "jobs.target", null, { idempotencyKey: "nested-dev" });
    "#;
    let target = "export default () => null;\n";
    let parent = "export default async (ctx) => ctx.runAction('actions.child', null);\n";
    let bundle = SafeEsmBundleV1::from_sources([child, target, parent])?;
    let artifact: Arc<[u8]> = encode_safe_esm_bundle(&bundle)?.into();
    let release_id = ReleaseId::generate();
    let child_id = FunctionId::generate();
    let target_id = FunctionId::generate();
    let parent_id = FunctionId::generate();
    let function =
        |id, name: &str, source: &str, visibility, capabilities| -> Result<_, Box<dyn Error>> {
            Ok(FunctionManifest {
                id,
                name: name.parse()?,
                function_type: FunctionType::Action,
                visibility,
                auth_policy: AuthPolicy::None,
                runtime_class: RuntimeClass::SafeV8,
                implementation_hash: Sha256Digest::of(source.as_bytes()),
                arguments_contract_hash: Sha256Digest::from_bytes([4; 32]),
                result_contract_hash: Sha256Digest::from_bytes([5; 32]),
                capabilities,
            })
        };
    let manifest = ReleaseManifestV1 {
        release_id,
        project_id: scope.project_id(),
        build_id: BuildId::generate(),
        created_at: TimestampMicros::new(1_700_000_000_000_000),
        runtime_version: "platform-js-1".parse()?,
        artifact: bundle.descriptor()?,
        function_contract_hash: Sha256Digest::from_bytes([1; 32]),
        schema_contract_hash: Sha256Digest::from_bytes([2; 32]),
        index_contract_hash: Sha256Digest::from_bytes([3; 32]),
        functions: vec![
            function(
                child_id,
                "actions.child",
                child,
                FunctionVisibility::Internal,
                vec![Capability::SchedulerCreate],
            )?,
            function(
                target_id,
                "jobs.target",
                target,
                FunctionVisibility::Internal,
                Vec::new(),
            )?,
            function(
                parent_id,
                "tests.parent",
                parent,
                FunctionVisibility::Public,
                vec![Capability::FunctionAction],
            )?,
        ],
        cron_definitions: Vec::new(),
    };
    Ok(InvocationRequest::new(
        scope,
        release_id,
        RequestId::generate(),
        InvocationId::generate(),
        parent_id,
        Arc::new(manifest),
        artifact,
        CanonicalValue::Null,
        Duration::from_secs(2),
        CancellationToken::new(),
    )?)
}

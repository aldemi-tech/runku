//! Declarative source build and immutable output conformance.

use std::{
    error::Error,
    path::Path,
    sync::{Arc, Barrier},
};

use runku_build::{BuildError, BuildMetadata, build_project, source_fingerprint};
use runku_contracts::{Contract, decode_contract, decode_document_schema};
use runku_core::{BuildId, ProjectId, ReleaseId};
use runku_releases::{
    ArtifactFormat, Capability, FunctionType, RuntimeClass, decode_node_esm_bundle,
    decode_release_manifest, decode_safe_esm_bundle,
};
use runku_schema::decode_schema_catalog;
use runku_value::TimestampMicros;
use tempfile::tempdir;
use ulid::Ulid;

type TestResult = Result<(), Box<dyn Error>>;

const SCHEMA: &str = r#"
import { defineSchema, defineTable } from "@runku/server"
import { message } from "./model"
export default defineSchema({
  messages: defineTable(message).index("by_rank", ["rank"]),
})
"#;

const MODEL: &str = r#"
import { v } from "@runku/server"
export const body = v.string({ minBytes: 1, maxBytes: 200 })
export const message = v.object({
  body,
  rank: v.int64({ minimum: 0, maximum: 100 }),
  raw: v.optional(v.bytes({ maxBytes: 16 })),
})
export const insertArguments = v.object({
  documentId: v.documentId("messages"),
  value: message,
})
export const echoInput = v.pick(message, ["body"])
"#;

const FUNCTIONS: &str = r#"
import { mutation, query, v } from "@runku/server"
import schema from "./schema"
import { echoInput, insertArguments, message } from "./model"
export const echo = query({
  auth: "none", visibility: "public", capabilities: ["db:read"],
  args: echoInput, returns: v.string({ minBytes: 1, maxBytes: 200 }),
  async handler(_ctx, input) { return input.body },
})
export const insert = mutation({
  auth: "none", visibility: "internal",
  capabilities: ["db:read", "db:write", "scheduler:create"],
  args: insertArguments, returns: message,
  async handler(ctx, input) {
    await ctx.db.insert(schema.tables.messages, input.documentId, input.value)
    return input.value
  },
})
"#;

const CRONS: &str = r#"
import { cron, value } from "@runku/server"
export const hourly = cron({
  schedule: "0 * * * *",
  function: "functions.insert",
  args: {
    documentId: value.id("doc_00000000000000000000000001"),
    value: { body: "scheduled", rank: value.int64(1n), raw: value.bytes([1, 2, 3]) },
  },
})
"#;

fn metadata(seed: u128) -> BuildMetadata {
    BuildMetadata {
        release_id: ReleaseId::from_ulid(Ulid::from(seed)),
        build_id: BuildId::from_ulid(Ulid::from(seed + 1)),
        created_at: TimestampMicros::new(1_800_000_000_000_000),
    }
}

fn project() -> ProjectId {
    ProjectId::from_ulid(Ulid::from(99_u128))
}

fn prepare(root: &Path) -> TestResult {
    std::fs::create_dir(root.join(".runku"))?;
    std::fs::create_dir(root.join("runku"))?;
    std::fs::write(root.join("runku/schema.ts"), SCHEMA)?;
    std::fs::write(root.join("runku/model.ts"), MODEL)?;
    std::fs::write(root.join("runku/functions.ts"), FUNCTIONS)?;
    std::fs::write(root.join("runku/crons.ts"), CRONS)?;
    Ok(())
}

#[test]
fn declarations_generate_functions_contracts_schema_indexes_and_shared_module() -> TestResult {
    let directory = tempdir()?;
    prepare(directory.path())?;
    let output = build_project(
        directory.path(),
        Path::new("runku"),
        project(),
        metadata(10),
    )?;
    let manifest = decode_release_manifest(&std::fs::read(&output.manifest_path)?)?;
    let artifact = std::fs::read(&output.artifact_path)?;
    let bundle = decode_safe_esm_bundle(&artifact)?;

    assert_eq!(manifest.functions.len(), 2);
    assert_eq!(manifest.runtime_version.as_str(), "runku-js-1");
    assert_eq!(manifest.cron_definitions.len(), 1);
    assert_eq!(manifest.cron_definitions[0].name.as_str(), "crons.hourly");
    assert_eq!(manifest.functions[0].name.as_str(), "functions.echo");
    assert_eq!(manifest.functions[1].name.as_str(), "functions.insert");
    assert_eq!(manifest.functions[0].function_type, FunctionType::Query);
    assert_eq!(manifest.functions[1].function_type, FunctionType::Mutation);
    assert_eq!(manifest.functions[0].capabilities, vec![Capability::DbRead]);
    assert_eq!(
        manifest.functions[1].capabilities,
        vec![
            Capability::DbRead,
            Capability::DbWrite,
            Capability::SchedulerCreate
        ]
    );
    assert_eq!(
        manifest.functions[0].implementation_hash,
        manifest.functions[1].implementation_hash
    );
    let implementation = bundle
        .source(manifest.functions[0].implementation_hash)
        .ok_or("missing implementation")?;
    assert!(implementation.contains("export const echo"));
    assert!(implementation.contains("export const insert"));
    assert!(!implementation.contains("import "));

    let schema = bundle
        .resource(manifest.schema_contract_hash)
        .ok_or("missing schema")?;
    let schema = decode_document_schema(schema.as_bytes())?;
    assert_eq!(schema.tables.len(), 1);
    assert_eq!(schema.tables[0].name, "messages");
    let indexes = bundle
        .resource(manifest.index_contract_hash)
        .ok_or("missing index catalog")?;
    let indexes = decode_schema_catalog(indexes.as_bytes())?;
    assert_eq!(indexes.indexes().len(), 1);
    assert_eq!(indexes.indexes()[0].name(), "by_rank");
    for function in &manifest.functions {
        decode_contract(
            bundle
                .resource(function.arguments_contract_hash)
                .ok_or("missing args")?
                .as_bytes(),
        )?;
        decode_contract(
            bundle
                .resource(function.result_contract_hash)
                .ok_or("missing result")?
                .as_bytes(),
        )?;
    }
    let insert_arguments = decode_contract(
        bundle
            .resource(manifest.functions[1].arguments_contract_hash)
            .ok_or("missing insert arguments")?
            .as_bytes(),
    )?;
    let Contract::Object { fields, .. } = insert_arguments else {
        return Err("insert arguments are not an object".into());
    };
    assert_eq!(
        fields["documentId"],
        Contract::DocumentId {
            table: "messages".to_owned()
        }
    );
    let generated = String::from_utf8(std::fs::read(output.generated_types_path)?)?;
    assert!(generated.contains("readonly \"functions.echo\""));
    assert!(generated.contains("readonly \"messages\""));
    assert!(generated.contains("readonly \"by_rank\""));
    assert!(generated.contains("DocumentId<\"messages\">"));
    assert_eq!(
        generated,
        String::from_utf8(std::fs::read(output.stable_generated_types_path)?)?
    );
    assert_eq!(
        output.source_fingerprint,
        source_fingerprint(directory.path(), Path::new("runku"))?
    );
    Ok(())
}

#[test]
fn fingerprint_tracks_add_change_remove_and_is_stable() -> TestResult {
    let directory = tempdir()?;
    prepare(directory.path())?;
    let initial = source_fingerprint(directory.path(), Path::new("runku"))?;
    assert_eq!(
        initial,
        source_fingerprint(directory.path(), Path::new("runku"))?
    );
    std::fs::write(
        directory.path().join("runku/helper.ts"),
        "export const n = 1",
    )?;
    let added = source_fingerprint(directory.path(), Path::new("runku"))?;
    assert_ne!(initial, added);
    std::fs::write(
        directory.path().join("runku/helper.ts"),
        "export const n = 2",
    )?;
    assert_ne!(
        added,
        source_fingerprint(directory.path(), Path::new("runku"))?
    );
    std::fs::remove_file(directory.path().join("runku/helper.ts"))?;
    assert_eq!(
        initial,
        source_fingerprint(directory.path(), Path::new("runku"))?
    );
    Ok(())
}

#[test]
fn invalid_declarations_imports_paths_and_node_fail_closed() -> TestResult {
    let cases = [
        FUNCTIONS.replace(
            "capabilities: [\"db:read\"]",
            "capabilities: [\"db:write\"]",
        ),
        FUNCTIONS.replace("args: echoInput", "args: dynamicValidator()"),
        FUNCTIONS.replace("auth: \"none\"", "auth: process.env.AUTH"),
        FUNCTIONS.replace(
            "import schema",
            "import lodash from \"lodash\"\nimport schema",
        ),
        format!("\"use runku node\"\n{FUNCTIONS}"),
    ];
    for (index, source) in cases.into_iter().enumerate() {
        let directory = tempdir()?;
        prepare(directory.path())?;
        std::fs::write(directory.path().join("runku/functions.ts"), source)?;
        let result = build_project(
            directory.path(),
            Path::new("runku"),
            project(),
            metadata(100 + index as u128 * 2),
        );
        assert!(matches!(
            result,
            Err(BuildError::InvalidConfig | BuildError::SourcePolicy | BuildError::Unsupported)
        ));
    }
    let directory = tempdir()?;
    prepare(directory.path())?;
    std::fs::write(
        directory.path().join("runku/model.ts"),
        MODEL.replace("v.documentId(\"messages\")", "v.documentId(\"missing\")"),
    )?;
    assert_eq!(
        build_project(
            directory.path(),
            Path::new("runku"),
            project(),
            metadata(175),
        ),
        Err(BuildError::InvalidConfig),
    );
    for (index, crons) in [
        CRONS.replace("functions.insert", "missing.target"),
        CRONS.replace("value.int64(1n)", "value.int64(101n)"),
        CRONS.replace("0 * * * *", "not-a-schedule"),
    ]
    .into_iter()
    .enumerate()
    {
        let directory = tempdir()?;
        prepare(directory.path())?;
        std::fs::write(directory.path().join("runku/crons.ts"), crons)?;
        assert_eq!(
            build_project(
                directory.path(),
                Path::new("runku"),
                project(),
                metadata(180 + index as u128 * 2),
            ),
            Err(BuildError::InvalidConfig),
        );
    }
    let node_action = r#"
"use runku node"
import { action, v } from "@runku/server"
import fs from "node:fs"
export const work = action({
  auth: "service", visibility: "internal", capabilities: [],
  args: v.null(), returns: v.null(), handler() { return null },
})
"#;
    let directory = tempdir()?;
    prepare(directory.path())?;
    std::fs::write(directory.path().join("runku/functions.ts"), node_action)?;
    std::fs::remove_file(directory.path().join("runku/crons.ts"))?;
    let output = build_project(
        directory.path(),
        Path::new("runku"),
        project(),
        metadata(195),
    )?;
    let manifest = decode_release_manifest(&std::fs::read(output.manifest_path)?)?;
    let artifact = std::fs::read(output.artifact_path)?;
    let bundle = decode_node_esm_bundle(&artifact)?;
    assert_eq!(manifest.runtime_version.as_str(), "runku-node-1");
    assert_eq!(manifest.artifact.format, ArtifactFormat::NodeEsmBundleV1);
    assert_eq!(manifest.functions[0].runtime_class, RuntimeClass::FullNode);
    let source = bundle
        .source(manifest.functions[0].implementation_hash)
        .ok_or("missing Node implementation")?;
    assert!(source.contains("createRequire"));
    assert!(source.contains("node:fs"));
    bundle.verify_manifest(&manifest, &artifact)?;
    assert_eq!(
        build_project(Path::new("/"), Path::new("runku"), project(), metadata(200)),
        Err(BuildError::InvalidPath)
    );
    Ok(())
}

#[test]
fn node_directive_selects_the_complete_module_graph_and_mixed_release() -> TestResult {
    let directory = tempdir()?;
    prepare(directory.path())?;
    std::fs::remove_file(directory.path().join("runku/functions.ts"))?;
    std::fs::remove_file(directory.path().join("runku/crons.ts"))?;
    std::fs::write(
        directory.path().join("runku/shared.ts"),
        "export const decorate = (value: string) => `shared:${value}`\n",
    )?;
    std::fs::write(
        directory.path().join("runku/node-digest.ts"),
        r#"
import { createHash } from "node:crypto"
export const digest = (value: string) => createHash("sha256").update(value).digest("hex")
"#,
    )?;
    std::fs::write(
        directory.path().join("runku/safe.ts"),
        r#"
import { action, v } from "@runku/server"
import { decorate } from "./shared.js"
export const echo = action({
  auth: "none", visibility: "internal", capabilities: [],
  args: v.string(), returns: v.string(),
  handler(_ctx, input) { return decorate(input) },
})
"#,
    )?;
    std::fs::write(
        directory.path().join("runku/node.ts"),
        r#"
"use runku node"
import { action, v } from "@runku/server"
import { decorate } from "./shared.js"
import { digest as nodeDigest } from "./node-digest.js"
export const digest = action({
  auth: "none", visibility: "public", capabilities: ["function:action"],
  args: v.string(), returns: v.string(),
  async handler(ctx, input) {
    const safe = await ctx.runAction("safe.echo", decorate(input))
    return nodeDigest(String(safe))
  },
})
"#,
    )?;

    let output = build_project(
        directory.path(),
        Path::new("runku"),
        project(),
        metadata(205),
    )?;
    let manifest = decode_release_manifest(&std::fs::read(output.manifest_path)?)?;
    let artifact = std::fs::read(output.artifact_path)?;
    let bundle = decode_node_esm_bundle(&artifact)?;
    assert_eq!(manifest.runtime_version.as_str(), "runku-hybrid-1");
    assert_eq!(manifest.artifact.format, ArtifactFormat::NodeEsmBundleV1);
    assert!(
        manifest
            .functions
            .iter()
            .any(|function| function.runtime_class == RuntimeClass::SafeV8)
    );
    let node = manifest
        .functions
        .iter()
        .find(|function| function.name.as_str() == "node.digest")
        .ok_or("missing Node function")?;
    assert_eq!(node.runtime_class, RuntimeClass::FullNode);
    let implementation = bundle
        .source(node.implementation_hash)
        .ok_or("missing Node implementation")?;
    assert!(implementation.contains("node:crypto"));
    assert!(implementation.contains("shared:"));
    bundle.verify_manifest(&manifest, &artifact)?;
    Ok(())
}

#[test]
fn runtime_module_graph_crossings_fail_closed() -> TestResult {
    let safe_imports_node = (
        r#"
import { action, v } from "@runku/server"
import { nodeValue } from "./node-helper.js"
export const work = action({
  auth: "none", visibility: "public", capabilities: [],
  args: v.null(), returns: v.string(), handler() { return nodeValue },
})
"#,
        r#"
import { randomUUID } from "node:crypto"
export const nodeValue = randomUUID()
"#,
    );
    let node_imports_safe_function = (
        r#"
"use runku node"
import { action, v } from "@runku/server"
import { helper } from "./node-helper.js"
export const work = action({
  auth: "none", visibility: "public", capabilities: [],
  args: v.null(), returns: v.string(), handler() { return String(helper) },
})
"#,
        r#"
import { action, v } from "@runku/server"
export const helper = action({
  auth: "none", visibility: "internal", capabilities: [],
  args: v.null(), returns: v.string(), handler() { return "safe" },
})
"#,
    );

    for (index, (entry, helper)) in [safe_imports_node, node_imports_safe_function]
        .into_iter()
        .enumerate()
    {
        let directory = tempdir()?;
        prepare(directory.path())?;
        std::fs::write(directory.path().join("runku/functions.ts"), entry)?;
        std::fs::write(directory.path().join("runku/node-helper.ts"), helper)?;
        std::fs::remove_file(directory.path().join("runku/crons.ts"))?;
        assert_eq!(
            build_project(
                directory.path(),
                Path::new("runku"),
                project(),
                metadata(215 + index as u128 * 2),
            ),
            Err(BuildError::SourcePolicy)
        );
    }
    Ok(())
}

#[cfg(unix)]
#[test]
fn source_symlinks_are_rejected() -> TestResult {
    use std::os::unix::fs::symlink;
    let directory = tempdir()?;
    prepare(directory.path())?;
    let outside = tempdir()?;
    std::fs::write(outside.path().join("outside.ts"), FUNCTIONS)?;
    symlink(
        outside.path().join("outside.ts"),
        directory.path().join("runku/outside.ts"),
    )?;
    assert_eq!(
        build_project(
            directory.path(),
            Path::new("runku"),
            project(),
            metadata(210)
        ),
        Err(BuildError::InvalidPath)
    );
    Ok(())
}

#[test]
fn output_is_replayable_conflict_safe_and_concurrent() -> TestResult {
    let directory = tempdir()?;
    prepare(directory.path())?;
    let selected = metadata(300);
    let first = build_project(directory.path(), Path::new("runku"), project(), selected)?;
    let replay = build_project(directory.path(), Path::new("runku"), project(), selected)?;
    assert!(replay.replayed);
    assert_eq!(first.manifest_digest, replay.manifest_digest);
    std::fs::write(&first.artifact_path, b"tampered")?;
    assert_eq!(
        build_project(directory.path(), Path::new("runku"), project(), selected),
        Err(BuildError::Conflict)
    );

    let concurrent = tempdir()?;
    prepare(concurrent.path())?;
    let root = Arc::new(concurrent.path().to_path_buf());
    let barrier = Arc::new(Barrier::new(2));
    let handles = (0..2)
        .map(|_| {
            let root = Arc::clone(&root);
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                build_project(&root, Path::new("runku"), project(), metadata(400))
            })
        })
        .collect::<Vec<_>>();
    let results = handles
        .into_iter()
        .map(|handle| handle.join().map_err(|_| "thread panic"))
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 2);
    assert_eq!(
        results
            .iter()
            .filter_map(|result| result.as_ref().ok())
            .filter(|output| output.replayed)
            .count(),
        1
    );
    Ok(())
}

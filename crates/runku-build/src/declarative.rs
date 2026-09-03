use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write,
    path::{Component, Path, PathBuf},
    sync::Arc,
};

use deno_ast::{
    MediaType, ModuleSpecifier, ParseParams, ProgramRef, parse_module,
    swc::ast::{
        Callee, Decl, Expr, ExprOrSpread, KeyValueProp, Lit, ModuleDecl, ModuleItem, ObjectLit,
        Pat, Prop, PropName, PropOrSpread, VarDeclKind,
    },
    swc::ecma_visit::{VisitMut, VisitMutWith},
};
use runku_contracts::{Contract, DocumentSchemaV1, DocumentTableContract, FiniteBound};
use runku_core::{FunctionName, IndexId, ProjectId, TableId};
use runku_releases::{
    AuthPolicy, Capability, CronName, CronSchedule, FunctionType, FunctionVisibility, RuntimeClass,
    Sha256Digest,
};
use runku_schema::{FieldPath, IndexDefinition, SchemaCatalog};
use runku_value::{CanonicalValue, FiniteF64, TimestampMicros, TypedId};
use sha2::{Digest, Sha256};
use ulid::Ulid;

use crate::{BuildError, compiler::transpile_common_js};

const SOURCE_ROOT_DEFAULT: &str = "runku";
const SOURCE_MAX_MODULES: usize = 1_000;
const SOURCE_MAX_TOTAL_BYTES: u64 = 64 * 1024 * 1024;
const SOURCE_MAX_MODULE_BYTES: u64 = 8 * 1024 * 1024;
const PATH_MAX_BYTES: usize = 512;
const FINGERPRINT_DOMAIN: &[u8] = b"RUNKU_DECLARATIVE_SOURCE_V1\0";
const TABLE_ID_DOMAIN: &[u8] = b"RUNKU_TABLE_ID_V1";
const INDEX_ID_DOMAIN: &[u8] = b"RUNKU_INDEX_ID_V1";

type DeclaredIndexes = Vec<(String, Vec<Vec<String>>)>;
type IndexIdMap = BTreeMap<String, BTreeMap<String, String>>;
type SchemaBuild = (
    DocumentSchemaV1,
    SchemaCatalog,
    BTreeMap<String, String>,
    IndexIdMap,
);

pub(crate) struct LoadedConfig {
    pub functions: Vec<LoadedFunction>,
    pub crons: Vec<LoadedCron>,
    pub schema: DocumentSchemaV1,
    pub index_catalog: SchemaCatalog,
    pub fingerprint: Sha256Digest,
}

pub(crate) struct LoadedFunction {
    pub name: FunctionName,
    pub source_path: PathBuf,
    pub source_text: String,
    pub function_type: FunctionType,
    pub visibility: FunctionVisibility,
    pub auth_policy: AuthPolicy,
    pub runtime_class: RuntimeClass,
    pub capabilities: Vec<Capability>,
    pub arguments_contract: Contract,
    pub result_contract: Contract,
}

pub(crate) struct LoadedCron {
    pub name: CronName,
    pub schedule: CronSchedule,
    pub function: FunctionName,
    pub args: CanonicalValue,
}

#[derive(Clone)]
struct FunctionDeclaration {
    export_name: String,
    logical_name: FunctionName,
    function_type: FunctionType,
    visibility: FunctionVisibility,
    auth_policy: AuthPolicy,
    runtime_class: RuntimeClass,
    capabilities: Vec<Capability>,
    arguments_contract: Contract,
    result_contract: Contract,
}

#[derive(Clone)]
struct CronDeclaration {
    name: CronName,
    schedule: CronSchedule,
    function: FunctionName,
    args: CanonicalValue,
}

struct SourceModule {
    id: String,
    path: PathBuf,
    source: String,
    node_runtime: bool,
    common_js: String,
    dependencies: BTreeMap<String, String>,
    functions: Vec<FunctionDeclaration>,
    crons: Vec<CronDeclaration>,
    schema: Option<SchemaDeclaration>,
}

struct SchemaDeclaration {
    tables: Vec<SchemaTableDeclaration>,
}

struct SchemaTableDeclaration {
    name: String,
    contract: Contract,
    indexes: Vec<(String, Vec<Vec<String>>)>,
}

pub(crate) fn load_project(
    root: &Path,
    source_dir: &Path,
    project_id: ProjectId,
) -> Result<LoadedConfig, BuildError> {
    let root = canonical_root(root)?;
    let source_dir = canonical_source_dir(&root, source_dir)?;
    let files = discover_sources(&source_dir)?;
    let mut modules = BTreeMap::new();
    let mut total = 0_u64;
    for path in files {
        let bytes = std::fs::read(&path).map_err(|_| BuildError::Unavailable)?;
        total = total
            .checked_add(u64::try_from(bytes.len()).map_err(|_| BuildError::LimitExceeded)?)
            .ok_or(BuildError::LimitExceeded)?;
        if total > SOURCE_MAX_TOTAL_BYTES {
            return Err(BuildError::LimitExceeded);
        }
        let source = String::from_utf8(bytes).map_err(|_| BuildError::SourceSyntax)?;
        let relative = path
            .strip_prefix(&root)
            .map_err(|_| BuildError::InvalidPath)?;
        let id = path_text(relative)?;
        let module = parse_source_module(&root, &source_dir, path, id.clone(), source)?;
        if modules.insert(id, module).is_some() {
            return Err(BuildError::InvalidConfig);
        }
    }
    resolve_dependencies(&root, &mut modules)?;

    let schema_declarations = modules
        .values()
        .filter_map(|module| module.schema.as_ref())
        .collect::<Vec<_>>();
    if schema_declarations.len() != 1 {
        return Err(BuildError::InvalidConfig);
    }
    let (schema, index_catalog, table_ids, index_ids) =
        build_schema(project_id, schema_declarations[0])?;
    let table_names = schema
        .tables
        .iter()
        .map(|table| table.name.as_str())
        .collect::<BTreeSet<_>>();
    for module in modules.values() {
        for function in &module.functions {
            validate_document_id_references(&function.arguments_contract, &table_names)?;
            validate_document_id_references(&function.result_contract, &table_names)?;
        }
    }

    let mut functions = Vec::new();
    let mut names = BTreeSet::new();
    for module in modules.values() {
        if module.functions.is_empty() {
            continue;
        }
        let implementation = compile_runtime_module(module, &modules, &table_ids, &index_ids)?;
        for declaration in &module.functions {
            if !names.insert(declaration.logical_name.clone()) {
                return Err(BuildError::InvalidConfig);
            }
            functions.push(LoadedFunction {
                name: declaration.logical_name.clone(),
                source_path: module.path.clone(),
                source_text: implementation.clone(),
                function_type: declaration.function_type,
                visibility: declaration.visibility,
                auth_policy: declaration.auth_policy,
                runtime_class: declaration.runtime_class,
                capabilities: declaration.capabilities.clone(),
                arguments_contract: declaration.arguments_contract.clone(),
                result_contract: declaration.result_contract.clone(),
            });
        }
    }
    functions.sort_by(|left, right| left.name.cmp(&right.name));
    if functions.is_empty() {
        return Err(BuildError::InvalidConfig);
    }
    let mut cron_names = BTreeSet::new();
    let mut crons = modules
        .values()
        .flat_map(|module| module.crons.iter())
        .map(|declaration| {
            if !cron_names.insert(declaration.name.clone())
                || !names.contains(&declaration.function)
            {
                return Err(BuildError::InvalidConfig);
            }
            Ok(LoadedCron {
                name: declaration.name.clone(),
                schedule: declaration.schedule.clone(),
                function: declaration.function.clone(),
                args: declaration.args.clone(),
            })
        })
        .collect::<Result<Vec<_>, BuildError>>()?;
    crons.sort_by(|left, right| left.name.cmp(&right.name));
    let fingerprint = fingerprint_modules(&root, &source_dir, modules.values())?;
    Ok(LoadedConfig {
        functions,
        crons,
        schema,
        index_catalog,
        fingerprint,
    })
}

fn validate_document_id_references(
    contract: &Contract,
    tables: &BTreeSet<&str>,
) -> Result<(), BuildError> {
    match contract {
        Contract::DocumentId { table } if !tables.contains(table.as_str()) => {
            Err(BuildError::InvalidConfig)
        }
        Contract::Array { items, .. } => validate_document_id_references(items, tables),
        Contract::Object { fields, .. } => fields
            .values()
            .try_for_each(|field| validate_document_id_references(field, tables)),
        Contract::Union { variants } => variants
            .iter()
            .try_for_each(|variant| validate_document_id_references(variant, tables)),
        _ => Ok(()),
    }
}

pub(crate) fn input_fingerprint(
    root: &Path,
    source_dir: &Path,
) -> Result<Sha256Digest, BuildError> {
    let root = canonical_root(root)?;
    let source_dir = canonical_source_dir(&root, source_dir)?;
    let files = discover_sources(&source_dir)?;
    let mut hash = Sha256::new();
    hash.update(FINGERPRINT_DOMAIN);
    hash_field(
        &mut hash,
        source_dir
            .strip_prefix(&root)
            .map_err(|_| BuildError::InvalidPath)?
            .as_os_str()
            .as_encoded_bytes(),
    )?;
    hash.update(
        u32::try_from(files.len())
            .map_err(|_| BuildError::LimitExceeded)?
            .to_be_bytes(),
    );
    let mut total = 0_u64;
    for path in files {
        let relative = path
            .strip_prefix(&source_dir)
            .map_err(|_| BuildError::InvalidPath)?;
        let bytes = std::fs::read(&path).map_err(|_| BuildError::Unavailable)?;
        total = total
            .checked_add(u64::try_from(bytes.len()).map_err(|_| BuildError::LimitExceeded)?)
            .ok_or(BuildError::LimitExceeded)?;
        if total > SOURCE_MAX_TOTAL_BYTES {
            return Err(BuildError::LimitExceeded);
        }
        hash_field(&mut hash, relative.as_os_str().as_encoded_bytes())?;
        hash_field(&mut hash, &bytes)?;
    }
    Ok(Sha256Digest::from_bytes(hash.finalize().into()))
}

#[allow(clippy::too_many_lines)]
fn parse_source_module(
    root: &Path,
    source_root: &Path,
    path: PathBuf,
    id: String,
    source: String,
) -> Result<SourceModule, BuildError> {
    let specifier = ModuleSpecifier::from_file_path(&path).map_err(|()| BuildError::InvalidPath)?;
    let parsed = parse_module(ParseParams {
        specifier,
        text: Arc::<str>::from(source.as_str()),
        media_type: media_type(&path)?,
        capture_tokens: false,
        scope_analysis: true,
        maybe_syntax: None,
    })
    .map_err(|_| BuildError::SourceSyntax)?;
    let ProgramRef::Module(program) = parsed.program_ref() else {
        return Err(BuildError::SourcePolicy);
    };
    let node_runtime = program.body.first().is_some_and(is_node_directive);
    if !node_runtime && program.body.iter().any(is_node_directive) {
        return Err(BuildError::InvalidConfig);
    }
    let mut constants = BTreeMap::new();
    let mut imports = Vec::new();
    let mut sdk_bindings = BTreeSet::new();
    for item in &program.body {
        match item {
            ModuleItem::ModuleDecl(ModuleDecl::Import(import)) => {
                if import.with.is_some()
                    || !matches!(import.phase, deno_ast::swc::ast::ImportPhase::Evaluation)
                {
                    return Err(BuildError::SourcePolicy);
                }
                if !import.type_only {
                    let specifier = import
                        .src
                        .value
                        .as_str()
                        .ok_or(BuildError::SourcePolicy)?
                        .to_owned();
                    if specifier == "@runku/server" {
                        for binding in &import.specifiers {
                            let deno_ast::swc::ast::ImportSpecifier::Named(binding) = binding
                            else {
                                return Err(BuildError::SourcePolicy);
                            };
                            if binding.is_type_only {
                                continue;
                            }
                            let imported = match binding.imported.as_ref() {
                                None => binding.local.sym.as_ref(),
                                Some(deno_ast::swc::ast::ModuleExportName::Ident(name)) => {
                                    name.sym.as_ref()
                                }
                                Some(deno_ast::swc::ast::ModuleExportName::Str(_)) => {
                                    return Err(BuildError::SourcePolicy);
                                }
                            };
                            if imported != binding.local.sym.as_ref()
                                || !matches!(
                                    imported,
                                    "query"
                                        | "mutation"
                                        | "action"
                                        | "cron"
                                        | "value"
                                        | "v"
                                        | "defineSchema"
                                        | "defineTable"
                                )
                                || !sdk_bindings.insert(imported.to_owned())
                            {
                                return Err(BuildError::SourcePolicy);
                            }
                        }
                    } else if specifier.starts_with('.') {
                        for binding in &import.specifiers {
                            let deno_ast::swc::ast::ImportSpecifier::Named(binding) = binding
                            else {
                                continue;
                            };
                            if binding.is_type_only {
                                continue;
                            }
                            let imported = match binding.imported.as_ref() {
                                None => binding.local.sym.as_ref(),
                                Some(deno_ast::swc::ast::ModuleExportName::Ident(name)) => {
                                    name.sym.as_ref()
                                }
                                Some(deno_ast::swc::ast::ModuleExportName::Str(_)) => {
                                    return Err(BuildError::SourcePolicy);
                                }
                            };
                            let mut visited = BTreeSet::new();
                            let expression = load_exported_constant(
                                source_root,
                                &path,
                                &specifier,
                                imported,
                                0,
                                &mut visited,
                            );
                            let expression = match expression {
                                Ok(expression) => expression,
                                // Relative imports serve both static declaration composition and
                                // ordinary runtime code. A value whose graph reaches a Node/npm
                                // import cannot be inlined as schema metadata, but remains a valid
                                // runtime binding; entrypoint graph policy validates it later.
                                Err(BuildError::SourcePolicy) => continue,
                                Err(error) => return Err(error),
                            };
                            if constants
                                .insert(binding.local.sym.to_string(), Box::new(expression))
                                .is_some()
                            {
                                return Err(BuildError::SourcePolicy);
                            }
                        }
                    }
                    imports.push(specifier);
                }
            }
            ModuleItem::Stmt(deno_ast::swc::ast::Stmt::Decl(Decl::Var(variables)))
            | ModuleItem::ModuleDecl(ModuleDecl::ExportDecl(deno_ast::swc::ast::ExportDecl {
                decl: Decl::Var(variables),
                ..
            })) if variables.kind == VarDeclKind::Const => {
                for declaration in &variables.decls {
                    if let (Pat::Ident(name), Some(value)) = (&declaration.name, &declaration.init)
                    {
                        constants.insert(name.id.sym.to_string(), value.clone());
                    }
                }
            }
            _ => {}
        }
    }

    let module_namespace = module_namespace(source_root, &path)?;
    let mut functions = Vec::new();
    let mut crons = Vec::new();
    let mut schema = None;
    for item in &program.body {
        match item {
            ModuleItem::ModuleDecl(ModuleDecl::ExportDecl(export)) => {
                let Decl::Var(variables) = &export.decl else {
                    continue;
                };
                if variables.kind != VarDeclKind::Const {
                    return Err(BuildError::InvalidConfig);
                }
                for declaration in &variables.decls {
                    let Pat::Ident(export) = &declaration.name else {
                        return Err(BuildError::InvalidConfig);
                    };
                    let Some(value) = declaration.init.as_deref() else {
                        return Err(BuildError::InvalidConfig);
                    };
                    if let Some(function_type) = function_call_kind(value) {
                        let export_name = export.id.sym.to_string();
                        let logical = format!("{module_namespace}.{export_name}")
                            .parse()
                            .map_err(|_| BuildError::InvalidConfig)?;
                        functions.push(parse_function(
                            value,
                            export_name,
                            logical,
                            function_type,
                            node_runtime,
                            &constants,
                        )?);
                    } else if is_call_named(value, "cron") {
                        let export_name = export.id.sym.to_string();
                        let logical = format!("{module_namespace}.{export_name}")
                            .parse()
                            .map_err(|_| BuildError::InvalidConfig)?;
                        crons.push(parse_cron(value, logical, &constants)?);
                    }
                }
            }
            ModuleItem::ModuleDecl(ModuleDecl::ExportDefaultExpr(export)) => {
                if schema.is_some() {
                    return Err(BuildError::InvalidConfig);
                }
                schema = parse_schema(export.expr.as_ref(), &constants)?;
            }
            ModuleItem::ModuleDecl(ModuleDecl::ExportNamed(_) | ModuleDecl::ExportAll(_)) => {
                return Err(BuildError::SourcePolicy);
            }
            _ => {}
        }
    }
    if node_runtime
        && (!crons.is_empty()
            || functions
                .iter()
                .any(|function| function.function_type != FunctionType::Action))
    {
        return Err(BuildError::InvalidConfig);
    }
    for function in &functions {
        let helper = match function.function_type {
            FunctionType::Query => "query",
            FunctionType::Mutation => "mutation",
            FunctionType::Action => "action",
        };
        if !sdk_bindings.contains(helper) {
            return Err(BuildError::SourcePolicy);
        }
    }
    if !crons.is_empty() && (!sdk_bindings.contains("cron") || !sdk_bindings.contains("value")) {
        return Err(BuildError::SourcePolicy);
    }
    if let Some(schema) = &schema
        && (!sdk_bindings.contains("defineSchema")
            || (!schema.tables.is_empty() && !sdk_bindings.contains("defineTable")))
    {
        return Err(BuildError::SourcePolicy);
    }
    let common_js = transpile_common_js(&path, &source)?;
    let _ = root;
    Ok(SourceModule {
        id,
        path,
        source,
        node_runtime,
        common_js,
        dependencies: imports
            .into_iter()
            .map(|specifier| (specifier, String::new()))
            .collect(),
        functions,
        crons,
        schema,
    })
}

fn parse_function(
    expression: &Expr,
    export_name: String,
    logical_name: FunctionName,
    function_type: FunctionType,
    node_runtime: bool,
    constants: &BTreeMap<String, Box<Expr>>,
) -> Result<FunctionDeclaration, BuildError> {
    let call = call_expression(expression)?;
    let definition = call.args.first().ok_or(BuildError::InvalidConfig)?;
    if call.args.len() != 1 || definition.spread.is_some() {
        return Err(BuildError::InvalidConfig);
    }
    let object = object_expression(definition.expr.as_ref(), constants)?;
    let (properties, method_handler) = function_properties(object)?;
    let allowed = BTreeSet::from([
        "args",
        "returns",
        "auth",
        "visibility",
        "capabilities",
        "handler",
    ]);
    if properties.keys().any(|key| !allowed.contains(key.as_str()))
        || properties.len() + usize::from(method_handler) != allowed.len()
    {
        return Err(BuildError::InvalidConfig);
    }
    let arguments_contract = parse_validator(required(&properties, "args")?, constants, 0)?;
    let result_contract = parse_validator(required(&properties, "returns")?, constants, 0)?;
    arguments_contract
        .validate_definition()
        .map_err(crate::map_contract)?;
    result_contract
        .validate_definition()
        .map_err(crate::map_contract)?;
    if let Some(handler) = properties.get("handler") {
        ensure_handler(handler)?;
    } else if !method_handler {
        return Err(BuildError::InvalidConfig);
    }
    let auth_policy = match string_literal(required(&properties, "auth")?)? {
        "none" => AuthPolicy::None,
        "optional" => AuthPolicy::Optional,
        "guest" => AuthPolicy::Guest,
        "user" => AuthPolicy::User,
        "service" => AuthPolicy::Service,
        _ => return Err(BuildError::InvalidConfig),
    };
    let visibility = match string_literal(required(&properties, "visibility")?)? {
        "public" => FunctionVisibility::Public,
        "internal" => FunctionVisibility::Internal,
        _ => return Err(BuildError::InvalidConfig),
    };
    let mut capabilities = string_array(required(&properties, "capabilities")?)?
        .into_iter()
        .map(parse_capability)
        .collect::<Result<Vec<_>, _>>()?;
    capabilities.sort();
    if capabilities.windows(2).any(|pair| pair[0] == pair[1])
        || capabilities
            .iter()
            .any(|capability| !capability_allowed(function_type, capability))
    {
        return Err(BuildError::InvalidConfig);
    }
    Ok(FunctionDeclaration {
        export_name,
        logical_name,
        function_type,
        visibility,
        auth_policy,
        runtime_class: if node_runtime {
            RuntimeClass::FullNode
        } else {
            RuntimeClass::SafeV8
        },
        capabilities,
        arguments_contract,
        result_contract,
    })
}

fn parse_cron(
    expression: &Expr,
    name: CronName,
    constants: &BTreeMap<String, Box<Expr>>,
) -> Result<CronDeclaration, BuildError> {
    let call = call_expression(expression)?;
    if call.args.len() != 1 || call.args[0].spread.is_some() {
        return Err(BuildError::InvalidConfig);
    }
    let object = object_expression(call.args[0].expr.as_ref(), constants)?;
    let properties = object_properties(object)?;
    if properties.len() != 3
        || properties
            .keys()
            .any(|key| !matches!(key.as_str(), "schedule" | "function" | "args"))
    {
        return Err(BuildError::InvalidConfig);
    }
    Ok(CronDeclaration {
        name,
        schedule: string_literal(required(&properties, "schedule")?)?
            .parse()
            .map_err(|_| BuildError::InvalidConfig)?,
        function: string_literal(required(&properties, "function")?)?
            .parse()
            .map_err(|_| BuildError::InvalidConfig)?,
        args: parse_canonical_value(required(&properties, "args")?, constants, 0)?,
    })
}

fn parse_canonical_value(
    expression: &Expr,
    constants: &BTreeMap<String, Box<Expr>>,
    depth: usize,
) -> Result<CanonicalValue, BuildError> {
    if depth > 64 {
        return Err(BuildError::LimitExceeded);
    }
    let expression = resolve_expression(expression, constants, depth)?;
    match expression {
        Expr::Lit(Lit::Null(_)) => Ok(CanonicalValue::Null),
        Expr::Lit(Lit::Bool(value)) => Ok(CanonicalValue::Boolean(value.value)),
        Expr::Lit(Lit::Str(value)) => Ok(CanonicalValue::String(
            value
                .value
                .as_str()
                .ok_or(BuildError::InvalidConfig)?
                .to_owned(),
        )),
        Expr::Lit(Lit::Num(value)) => Ok(CanonicalValue::Float64(
            FiniteF64::new(value.value).map_err(|_| BuildError::InvalidConfig)?,
        )),
        Expr::Lit(Lit::BigInt(_)) => Ok(CanonicalValue::Int64(integer_literal(expression)?)),
        Expr::Array(array) => array
            .elems
            .iter()
            .map(|element| {
                let ExprOrSpread { spread: None, expr } =
                    element.as_ref().ok_or(BuildError::InvalidConfig)?
                else {
                    return Err(BuildError::InvalidConfig);
                };
                parse_canonical_value(expr.as_ref(), constants, depth + 1)
            })
            .collect::<Result<Vec<_>, _>>()
            .map(CanonicalValue::Array),
        Expr::Object(object) => object_properties(object)?
            .into_iter()
            .map(|(name, value)| Ok((name, parse_canonical_value(value, constants, depth + 1)?)))
            .collect::<Result<BTreeMap<_, _>, BuildError>>()
            .map(CanonicalValue::Object),
        Expr::Call(call) => parse_canonical_constructor(call, constants, depth),
        _ => Err(BuildError::InvalidConfig),
    }
}

fn parse_canonical_constructor(
    call: &deno_ast::swc::ast::CallExpr,
    constants: &BTreeMap<String, Box<Expr>>,
    depth: usize,
) -> Result<CanonicalValue, BuildError> {
    if call.args.len() != 1 || call.args[0].spread.is_some() {
        return Err(BuildError::InvalidConfig);
    }
    let name = helper_name(call, "value")?;
    let argument = resolve_expression(call.args[0].expr.as_ref(), constants, depth + 1)?;
    match name {
        "int64" => Ok(CanonicalValue::Int64(integer_literal(argument)?)),
        "float64" => Ok(CanonicalValue::Float64(
            FiniteF64::new(number_literal(argument)?).map_err(|_| BuildError::InvalidConfig)?,
        )),
        "timestamp" => Ok(CanonicalValue::Timestamp(TimestampMicros::new(
            integer_literal(argument)?,
        ))),
        "id" => Ok(CanonicalValue::TypedId(
            string_literal(argument)?
                .parse::<TypedId>()
                .map_err(|_| BuildError::InvalidConfig)?,
        )),
        "bytes" => {
            let Expr::Array(array) = argument else {
                return Err(BuildError::InvalidConfig);
            };
            let bytes = array
                .elems
                .iter()
                .map(|element| {
                    let ExprOrSpread { spread: None, expr } =
                        element.as_ref().ok_or(BuildError::InvalidConfig)?
                    else {
                        return Err(BuildError::InvalidConfig);
                    };
                    u8::try_from(integer_literal(expr.as_ref())?)
                        .map_err(|_| BuildError::InvalidConfig)
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(CanonicalValue::Bytes(bytes))
        }
        _ => Err(BuildError::InvalidConfig),
    }
}

fn parse_schema(
    expression: &Expr,
    constants: &BTreeMap<String, Box<Expr>>,
) -> Result<Option<SchemaDeclaration>, BuildError> {
    let expression = resolve_expression(expression, constants, 0)?;
    let Expr::Call(call) = expression else {
        return Ok(None);
    };
    if callee_name(call)? != "defineSchema" || call.args.len() != 1 || call.args[0].spread.is_some()
    {
        return Ok(None);
    }
    let tables = object_expression(call.args[0].expr.as_ref(), constants)?;
    let mut declarations = Vec::new();
    for (name, expression) in object_properties(tables)? {
        let (contract, indexes) = parse_table(expression, constants)?;
        declarations.push(SchemaTableDeclaration {
            name,
            contract,
            indexes,
        });
    }
    declarations.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(Some(SchemaDeclaration {
        tables: declarations,
    }))
}

fn parse_table(
    expression: &Expr,
    constants: &BTreeMap<String, Box<Expr>>,
) -> Result<(Contract, DeclaredIndexes), BuildError> {
    let expression = resolve_expression(expression, constants, 0)?;
    if let Expr::Call(call) = expression
        && let Callee::Expr(callee) = &call.callee
        && let Expr::Member(member) = callee.as_ref()
        && member_property(member) == Some("index")
    {
        if call.args.len() != 2 || call.args.iter().any(|argument| argument.spread.is_some()) {
            return Err(BuildError::InvalidConfig);
        }
        let (contract, mut indexes) = parse_table(member.obj.as_ref(), constants)?;
        let name = string_literal(call.args[0].expr.as_ref())?.to_owned();
        let fields = string_array(call.args[1].expr.as_ref())?
            .into_iter()
            .map(|field| {
                let segments = field.split('.').map(str::to_owned).collect::<Vec<_>>();
                FieldPath::new(segments.clone()).map_err(|_| BuildError::InvalidConfig)?;
                Ok(segments)
            })
            .collect::<Result<Vec<_>, BuildError>>()?;
        indexes.push((name, fields));
        return Ok((contract, indexes));
    }
    let call = call_expression(expression)?;
    if callee_name(call)? != "defineTable" || call.args.len() != 1 || call.args[0].spread.is_some()
    {
        return Err(BuildError::InvalidConfig);
    }
    Ok((
        parse_validator(call.args[0].expr.as_ref(), constants, 0)?,
        Vec::new(),
    ))
}

fn build_schema(
    project_id: ProjectId,
    declaration: &SchemaDeclaration,
) -> Result<SchemaBuild, BuildError> {
    let mut tables = Vec::new();
    let mut indexes = Vec::new();
    let mut table_ids = BTreeMap::new();
    let mut index_ids: IndexIdMap = BTreeMap::new();
    for table in &declaration.tables {
        let table_id = stable_table_id(project_id, &table.name);
        table_ids.insert(table.name.clone(), table_id.to_string());
        tables.push(DocumentTableContract {
            id: table_id,
            name: table.name.clone(),
            document_contract: table.contract.clone(),
        });
        for (name, fields) in &table.indexes {
            let index_id = stable_index_id(project_id, &table.name, name);
            index_ids
                .entry(table.name.clone())
                .or_default()
                .insert(name.clone(), index_id.to_string());
            indexes.push(
                IndexDefinition::new(
                    index_id,
                    table_id,
                    name.clone(),
                    fields
                        .iter()
                        .cloned()
                        .map(FieldPath::new)
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(|_| BuildError::InvalidConfig)?,
                )
                .map_err(|_| BuildError::InvalidConfig)?,
            );
        }
    }
    let schema = DocumentSchemaV1::new(tables).map_err(crate::map_contract)?;
    let catalog = SchemaCatalog::new(project_id, indexes).map_err(|_| BuildError::InvalidConfig)?;
    Ok((schema, catalog, table_ids, index_ids))
}

fn compile_runtime_module(
    entry: &SourceModule,
    modules: &BTreeMap<String, SourceModule>,
    table_ids: &BTreeMap<String, String>,
    index_ids: &BTreeMap<String, BTreeMap<String, String>>,
) -> Result<String, BuildError> {
    let mut reachable = BTreeSet::new();
    collect_reachable(&entry.id, modules, &mut reachable)?;
    for id in &reachable {
        let module = modules.get(id).ok_or(BuildError::Internal)?;
        let imports_node_dependency = module.dependencies.iter().any(|(specifier, target)| {
            specifier != "@runku/server" && !specifier.starts_with('.') && target.is_empty()
        });
        if (!entry.node_runtime && (module.node_runtime || imports_node_dependency))
            || (entry.node_runtime
                && !module.node_runtime
                && !module.functions.is_empty()
                && module.id != entry.id)
        {
            return Err(BuildError::SourcePolicy);
        }
    }
    let mut factories = String::new();
    let mut dependency_map: BTreeMap<&str, &BTreeMap<String, String>> = BTreeMap::new();
    for id in &reachable {
        let module = modules.get(id).ok_or(BuildError::Internal)?;
        if !factories.is_empty() {
            factories.push(',');
        }
        factories.push_str(&serde_json::to_string(id).map_err(|_| BuildError::Internal)?);
        factories.push_str(":function(module,exports,require){\n");
        factories.push_str(&module.common_js);
        factories.push_str("\n}");
        dependency_map.insert(id, &module.dependencies);
    }
    let dependencies = serde_json::to_string(&dependency_map).map_err(|_| BuildError::Internal)?;
    let tables = serde_json::to_string(table_ids).map_err(|_| BuildError::Internal)?;
    let indexes = serde_json::to_string(index_ids).map_err(|_| BuildError::Internal)?;
    let entry_id = serde_json::to_string(&entry.id).map_err(|_| BuildError::Internal)?;
    let native_prelude = if entry.node_runtime {
        "import {createRequire as __createRequire} from \"node:module\";\nconst __nativeRequire=__createRequire(import.meta.url);\n"
    } else {
        ""
    };
    let unresolved_fallback = if entry.node_runtime {
        "return __nativeRequire(specifier)"
    } else {
        "throw new Error(\"MODULE_NOT_FOUND\")"
    };
    let mut output = format!(
        "{native_prelude}const __tableIds={tables};\nconst __indexIds={indexes};\n\
const __v=Object.freeze({{any:()=>({{}}),null:()=>({{}}),boolean:()=>({{}}),int64:()=>({{}}),float64:()=>({{}}),string:()=>({{}}),bytes:()=>({{}}),timestamp:()=>({{}}),id:()=>({{}}),documentId:()=>({{}}),array:()=>({{}}),object:()=>({{}}),pick:()=>({{}}),union:()=>({{}}),optional:()=>({{}})}});\n\
function __defineTable(document){{const indexes=[];const table={{document,indexes,index(name,fields){{indexes.push({{name,fields}});return table}}}};return table}}\n\
const __sdk=Object.freeze({{query:(definition)=>definition,mutation:(definition)=>definition,action:(definition)=>definition,cron:(definition)=>definition,value:Object.freeze({{int64:(value)=>value,float64:(value)=>value,timestamp:(value)=>Runku.timestamp(value),id:(value)=>Runku.id(value),bytes:(value)=>new Uint8Array(value)}}),v:__v,defineTable:__defineTable,defineSchema:(definitions)=>Object.freeze({{definitions,tables:__tableIds,indexes:__indexIds}})}});\n\
const __factories={{{factories}}};\nconst __deps={dependencies};\nconst __cache=Object.create(null);\n\
function __load(id){{if(Object.prototype.hasOwnProperty.call(__cache,id))return __cache[id].exports;const factory=__factories[id];if(typeof factory!==\"function\")throw new Error(\"MODULE_NOT_FOUND\");const module={{exports:{{}}}};__cache[id]=module;factory(module,module.exports,(specifier)=>{{if(specifier===\"@runku/server\")return __sdk;const target=__deps[id]&&__deps[id][specifier];if(typeof target!==\"string\"||target.length===0){{{unresolved_fallback}}};return __load(target)}});return module.exports}}\n\
const __entry=__load({entry_id});\n"
    );
    for function in &entry.functions {
        let export = &function.export_name;
        writeln!(
            output,
            "export const {export}=(context,input)=>__entry[{quoted}].handler(context,input);\n",
            quoted = serde_json::to_string(export).map_err(|_| BuildError::Internal)?
        )
        .map_err(|_| BuildError::Internal)?;
    }
    if output.len() > 8 * 1024 * 1024 {
        return Err(BuildError::LimitExceeded);
    }
    Ok(output)
}

fn collect_reachable(
    id: &str,
    modules: &BTreeMap<String, SourceModule>,
    reachable: &mut BTreeSet<String>,
) -> Result<(), BuildError> {
    if !reachable.insert(id.to_owned()) {
        return Ok(());
    }
    let module = modules.get(id).ok_or(BuildError::Internal)?;
    for target in module
        .dependencies
        .values()
        .filter(|target| !target.is_empty())
    {
        collect_reachable(target, modules, reachable)?;
    }
    Ok(())
}

fn resolve_dependencies(
    root: &Path,
    modules: &mut BTreeMap<String, SourceModule>,
) -> Result<(), BuildError> {
    let paths = modules
        .iter()
        .map(|(id, module)| (module.path.clone(), id.clone()))
        .collect::<BTreeMap<_, _>>();
    for module in modules.values_mut() {
        for (specifier, target) in &mut module.dependencies {
            if specifier == "@runku/server" {
                continue;
            }
            if !specifier.starts_with('.') {
                // Runtime policy is evaluated from each Function entrypoint after the complete
                // reachable graph is known. A plain helper may therefore use Node dependencies
                // when it is reachable only from a `"use runku node"` entrypoint; the same helper
                // fails closed as soon as a Safe V8 entrypoint reaches it.
                continue;
            }
            let parent = module.path.parent().ok_or(BuildError::InvalidPath)?;
            let unresolved = parent.join(specifier);
            let candidates = dependency_candidates(&unresolved);
            let resolved = candidates
                .into_iter()
                .find_map(|candidate| {
                    let canonical = std::fs::canonicalize(candidate).ok()?;
                    if !canonical.starts_with(root) {
                        return None;
                    }
                    paths.get(&canonical).cloned()
                })
                .ok_or(BuildError::InvalidPath)?;
            *target = resolved;
        }
    }
    Ok(())
}

fn dependency_candidates(path: &Path) -> Vec<PathBuf> {
    if path.extension().and_then(|extension| extension.to_str()) == Some("js") {
        vec![path.with_extension("ts"), path.to_path_buf()]
    } else if path.extension().and_then(|extension| extension.to_str()) == Some("mjs") {
        vec![path.with_extension("mts"), path.to_path_buf()]
    } else if path.extension().is_some() {
        vec![path.to_path_buf()]
    } else {
        ["ts", "mts", "js", "mjs"]
            .into_iter()
            .map(|extension| path.with_extension(extension))
            .chain(
                ["ts", "mts", "js", "mjs"]
                    .into_iter()
                    .map(|extension| path.join(format!("index.{extension}"))),
            )
            .collect()
    }
}

#[allow(clippy::too_many_lines)]
fn load_exported_constant(
    source_root: &Path,
    importer: &Path,
    specifier: &str,
    export_name: &str,
    depth: usize,
    visited: &mut BTreeSet<(PathBuf, String)>,
) -> Result<Expr, BuildError> {
    if depth > 32 || !specifier.starts_with('.') {
        return Err(BuildError::SourcePolicy);
    }
    let parent = importer.parent().ok_or(BuildError::InvalidPath)?;
    let target = dependency_candidates(&parent.join(specifier))
        .into_iter()
        .find_map(|candidate| {
            let metadata = std::fs::symlink_metadata(&candidate).ok()?;
            if !metadata.is_file() || metadata.file_type().is_symlink() {
                return None;
            }
            let canonical = std::fs::canonicalize(candidate).ok()?;
            canonical.starts_with(source_root).then_some(canonical)
        })
        .ok_or(BuildError::InvalidPath)?;
    let key = (target.clone(), export_name.to_owned());
    if !visited.insert(key.clone()) {
        return Err(BuildError::InvalidConfig);
    }
    let bytes = std::fs::read(&target).map_err(|_| BuildError::Unavailable)?;
    if bytes.is_empty()
        || u64::try_from(bytes.len()).map_err(|_| BuildError::LimitExceeded)?
            > SOURCE_MAX_MODULE_BYTES
    {
        return Err(BuildError::LimitExceeded);
    }
    let source = String::from_utf8(bytes).map_err(|_| BuildError::SourceSyntax)?;
    let parsed = parse_module(ParseParams {
        specifier: ModuleSpecifier::from_file_path(&target)
            .map_err(|()| BuildError::InvalidPath)?,
        text: Arc::<str>::from(source),
        media_type: media_type(&target)?,
        capture_tokens: false,
        scope_analysis: false,
        maybe_syntax: None,
    })
    .map_err(|_| BuildError::SourceSyntax)?;
    let ProgramRef::Module(program) = parsed.program_ref() else {
        return Err(BuildError::SourcePolicy);
    };
    let mut constants = BTreeMap::new();
    let mut exports = BTreeSet::new();
    for item in &program.body {
        match item {
            ModuleItem::ModuleDecl(ModuleDecl::Import(import)) if !import.type_only => {
                let nested_specifier = import.src.value.as_str().ok_or(BuildError::SourcePolicy)?;
                if nested_specifier == "@runku/server" {
                    continue;
                }
                for binding in &import.specifiers {
                    let deno_ast::swc::ast::ImportSpecifier::Named(binding) = binding else {
                        continue;
                    };
                    if binding.is_type_only {
                        continue;
                    }
                    let imported_name = match binding.imported.as_ref() {
                        None => binding.local.sym.as_ref(),
                        Some(deno_ast::swc::ast::ModuleExportName::Ident(name)) => {
                            name.sym.as_ref()
                        }
                        Some(deno_ast::swc::ast::ModuleExportName::Str(_)) => {
                            return Err(BuildError::SourcePolicy);
                        }
                    };
                    let expression = load_exported_constant(
                        source_root,
                        &target,
                        nested_specifier,
                        imported_name,
                        depth + 1,
                        visited,
                    )?;
                    constants.insert(binding.local.sym.to_string(), Box::new(expression));
                }
            }
            ModuleItem::Stmt(deno_ast::swc::ast::Stmt::Decl(Decl::Var(variables)))
                if variables.kind == VarDeclKind::Const =>
            {
                collect_constants(variables, &mut constants, None)?;
            }
            ModuleItem::ModuleDecl(ModuleDecl::ExportDecl(deno_ast::swc::ast::ExportDecl {
                decl: Decl::Var(variables),
                ..
            })) if variables.kind == VarDeclKind::Const => {
                collect_constants(variables, &mut constants, Some(&mut exports))?;
            }
            _ => {}
        }
    }
    let mut expression = constants
        .get(export_name)
        .filter(|_| exports.contains(export_name))
        .map(|expression| expression.as_ref().clone())
        .ok_or(BuildError::InvalidConfig)?;
    let mut inliner = ConstantInliner {
        constants: &constants,
        active: BTreeSet::new(),
        invalid: false,
        depth: 0,
    };
    expression.visit_mut_with(&mut inliner);
    visited.remove(&key);
    if inliner.invalid {
        Err(BuildError::InvalidConfig)
    } else {
        Ok(expression)
    }
}

fn collect_constants(
    variables: &deno_ast::swc::ast::VarDecl,
    constants: &mut BTreeMap<String, Box<Expr>>,
    mut exports: Option<&mut BTreeSet<String>>,
) -> Result<(), BuildError> {
    for declaration in &variables.decls {
        let Pat::Ident(name) = &declaration.name else {
            return Err(BuildError::InvalidConfig);
        };
        let value = declaration.init.clone().ok_or(BuildError::InvalidConfig)?;
        let name = name.id.sym.to_string();
        if constants.insert(name.clone(), value).is_some() {
            return Err(BuildError::InvalidConfig);
        }
        if let Some(exports) = exports.as_deref_mut() {
            exports.insert(name);
        }
    }
    Ok(())
}

struct ConstantInliner<'a> {
    constants: &'a BTreeMap<String, Box<Expr>>,
    active: BTreeSet<String>,
    invalid: bool,
    depth: usize,
}

impl VisitMut for ConstantInliner<'_> {
    fn visit_mut_prop(&mut self, property: &mut Prop) {
        if let Prop::Shorthand(identifier) = property
            && let Some(replacement) = self.constants.get(identifier.sym.as_ref())
        {
            let name = identifier.sym.to_string();
            if !self.active.insert(name.clone()) || self.depth > 32 {
                self.invalid = true;
                return;
            }
            let mut value = replacement.clone();
            self.depth += 1;
            value.visit_mut_with(self);
            self.depth -= 1;
            self.active.remove(&name);
            *property = Prop::KeyValue(KeyValueProp {
                key: PropName::Ident(identifier.clone().into()),
                value,
            });
            return;
        }
        property.visit_mut_children_with(self);
    }

    fn visit_mut_expr(&mut self, expression: &mut Expr) {
        if self.invalid || self.depth > 32 {
            self.invalid = true;
            return;
        }
        if let Expr::Ident(identifier) = expression
            && let Some(replacement) = self.constants.get(identifier.sym.as_ref())
        {
            let name = identifier.sym.to_string();
            if !self.active.insert(name.clone()) {
                self.invalid = true;
                return;
            }
            *expression = replacement.as_ref().clone();
            self.depth += 1;
            expression.visit_mut_with(self);
            self.depth -= 1;
            self.active.remove(&name);
            return;
        }
        expression.visit_mut_children_with(self);
    }
}

fn parse_validator(
    expression: &Expr,
    constants: &BTreeMap<String, Box<Expr>>,
    depth: usize,
) -> Result<Contract, BuildError> {
    if depth > 32 {
        return Err(BuildError::LimitExceeded);
    }
    let expression = resolve_expression(expression, constants, depth)?;
    let call = call_expression(expression)?;
    let name = validator_name(call)?;
    let contract = match name {
        "any" if call.args.is_empty() => Contract::Any,
        "null" if call.args.is_empty() => Contract::Null,
        "boolean" if call.args.is_empty() => Contract::Boolean,
        "timestamp" if call.args.is_empty() => Contract::Timestamp,
        "int64" => {
            let (minimum, maximum) = integer_bounds(call.args.first())?;
            Contract::Int64 { minimum, maximum }
        }
        "float64" => {
            let (minimum, maximum) = float_bounds(call.args.first())?;
            Contract::Float64 { minimum, maximum }
        }
        "string" | "bytes" => {
            let (minimum_bytes, maximum_bytes) = byte_bounds(call.args.first())?;
            if name == "string" {
                Contract::String {
                    minimum_bytes,
                    maximum_bytes,
                }
            } else {
                Contract::Bytes {
                    minimum_bytes,
                    maximum_bytes,
                }
            }
        }
        "id" if call.args.len() <= 1 => Contract::TypedId {
            kind: call
                .args
                .first()
                .map(|argument| string_literal(argument.expr.as_ref()).map(str::to_owned))
                .transpose()?,
        },
        "documentId" if call.args.len() == 1 => Contract::DocumentId {
            table: string_literal(call.args[0].expr.as_ref())?.to_owned(),
        },
        "pick" if call.args.len() == 2 => parse_pick_validator(call, constants, depth)?,
        "array" if (1..=2).contains(&call.args.len()) => {
            let items = parse_validator(call.args[0].expr.as_ref(), constants, depth + 1)?;
            let (minimum_items, maximum_items) = item_bounds(call.args.get(1))?;
            Contract::Array {
                items: Box::new(items),
                minimum_items,
                maximum_items,
            }
        }
        "object" if call.args.len() == 1 => {
            let fields_object = object_expression(call.args[0].expr.as_ref(), constants)?;
            let mut fields = BTreeMap::new();
            let mut optional = BTreeSet::new();
            for (key, value) in validator_object_properties(fields_object, constants)? {
                let resolved = resolve_expression(value, constants, depth + 1)?;
                if validator_call_name(resolved) == Some("optional") {
                    let optional_call = call_expression(resolved)?;
                    if optional_call.args.len() != 1 {
                        return Err(BuildError::InvalidConfig);
                    }
                    optional.insert(key.clone());
                    fields.insert(
                        key,
                        parse_validator(optional_call.args[0].expr.as_ref(), constants, depth + 1)?,
                    );
                } else {
                    fields.insert(key, parse_validator(value, constants, depth + 1)?);
                }
            }
            Contract::Object { fields, optional }
        }
        "union" if (2..=16).contains(&call.args.len()) => Contract::Union {
            variants: call
                .args
                .iter()
                .map(|argument| parse_validator(argument.expr.as_ref(), constants, depth + 1))
                .collect::<Result<Vec<_>, _>>()?,
        },
        _ => return Err(BuildError::InvalidConfig),
    };
    contract
        .validate_definition()
        .map_err(crate::map_contract)?;
    Ok(contract)
}

fn parse_pick_validator(
    call: &deno_ast::swc::ast::CallExpr,
    constants: &BTreeMap<String, Box<Expr>>,
    depth: usize,
) -> Result<Contract, BuildError> {
    let Contract::Object {
        fields: source_fields,
        optional: source_optional,
    } = parse_validator(call.args[0].expr.as_ref(), constants, depth + 1)?
    else {
        return Err(BuildError::InvalidConfig);
    };
    let names = string_array(call.args[1].expr.as_ref())?;
    let mut fields = BTreeMap::new();
    let mut optional = BTreeSet::new();
    for name in names {
        let value = source_fields
            .get(name)
            .cloned()
            .ok_or(BuildError::InvalidConfig)?;
        if fields.insert(name.to_owned(), value).is_some() {
            return Err(BuildError::InvalidConfig);
        }
        if source_optional.contains(name) {
            optional.insert(name.to_owned());
        }
    }
    Ok(Contract::Object { fields, optional })
}

fn validator_object_properties<'a>(
    object: &'a ObjectLit,
    constants: &'a BTreeMap<String, Box<Expr>>,
) -> Result<BTreeMap<String, &'a Expr>, BuildError> {
    let mut properties = BTreeMap::new();
    for property in &object.props {
        let PropOrSpread::Prop(property) = property else {
            return Err(BuildError::InvalidConfig);
        };
        let (name, value) = match property.as_ref() {
            Prop::KeyValue(property) => (property_name(&property.key)?, property.value.as_ref()),
            Prop::Shorthand(identifier) => {
                let name = identifier.sym.to_string();
                let value = constants
                    .get(name.as_str())
                    .map(Box::as_ref)
                    .ok_or(BuildError::InvalidConfig)?;
                (name, value)
            }
            _ => return Err(BuildError::InvalidConfig),
        };
        if properties.insert(name, value).is_some() {
            return Err(BuildError::InvalidConfig);
        }
    }
    Ok(properties)
}

fn object_properties(object: &ObjectLit) -> Result<BTreeMap<String, &Expr>, BuildError> {
    let mut properties = BTreeMap::new();
    for property in &object.props {
        let PropOrSpread::Prop(property) = property else {
            return Err(BuildError::InvalidConfig);
        };
        match property.as_ref() {
            Prop::KeyValue(property) => {
                let name = property_name(&property.key)?;
                if properties.insert(name, property.value.as_ref()).is_some() {
                    return Err(BuildError::InvalidConfig);
                }
            }
            _ => return Err(BuildError::InvalidConfig),
        }
    }
    Ok(properties)
}

fn function_properties(object: &ObjectLit) -> Result<(BTreeMap<String, &Expr>, bool), BuildError> {
    let mut properties = BTreeMap::new();
    let mut method_handler = false;
    for property in &object.props {
        let PropOrSpread::Prop(property) = property else {
            return Err(BuildError::InvalidConfig);
        };
        match property.as_ref() {
            Prop::KeyValue(property) => {
                let name = property_name(&property.key)?;
                if properties.insert(name, property.value.as_ref()).is_some() {
                    return Err(BuildError::InvalidConfig);
                }
            }
            Prop::Method(property) if property_name(&property.key)? == "handler" => {
                if method_handler || properties.contains_key("handler") {
                    return Err(BuildError::InvalidConfig);
                }
                method_handler = true;
            }
            _ => return Err(BuildError::InvalidConfig),
        }
    }
    Ok((properties, method_handler))
}

fn required<'a>(
    properties: &'a BTreeMap<String, &'a Expr>,
    name: &str,
) -> Result<&'a Expr, BuildError> {
    properties
        .get(name)
        .copied()
        .ok_or(BuildError::InvalidConfig)
}

fn ensure_handler(expression: &Expr) -> Result<(), BuildError> {
    if matches!(expression, Expr::Arrow(_) | Expr::Fn(_) | Expr::Invalid(_)) {
        Ok(())
    } else {
        Err(BuildError::InvalidConfig)
    }
}

fn resolve_expression<'a>(
    mut expression: &'a Expr,
    constants: &'a BTreeMap<String, Box<Expr>>,
    mut depth: usize,
) -> Result<&'a Expr, BuildError> {
    loop {
        if depth > 32 {
            return Err(BuildError::LimitExceeded);
        }
        expression = match expression {
            Expr::Ident(identifier) => constants
                .get(identifier.sym.as_ref())
                .map(Box::as_ref)
                .ok_or(BuildError::InvalidConfig)?,
            Expr::Paren(value) => value.expr.as_ref(),
            Expr::TsAs(value) => value.expr.as_ref(),
            Expr::TsSatisfies(value) => value.expr.as_ref(),
            Expr::TsConstAssertion(value) => value.expr.as_ref(),
            Expr::TsNonNull(value) => value.expr.as_ref(),
            _ => return Ok(expression),
        };
        depth += 1;
    }
}

fn object_expression<'a>(
    expression: &'a Expr,
    constants: &'a BTreeMap<String, Box<Expr>>,
) -> Result<&'a ObjectLit, BuildError> {
    match resolve_expression(expression, constants, 0)? {
        Expr::Object(object) => Ok(object),
        _ => Err(BuildError::InvalidConfig),
    }
}

fn call_expression(expression: &Expr) -> Result<&deno_ast::swc::ast::CallExpr, BuildError> {
    match expression {
        Expr::Call(call) => Ok(call),
        _ => Err(BuildError::InvalidConfig),
    }
}

fn function_call_kind(expression: &Expr) -> Option<FunctionType> {
    let Expr::Call(call) = expression else {
        return None;
    };
    match callee_name(call).ok()? {
        "query" => Some(FunctionType::Query),
        "mutation" => Some(FunctionType::Mutation),
        "action" => Some(FunctionType::Action),
        _ => None,
    }
}

fn is_call_named(expression: &Expr, expected: &str) -> bool {
    matches!(expression, Expr::Call(call) if callee_name(call).ok() == Some(expected))
}

fn callee_name(call: &deno_ast::swc::ast::CallExpr) -> Result<&str, BuildError> {
    match &call.callee {
        Callee::Expr(expression) => match expression.as_ref() {
            Expr::Ident(identifier) => Ok(identifier.sym.as_ref()),
            _ => Err(BuildError::InvalidConfig),
        },
        _ => Err(BuildError::InvalidConfig),
    }
}

fn validator_name(call: &deno_ast::swc::ast::CallExpr) -> Result<&str, BuildError> {
    helper_name(call, "v")
}

fn helper_name<'a>(
    call: &'a deno_ast::swc::ast::CallExpr,
    object_name: &str,
) -> Result<&'a str, BuildError> {
    match &call.callee {
        Callee::Expr(expression) => match expression.as_ref() {
            Expr::Member(member) if matches!(member.obj.as_ref(), Expr::Ident(identifier) if identifier.sym == *object_name) => {
                member_property(member).ok_or(BuildError::InvalidConfig)
            }
            _ => Err(BuildError::InvalidConfig),
        },
        _ => Err(BuildError::InvalidConfig),
    }
}

fn validator_call_name(expression: &Expr) -> Option<&str> {
    let Expr::Call(call) = expression else {
        return None;
    };
    validator_name(call).ok()
}

fn member_property(member: &deno_ast::swc::ast::MemberExpr) -> Option<&str> {
    match &member.prop {
        deno_ast::swc::ast::MemberProp::Ident(identifier) => Some(identifier.sym.as_ref()),
        _ => None,
    }
}

fn property_name(name: &PropName) -> Result<String, BuildError> {
    match name {
        PropName::Ident(identifier) => Ok(identifier.sym.to_string()),
        PropName::Str(value) => value
            .value
            .as_str()
            .map(str::to_owned)
            .ok_or(BuildError::InvalidConfig),
        _ => Err(BuildError::InvalidConfig),
    }
}

fn string_literal(expression: &Expr) -> Result<&str, BuildError> {
    match expression {
        Expr::Lit(Lit::Str(value)) => value.value.as_str().ok_or(BuildError::InvalidConfig),
        _ => Err(BuildError::InvalidConfig),
    }
}

fn string_array(expression: &Expr) -> Result<Vec<&str>, BuildError> {
    let Expr::Array(array) = expression else {
        return Err(BuildError::InvalidConfig);
    };
    array
        .elems
        .iter()
        .map(|element| {
            let ExprOrSpread { spread: None, expr } =
                element.as_ref().ok_or(BuildError::InvalidConfig)?
            else {
                return Err(BuildError::InvalidConfig);
            };
            string_literal(expr.as_ref())
        })
        .collect()
}

fn integer_bounds(
    argument: Option<&ExprOrSpread>,
) -> Result<(Option<i64>, Option<i64>), BuildError> {
    let Some(argument) = argument else {
        return Ok((None, None));
    };
    if argument.spread.is_some() {
        return Err(BuildError::InvalidConfig);
    }
    let Expr::Object(object) = argument.expr.as_ref() else {
        return Err(BuildError::InvalidConfig);
    };
    let properties = object_properties(object)?;
    reject_unknown_options(&properties, &["minimum", "maximum"])?;
    Ok((
        properties
            .get("minimum")
            .map(|value| integer_literal(value))
            .transpose()?,
        properties
            .get("maximum")
            .map(|value| integer_literal(value))
            .transpose()?,
    ))
}

fn float_bounds(
    argument: Option<&ExprOrSpread>,
) -> Result<(Option<FiniteBound>, Option<FiniteBound>), BuildError> {
    let Some(argument) = argument else {
        return Ok((None, None));
    };
    if argument.spread.is_some() {
        return Err(BuildError::InvalidConfig);
    }
    let Expr::Object(object) = argument.expr.as_ref() else {
        return Err(BuildError::InvalidConfig);
    };
    let properties = object_properties(object)?;
    reject_unknown_options(&properties, &["minimum", "maximum"])?;
    let parse = |value: &&Expr| {
        number_literal(value)
            .and_then(|number| FiniteBound::new(number).map_err(crate::map_contract))
    };
    Ok((
        properties.get("minimum").map(parse).transpose()?,
        properties.get("maximum").map(parse).transpose()?,
    ))
}

fn byte_bounds(argument: Option<&ExprOrSpread>) -> Result<(Option<u32>, Option<u32>), BuildError> {
    option_u32_bounds(argument, "minBytes", "maxBytes")
}

fn item_bounds(argument: Option<&ExprOrSpread>) -> Result<(Option<u32>, Option<u32>), BuildError> {
    option_u32_bounds(argument, "minItems", "maxItems")
}

fn option_u32_bounds(
    argument: Option<&ExprOrSpread>,
    minimum: &str,
    maximum: &str,
) -> Result<(Option<u32>, Option<u32>), BuildError> {
    let Some(argument) = argument else {
        return Ok((None, None));
    };
    if argument.spread.is_some() {
        return Err(BuildError::InvalidConfig);
    }
    let Expr::Object(object) = argument.expr.as_ref() else {
        return Err(BuildError::InvalidConfig);
    };
    let properties = object_properties(object)?;
    reject_unknown_options(&properties, &[minimum, maximum])?;
    let parse = |value: &&Expr| {
        u32::try_from(integer_literal(value)?).map_err(|_| BuildError::InvalidConfig)
    };
    Ok((
        properties.get(minimum).map(parse).transpose()?,
        properties.get(maximum).map(parse).transpose()?,
    ))
}

fn reject_unknown_options(
    properties: &BTreeMap<String, &Expr>,
    allowed: &[&str],
) -> Result<(), BuildError> {
    if properties.keys().all(|key| allowed.contains(&key.as_str())) {
        Ok(())
    } else {
        Err(BuildError::InvalidConfig)
    }
}

fn integer_literal(expression: &Expr) -> Result<i64, BuildError> {
    match expression {
        Expr::Lit(Lit::Num(value)) if value.value.is_finite() && value.value.fract() == 0.0 => {
            value
                .value
                .to_string()
                .parse()
                .map_err(|_| BuildError::InvalidConfig)
        }
        Expr::Lit(Lit::BigInt(value)) => value
            .value
            .to_string()
            .parse()
            .map_err(|_| BuildError::InvalidConfig),
        _ => Err(BuildError::InvalidConfig),
    }
}

fn number_literal(expression: &Expr) -> Result<f64, BuildError> {
    match expression {
        Expr::Lit(Lit::Num(value)) if value.value.is_finite() => Ok(value.value),
        _ => Err(BuildError::InvalidConfig),
    }
}

fn capability_allowed(function_type: FunctionType, capability: &Capability) -> bool {
    match function_type {
        FunctionType::Query => matches!(
            capability,
            Capability::DbRead | Capability::AuthRead | Capability::FunctionQuery
        ),
        FunctionType::Mutation => matches!(
            capability,
            Capability::DbRead
                | Capability::DbWrite
                | Capability::AuthRead
                | Capability::FunctionQuery
                | Capability::FunctionMutation
                | Capability::SchedulerCreate
        ),
        FunctionType::Action => matches!(
            capability,
            Capability::AuthRead
                | Capability::FunctionQuery
                | Capability::FunctionMutation
                | Capability::FunctionAction
                | Capability::NetworkHttps
                | Capability::SchedulerCreate
                | Capability::FileRead
                | Capability::FileWrite
                | Capability::Secret(_)
        ),
    }
}

fn parse_capability(value: &str) -> Result<Capability, BuildError> {
    match value {
        "db:read" => Ok(Capability::DbRead),
        "db:write" => Ok(Capability::DbWrite),
        "auth:read" => Ok(Capability::AuthRead),
        "function:query" => Ok(Capability::FunctionQuery),
        "function:mutation" => Ok(Capability::FunctionMutation),
        "function:action" => Ok(Capability::FunctionAction),
        "network:https" => Ok(Capability::NetworkHttps),
        "scheduler:create" => Ok(Capability::SchedulerCreate),
        "storage:read" => Ok(Capability::FileRead),
        "storage:write" => Ok(Capability::FileWrite),
        _ => Err(BuildError::InvalidConfig),
    }
}

fn is_node_directive(item: &ModuleItem) -> bool {
    matches!(item, ModuleItem::Stmt(deno_ast::swc::ast::Stmt::Expr(statement)) if matches!(statement.expr.as_ref(), Expr::Lit(Lit::Str(value)) if value.value == *"use runku node"))
}

fn module_namespace(source_root: &Path, path: &Path) -> Result<String, BuildError> {
    let relative = path
        .strip_prefix(source_root)
        .map_err(|_| BuildError::InvalidPath)?;
    let mut components = relative
        .components()
        .map(|component| {
            let Component::Normal(value) = component else {
                return Err(BuildError::InvalidPath);
            };
            value
                .to_str()
                .map(str::to_owned)
                .ok_or(BuildError::InvalidPath)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let file = components.last_mut().ok_or(BuildError::InvalidPath)?;
    *file = Path::new(file)
        .file_stem()
        .and_then(|value| value.to_str())
        .map(str::to_owned)
        .ok_or(BuildError::InvalidPath)?;
    if components.iter().any(|component| {
        component.is_empty()
            || !component
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    }) {
        return Err(BuildError::InvalidPath);
    }
    Ok(components.join("."))
}

fn stable_table_id(project_id: ProjectId, name: &str) -> TableId {
    TableId::from_ulid(stable_ulid(
        TABLE_ID_DOMAIN,
        &[project_id.to_string().as_bytes(), name.as_bytes()],
    ))
}

fn stable_index_id(project_id: ProjectId, table: &str, name: &str) -> IndexId {
    IndexId::from_ulid(stable_ulid(
        INDEX_ID_DOMAIN,
        &[
            project_id.to_string().as_bytes(),
            table.as_bytes(),
            name.as_bytes(),
        ],
    ))
}

fn stable_ulid(domain: &[u8], fields: &[&[u8]]) -> Ulid {
    let mut hash = Sha256::new();
    hash.update(domain);
    for field in fields {
        hash.update(u32::try_from(field.len()).unwrap_or(u32::MAX).to_be_bytes());
        hash.update(field);
    }
    let digest: [u8; 32] = hash.finalize().into();
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    Ulid::from(u128::from_be_bytes(bytes))
}

fn fingerprint_modules<'a>(
    root: &Path,
    source_root: &Path,
    modules: impl Iterator<Item = &'a SourceModule>,
) -> Result<Sha256Digest, BuildError> {
    let modules = modules.collect::<Vec<_>>();
    let mut hash = Sha256::new();
    hash.update(FINGERPRINT_DOMAIN);
    hash_field(
        &mut hash,
        source_root
            .strip_prefix(root)
            .map_err(|_| BuildError::InvalidPath)?
            .as_os_str()
            .as_encoded_bytes(),
    )?;
    hash.update(
        u32::try_from(modules.len())
            .map_err(|_| BuildError::LimitExceeded)?
            .to_be_bytes(),
    );
    for module in modules {
        hash_field(
            &mut hash,
            module
                .path
                .strip_prefix(source_root)
                .map_err(|_| BuildError::InvalidPath)?
                .as_os_str()
                .as_encoded_bytes(),
        )?;
        hash_field(&mut hash, module.source.as_bytes())?;
    }
    Ok(Sha256Digest::from_bytes(hash.finalize().into()))
}

fn discover_sources(source_root: &Path) -> Result<Vec<PathBuf>, BuildError> {
    let mut pending = vec![source_root.to_path_buf()];
    let mut files = Vec::new();
    while let Some(directory) = pending.pop() {
        let mut entries = std::fs::read_dir(&directory)
            .map_err(|_| BuildError::Unavailable)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| BuildError::Unavailable)?;
        entries.sort_by_key(std::fs::DirEntry::file_name);
        for entry in entries.into_iter().rev() {
            let metadata = entry.file_type().map_err(|_| BuildError::Unavailable)?;
            if metadata.is_symlink() {
                return Err(BuildError::InvalidPath);
            }
            let path = entry.path();
            if metadata.is_dir() {
                if entry.file_name() == "_generated" {
                    continue;
                }
                pending.push(path);
                continue;
            }
            if !metadata.is_file()
                || !matches!(
                    path.extension().and_then(|value| value.to_str()),
                    Some("ts" | "mts" | "js" | "mjs")
                )
            {
                continue;
            }
            let length = entry.metadata().map_err(|_| BuildError::Unavailable)?.len();
            if length == 0 || length > SOURCE_MAX_MODULE_BYTES {
                return Err(if length > SOURCE_MAX_MODULE_BYTES {
                    BuildError::LimitExceeded
                } else {
                    BuildError::InvalidPath
                });
            }
            files.push(std::fs::canonicalize(path).map_err(|_| BuildError::InvalidPath)?);
            if files.len() > SOURCE_MAX_MODULES {
                return Err(BuildError::LimitExceeded);
            }
        }
    }
    files.sort();
    Ok(files)
}

fn canonical_source_dir(root: &Path, relative: &Path) -> Result<PathBuf, BuildError> {
    let relative = if relative.as_os_str().is_empty() {
        Path::new(SOURCE_ROOT_DEFAULT)
    } else {
        relative
    };
    validate_relative_path(relative)?;
    let candidate = root.join(relative);
    let metadata = std::fs::symlink_metadata(&candidate).map_err(|_| BuildError::InvalidPath)?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(BuildError::InvalidPath);
    }
    let canonical = std::fs::canonicalize(candidate).map_err(|_| BuildError::InvalidPath)?;
    if !canonical.starts_with(root) {
        return Err(BuildError::InvalidPath);
    }
    Ok(canonical)
}

fn canonical_root(root: &Path) -> Result<PathBuf, BuildError> {
    let metadata = std::fs::symlink_metadata(root).map_err(|_| BuildError::InvalidPath)?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() || root.parent().is_none() {
        return Err(BuildError::InvalidPath);
    }
    let root = std::fs::canonicalize(root).map_err(|_| BuildError::InvalidPath)?;
    if root.parent().is_none()
        || std::env::var_os("HOME")
            .and_then(|home| std::fs::canonicalize(home).ok())
            .is_some_and(|home| home == root)
    {
        return Err(BuildError::InvalidPath);
    }
    Ok(root)
}

fn validate_relative_path(relative: &Path) -> Result<(), BuildError> {
    if relative.as_os_str().is_empty()
        || relative.as_os_str().as_encoded_bytes().len() > PATH_MAX_BYTES
        || relative.is_absolute()
        || !relative
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
    {
        Err(BuildError::InvalidPath)
    } else {
        Ok(())
    }
}

fn media_type(path: &Path) -> Result<MediaType, BuildError> {
    match path.extension().and_then(|value| value.to_str()) {
        Some("ts" | "mts") => Ok(MediaType::TypeScript),
        Some("js" | "mjs") => Ok(MediaType::JavaScript),
        _ => Err(BuildError::Unsupported),
    }
}

fn path_text(path: &Path) -> Result<String, BuildError> {
    path.to_str()
        .map(|value| value.replace('\\', "/"))
        .ok_or(BuildError::InvalidPath)
}

fn hash_field(hash: &mut Sha256, bytes: &[u8]) -> Result<(), BuildError> {
    hash.update(
        u64::try_from(bytes.len())
            .map_err(|_| BuildError::LimitExceeded)?
            .to_be_bytes(),
    );
    hash.update(bytes);
    Ok(())
}

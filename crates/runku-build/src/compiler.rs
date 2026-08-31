use std::{path::Path, sync::Arc};

use deno_ast::{
    EmitOptions, MediaType, ModuleKind, ModuleSpecifier, ParseParams, ProgramRef, SourceMap,
    SourceMapOption, TranspileModuleOptions, TranspileOptions, emit, parse_module,
    swc::{
        ast::{
            ArrowExpr, AwaitExpr, CallExpr, Callee, Decl, Decorator, DefaultDecl, Expr, Function,
            ModuleDecl, ModuleItem, Pat,
        },
        ast::{Pass, Program},
        common::comments::NoopComments,
        ecma_visit::{Visit, VisitWith},
        transforms::resolver as scope_resolver,
        transforms::{
            fixer::fixer,
            helpers::{HELPERS, Helpers, inject_helpers},
        },
    },
};
use swc_ecma_transforms_module::{
    common_js::{Config as CommonJsConfig, FeatureFlag, common_js},
    path::Resolver,
};

use crate::BuildError;

pub(crate) fn compile_module(path: &Path, source: &str) -> Result<String, BuildError> {
    let specifier = ModuleSpecifier::from_file_path(path).map_err(|()| BuildError::InvalidPath)?;
    let media_type = match path.extension().and_then(|value| value.to_str()) {
        Some("ts" | "mts") => MediaType::TypeScript,
        Some("js" | "mjs") => MediaType::JavaScript,
        _ => return Err(BuildError::Unsupported),
    };
    let parsed = parse_module(ParseParams {
        specifier,
        text: Arc::<str>::from(source),
        media_type,
        capture_tokens: false,
        scope_analysis: false,
        maybe_syntax: None,
    })
    .map_err(|_| BuildError::SourceSyntax)?;
    validate_module(parsed.program_ref())?;
    let emitted = parsed
        .transpile(
            &TranspileOptions {
                jsx: None,
                ..TranspileOptions::default()
            },
            &TranspileModuleOptions {
                module_kind: Some(ModuleKind::Esm),
            },
            &EmitOptions {
                source_map: SourceMapOption::None,
                source_map_base: None,
                source_map_file: None,
                inline_sources: false,
                remove_comments: true,
            },
        )
        .map_err(|_| BuildError::SourceSyntax)?
        .into_source()
        .text;
    if emitted.is_empty() || !emitted.contains("export") {
        return Err(BuildError::SourcePolicy);
    }
    let emitted_program = parse_module(ParseParams {
        specifier: ModuleSpecifier::parse("runku:/build/emitted.js")
            .map_err(|_| BuildError::Internal)?,
        text: Arc::<str>::from(emitted.as_str()),
        media_type: MediaType::JavaScript,
        capture_tokens: false,
        scope_analysis: false,
        maybe_syntax: None,
    })
    .map_err(|_| BuildError::Internal)?;
    validate_module(emitted_program.program_ref())?;
    Ok(emitted)
}

pub(crate) fn transpile_common_js(path: &Path, source: &str) -> Result<String, BuildError> {
    let specifier = ModuleSpecifier::from_file_path(path).map_err(|()| BuildError::InvalidPath)?;
    let media_type = media_type(path)?;
    let parsed = parse_module(ParseParams {
        specifier,
        text: Arc::<str>::from(source),
        media_type,
        capture_tokens: false,
        scope_analysis: true,
        maybe_syntax: None,
    })
    .map_err(|_| BuildError::SourceSyntax)?;
    let mut policy = SourcePolicy::default();
    parsed.program_ref().visit_with(&mut policy);
    if policy.denied {
        return Err(BuildError::SourcePolicy);
    }
    let stripped = parsed
        .transpile(
            &TranspileOptions {
                jsx: None,
                ..TranspileOptions::default()
            },
            &TranspileModuleOptions {
                module_kind: Some(ModuleKind::Esm),
            },
            &EmitOptions {
                source_map: SourceMapOption::None,
                source_map_base: None,
                source_map_file: None,
                inline_sources: false,
                remove_comments: true,
            },
        )
        .map_err(|_| BuildError::SourceSyntax)?
        .into_source()
        .text;
    let stripped = parse_module(ParseParams {
        specifier: ModuleSpecifier::from_file_path(path).map_err(|()| BuildError::InvalidPath)?,
        text: Arc::<str>::from(stripped.as_str()),
        media_type: MediaType::JavaScript,
        capture_tokens: false,
        scope_analysis: true,
        maybe_syntax: None,
    })
    .map_err(|_| BuildError::SourceSyntax)?;
    let mut program = stripped.program().as_ref().clone();
    stripped.globals().with(|marks| {
        HELPERS.set(&Helpers::new(false), || {
            scope_resolver(marks.unresolved, marks.top_level, false).process(&mut program);
            common_js(
                Resolver::Default,
                marks.unresolved,
                CommonJsConfig::default(),
                FeatureFlag {
                    support_block_scoping: true,
                    support_arrow: true,
                },
            )
            .process(&mut program);
            inject_helpers(marks.top_level).process(&mut program);
            fixer(None).process(&mut program);
        });
    });
    let source_map = SourceMap::single(
        ModuleSpecifier::from_file_path(path).map_err(|()| BuildError::InvalidPath)?,
        source.to_owned(),
    );
    emit(
        match &program {
            Program::Module(module) => ProgramRef::Module(module),
            Program::Script(script) => ProgramRef::Script(script),
        },
        &NoopComments,
        &source_map,
        &EmitOptions {
            source_map: SourceMapOption::None,
            source_map_base: None,
            source_map_file: None,
            inline_sources: false,
            remove_comments: true,
        },
    )
    .map_err(|_| BuildError::SourceSyntax)
    .map(|output| output.text)
}

fn media_type(path: &Path) -> Result<MediaType, BuildError> {
    match path.extension().and_then(|value| value.to_str()) {
        Some("ts" | "mts") => Ok(MediaType::TypeScript),
        Some("js" | "mjs") => Ok(MediaType::JavaScript),
        _ => Err(BuildError::Unsupported),
    }
}

fn validate_module(program: ProgramRef<'_>) -> Result<(), BuildError> {
    let ProgramRef::Module(module) = program else {
        return Err(BuildError::SourcePolicy);
    };
    let mut policy = SourcePolicy::default();
    module.visit_with(&mut policy);
    if policy.denied {
        return Err(BuildError::SourcePolicy);
    }
    let mut function_exports = 0_u16;
    for item in &module.body {
        let ModuleItem::ModuleDecl(declaration) = item else {
            continue;
        };
        match declaration {
            ModuleDecl::Import(import) => {
                if !import.type_only {
                    return Err(BuildError::SourcePolicy);
                }
            }
            ModuleDecl::ExportDecl(export) => {
                let Decl::Var(variables) = &export.decl else {
                    return Err(BuildError::SourcePolicy);
                };
                for declaration in &variables.decls {
                    if !matches!(declaration.name, Pat::Ident(_))
                        || !matches!(
                            declaration.init.as_deref(),
                            Some(Expr::Arrow(_) | Expr::Fn(_))
                        )
                    {
                        return Err(BuildError::SourcePolicy);
                    }
                    function_exports = function_exports.saturating_add(1);
                }
            }
            ModuleDecl::ExportDefaultDecl(default) => {
                if !matches!(default.decl, DefaultDecl::Fn(_)) {
                    return Err(BuildError::SourcePolicy);
                }
                function_exports = function_exports.saturating_add(1);
            }
            ModuleDecl::ExportDefaultExpr(default) => {
                if !matches!(&*default.expr, Expr::Arrow(_) | Expr::Fn(_)) {
                    return Err(BuildError::SourcePolicy);
                }
                function_exports = function_exports.saturating_add(1);
            }
            ModuleDecl::ExportNamed(_)
            | ModuleDecl::ExportAll(_)
            | ModuleDecl::TsImportEquals(_)
            | ModuleDecl::TsExportAssignment(_)
            | ModuleDecl::TsNamespaceExport(_) => return Err(BuildError::SourcePolicy),
        }
    }
    if function_exports > 0 {
        Ok(())
    } else {
        Err(BuildError::SourcePolicy)
    }
}

#[derive(Default)]
struct SourcePolicy {
    function_depth: usize,
    denied: bool,
}

impl Visit for SourcePolicy {
    fn visit_call_expr(&mut self, expression: &CallExpr) {
        if matches!(expression.callee, Callee::Import(_)) {
            self.denied = true;
            return;
        }
        expression.visit_children_with(self);
    }

    fn visit_await_expr(&mut self, expression: &AwaitExpr) {
        if self.function_depth == 0 {
            self.denied = true;
            return;
        }
        expression.visit_children_with(self);
    }

    fn visit_function(&mut self, function: &Function) {
        self.function_depth = self.function_depth.saturating_add(1);
        function.visit_children_with(self);
        self.function_depth = self.function_depth.saturating_sub(1);
    }

    fn visit_arrow_expr(&mut self, expression: &ArrowExpr) {
        self.function_depth = self.function_depth.saturating_add(1);
        expression.visit_children_with(self);
        self.function_depth = self.function_depth.saturating_sub(1);
    }

    fn visit_decorator(&mut self, _: &Decorator) {
        self.denied = true;
    }
}

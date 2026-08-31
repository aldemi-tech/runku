#!/usr/bin/env node

import { readFile, writeFile, mkdir } from "node:fs/promises";
import { resolve } from "node:path";

const [outputArgument, ...logArguments] = process.argv.slice(2);
if (!outputArgument || logArguments.length === 0) {
  console.error("usage: full-node-performance-report.mjs OUTPUT_DIR LOG...");
  process.exit(2);
}

const outputDirectory = resolve(outputArgument);
await mkdir(outputDirectory, { recursive: true });
const spans = [];
const reports = [];
const invocations = new Map();
for (const argument of logArguments) {
  const lines = (await readFile(argument, "utf8")).split(/\r?\n/);
  for (const line of lines) {
    if (line.startsWith("RUNKU_PERFORMANCE_SPAN ")) {
      spans.push(JSON.parse(line.slice("RUNKU_PERFORMANCE_SPAN ".length)));
    } else if (line.startsWith("RUNKU_BENCHMARK_INVOCATION ")) {
      const marker = JSON.parse(line.slice("RUNKU_BENCHMARK_INVOCATION ".length));
      invocations.set(marker.invocation_id, marker);
    } else if (line.startsWith("RUNKU_BENCHMARK_REPORT ")) {
      reports.push(JSON.parse(line.slice("RUNKU_BENCHMARK_REPORT ".length)));
    }
  }
}
if (reports.length === 0 || spans.length === 0) {
  throw new Error("benchmark logs contain no report or spans");
}

const cases = reports.flatMap((report) => report.cases);
const routingChecks = reports.flatMap((report) => report.routing_checks ?? []);
const openLoopChecks = reports.flatMap((report) => report.open_loop_checks ?? []);
let dockerStats = [];
try {
  dockerStats = (await readFile(resolve(outputDirectory, "docker-stats.jsonl"), "utf8"))
    .split(/\r?\n/)
    .filter(Boolean)
    .map((line) => JSON.parse(line));
} catch (error) {
  if (error?.code !== "ENOENT") throw error;
}
const aggregate = new Map();
for (const span of spans) {
  const marker = invocations.get(span.invocation_id) ?? { case: "unclassified", phase: "unclassified" };
  const key = [marker.case, marker.phase, span.runtime, span.component, span.operation].join("/");
  let row = aggregate.get(key);
  if (!row) {
    row = {
      case: marker.case,
      phase: marker.phase,
      runtime: span.runtime,
      component: span.component,
      operation: span.operation,
      durations: [],
      inputBytes: [],
      outputBytes: [],
      cpuMicros: [],
      memoryBytes: [],
      outcomes: {},
    };
    aggregate.set(key, row);
  }
  row.durations.push(span.duration_micros);
  if (span.input_bytes !== null) row.inputBytes.push(span.input_bytes);
  if (span.output_bytes !== null) row.outputBytes.push(span.output_bytes);
  const resources = span.resources;
  if (resources) {
    const cpu = resources.cpu_total_micros ??
      ((resources.user_cpu_micros ?? 0) + (resources.system_cpu_micros ?? 0));
    if (cpu > 0) row.cpuMicros.push(cpu);
    const memory = resources.peak_memory_bytes ?? resources.memory_bytes;
    if (memory !== null && memory !== undefined) row.memoryBytes.push(memory);
  }
  row.outcomes[span.outcome] = (row.outcomes[span.outcome] ?? 0) + 1;
}

function percentile(values, value) {
  if (values.length === 0) return null;
  const sorted = [...values].sort((left, right) => left - right);
  const rank = Math.ceil(sorted.length * value / 100);
  return sorted[Math.max(0, rank - 1)];
}

function average(values) {
  if (values.length === 0) return null;
  return Math.round(values.reduce((total, value) => total + value, 0) / values.length);
}

function dockerBytes(value) {
  const match = /^([0-9.]+)\s*([KMGT]?i?B)$/.exec(value.trim());
  if (!match) return null;
  const units = { B: 1, kB: 1e3, KB: 1e3, KiB: 1024, MB: 1e6, MiB: 1024 ** 2,
    GB: 1e9, GiB: 1024 ** 3, TB: 1e12, TiB: 1024 ** 4 };
  return Math.round(Number(match[1]) * units[match[2]]);
}

const dockerResourceGroups = new Map();
for (const sample of dockerStats) {
  const stats = sample.stats ?? {};
  const name = stats.Name ?? stats.Container ?? "unknown";
  const row = dockerResourceGroups.get(name) ?? { name, cpu: [], memory: [], pids: [] };
  const cpu = Number.parseFloat(String(stats.CPUPerc ?? "").replace("%", ""));
  const memory = dockerBytes(String(stats.MemUsage ?? "").split("/")[0] ?? "");
  const pids = Number.parseInt(stats.PIDs, 10);
  if (Number.isFinite(cpu)) row.cpu.push(cpu);
  if (memory !== null) row.memory.push(memory);
  if (Number.isFinite(pids)) row.pids.push(pids);
  dockerResourceGroups.set(name, row);
}
const infrastructureResources = [...dockerResourceGroups.values()]
  .map((row) => ({
    container: row.name,
    samples: Math.max(row.cpu.length, row.memory.length, row.pids.length),
    average_cpu_percent: row.cpu.length === 0 ? null :
      Math.round(row.cpu.reduce((sum, value) => sum + value, 0) * 100 / row.cpu.length) / 100,
    p95_cpu_percent: percentile(row.cpu, 95),
    max_cpu_percent: row.cpu.length === 0 ? null : Math.max(...row.cpu),
    max_memory_bytes: row.memory.length === 0 ? null : Math.max(...row.memory),
    max_pids: row.pids.length === 0 ? null : Math.max(...row.pids),
  }))
  .sort((left, right) => left.container.localeCompare(right.container));

function formatOutcomes(outcomes) {
  return Object.entries(outcomes)
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([outcome, count]) => `${outcome}:${count}`)
    .join(", ");
}

const components = [...aggregate.values()]
  .map((row) => ({
    runtime: row.runtime,
    case: row.case,
    phase: row.phase,
    component: row.component,
    operation: row.operation,
    samples: row.durations.length,
    p50_micros: percentile(row.durations, 50),
    p95_micros: percentile(row.durations, 95),
    p99_micros: percentile(row.durations, 99),
    average_input_bytes: average(row.inputBytes),
    average_output_bytes: average(row.outputBytes),
    average_cpu_micros: average(row.cpuMicros),
    max_memory_bytes: row.memoryBytes.length === 0 ? null : Math.max(...row.memoryBytes),
    outcomes: row.outcomes,
  }))
  .sort((left, right) =>
    left.case.localeCompare(right.case) ||
    left.phase.localeCompare(right.phase) ||
    left.runtime.localeCompare(right.runtime) ||
    left.component.localeCompare(right.component) ||
    left.operation.localeCompare(right.operation));

const pipelineStages = [
  { label: "Gateway total", runtime: "remote_gateway", component: "gateway", operation: "invocation" },
  { label: "Control register", runtime: "remote_gateway", component: "control_plane", operation: "register" },
  { label: "NATS JetStream publish", runtime: "remote_gateway", component: "queue", operation: "publish" },
  { label: "Queue wait / claim", runtime: "remote_gateway", component: "queue", operation: "queue_wait" },
  { label: "Agent begin preparing", runtime: "remote_agent", component: "control_plane", operation: "begin_preparing" },
  { label: "Release lookup", runtime: "remote_agent", component: "release_repository", operation: "resolve_release" },
  { label: "S3 artifact fetch", runtime: "remote_agent", component: "artifact_store", operation: "fetch_artifact" },
  { label: "OCI image prepare", runtime: "remote_agent", component: "oci_image", operation: "prepare_runtime" },
  { label: "NATS durable ACK", runtime: "remote_agent", component: "queue", operation: "acknowledge" },
  { label: "Control begin running", runtime: "remote_agent", component: "control_plane", operation: "begin_running" },
  { label: "Runtime total", component: "runtime", operation: "invocation" },
  { label: "Node runner", component: "node_process", operation: "execute_runner" },
  { label: "Control complete", runtime: "remote_agent", component: "control_plane", operation: "complete" },
  { label: "Result propagation wait", runtime: "remote_gateway", component: "result", operation: "result_wait" },
];
const remoteCases = cases
  .filter((value) => value.case.startsWith("gateway_nats_s3_agent_"))
  .map((value) => value.case);
const remotePipeline = [];
for (const caseName of remoteCases) {
  for (const phase of ["warm", "concurrent"]) {
    for (const stage of pipelineStages) {
      const row = components.find((value) =>
        value.case === caseName &&
        value.phase === phase &&
        value.component === stage.component &&
        value.operation === stage.operation &&
        (!stage.runtime || value.runtime === stage.runtime));
      if (row) remotePipeline.push({ stage: stage.label, ...row });
    }
  }
}

const summary = {
  format_version: 1,
  generated_at: new Date().toISOString(),
  source_logs: logArguments.map((value) => resolve(value)),
  benchmark_parameters: reports.map(({ warmups, iterations, concurrent_requests, concurrency }) =>
    ({ warmups, iterations, concurrent_requests, concurrency })),
  cases,
  routing_checks: routingChecks,
  open_loop_checks: openLoopChecks,
  infrastructure_resources: infrastructureResources,
  remote_pipeline: remotePipeline,
  components,
  span_count: spans.length,
};

await writeFile(
  resolve(outputDirectory, "spans.jsonl"),
  `${spans.map((span) => JSON.stringify(span)).join("\n")}\n`,
);
await writeFile(
  resolve(outputDirectory, "classified-spans.jsonl"),
  `${spans.map((span) => JSON.stringify({
    ...(invocations.get(span.invocation_id) ?? { case: "unclassified", phase: "unclassified" }),
    span,
  })).join("\n")}\n`,
);
await writeFile(
  resolve(outputDirectory, "summary.json"),
  `${JSON.stringify(summary, null, 2)}\n`,
);

const markdown = [
  "# Full Node execution performance benchmark",
  "",
  `Generated: ${summary.generated_at}`,
  "",
  "Raw correlated spans are preserved in `spans.jsonl`; values are byte counts and stable outcomes, never payload bodies.",
  "",
  "## End-to-end cases",
  "",
  "| Case | Cold µs | Warm p50 µs | Warm p95 µs | Warm p99 µs | Concurrent req/s | Pool hit/miss | Pool workers | Peak memory bytes | Avg CPU µs | Spans | Abandoned |",
  "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
  ...cases.map((value) => `| ${value.case} | ${value.cold_micros} | ${value.warm_p50_micros} | ${value.warm_p95_micros} | ${value.warm_p99_micros} | ${value.throughput_requests_per_second} | ${value.warm_pool ? `${value.warm_pool.hits}/${value.warm_pool.reconnects}` : "n/a"} | ${value.warm_pool?.workers ?? "n/a"} | ${value.max_peak_memory_bytes ?? "n/a"} | ${value.average_cpu_micros ?? "n/a"} | ${value.spans} | ${value.abandoned_spans} |`),
  "",
  "## Scaled warm routing correctness",
  "",
  "Every row requires exact function, input token, invocation ID and Gateway result correlation for every concurrent request.",
  "",
  "| Case | Requests | Functions | Slots | Peak active | Unique request IDs | Unique invocation IDs | Pool hit/miss | Mismatches | Elapsed µs |",
  "|---|---:|---|---:|---:|---:|---:|---:|---:|---:|",
  ...routingChecks.map((value) => `| ${value.case} | ${value.requests} | ${value.functions.join(", ")} | ${value.configured_slots} | ${value.peak_agent_concurrency} | ${value.unique_request_ids} | ${value.unique_invocation_ids} | ${value.warm_pool_hits}/${value.warm_pool_misses} | ${value.mismatches} | ${value.elapsed_micros} |`),
  "",
  "## Open-loop sustained load",
  "",
  "Requests are injected at the configured rate independently of completion; completion throughput includes queue drain.",
  "",
  "| Case | Target req/s | Injection s | Requests | Success/fail/mismatch | Slots/peak | Pool hit/miss | Injection µs | Completion µs | Completion req/s | Latency p50/p95/p99 µs |",
  "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
  ...openLoopChecks.map((value) => `| ${value.case} | ${value.target_requests_per_second} | ${value.injection_duration_secs} | ${value.scheduled_requests} | ${value.succeeded}/${value.failed}/${value.mismatches} | ${value.configured_slots}/${value.peak_agent_concurrency} | ${value.warm_pool_hits}/${value.warm_pool_misses} | ${value.injection_elapsed_micros} | ${value.completion_elapsed_micros} | ${value.completion_throughput_requests_per_second} | ${value.latency_p50_micros}/${value.latency_p95_micros}/${value.latency_p99_micros} |`),
  "",
  "## Shared infrastructure resources",
  "",
  "Infrastructure samples cover shared Gateway/Agent, NATS, S3 and Firecracker VMM processes; they are not summed from per-invocation reservations.",
  "",
  "| Container | Samples | Avg CPU % | p95 CPU % | Max CPU % | Max memory bytes | Max PIDs |",
  "|---|---:|---:|---:|---:|---:|---:|",
  ...infrastructureResources.map((value) => `| ${value.container} | ${value.samples} | ${value.average_cpu_percent ?? "n/a"} | ${value.p95_cpu_percent ?? "n/a"} | ${value.max_cpu_percent ?? "n/a"} | ${value.max_memory_bytes ?? "n/a"} | ${value.max_pids ?? "n/a"} |`),
  "",
  "## Remote pipeline (NATS JetStream + S3 + Agent)",
  "",
  "Durations are correlated spans. `Gateway total`, `Runtime total`, `Node runner`, and `Result propagation wait` are nested/overlapping boundaries and must not be added together.",
  "",
  "| Case | Phase | Stage | Runtime | Samples | p50 µs | p95 µs | p99 µs | Avg CPU µs | Max memory bytes | Outcomes |",
  "|---|---|---|---|---:|---:|---:|---:|---:|---:|---|",
  ...remotePipeline.map((value) => `| ${value.case} | ${value.phase} | ${value.stage} | ${value.runtime} | ${value.samples} | ${value.p50_micros ?? "n/a"} | ${value.p95_micros ?? "n/a"} | ${value.p99_micros ?? "n/a"} | ${value.average_cpu_micros ?? "n/a"} | ${value.max_memory_bytes ?? "n/a"} | ${formatOutcomes(value.outcomes)} |`),
  "",
  "## Component spans",
  "",
  "| Case | Phase | Runtime | Component | Operation | Samples | p50 µs | p95 µs | p99 µs | Input bytes | Output bytes | Avg CPU µs | Max memory bytes | Outcomes |",
  "|---|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|",
  ...components.map((value) => `| ${value.case} | ${value.phase} | ${value.runtime} | ${value.component} | ${value.operation} | ${value.samples} | ${value.p50_micros ?? "n/a"} | ${value.p95_micros ?? "n/a"} | ${value.p99_micros ?? "n/a"} | ${value.average_input_bytes ?? "n/a"} | ${value.average_output_bytes ?? "n/a"} | ${value.average_cpu_micros ?? "n/a"} | ${value.max_memory_bytes ?? "n/a"} | ${formatOutcomes(value.outcomes)} |`),
  "",
];
await writeFile(resolve(outputDirectory, "summary.md"), `${markdown.join("\n")}\n`);
console.log(resolve(outputDirectory, "summary.md"));

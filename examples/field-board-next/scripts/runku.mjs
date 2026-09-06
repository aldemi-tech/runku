#!/usr/bin/env node

import { spawn, spawnSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";

const runkuArguments = process.argv.slice(2);
if (runkuArguments.length === 0) {
  fail("No Runku command was supplied.");
}

const override = process.env.RUNKU_BIN;
const manifest = fileURLToPath(new URL("../../../Cargo.toml", import.meta.url));
const repositoryRoot = dirname(manifest);
if (override === undefined) {
  const build = spawnSync(
    "cargo",
    ["build", "--quiet", "--manifest-path", manifest, "--package", "runku-cli"],
    { stdio: "inherit", windowsHide: false },
  );
  if (build.error !== undefined) {
    fail(`The source CLI could not be built through Cargo: ${build.error.message}`);
  }
  if (build.status !== 0) process.exit(build.status ?? 1);
}

const command = override ?? join(
  repositoryRoot,
  "target",
  "debug",
  process.platform === "win32" ? "runku.exe" : "runku",
);
const cliPackage = JSON.parse(readFileSync(join(repositoryRoot, "packages", "cli", "package.json"), "utf8"));
const expectedVersion = `runku ${cliPackage.version}`;
const version = spawnSync(command, ["--version"], {
  encoding: "utf8",
  windowsHide: false,
});
if (version.error !== undefined) {
  fail(`The selected Runku CLI could not be inspected: ${version.error.message}`);
}
const actualVersion = version.stdout.trim();
if (version.status !== 0 || actualVersion !== expectedVersion) {
  fail(`The selected CLI reports ${actualVersion || "an unreadable version"}; ${expectedVersion} is required.`);
}

const child = spawn(command, runkuArguments, {
  stdio: "inherit",
  windowsHide: false,
});

for (const signal of ["SIGINT", "SIGTERM"]) {
  process.on(signal, () => child.kill(signal));
}

child.on("error", (error) => {
  fail(
    override === undefined
      ? `The freshly built source CLI could not be started: ${error.message}`
      : `RUNKU_BIN could not be started: ${error.message}`,
  );
});

child.on("exit", (code, signal) => {
  if (signal !== null && process.platform !== "win32") {
    process.kill(process.pid, signal);
    return;
  }
  process.exit(code ?? 1);
});

function fail(message) {
  process.stderr.write(`error: FIELD_BOARD_RUNKU_CLI_UNAVAILABLE\nmessage: ${message}\n`);
  process.exit(1);
}

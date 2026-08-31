.PHONY: toolchain toolchain-check check fmt fmt-check lint test docs incomplete-check js-install install-cli install-cli-check chat-example-check chat-example-e2e-check node-example-check storage-up storage-down storage-check storage-benchmark artifact-benchmark release-repository-check release-repository-benchmark runtime-check runtime-benchmark full-node-local-check full-node-docker-check full-node-evidence-check full-node-performance-benchmark firecracker-production-check remote-execution-infra-check action-https-check action-https-benchmark query-engine-check query-engine-benchmark mutation-engine-check mutation-engine-benchmark schema-index-check schema-index-benchmark realtime-check realtime-benchmark scheduling-check scheduling-benchmark identity-keyring-check identity-keyring-benchmark identity-gateway-check guest-identity-check jwt-identity-check identity-provider-check protocol-check gateway-http-check gateway-product-check sdk-typescript-check sdk-server-check development-workspace-check cron-check websocket-realtime-check local-process-check source-build-check local-key-management-check source-watch-check contracts-codegen-check release-lifecycle-check nested-function-check operational-logs-check otlp-export-check development-access-check remote-workspace-protocol-check remote-workspace-service-check remote-workspace-client-check remote-release-freeze-check remote-workspace-check

RUST_TOOLCHAIN_CHANNEL := $(shell sed -n 's/^channel = "\([^"]*\)"/\1/p' rust-toolchain.toml)

toolchain:
	@command -v rustup >/dev/null 2>&1 || { echo "rustup is required: https://rustup.rs"; exit 1; }
	@test -n "$(RUST_TOOLCHAIN_CHANNEL)" || { echo "rust-toolchain.toml does not declare a channel"; exit 1; }
	rustup toolchain install "$(RUST_TOOLCHAIN_CHANNEL)" --profile minimal --component clippy --component rustfmt --component rust-analyzer --component rust-src
	@$(MAKE) --no-print-directory toolchain-check

toolchain-check:
	@expected="$(RUST_TOOLCHAIN_CHANNEL)"; \
	actual=$$(rustc --version | awk '{print $$2}'); \
	test "$$actual" = "$$expected" || { echo "expected rustc $$expected, found $$actual"; exit 1; }; \
	cargo --version; \
	rust-analyzer --version

check: toolchain-check fmt-check lint test docs incomplete-check sdk-typescript-check sdk-server-check chat-example-check node-example-check

fmt:
	cargo fmt --all

fmt-check:
	cargo fmt --all --check

lint:
	cargo clippy --workspace --all-targets --all-features -- -D warnings

test: sdk-typescript-check
	cargo test --workspace --all-features --locked

docs:
	RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps --locked

incomplete-check:
	@! rg -n "TODO|FIXME|todo!|unimplemented!|#\[ignore" crates protocol packages Cargo.toml rust-toolchain.toml rustfmt.toml

js-install:
	pnpm install --frozen-lockfile

install-cli:
	cargo install --path crates/runku-cli --locked --force $(if $(strip $(CARGO_INSTALL_ROOT)),--root "$(CARGO_INSTALL_ROOT)",)

install-cli-check:
	@install_root=$$(mktemp -d); \
	trap 'rm -rf "$$install_root"' EXIT; \
	$(MAKE) --no-print-directory install-cli CARGO_INSTALL_ROOT="$$install_root"; \
	"$$install_root/bin/runku" --version | rg -x 'runku 0\.1\.0'; \
	"$$install_root/bin/runku" --help | rg -F 'runku dev [--root PATH]'

chat-example-check: js-install
	@node -e 'const [major, minor, patch] = process.versions.node.split(".").map(Number); if (major < 20 || (major === 20 && (minor < 18 || (minor === 18 && patch < 1)))) { console.error("Runku Chat requires Node.js >=20.18.1 (see examples/chat-next/.nvmrc)"); process.exit(1) }'
	cargo build -p runku-cli --release --locked
	cd examples/chat-next && BETTER_AUTH_SECRET='runku-chat-check-secret-with-at-least-32-characters' BETTER_AUTH_DATABASE_PATH='.data/auth.check.sqlite3' pnpm auth:migrate
	$(CURDIR)/target/release/runku init --root examples/chat-next
	$(CURDIR)/target/release/runku dev --root examples/chat-next --prepare
	$(CURDIR)/target/release/runku build --root examples/chat-next
	cd examples/chat-next && BETTER_AUTH_SECRET='runku-chat-check-secret-with-at-least-32-characters' BETTER_AUTH_DATABASE_PATH='.data/auth.check.sqlite3' pnpm check
	@! rg -a -q 'rk_sec_v1_[0-7][0-9A-HJKMNP-TV-Z]{25}\.[A-Za-z0-9_-]{43}' examples/chat-next/.next/static
	cd examples/chat-next && pnpm audit --audit-level=high

chat-example-e2e-check: chat-example-check
	cd examples/chat-next && BETTER_AUTH_SECRET='runku-chat-e2e-secret-with-at-least-32-characters' BETTER_AUTH_DATABASE_PATH='.data/auth.e2e.sqlite3' pnpm auth:migrate
	cd examples/chat-next && BETTER_AUTH_SECRET='runku-chat-e2e-secret-with-at-least-32-characters' BETTER_AUTH_DATABASE_PATH='.data/auth.e2e.sqlite3' RUNKU_BIN="$(CURDIR)/target/release/runku" PATH="$(CURDIR)/target/release:$$PATH" pnpm test:e2e

node-example-check: js-install sdk-typescript-check sdk-server-check
	@node -e 'const [major, minor, patch] = process.versions.node.split(".").map(Number); if (major < 20 || (major === 20 && (minor < 18 || (minor === 18 && patch < 1)))) { console.error("Runku Node example requires Node.js >=20.18.1"); process.exit(1) }'
	cargo build -p runku-cli --release --locked
	cd examples/node-actions && RUNKU_BIN="$(CURDIR)/target/release/runku" PATH="$(CURDIR)/target/release:$$PATH" pnpm validate

storage-up:
	docker compose -f compose.storage.yml up -d --wait

storage-down:
	docker compose -f compose.storage.yml down

storage-check: storage-up
	RUNKU_TEST_POSTGRES_URL="postgres://runku:runku_local_test_only@127.0.0.1:$${RUNKU_POSTGRES_PORT:-55432}/runku_test" cargo test -p runku-data-postgres --test postgres_conformance --locked -- --test-threads=1

storage-benchmark: storage-check
	docker compose -f compose.storage.yml exec -T postgres psql -X -U runku -d runku_test -f /workspace/benchmarks/storage/postgres-index-baseline.sql

artifact-benchmark:
	cargo run -p runku-releases --example artifact_baseline --release --locked

release-repository-check: storage-up
	RUNKU_TEST_POSTGRES_URL="postgres://runku:runku_local_test_only@127.0.0.1:$${RUNKU_POSTGRES_PORT:-55432}/runku_test" cargo test -p runku-release-repository --test conformance --locked -- --test-threads=1

release-repository-benchmark:
	cargo run -p runku-release-repository --example repository_baseline --release --locked

runtime-check:
	cargo test -p runku-runtime --test runtime_conformance --locked -- --test-threads=1

runtime-benchmark:
	cargo run -p runku-runtime --example runtime_baseline --release --locked

full-node-local-check:
	cargo test -p runku-build --test build invalid_declarations_imports_paths_and_node_fail_closed --locked -- --exact
	cargo test -p runku-node-runtime --test local_runtime --locked
	cargo test -p runku-cli --test cli full_node_source_build_publish_and_dev_use_the_machine_node --locked -- --exact

full-node-docker-check:
	RUNKU_FULL_NODE_DOCKER_TEST=1 cargo test -p runku-gateway --test product_vertical full_node_channel_promotion_and_rollback_use_exact_oci_artifacts --locked -- --exact --nocapture

full-node-evidence-check:
	./scripts/full-node-evidence.sh

full-node-performance-benchmark:
	./scripts/full-node-performance-benchmark.sh

remote-execution-infra-check:
	./scripts/remote-execution-infra-evidence.sh

firecracker-production-check:
	test -n "$(RUNKU_FIRECRACKER_ASSET_DIR)"
	RUNKU_FIRECRACKER_ASSET_DIR="$(RUNKU_FIRECRACKER_ASSET_DIR)" ./scripts/full-node-performance-benchmark.sh

action-https-check:
	cargo test -p runku-runtime --lib --locked https::tests
	cargo test -p runku-runtime --test runtime_conformance --locked action_https_is_capability_scoped_typed_and_recovers_after_error -- --test-threads=1

action-https-benchmark: runtime-benchmark

query-engine-check: storage-up
	RUNKU_TEST_POSTGRES_URL="postgres://runku:runku_local_test_only@127.0.0.1:$${RUNKU_POSTGRES_PORT:-55432}/runku_test" cargo test -p runku-execution --locked -- --test-threads=1

query-engine-benchmark:
	cargo run -p runku-execution --example query_baseline --release --locked

mutation-engine-check: storage-up
	RUNKU_TEST_POSTGRES_URL="postgres://runku:runku_local_test_only@127.0.0.1:$${RUNKU_POSTGRES_PORT:-55432}/runku_test" cargo test -p runku-execution --test mutation_engine --locked -- --test-threads=1

mutation-engine-benchmark:
	cargo run -p runku-execution --example mutation_baseline --release --locked

schema-index-check: storage-up
	RUNKU_TEST_POSTGRES_URL="postgres://runku:runku_local_test_only@127.0.0.1:$${RUNKU_POSTGRES_PORT:-55432}/runku_test" cargo test -p runku-schema -p runku-execution --locked -- --test-threads=1

schema-index-benchmark:
	cargo run -p runku-execution --example schema_index_baseline --release --locked

realtime-check: storage-up
	RUNKU_TEST_POSTGRES_URL="postgres://runku:runku_local_test_only@127.0.0.1:$${RUNKU_POSTGRES_PORT:-55432}/runku_test" cargo test -p runku-realtime -p runku-data-sqlite -p runku-data-postgres --locked -- --test-threads=1

realtime-benchmark:
	cargo run -p runku-realtime --example realtime_baseline --release --locked

scheduling-check: storage-up
	RUNKU_TEST_POSTGRES_URL="postgres://runku:runku_local_test_only@127.0.0.1:$${RUNKU_POSTGRES_PORT:-55432}/runku_test" cargo test -p runku-runtime -p runku-execution -p runku-data-sqlite -p runku-data-postgres --locked -- --test-threads=1

scheduling-benchmark:
	cargo run -p runku-execution --example scheduling_baseline --release --locked

identity-keyring-check: storage-up
	RUNKU_TEST_POSTGRES_URL="postgres://runku:runku_local_test_only@127.0.0.1:$${RUNKU_POSTGRES_PORT:-55432}/runku_test" cargo test -p runku-identity -p runku-identity-repository --locked -- --test-threads=1

identity-keyring-benchmark:
	cargo run -p runku-identity-repository --example keyring_baseline --release --locked

identity-gateway-check:
	cargo test -p runku-identity --locked -- --test-threads=1

guest-identity-check:
	cargo test -p runku-identity --locked guest::tests -- --test-threads=1

jwt-identity-check:
	cargo test -p runku-identity --locked jwt::tests -- --test-threads=1

identity-provider-check:
	cargo test -p runku-identity-provider --all-features --locked -- --test-threads=1

protocol-check:
	cargo test -p runku-protocol --all-features --locked -- --test-threads=1

gateway-http-check:
	cargo test -p runku-gateway --all-features --locked -- --test-threads=1

gateway-product-check:
	cargo test -p runku-gateway --test product_vertical --all-features --locked -- --test-threads=1

sdk-typescript-check: js-install
	cd packages/client && pnpm check

sdk-server-check: js-install
	cd packages/server && pnpm check

development-workspace-check: storage-up
	RUNKU_TEST_POSTGRES_URL="postgres://runku:runku_local_test_only@127.0.0.1:$${RUNKU_POSTGRES_PORT:-55432}/runku_test" cargo test -p runku-development --test conformance --all-features --locked -- --test-threads=1
	cargo test -p runku-gateway --test product_vertical --all-features --locked -- --test-threads=1

cron-check: storage-up
	RUNKU_TEST_POSTGRES_URL="postgres://runku:runku_local_test_only@127.0.0.1:$${RUNKU_POSTGRES_PORT:-55432}/runku_test" cargo test -p runku-releases -p runku-cron --all-features --locked -- --test-threads=1

websocket-realtime-check:
	cargo test -p runku-protocol -p runku-realtime -p runku-gateway --all-features --locked -- --test-threads=1
	cd packages/client && pnpm check

local-process-check:
	cargo test -p runku-local -p runku-cli --all-features --locked -- --test-threads=1
	cargo clippy -p runku-local -p runku-cli --all-targets --all-features -- -D warnings
	cd packages/client && pnpm check

source-build-check:
	cargo test -p runku-build -p runku-cli --all-features --locked -- --test-threads=1
	cargo clippy -p runku-build -p runku-cli -p runku-runtime -p runku-execution --all-targets --all-features -- -D warnings
	cd packages/server && pnpm check
	cd packages/client && pnpm check

local-key-management-check: sdk-typescript-check
	cargo test -p runku-local -p runku-cli --all-features --locked -- --test-threads=1
	cargo clippy -p runku-local -p runku-cli --all-targets --all-features -- -D warnings

source-watch-check: sdk-typescript-check
	cargo test -p runku-build -p runku-local -p runku-cli --all-features --locked -- --test-threads=1
	cargo clippy -p runku-build -p runku-local -p runku-cli --all-targets --all-features -- -D warnings

contracts-codegen-check: sdk-typescript-check
	cargo test -p runku-contracts -p runku-releases -p runku-build -p runku-runtime -p runku-gateway -p runku-cli --all-features --locked -- --test-threads=1
	cargo clippy -p runku-contracts -p runku-releases -p runku-build -p runku-runtime -p runku-gateway -p runku-cli --all-targets --all-features -- -D warnings
	cd packages/server && pnpm check

release-lifecycle-check:
	cargo test -p runku-compatibility -p runku-releases -p runku-release-repository -p runku-cron -p runku-local -p runku-cli --all-features --locked -- --test-threads=1
	cargo clippy -p runku-compatibility -p runku-releases -p runku-release-repository -p runku-cron -p runku-local -p runku-cli --all-targets --all-features -- -D warnings

nested-function-check:
	cargo test -p runku-identity -p runku-runtime -p runku-execution -p runku-gateway --all-features --locked -- --test-threads=1
	cargo clippy -p runku-identity -p runku-runtime -p runku-execution -p runku-gateway --all-targets --all-features -- -D warnings

operational-logs-check: storage-up sdk-typescript-check sdk-server-check
	RUNKU_TEST_POSTGRES_URL="postgres://runku:runku_local_test_only@127.0.0.1:$${RUNKU_POSTGRES_PORT:-55432}/runku_test" cargo test -p runku-observability -p runku-runtime -p runku-gateway -p runku-local -p runku-cli --all-features --locked -- --test-threads=1
	cargo clippy -p runku-observability -p runku-runtime -p runku-gateway -p runku-local -p runku-cli --all-targets --all-features -- -D warnings

otlp-export-check: storage-up sdk-typescript-check sdk-server-check
	RUNKU_TEST_POSTGRES_URL="postgres://runku:runku_local_test_only@127.0.0.1:$${RUNKU_POSTGRES_PORT:-55432}/runku_test" cargo test -p runku-otel -p runku-observability -p runku-local -p runku-cli --all-features --locked -- --test-threads=1
	cargo clippy -p runku-otel -p runku-observability -p runku-local -p runku-cli --all-targets --all-features -- -D warnings

development-access-check: storage-up
	RUNKU_TEST_POSTGRES_URL="postgres://runku:runku_local_test_only@127.0.0.1:$${RUNKU_POSTGRES_PORT:-55432}/runku_test" cargo test -p runku-development-access -p runku-local -p runku-cli --all-features --locked -- --test-threads=1
	cargo clippy -p runku-development-access -p runku-local -p runku-cli --all-targets --all-features -- -D warnings

remote-workspace-protocol-check:
	cargo test -p runku-protocol --all-features --locked -- --test-threads=1
	cargo clippy -p runku-protocol --all-targets --all-features -- -D warnings

remote-workspace-service-check: storage-up
	RUNKU_TEST_POSTGRES_URL="postgres://runku:runku_local_test_only@127.0.0.1:$${RUNKU_POSTGRES_PORT:-55432}/runku_test" cargo test -p runku-development-service --all-features --locked -- --test-threads=1
	cargo clippy -p runku-development-service --all-targets --all-features -- -D warnings

remote-workspace-client-check: storage-up
	RUNKU_TEST_POSTGRES_URL="postgres://runku:runku_local_test_only@127.0.0.1:$${RUNKU_POSTGRES_PORT:-55432}/runku_test" cargo test -p runku-protocol -p runku-development-service -p runku-development-client -p runku-build -p runku-cli --all-features --locked -- --test-threads=1
	cargo clippy -p runku-protocol -p runku-development-service -p runku-development-client -p runku-build -p runku-cli --all-targets --all-features -- -D warnings

remote-release-freeze-check: storage-up
	RUNKU_TEST_POSTGRES_URL="postgres://runku:runku_local_test_only@127.0.0.1:$${RUNKU_POSTGRES_PORT:-55432}/runku_test" cargo test -p runku-protocol -p runku-development -p runku-release-repository -p runku-compatibility -p runku-gateway -p runku-development-service -p runku-development-client -p runku-build -p runku-cli --all-features --locked -- --test-threads=1
	cargo clippy -p runku-protocol -p runku-development -p runku-release-repository -p runku-compatibility -p runku-gateway -p runku-development-service -p runku-development-client -p runku-build -p runku-cli --all-targets --all-features -- -D warnings

remote-workspace-check: storage-up sdk-typescript-check sdk-server-check
	RUNKU_TEST_POSTGRES_URL="postgres://runku:runku_local_test_only@127.0.0.1:$${RUNKU_POSTGRES_PORT:-55432}/runku_test" cargo test -p runku-development-access -p runku-development -p runku-release-repository -p runku-compatibility -p runku-protocol -p runku-gateway -p runku-development-service -p runku-development-client -p runku-build -p runku-local -p runku-cli --all-features --locked -- --test-threads=1
	cargo clippy -p runku-development-access -p runku-development -p runku-release-repository -p runku-compatibility -p runku-protocol -p runku-gateway -p runku-development-service -p runku-development-client -p runku-build -p runku-local -p runku-cli --all-targets --all-features -- -D warnings

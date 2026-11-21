# Targets mirror the "Команды" section of AGENTS.md.
# Targets for directories that do not exist yet skip without error.

.PHONY: build generate-check lint test test-integration helm-lint tf-validate ansible-lint psa infra-test talos-check precommit

build:
	go build ./...

# Generated code must match contracts/: go generate must not change the working tree.
# Uncommitted changes made before the run do not fail the check.
TREE_STATE = { git diff HEAD; git ls-files -o --exclude-standard -z | xargs -0 shasum 2>/dev/null; } | shasum

generate-check:
	@before=$$($(TREE_STATE)); \
	go generate ./... || exit 1; \
	after=$$($(TREE_STATE)); \
	if [ "$$before" != "$$after" ]; then git status --short; echo "generate-check: generated code is stale, run go generate ./... and commit"; exit 1; fi

lint:
	golangci-lint run

# Unit and property-based tests.
test:
	go test -race ./...

# Testcontainers, requires Docker.
test-integration:
	go test -race -tags integration ./...

# Library charts are linted but not rendered; slopay-lib is rendered through its example consumer.
helm-lint:
	@charts=$$(for f in charts/*/Chart.yaml; do [ -f "$$f" ] && dirname "$$f"; done); \
	if [ -z "$$charts" ]; then echo "helm-lint: no charts/*/Chart.yaml, skipping"; exit 0; fi; \
	set -e; \
	for c in $$charts; do \
		if grep -q '^dependencies:' "$$c/Chart.yaml"; then helm dependency build "$$c"; fi; \
	done; \
	helm lint $$charts; \
	for c in $$charts; do \
		if ! grep -q '^type: *library' "$$c/Chart.yaml"; then helm template "$$c" > /dev/null; fi; \
	done; \
	if [ -x charts/slopay-lib/example/test.sh ]; then charts/slopay-lib/example/test.sh; fi

tf-validate:
	@if [ ! -d infra/terraform ]; then echo "tf-validate: no infra/terraform/, skipping"; exit 0; fi; \
	terraform -chdir=infra/terraform init -backend=false -input=false && \
	terraform -chdir=infra/terraform validate

ansible-lint:
	@if [ ! -d infra/ansible ]; then echo "ansible-lint: no infra/ansible/, skipping"; exit 0; fi; \
	ansible-lint infra/ansible

# PowerShell scripts in infra/hyperv: PSScriptAnalyzer and Pester, neither needs Hyper-V.
# Skipped without pwsh; modules: Install-Module PSScriptAnalyzer, Pester (versions pinned in pr.yml).
psa:
	@if [ ! -d infra/hyperv ]; then echo "psa: no infra/hyperv/, skipping"; exit 0; fi; \
	if ! command -v pwsh >/dev/null; then echo "psa: pwsh not installed, skipping"; exit 0; fi; \
	pwsh -NoProfile -File infra/hyperv/Invoke-Checks.ps1 -Check Lint

infra-test:
	@if [ ! -d infra/hyperv ]; then echo "infra-test: no infra/hyperv/, skipping"; exit 0; fi; \
	if ! command -v pwsh >/dev/null; then echo "infra-test: pwsh not installed, skipping"; exit 0; fi; \
	pwsh -NoProfile -File infra/hyperv/Invoke-Checks.ps1 -Check Test

# infra/talos: gen.sh syntax and patch rendering on the example values; needs neither talosctl nor a cluster.
talos-check:
	@if [ ! -d infra/talos ]; then echo "talos-check: no infra/talos/, skipping"; exit 0; fi; \
	set -e; bash -n infra/talos/gen.sh; \
	tmp=$$(mktemp -d); trap 'rm -rf "$$tmp"' EXIT; \
	sed 's/CHANGE_ME/v0.0.0/' infra/talos/env.example > "$$tmp/env"; \
	ENV_FILE="$$tmp/env" OUT_DIR="$$tmp/out" infra/talos/gen.sh --render; \
	grep -q 'talos-cp-1' "$$tmp/out/patches/controlplane.yaml"; \
	grep -q 'physical: true' "$$tmp/out/patches/worker.yaml"; \
	grep -q 'kind: UserVolumeConfig' "$$tmp/out/patches/worker-data-volume.yaml"

# gitleaks and basic checks; once per clone: pre-commit install
precommit:
	pre-commit run --all-files

# Targets mirror the "Команды" section of AGENTS.md.
# Targets for directories that do not exist yet skip without error.

.PHONY: build generate-check lint test test-integration helm-lint tf-validate ansible-lint ansible-test bootstrap-check stand-secrets-check karpenter-kwok-check runbook-check ci-check erc20-test policy precommit

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

# Library charts are linted but not rendered; weigate-lib is rendered through its example consumer.
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
	if [ -x charts/weigate-lib/example/test.sh ]; then charts/weigate-lib/example/test.sh; fi

tf-validate:
	@if [ ! -d infra/terraform ]; then echo "tf-validate: no infra/terraform/, skipping"; exit 0; fi; \
	for d in infra/terraform/*/; do \
		terraform -chdir=$$d init -backend=false -input=false || exit 1; \
		terraform -chdir=$$d validate || exit 1; \
		if [ -d $$d/tests ]; then terraform -chdir=$$d test || exit 1; fi; \
	done

ansible-lint:
	@if [ ! -d infra/ansible ]; then echo "ansible-lint: no infra/ansible/, skipping"; exit 0; fi; \
	ansible-lint infra/ansible

# infra/ansible/roles/strimzi: both profiles generated into a temporary directory and checked. Needs ansible and yq; no cluster.
ansible-test:
	@if [ ! -f infra/ansible/roles/strimzi/tests/test.sh ]; then echo "ansible-test: no role tests, skipping"; exit 0; fi; \
	infra/ansible/roles/strimzi/tests/test.sh

# infra/bootstrap: shellcheck, helm template of Argo CD with the script's values, health-check and RBAC checks.
# Needs helm and yq plus network access to the Argo CD chart repo; no cluster.
bootstrap-check:
	@if [ ! -d infra/bootstrap ]; then echo "bootstrap-check: no infra/bootstrap/, skipping"; exit 0; fi; \
	set -e; \
	if command -v shellcheck >/dev/null; then shellcheck infra/bootstrap/*.sh; \
	elif [ -n "$$CI" ]; then echo "bootstrap-check: shellcheck is required in CI"; exit 1; \
	else echo "bootstrap-check: shellcheck not installed, skipping it"; fi; \
	infra/bootstrap/test.sh

# infra/stand-secrets: shellcheck and the script against a fake kubectl and cast; needs openssl and jq, no cluster.
stand-secrets-check:
	@if [ ! -d infra/stand-secrets ]; then echo "stand-secrets-check: no infra/stand-secrets/, skipping"; exit 0; fi; \
	set -e; \
	if command -v shellcheck >/dev/null; then shellcheck infra/stand-secrets/*.sh; \
	elif [ -n "$$CI" ]; then echo "stand-secrets-check: shellcheck is required in CI"; exit 1; \
	else echo "stand-secrets-check: shellcheck not installed, skipping it"; fi; \
	infra/stand-secrets/test.sh

# infra/karpenter-kwok: shellcheck and build.sh against a fake git and ko; no network, no registry.
karpenter-kwok-check:
	@if [ ! -d infra/karpenter-kwok ]; then echo "karpenter-kwok-check: no infra/karpenter-kwok/, skipping"; exit 0; fi; \
	set -e; \
	if command -v shellcheck >/dev/null; then shellcheck infra/karpenter-kwok/*.sh; \
	elif [ -n "$$CI" ]; then echo "karpenter-kwok-check: shellcheck is required in CI"; exit 1; \
	else echo "karpenter-kwok-check: shellcheck not installed, skipping it"; fi; \
	infra/karpenter-kwok/test.sh

# docs/runbooks: commands and paths of the deploy runbook match infra/ and .github/; needs grep only.
runbook-check:
	@if [ ! -x docs/runbooks/test.sh ]; then echo "runbook-check: no docs/runbooks/test.sh, skipping"; exit 0; fi; \
	set -e; \
	if command -v shellcheck >/dev/null; then shellcheck docs/runbooks/test.sh; \
	elif [ -n "$$CI" ]; then echo "runbook-check: shellcheck is required in CI"; exit 1; \
	else echo "runbook-check: shellcheck not installed, skipping it"; fi; \
	docs/runbooks/test.sh

# .github: actionlint (with shellcheck of run: blocks) and dry runs of the image build scripts; needs git and yq.
ci-check:
	@set -e; \
	if command -v actionlint >/dev/null; then actionlint; \
	elif [ -n "$$CI" ]; then echo "ci-check: actionlint is required in CI"; exit 1; \
	else echo "ci-check: actionlint not installed, skipping it"; fi; \
	if command -v shellcheck >/dev/null; then shellcheck .github/scripts/*.sh; \
	elif [ -n "$$CI" ]; then echo "ci-check: shellcheck is required in CI"; exit 1; \
	else echo "ci-check: shellcheck not installed, skipping it"; fi; \
	.github/scripts/changed-components_test.sh; \
	.github/scripts/bump-gitops-tags_test.sh

# testkit/erc20: forge test; forge-std is fetched into the git-ignored lib/ on first run, needs foundry and network.
erc20-test:
	@set -e; \
	if ! command -v forge >/dev/null; then \
		if [ -n "$$CI" ]; then echo "erc20-test: forge is required in CI"; exit 1; fi; \
		echo "erc20-test: forge not installed, skipping"; exit 0; \
	fi; \
	cd testkit/erc20; \
	if [ ! -d lib/forge-std ]; then forge install --no-git foundry-rs/forge-std@v1.17.0; fi; \
	forge test

# No Bitnami images or charts in charts, Dockerfiles and infra.
policy:
	.github/scripts/policy.sh

# gitleaks and basic checks; once per clone: pre-commit install
precommit:
	pre-commit run --all-files

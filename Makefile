# Targets mirror the "Команды" section of AGENTS.md.
# Targets for directories that do not exist yet skip without error.

.PHONY: build generate-check lint test test-integration helm-lint tf-validate ansible-lint precommit

build:
	go build ./...

# Generated code must match contracts/.
generate-check:
	go generate ./...
	git diff --exit-code
	@test -z "$$(git status --porcelain --untracked-files=all)" || { git status --short; echo "go generate produced untracked files"; exit 1; }

lint:
	golangci-lint run

# Unit and property-based tests.
test:
	go test -race ./...

# Testcontainers, requires Docker.
test-integration:
	go test -race -tags integration ./...

# Library charts are linted but not rendered.
helm-lint:
	@if [ ! -d charts ]; then echo "helm-lint: no charts/, skipping"; exit 0; fi; \
	set -e; \
	for c in charts/*/; do \
		if grep -q '^dependencies:' "$$c/Chart.yaml"; then helm dependency build "$$c"; fi; \
	done; \
	helm lint charts/*; \
	for c in charts/*/; do \
		if ! grep -q '^type: *library' "$$c/Chart.yaml"; then helm template "$$c" > /dev/null; fi; \
	done

tf-validate:
	@if [ ! -d infra/terraform ]; then echo "tf-validate: no infra/terraform/, skipping"; exit 0; fi; \
	terraform -chdir=infra/terraform init -backend=false -input=false && \
	terraform -chdir=infra/terraform validate

ansible-lint:
	@if [ ! -d infra/ansible ]; then echo "ansible-lint: no infra/ansible/, skipping"; exit 0; fi; \
	ansible-lint infra/ansible

# gitleaks and basic checks; once per clone: pre-commit install
precommit:
	pre-commit run --all-files

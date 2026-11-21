#!/usr/bin/env bash
# Dry run of changed-components.sh on a throwaway git repository: one commit per rule of the table
# in AGENTS.md ("Сборка образов"). Needs only git.
set -euo pipefail

script="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/changed-components.sh"
bash -n "$script"

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
cd "$tmp"
git init -q
git config user.name test
git config user.email test@example.invalid
git config commit.gpgsign false

put() { mkdir -p "$(dirname "$1")"; echo "${2:-x}" >> "$1"; }

# Services with event schemas in the image, one of them with code generated from its OpenAPI contract.
put services/a-service/Dockerfile 'COPY contracts/schemas/ contracts/schemas/'
put services/a-service/internal/api/generate.go \
  '//go:generate go tool oapi-codegen -config oapi-codegen.yaml ../../../../contracts/openapi/a-service.yaml'
put services/b-service/Dockerfile 'COPY contracts/schemas.go contracts/'
put services/b-service/main.go
# A test component without schemas, a tool and a testkit directory without an image.
put testkit/mock/Dockerfile 'COPY pkg/ pkg/'
put tools/bridge/Dockerfile 'COPY pkg/ pkg/'
put testkit/stand-setup/main.go
put pkg/money/money.go
put go.mod
put go.sum
put .dockerignore
put .github/workflows/build.yml
put .github/workflows/pr.yml
put contracts/openapi/a-service.yaml
put contracts/openapi/b-service.yaml
put contracts/asyncapi/a-service.yaml
put contracts/schemas.go
put contracts/schemas/invoice.json
put charts/a-service/Chart.yaml
put charts/weigate-lib/Chart.yaml
put docs/SRS.md
git add -A
git commit -qm init

all='["a-service","b-service","bridge","mock"]'
fail=0

# check <description> <expected> <path>...: commits a change of the paths and runs the script on it.
check() {
  local name="$1" want="$2" got
  shift 2
  for p in "$@"; do put "$p"; done
  git add -A
  git commit -qm "$name"
  got="$("$script" HEAD~1 HEAD)"
  if [ "$got" = "$want" ]; then
    echo "ok   $name"
  else
    echo "FAIL $name: want $want, got $got"
    fail=1
  fi
}

check "service code" '["a-service"]' services/a-service/main.go
check "two components" '["b-service","mock"]' services/b-service/main.go testkit/mock/main.go
check "tool" '["bridge"]' tools/bridge/main.go
check "Dockerfile of a component" '["mock"]' testkit/mock/Dockerfile
check "testkit without an image" '[]' testkit/stand-setup/main.go
check "pkg" "$all" pkg/money/money.go
check "go.mod" "$all" go.mod
check "go.sum" "$all" go.sum
check ".dockerignore" "$all" .dockerignore
check "build workflow" "$all" .github/workflows/build.yml
check "other workflow" '[]' .github/workflows/pr.yml
check "OpenAPI contract with generated code" '["a-service"]' contracts/openapi/a-service.yaml
check "OpenAPI contract without generated code" '[]' contracts/openapi/b-service.yaml
check "AsyncAPI contract" '[]' contracts/asyncapi/a-service.yaml
check "event schema" '["a-service","b-service"]' contracts/schemas/invoice.json
check "schemas package" '["a-service","b-service"]' contracts/schemas.go
check "charts" '[]' charts/a-service/Chart.yaml charts/weigate-lib/Chart.yaml
check "docs" '[]' docs/SRS.md

git mv -k services/b-service/main.go services/a-service/moved.go
git commit -qm "move between components"
got="$("$script" HEAD~1 HEAD)"
if [ "$got" = '["a-service","b-service"]' ]; then echo "ok   move between components"; else echo "FAIL move: $got"; fail=1; fi

for base in "" 0000000000000000000000000000000000000000 1234567; do
  got="$("$script" "$base" HEAD)"
  if [ "$got" = "$all" ]; then echo "ok   base '$base' selects all"; else echo "FAIL base '$base': $got"; fail=1; fi
done
got="$("$script" --all)"
if [ "$got" = "$all" ]; then echo "ok   --all"; else echo "FAIL --all: $got"; fail=1; fi

if "$script" HEAD 2>/dev/null; then echo "FAIL one argument accepted"; fail=1; else echo "ok   usage error"; fi

exit "$fail"

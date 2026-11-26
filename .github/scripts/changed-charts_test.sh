#!/usr/bin/env bash
# Dry run of changed-charts.sh on a throwaway git repository. Needs only git.
set -euo pipefail

script="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/changed-charts.sh"
bash -n "$script"

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
cd "$tmp"
git init -q
git config user.name test
git config user.email test@example.invalid
git config commit.gpgsign false

write_chart() {
  local name="$1" version="$2" type="${3:-application}"
  mkdir -p "charts/$name/templates"
  cat > "charts/$name/Chart.yaml" <<YAML
apiVersion: v2
name: $name
type: $type
version: $version
YAML
}

write_chart a-service 0.1.0
write_chart b-service 0.2.0
write_chart weigate-lib 0.1.0 library
git add -A
git commit -qm init

fail=0

expect() {
  local name="$1" want="$2" got="$3"
  if [ "$got" = "$want" ]; then echo "ok   $name"; else echo "FAIL $name: want $want, got $got"; fail=1; fi
}

# Template change without version bump: nothing to publish.
echo "# note" >> charts/a-service/templates/x.yaml
git add -A && git commit -qm "template only"
expect "template without version bump" '[]' "$("$script" HEAD~1 HEAD)"

# Version bump of a single application chart.
write_chart a-service 0.1.1
git add -A && git commit -qm "bump a-service"
expect "single app chart version bump" '["a-service"]' "$("$script" HEAD~1 HEAD)"

# Library chart version bump is ignored.
write_chart weigate-lib 0.1.1 library
git add -A && git commit -qm "bump library"
expect "library chart ignored" '[]' "$("$script" HEAD~1 HEAD)"

# New application chart added.
write_chart c-service 0.1.0
git add -A && git commit -qm "new chart"
expect "new app chart" '["c-service"]' "$("$script" HEAD~1 HEAD)"

# Two charts bumped together.
write_chart a-service 0.1.2
write_chart b-service 0.2.1
git add -A && git commit -qm "bump two"
expect "two charts bumped" '["a-service","b-service"]' "$("$script" HEAD~1 HEAD)"

all_now='["a-service","b-service","c-service"]'
for base in "" 0000000000000000000000000000000000000000 1234567; do
  expect "base '$base' selects all" "$all_now" "$("$script" "$base" HEAD)"
done
expect "--all" "$all_now" "$("$script" --all)"

if "$script" HEAD 2>/dev/null; then echo "FAIL one argument accepted"; fail=1; else echo "ok   usage error"; fi

exit "$fail"

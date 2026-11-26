#!/usr/bin/env bash
# Checks bump-gitops-charts.sh on a throwaway gitops tree and weigate charts directory.
# Needs yq (mikefarah).
set -euo pipefail

script="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/bump-gitops-charts.sh"
bash -n "$script"

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
gitops="$tmp/gitops"
weigate="$tmp/weigate"
fail=0
ok() { echo "ok   $1"; }
bad() { echo "FAIL $1"; fail=1; }
expect() {
  if [ "$3" = "$2" ]; then ok "$1"; else bad "$1: want '$2', got '$3'"; fi
}

chart() {
  mkdir -p "$weigate/charts/$1"
  printf 'apiVersion: v2\nname: %s\nversion: %s\n' "$1" "$2" > "$weigate/charts/$1/Chart.yaml"
}

app() {
  mkdir -p "$gitops/apps/services"
  cat > "$gitops/apps/services/$1.yaml" <<APP
# Wave 4: $1
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: $1
  namespace: argocd
spec:
  project: weigate
  sources:
    - repoURL: zot.registry.svc:5000/weigate/charts
      chart: $1
      targetRevision: $2
      helm:
        releaseName: $1
        valueFiles:
          - \$values/services/$1/values.yaml
    - repoURL: https://github.com/m5tshift/weigate-gitops
      targetRevision: main
      ref: values
APP
}

chart a 0.2.0
app a 0.1.0
cp "$gitops/apps/services/a.yaml" "$tmp/a.orig"
out="$("$script" "$gitops" "$weigate" a)"
expect "prints updated chart" a "$out"
expect "targetRevision bumped" "0.2.0" "$(yq -r '.spec.sources[0].targetRevision' "$gitops/apps/services/a.yaml")"
expect "values ref targetRevision untouched" "main" "$(yq -r '.spec.sources[1].targetRevision' "$gitops/apps/services/a.yaml")"
changed="$(diff "$tmp/a.orig" "$gitops/apps/services/a.yaml" | grep -c '^[<>]' || true)"
expect "only targetRevision line changes" 2 "$changed"

# Same version: no-op.
expect "same version is no-op" "" "$("$script" "$gitops" "$weigate" a)"

# Older version: must not roll back.
chart a 0.1.9
out="$("$script" "$gitops" "$weigate" a 2>/dev/null)"
expect "older version prints nothing" "" "$out"
expect "older version not rolled back" "0.2.0" "$(yq -r '.spec.sources[0].targetRevision' "$gitops/apps/services/a.yaml")"

# Undeployed chart skipped; deployed one updated.
chart b 0.3.0
app b 0.2.0
chart undeployed 0.1.0
out="$("$script" "$gitops" "$weigate" undeployed b 2>/dev/null)"
expect "undeployed skipped, deployed updated" b "$out"

# Missing Chart.yaml fails.
if "$script" "$gitops" "$weigate" nope 2>/dev/null; then bad "missing chart accepted"; else ok "missing chart rejected"; fi

exit "$fail"

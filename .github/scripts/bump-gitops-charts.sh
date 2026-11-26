#!/usr/bin/env bash
# Sets targetRevision of the Zot OCI chart source in apps/services/<chart>.yaml of a weigate-gitops
# checkout (SRS CI-5.3) to the version in charts/<chart>/Chart.yaml of weigate, and prints the names
# of the updated charts. Leaves the rest of the file (comments included) intact.
# A chart without an Application or without a Zot chart source is skipped.
# A chart whose current targetRevision is already newer (semver) is skipped so an older run cannot roll back.
#   bump-gitops-charts.sh <gitops-dir> <weigate-dir> <chart>...
# Needs yq (mikefarah) to read Chart.yaml and verify the result.
set -euo pipefail

if [ $# -lt 3 ]; then
  echo "usage: $0 <gitops-dir> <weigate-dir> <chart>..." >&2
  exit 2
fi
gitops="$1"
weigate="$2"
shift 2

semver_gt() {
  # Returns 0 if $1 > $2 (numeric dot-separated versions).
  [ "$1" != "$2" ] && [ "$(printf '%s\n%s\n' "$1" "$2" | sort -V | tail -n1)" = "$1" ]
}

for c in "$@"; do
  chart_yaml="$weigate/charts/$c/Chart.yaml"
  if [ ! -f "$chart_yaml" ]; then
    echo "bump-gitops-charts: no $chart_yaml" >&2
    exit 1
  fi
  version="$(yq '.version' "$chart_yaml")"
  if ! [[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+([.-][0-9A-Za-z.-]+)?$ ]]; then
    echo "bump-gitops-charts: invalid chart version '$version' in $chart_yaml" >&2
    exit 2
  fi

  app="$gitops/apps/services/$c.yaml"
  if [ ! -f "$app" ]; then
    echo "bump-gitops-charts: $c is not deployed in apps/services; skipping" >&2
    continue
  fi

  current="$(yq -r '.spec.sources[] | select(.repoURL == "zot.registry.svc:5000/weigate/charts" and .chart == "'"$c"'") | .targetRevision' "$app")"
  if [ -z "$current" ]; then
    echo "bump-gitops-charts: $app has no Zot OCI chart source for $c; skipping" >&2
    continue
  fi
  if [ "$current" = "$version" ]; then continue; fi
  if semver_gt "$current" "$version"; then
    echo "bump-gitops-charts: $c is already at $current, newer than $version; skipping" >&2
    continue
  fi

  updated="$(awk -v ver="$version" '
    /repoURL:[[:space:]]*zot\.registry\.svc:5000\/weigate\/charts/ { in_zot = 1; print; next }
    in_zot && /^[[:space:]]*-[[:space:]]/ { in_zot = 0 }
    in_zot && /^  [^ ]/ { in_zot = 0 }
    in_zot && /^[[:space:]]*targetRevision:/ {
      sub(/targetRevision:.*/, "targetRevision: " ver)
      in_zot = 0
      print
      next
    }
    { print }
  ' "$app")"
  printf '%s\n' "$updated" > "$app"

  got="$(yq -r '.spec.sources[] | select(.repoURL == "zot.registry.svc:5000/weigate/charts" and .chart == "'"$c"'") | .targetRevision' "$app")"
  if [ "$got" != "$version" ]; then
    echo "bump-gitops-charts: failed to update targetRevision in $app (got '$got')" >&2
    exit 1
  fi
  echo "$c"
done

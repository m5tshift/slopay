#!/usr/bin/env bash
# Prints the application Helm charts whose Chart.yaml version changed as a JSON array of names (SRS CI-5.3).
# Library charts (type: library) are excluded.
#   changed-charts.sh --all          every application chart
#   changed-charts.sh <base> <head>  application charts whose version changed between base and head;
#                                    an empty, all-zero or unknown base selects all
# Run from the repository root with head checked out.
set -euo pipefail

usage() {
  echo "usage: $0 --all | <base> <head>" >&2
  exit 2
}

chart_version() {
  awk '/^version:[[:space:]]*/ { v=$2; gsub(/^["'\'']|["'\'']$/, "", v); print v; exit }'
}

app_charts() {
  local f
  for f in charts/*/Chart.yaml; do
    [ -f "$f" ] || continue
    if grep -qE '^type:[[:space:]]*library([[:space:]]|$)' "$f"; then continue; fi
    basename "$(dirname "$f")"
  done
}

all=0
base=""
head=""
if [ $# -eq 1 ] && [ "$1" = --all ]; then
  all=1
elif [ $# -eq 2 ]; then
  base="$1"
  head="$2"
  if [ -z "$base" ] || [[ "$base" =~ ^0+$ ]] || ! git cat-file -e "$base^{commit}" 2>/dev/null; then
    all=1
  fi
else
  usage
fi

selected=""
if [ "$all" -eq 1 ]; then
  selected="$(app_charts)"
else
  while IFS= read -r name; do
    [ -n "$name" ] || continue
    f="charts/$name/Chart.yaml"
    new_v="$(git show "$head:$f" 2>/dev/null | chart_version || true)"
    old_v="$(git show "$base:$f" 2>/dev/null | chart_version || true)"
    if [ -n "$new_v" ] && [ "$new_v" != "$old_v" ]; then
      selected="$selected"$'\n'"$name"
    fi
  done <<< "$(app_charts)"
fi

for c in $selected; do echo "$c"; done | sort -u |
  awk 'BEGIN { printf "[" } { printf "%s\"%s\"", (NR > 1 ? "," : ""), $0 } END { print "]" }'

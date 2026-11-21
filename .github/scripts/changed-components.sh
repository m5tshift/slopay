#!/usr/bin/env bash
# Prints the components whose images CI must rebuild as a JSON array of names (AGENTS.md, "Сборка образов").
# A component is a directory services/<name>, testkit/<name> or tools/<name> with a Dockerfile.
#   changed-components.sh --all          every component
#   changed-components.sh <base> <head>  components affected by the commits base..head; an empty,
#                                        all-zero or unknown base (first push, force push) selects all
# Run from the repository root with head checked out: components are looked up in the working tree.
set -euo pipefail

usage() {
  echo "usage: $0 --all | <base> <head>" >&2
  exit 2
}

component_dirs() {
  local f
  for f in services/*/Dockerfile testkit/*/Dockerfile tools/*/Dockerfile; do
    if [ -f "$f" ]; then dirname "$f"; fi
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

dirs="$(component_dirs)"
selected=""

if [ "$all" -eq 1 ]; then
  selected="$dirs"
else
  while IFS= read -r path; do
    [ -n "$path" ] || continue
    case "$path" in
      # Shared Go code and the build definition: every component.
      pkg/* | go.mod | go.sum | .dockerignore | .github/workflows/build.yml)
        selected="$dirs"
        break
        ;;
      services/*/* | testkit/*/* | tools/*/*)
        d="$(echo "$path" | cut -d/ -f1-2)"
        if [ -f "$d/Dockerfile" ]; then selected="$selected"$'\n'"$d"; fi
        ;;
      # Generated code: components whose go:generate reads the changed OpenAPI contract.
      contracts/openapi/*)
        for d in $dirs; do
          if grep -rqs --include='*.go' -e "go:generate.*$path" "$d"; then selected="$selected"$'\n'"$d"; fi
        done
        ;;
      # Event schemas embedded by pkg/kafka: components whose image copies them.
      contracts/schemas.go | contracts/schemas/*)
        for d in $dirs; do
          if grep -qs -e 'contracts/schemas' "$d/Dockerfile"; then selected="$selected"$'\n'"$d"; fi
        done
        ;;
    esac
  done <<< "$(git diff --no-renames --name-only "$base" "$head")"
fi

for d in $selected; do basename "$d"; done | sort -u |
  awk 'BEGIN { printf "[" } { printf "%s\"%s\"", (NR > 1 ? "," : ""), $0 } END { print "]" }'

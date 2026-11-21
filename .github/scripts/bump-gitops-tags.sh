#!/usr/bin/env bash
# Sets image.tag in services/<component>/values.yaml of a slopay-gitops checkout (SRS CI-5.2) and prints
# the names of the updated components. The rest of the file, comments included, is left as is.
# A component whose current tag is a later commit of slopay than <tag> is skipped, so a run of an older
# commit that finishes last does not roll the tag back.
#   bump-gitops-tags.sh <gitops-dir> <slopay-dir> <tag> <component>...
# Needs yq (mikefarah) to check the result.
set -euo pipefail

if [ $# -lt 4 ]; then
  echo "usage: $0 <gitops-dir> <slopay-dir> <tag> <component>..." >&2
  exit 2
fi
gitops="$1"
slopay="$2"
tag="$3"
shift 3
if ! [[ "$tag" =~ ^[0-9a-f]{7,40}$ ]]; then
  echo "bump-gitops-tags: tag must be a commit SHA, got '$tag'" >&2
  exit 2
fi

for c in "$@"; do
  f="$gitops/services/$c/values.yaml"
  if [ ! -f "$f" ]; then
    echo "bump-gitops-tags: $f not found" >&2
    exit 1
  fi
  current="$(yq '.image.tag' "$f")"
  if [ "$current" = "$tag" ]; then continue; fi
  if git -C "$slopay" merge-base --is-ancestor "$tag" "$current" 2>/dev/null; then
    echo "bump-gitops-tags: $c is already at $current, a later commit than $tag; skipping" >&2
    continue
  fi
  # The tag is quoted: an all-digit or exponent-like SHA would otherwise be read as a number.
  updated="$(awk -v tag="$tag" '
    /^image:/ { block = 1; print; next }
    block && /^[^ #]/ { block = 0 }
    block && /^  tag:/ { print "  tag: \"" tag "\""; next }
    { print }
  ' "$f")"
  printf '%s\n' "$updated" > "$f"
  if [ "$(yq '.image.tag' "$f")" != "$tag" ]; then
    echo "bump-gitops-tags: no image.tag line in $f" >&2
    exit 1
  fi
  echo "$c"
done

#!/usr/bin/env bash
# Checks bump-gitops-tags.sh on a throwaway gitops tree and a throwaway slopay history.
# Needs git and yq (mikefarah).
set -euo pipefail

script="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/bump-gitops-tags.sh"
bash -n "$script"

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
gitops="$tmp/gitops"
slopay="$tmp/slopay"
fail=0
ok() { echo "ok   $1"; }
bad() { echo "FAIL $1"; fail=1; }
# expect <name> <want> <got>
expect() {
  if [ "$3" = "$2" ]; then ok "$1"; else bad "$1: want '$2', got '$3'"; fi
}

git init -q "$slopay"
for i in 1 2; do
  git -C "$slopay" -c user.name=test -c user.email=test@example.invalid -c commit.gpgsign=false \
    commit -q --allow-empty -m "c$i"
done
old="$(git -C "$slopay" rev-parse --short=7 HEAD~1)"
new="$(git -C "$slopay" rev-parse --short=7 HEAD)"

# values <component> <tag>: a values file shaped like the ones in slopay-gitops.
values() {
  mkdir -p "$gitops/services/$1"
  cat > "$gitops/services/$1/values.yaml" <<VALUES
# $1 on the minimal stand.
image:
  repository: ghcr.io/m5tshift/slopay/$1
  # Updated by CI of slopay after the image build; not edited by hand.
  tag: $2

sidecar:
  tag: keep

replicaCount: 1
VALUES
}

# Stub tag that is not a commit of the history: replaced, only that line changes.
values a 8edacb2
cp "$gitops/services/a/values.yaml" "$tmp/a.orig"
out="$("$script" "$gitops" "$slopay" "$new" a)"
expect "prints the updated component" a "$out"
expect "image.tag set" "$new" "$(yq '.image.tag' "$gitops/services/a/values.yaml")"
expect "other tag keys kept" keep "$(yq '.sidecar.tag' "$gitops/services/a/values.yaml")"
changed="$(diff "$tmp/a.orig" "$gitops/services/a/values.yaml" | grep -c '^[<>]' || true)"
expect "only the tag line changes" 2 "$changed"

# Same tag again: nothing to do.
out="$("$script" "$gitops" "$slopay" "$new" a)"
expect "same tag is a no-op" "" "$out"

# An older commit must not roll the tag back.
out="$("$script" "$gitops" "$slopay" "$old" a 2>/dev/null)"
expect "older commit prints nothing" "" "$out"
expect "older commit skipped" "$new" "$(yq '.image.tag' "$gitops/services/a/values.yaml")"

# A newer commit replaces an older one; several components at once.
values b "$old"
values c "$old"
out="$("$script" "$gitops" "$slopay" "$new" b c | tr '\n' ' ')"
expect "newer commit, two components" "b c " "$out"

# An all-digit SHA stays a string.
values d 8edacb2
"$script" "$gitops" "$slopay" 1234567 d > /dev/null
expect "numeric-looking tag quoted" '!!str' "$(yq '.image.tag | tag' "$gitops/services/d/values.yaml")"

# A component that is not deployed (no values file) is skipped; the others are still updated.
values f 8edacb2
rc=0
out="$("$script" "$gitops" "$slopay" "$new" missing f 2>"$tmp/err")" || rc=$?
expect "undeployed component: exit code" 0 "$rc"
expect "undeployed component: deployed one updated" f "$out"
expect "undeployed component: tag set" "$new" "$(yq '.image.tag' "$gitops/services/f/values.yaml")"
expect "undeployed component: message" "bump-gitops-tags: missing is not deployed on the stand; skipping" "$(cat "$tmp/err")"
rc=0
out="$("$script" "$gitops" "$slopay" "$new" missing 2>/dev/null)" || rc=$?
expect "only undeployed components: exit code" 0 "$rc"
expect "only undeployed components: nothing printed" "" "$out"

# Errors: no image.tag, bad tag.
mkdir -p "$gitops/services/e"
echo "replicaCount: 1" > "$gitops/services/e/values.yaml"
if "$script" "$gitops" "$slopay" "$new" e 2>/dev/null; then bad "file without image.tag accepted"; else ok "no image.tag"; fi
if "$script" "$gitops" "$slopay" 'main;x' a 2>/dev/null; then bad "bad tag accepted"; else ok "bad tag"; fi

exit "$fail"

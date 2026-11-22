#!/usr/bin/env bash
# Offline checks for build.sh with fake git and ko: no network, no registry.
set -euo pipefail

dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
script="$dir/build.sh"
bash -n "$script"

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
mkdir "$tmp/bin"
export LOG="$tmp/argv.log"
: > "$LOG"
for c in git go; do
  cat > "$tmp/bin/$c" <<FAKE
#!/usr/bin/env bash
echo "$c \$*" >> "\$LOG"
FAKE
done
cat > "$tmp/bin/ko" <<'FAKE'
#!/usr/bin/env bash
echo "ko repo=$KO_DOCKER_REPO $*" >> "$LOG"
FAKE
chmod +x "$tmp/bin/"*

fail() { echo "test.sh: $*" >&2; cat "$LOG" >&2; exit 1; }

PATH="$tmp/bin:$PATH" "$script"
grep -q -- '--branch v1.15.1 https://github.com/kubernetes-sigs/karpenter' "$LOG" || fail "default tag is not v1.15.1"
grep -q 'ko repo=ghcr.io/m5tshift/weigate/karpenter-kwok .*--tags v1.15.1 sigs.k8s.io/karpenter/kwok' "$LOG" || fail "default ko call"

: > "$LOG"
PATH="$tmp/bin:$PATH" REGISTRY=zot.example/karpenter-kwok KARPENTER_VERSION=v1.15.0 "$script"
grep -q 'ko repo=zot.example/karpenter-kwok .*--tags v1.15.0' "$LOG" || fail "overrides ignored"

# README and script must carry the same pinned version.
grep -q 'v1.15.1' "$dir/README.md" || fail "README does not mention v1.15.1"

# A missing tool is an error.
mkdir "$tmp/empty"
if PATH="$tmp/empty" /bin/bash "$script" 2>/dev/null; then fail "missing tools not detected"; fi
echo "karpenter-kwok: ok"

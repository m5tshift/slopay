#!/usr/bin/env bash
# Fails if charts, Dockerfiles or infra use Bitnami images or charts (AGENTS.md).
# Documentation (*.md) and scripts (*.sh) are not scanned: they may name the ban.
set -euo pipefail
cd "$(dirname "$0")/../.."

out=$({
  grep -rniI bitnami charts infra --include='*.yaml' --include='*.yml' --include='*.tpl' --include='*.tf' || true
  find . -name 'Dockerfile*' -not -path './.git/*' -print0 | xargs -0 grep -niI bitnami || true
})
if [ -n "$out" ]; then
  echo "policy: Bitnami images and charts are forbidden"
  echo "$out"
  exit 1
fi
echo "policy: ok"

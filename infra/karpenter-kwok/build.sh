#!/usr/bin/env bash
# Builds Karpenter with the KWOK provider from the pinned upstream tag and pushes the image (SRS INF-1.2).
# The user runs it after `ko login <registry>`; the agent does not.
#   REGISTRY            image repository (default: GHCR; set it to the Zot address once Zot is up)
#   KARPENTER_VERSION   upstream tag (default: the pinned one)
set -euo pipefail

KARPENTER_VERSION="${KARPENTER_VERSION:-v1.15.1}"
REGISTRY="${REGISTRY:-ghcr.io/m5tshift/weigate/karpenter-kwok}"

for cmd in git go ko; do
  command -v "$cmd" >/dev/null || { echo "build.sh: $cmd is required" >&2; exit 1; }
done

src="$(mktemp -d)"
trap 'rm -rf "$src"' EXIT
git clone -q --depth 1 --branch "$KARPENTER_VERSION" https://github.com/kubernetes-sigs/karpenter "$src"

# -B: the image name is the repository itself; the tag is the Karpenter version.
cd "$src"
KO_DOCKER_REPO="$REGISTRY" ko build -B --platform linux/amd64 --tags "$KARPENTER_VERSION" sigs.k8s.io/karpenter/kwok

#!/usr/bin/env bash
# Offline checks for infra/bootstrap: bash syntax, helm template of Argo CD with the script's values,
# health-check in argocd-cm, namespaces, read-only agent role. Needs helm and yq; no cluster.
set -euo pipefail

dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
bash -n "$dir/bootstrap.sh"

# shellcheck disable=SC2016  # the pattern is a literal sed expression
version="$(sed -n 's/^ARGOCD_CHART_VERSION="\${ARGOCD_CHART_VERSION:-\(.*\)}"$/\1/p' "$dir/bootstrap.sh")"
[ -n "$version" ] || { echo "test: chart version not found in bootstrap.sh" >&2; exit 1; }

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
export HELM_REPOSITORY_CONFIG="$tmp/repos.yaml" HELM_REPOSITORY_CACHE="$tmp/cache"

helm template argocd argo-cd --repo https://argoproj.github.io/argo-helm --version "$version" \
  --namespace argocd --values "$dir/argocd-values.yaml" > "$tmp/rendered.yaml"

fail() { echo "test: $*" >&2; exit 1; }

[ "$(yq -N 'select(.kind == "ConfigMap" and .metadata.name == "argocd-cm") | .data["resource.customizations.health.argoproj.io_Application"] | contains("hs.status")' "$tmp/rendered.yaml")" = "true" ] \
  || fail "argocd-cm has no Application health check"
[ "$(yq -N 'select(.kind == "ConfigMap" and .metadata.name == "argocd-cmd-params-cm") | .data["server.insecure"]' "$tmp/rendered.yaml")" = "true" ] \
  || fail "server.insecure is not set"
[ -z "$(yq -N 'select(.kind == "Deployment" and .metadata.name == "argocd-dex-server") | .kind' "$tmp/rendered.yaml")" ] \
  || fail "dex is rendered"
! grep -qi bitnami "$tmp/rendered.yaml" || fail "bitnami in rendered chart"

[ "$(yq -N '.metadata.name' "$dir/namespaces.yaml" | sort | tr '\n' ' ')" = "apps argocd chain data kafka local-path-storage " ] \
  || fail "namespaces differ from apps/project.yaml destinations"

! yq -N 'select(.kind == "Role") | .rules[].verbs[]' "$dir/agent-rbac.yaml" | grep -qvxE 'get|list|watch' \
  || fail "agent role is not read-only"
! yq -N 'select(.kind == "Role") | .rules[].resources[]' "$dir/agent-rbac.yaml" | grep -qxE 'secrets|configmaps|\*' \
  || fail "agent role reads secrets or configmaps"
[ -z "$(yq -N 'select(.kind == "ClusterRole" or .kind == "ClusterRoleBinding") | .kind' "$dir/agent-rbac.yaml")" ] \
  || fail "agent RBAC has cluster scope"

echo "bootstrap-check: ok"

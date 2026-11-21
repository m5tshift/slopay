#!/usr/bin/env bash
# Bootstrap of the minimal stand (SRS 6.4, INF-2.2): namespaces, Argo CD, root Application, agent RBAC.
# Idempotent: every step can be repeated. Terraform replaces this script in #53.
# The script is run by the user with the admin kubeconfig (KUBECONFIG); the agent never runs it.
#   ./bootstrap.sh                          install everything
#   ./bootstrap.sh agent-kubeconfig         print a read-only kubeconfig for the agent (namespace apps) to stdout
set -euo pipefail

ARGOCD_CHART_VERSION="${ARGOCD_CHART_VERSION:-10.18.2}"
ARGOCD_CHART_REPO="https://argoproj.github.io/argo-helm"
ROOT_URL="${ROOT_URL:-https://raw.githubusercontent.com/m5tshift/slopay-gitops/main/apps/root.yaml}"
TOKEN_DURATION="${TOKEN_DURATION:-168h}"

dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

need() {
  command -v "$1" >/dev/null || { echo "bootstrap: $1 is required" >&2; exit 1; }
}

agent_kubeconfig() {
  need kubectl
  local token server ca
  token="$(kubectl create token agent-reader -n apps --duration="$TOKEN_DURATION")"
  server="$(kubectl config view --minify --raw -o jsonpath='{.clusters[0].cluster.server}')"
  ca="$(kubectl config view --minify --raw -o jsonpath='{.clusters[0].cluster.certificate-authority-data}')"
  cat <<KUBECONFIG
apiVersion: v1
kind: Config
clusters:
  - name: slopay
    cluster:
      server: ${server}
      certificate-authority-data: ${ca}
users:
  - name: agent-reader
    user:
      token: ${token}
contexts:
  - name: slopay-apps
    context:
      cluster: slopay
      user: agent-reader
      namespace: apps
current-context: slopay-apps
KUBECONFIG
}

install() {
  need kubectl
  need helm
  need curl
  kubectl cluster-info >/dev/null

  echo "bootstrap: namespaces"
  kubectl apply -f "$dir/namespaces.yaml"

  echo "bootstrap: Argo CD chart ${ARGOCD_CHART_VERSION}"
  helm upgrade --install argocd argo-cd \
    --repo "$ARGOCD_CHART_REPO" --version "$ARGOCD_CHART_VERSION" \
    --namespace argocd --values "$dir/argocd-values.yaml" --wait --timeout 10m

  kubectl wait --for=condition=Established --timeout=2m crd/applications.argoproj.io

  echo "bootstrap: root Application from ${ROOT_URL}"
  local tmp
  tmp="$(mktemp)"
  trap 'rm -f "$tmp"' EXIT
  curl -fsSL "$ROOT_URL" -o "$tmp"
  kubectl apply -f "$tmp"

  echo "bootstrap: agent RBAC"
  kubectl apply -f "$dir/agent-rbac.yaml"

  kubectl -n argocd get applications
}

case "${1:-install}" in
  install) install ;;
  agent-kubeconfig) agent_kubeconfig ;;
  *) echo "usage: $0 [install|agent-kubeconfig]" >&2; exit 2 ;;
esac

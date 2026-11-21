#!/usr/bin/env bash
# Generate Talos machine configs for the minimal stand (SRS 6.4) into infra/talos/generated/.
# Writes files only; nothing is applied to the nodes. Usage:
#   ./gen.sh              render patches and generate configs with talosctl
#   ./gen.sh --render     render patches only (talosctl is not needed)
#   ENV_FILE=path ./gen.sh   use another env file (default: ./env)
#   OUT_DIR=path ./gen.sh    use another output directory (default: ./generated)
set -euo pipefail

cd "$(dirname "$0")"
render_only=0
case "${1:-}" in
  "") ;;
  --render) render_only=1 ;;
  *) echo "usage: $0 [--render]" >&2; exit 2 ;;
esac

env_file=${ENV_FILE:-env}
[ -f "$env_file" ] || { echo "gen.sh: $env_file not found, copy env.example to env" >&2; exit 1; }
set -a
# shellcheck disable=SC1090
. "$env_file"
set +a

vars=(CLUSTER_NAME CP_IP W_IP PREFIXLEN GATEWAY DNS INSTALL_DISK TALOS_VERSION K8S_VERSION)
for v in "${vars[@]}"; do
  val=${!v:-}
  if [ -z "$val" ] || [ "$val" = CHANGE_ME ]; then
    echo "gen.sh: $v is not set in $env_file" >&2
    exit 1
  fi
done

out=${OUT_DIR:-generated}
mkdir -p "$out/patches"
chmod 700 "$out"
list=$(printf '${%s} ' "${vars[@]}")
for t in patches/*.tmpl; do
  envsubst "$list" < "$t" > "$out/patches/$(basename "${t%.tmpl}")"
done
if grep -rn '\${' "$out/patches"; then
  echo "gen.sh: unresolved variable in rendered patches" >&2
  exit 1
fi
echo "gen.sh: patches rendered to $out/patches"
[ "$render_only" = 1 ] && exit 0

command -v talosctl >/dev/null || { echo "gen.sh: talosctl not found" >&2; exit 1; }

# secrets.yaml holds the cluster CA keys: keep it, a rebuild with the same CA needs it.
[ -f "$out/secrets.yaml" ] || talosctl gen secrets --talos-version "$TALOS_VERSION" -o "$out/secrets.yaml"

args=(--with-secrets "$out/secrets.yaml"
  --talos-version "$TALOS_VERSION" --kubernetes-version "$K8S_VERSION"
  --config-patch-control-plane "@$out/patches/controlplane.yaml"
  --config-patch-worker "@$out/patches/worker.yaml"
  --config-patch-worker "@$out/patches/worker-data-volume.yaml"
  --output "$out" --force)
[ -z "${INSTALL_IMAGE:-}" ] || args+=(--install-image "$INSTALL_IMAGE")
talosctl gen config "$CLUSTER_NAME" "https://$CP_IP:6443" "${args[@]}"
chmod 600 "$out"/*.yaml "$out/talosconfig"
echo "gen.sh: configs written to $out (not in git)"

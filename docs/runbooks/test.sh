#!/usr/bin/env bash
# Offline check that the commands and paths of deploy-from-scratch.md match the code in infra/ and .github/.
# Needs only grep; no cluster.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
doc="$root/docs/runbooks/deploy-from-scratch.md"
fail=0

need() { # need <description> <file> <fixed string>
  if ! grep -qF -- "$3" "$2"; then echo "runbook-check: $1: '$3' not found in ${2#"$root"/}" >&2; fail=1; fi
}
in_doc() { need "runbook" "$doc" "$1"; }

# Files and scripts the runbook runs exist and are named in it.
for f in infra/hyperv/New-StandVMs.ps1 infra/hyperv/Remove-StandVMs.ps1 infra/hyperv/stand.minimal.json.example \
  infra/talos/gen.sh infra/talos/env.example infra/bootstrap/bootstrap.sh infra/stand-secrets/stand-secrets.sh; do
  [ -e "$root/$f" ] || { echo "runbook-check: $f does not exist" >&2; fail=1; }
  case "$f" in
    infra/hyperv/*.ps1) in_doc "$(basename "$f")" ;;
    infra/bootstrap/*.sh | infra/stand-secrets/*.sh) in_doc "$f" ;;
  esac
done

# Commands and flags of the runbook exist in the scripts.
in_doc 'infra/bootstrap/bootstrap.sh agent-kubeconfig'
need "bootstrap subcommand" "$root/infra/bootstrap/bootstrap.sh" 'agent-kubeconfig)'
in_doc '--dry-run'
need "stand-secrets flag" "$root/infra/stand-secrets/stand-secrets.sh" '--dry-run'
in_doc './gen.sh'
in_doc '-RemoveDisks'
need "Remove-StandVMs parameter" "$root/infra/hyperv/Remove-StandVMs.ps1" 'RemoveDisks'
need "Remove-StandVMs parameter" "$root/infra/hyperv/Remove-StandVMs.ps1" 'Name'
need "New-StandVMs parameter" "$root/infra/hyperv/New-StandVMs.ps1" 'ConfigPath'

# Variables of env.example are the ones named in the runbook.
for v in CP_IP W_IP PREFIXLEN GATEWAY DNS INSTALL_DISK TALOS_VERSION K8S_VERSION; do
  need "env.example" "$root/infra/talos/env.example" "$v="
  in_doc "$v"
done

# VM names and generated files.
for n in talos-cp-1 talos-w-1; do
  need "stand.minimal" "$root/infra/hyperv/stand.minimal.json.example" "$n"
  in_doc "$n"
done
in_doc 'generated/kubeconfig-admin'
need "talos README" "$root/infra/talos/README.md" 'generated/kubeconfig-admin'

# Image build and deploy key.
in_doc 'GITOPS_DEPLOY_KEY'
need "build workflow" "$root/.github/workflows/build.yml" 'secrets.GITOPS_DEPLOY_KEY'
in_doc 'ghcr.io/m5tshift/slopay/<component>'
# shellcheck disable=SC2016  # the pattern is literal workflow text
need "build workflow" "$root/.github/workflows/build.yml" 'ghcr.io/${{ github.repository }}'

# Secret named in the troubleshooting table.
need "stand-secrets" "$root/infra/stand-secrets/stand-secrets.sh" 'anvil-mnemonic'
in_doc 'anvil-mnemonic'

# Every component with a Dockerfile is listed as a GHCR package.
for d in services/*/Dockerfile testkit/*/Dockerfile; do
  [ -f "$root/$d" ] || continue
  c="$(basename "$(dirname "$d")")"
  in_doc "\`$c\`"
done

if [ "$fail" = 0 ]; then echo "runbook-check: ok"; fi
exit "$fail"

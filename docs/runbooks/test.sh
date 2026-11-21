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
for f in infra/terraform/hyperv/minimal.tfvars.example infra/terraform/hyperv/full.tfvars.example \
  infra/terraform/hyperv/variables.tf infra/terraform/hyperv/outputs.tf \
  infra/terraform/talos/minimal.tfvars.example infra/terraform/talos/variables.tf infra/terraform/talos/outputs.tf \
  infra/talos/patches/controlplane.yaml.tmpl infra/talos/patches/worker.yaml.tmpl infra/talos/patches/worker-data-volume.yaml.tmpl \
  infra/bootstrap/bootstrap.sh infra/stand-secrets/stand-secrets.sh; do
  [ -e "$root/$f" ] || { echo "runbook-check: $f does not exist" >&2; fail=1; }
  case "$f" in
    infra/bootstrap/*.sh | infra/stand-secrets/*.sh) in_doc "$f" ;;
  esac
done

# Commands and flags of the runbook exist in the scripts.
in_doc 'infra/bootstrap/bootstrap.sh agent-kubeconfig'
need "bootstrap subcommand" "$root/infra/bootstrap/bootstrap.sh" 'agent-kubeconfig)'
in_doc '--dry-run'
need "stand-secrets flag" "$root/infra/stand-secrets/stand-secrets.sh" '--dry-run'
in_doc 'infra/terraform/talos'
in_doc "-target='talos_image_factory_schematic.node'"
in_doc 'iso_urls.auto.tfvars.json'
in_doc 'extra_iso_urls'
need "talos outputs" "$root/infra/terraform/talos/outputs.tf" 'output "iso_urls"'
in_doc 'minimal.tfvars.example'
in_doc 'iso_urls'
in_doc 'winrm_host'
in_doc 'winrm_password'
in_doc 'Enable-PSRemoting'
in_doc 'output vm_macs'
need "hyperv outputs" "$root/infra/terraform/hyperv/outputs.tf" 'output "vm_macs"'
for v in iso_urls winrm_host winrm_password; do
  need "hyperv variables" "$root/infra/terraform/hyperv/variables.tf" "variable \"$v\""
done
need "hyperv README" "$root/infra/terraform/hyperv/README.md" '5986'
need "hyperv variables" "$root/infra/terraform/hyperv/variables.tf" 'variable "extra_iso_urls"'

# Variables of the talos layer are the ones named in the runbook.
for v in nodes gateway dns install_disk talos_version kubernetes_version config_dir; do
  need "talos variables" "$root/infra/terraform/talos/variables.tf" "variable \"$v\""
  in_doc "$v"
done

# VM names and generated files.
for n in talos-cp-1 talos-w-1; do
  need "minimal.tfvars" "$root/infra/terraform/hyperv/minimal.tfvars.example" "$n"
  in_doc "$n"
done
in_doc '<config_dir>/kubeconfig-admin'
need "talos layer README" "$root/infra/terraform/talos/README.md" 'kubeconfig-admin'

# Image build and deploy key.
in_doc 'GITOPS_DEPLOY_KEY'
need "build workflow" "$root/.github/workflows/build.yml" 'secrets.GITOPS_DEPLOY_KEY'
in_doc 'ghcr.io/m5tshift/weigate/<component>'
# shellcheck disable=SC2016  # the pattern is literal workflow text
need "build workflow" "$root/.github/workflows/build.yml" 'ghcr.io/${{ github.repository }}'

# Load balancer step.
in_doc 'infra/ansible/roles/lb'
in_doc 'WEIGATE_LB_VRRP_PASSWORD'
need "lb tasks" "$root/infra/ansible/roles/lb/tasks/main.yml" 'WEIGATE_LB_VRRP_PASSWORD'
for f in infra/ansible/lb.yml infra/ansible/inventory/hosts.yml.example infra/ansible/inventory/group_vars/lb.yml.example; do
  [ -e "$root/$f" ] || { echo "runbook-check: $f does not exist" >&2; fail=1; }
done

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

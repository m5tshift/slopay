# infra/terraform/platform: Cilium, неймспейсы, ArgoCD

Слой Terraform с провайдерами `hashicorp/kubernetes` 3.5.0 и `hashicorp/helm` 3.3.0 (ADR 0008; SRS INF-1.1a,
INF-2.1, INF-2.2, 4.4). Состояние локальное, вне Git. Слой применяется к чистому кластеру после
`infra/terraform/talos`. Агент только пишет код, применяет слой пользователь.

| Файл | Назначение |
|---|---|
| `cilium.tf` | Cilium и Hubble (`kubeProxyReplacement`, `cni.exclusive=false`, `socketLB.hostNamespaceOnly=true`); только при `install_cilium` (полный стенд) |
| `namespaces.tf` | неймспейсы из `infra/bootstrap/namespaces.yaml`; ServiceAccount, Role, RoleBinding `agent-reader` в `apps` |
| `argocd.tf` | ArgoCD (чарт и values из `infra/bootstrap`) и root Application на `weigate-gitops` через чарт `argocd-apps` |
| `minimal.tfvars.example`, `full.tfvars.example` | параметры минимального и полного стенда |

Cilium ставится первым: на полном стенде Talos собран без CNI и kube-proxy (`full_stand` в слое `talos`),
узлы не Ready до него. Остальные ресурсы ждут Cilium.

## Соотношение со скриптом `infra/bootstrap`

Скрипт остаётся для минимального стенда. Terraform использует его файлы (`namespaces.yaml`,
`argocd-values.yaml`) и не копирует их; версия чарта ArgoCD совпадает с `ARGOCD_CHART_VERSION`. Переход со
скрипта на Terraform — пересборка кластера: слой ничего не импортирует, а скрипт после перехода не запускают.

## Порядок (пользователь)

1. `cp full.tfvars.example terraform.tfvars` (минимальный стенд: `minimal.tfvars.example`), указать
   `kubeconfig_path` и `k8s_service_host`.
2. `terraform init`, затем `terraform apply`.
3. Проверка: `kubectl -n kube-system get pods -l k8s-app=cilium`, `kubectl -n argocd get applications`.

Проверка слоя в CI: `make tf-validate` (`terraform test` с `mock_provider`).

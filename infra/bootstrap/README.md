# infra/bootstrap: ArgoCD, неймспейсы, root Application

Bootstrap минимального стенда (SRS 6.4, INF-2.2). Скрипт запускает пользователь с админским kubeconfig;
агент его не запускает. Позже скрипт заменит Terraform (INF-2.1, #53).

| Файл | Назначение |
|---|---|
| `bootstrap.sh` | неймспейсы, ArgoCD, root Application, RBAC агента; подкоманда `agent-kubeconfig` |
| `namespaces.yaml` | `argocd`, `local-path-storage`, `kafka`, `data`, `chain`, `apps` (назначения `apps/project.yaml` в `weigate-gitops`) |
| `argocd-values.yaml` | values чарта `argo-cd`: dex, notifications и ApplicationSet выключены, `server.insecure`, requests и limits, Lua health-check для `argoproj.io/Application` |
| `agent-rbac.yaml` | ServiceAccount `agent-reader` в `apps`: только `get`, `list`, `watch`, без Secrets и ConfigMaps, без ClusterRole |
| `test.sh` | проверки без кластера (`make bootstrap-check`) |

Health-check Application нужен для App of Apps: без него sync-wave между дочерними Application не ждёт их
готовности.

PodSecurity: метка `privileged` только у `local-path-storage` (helper-поды local-path-provisioner используют
`hostPath`); остальные неймспейсы остаются на `baseline` по умолчанию Talos.

## Что делает скрипт

Каждый шаг можно повторять:

1. создаёт неймспейсы (`kubectl apply`);
2. ставит ArgoCD: `helm upgrade --install argocd argo-cd` из `https://argoproj.github.io/argo-helm`,
   версия чарта закреплена в `ARGOCD_CHART_VERSION` (в начале `bootstrap.sh`);
3. ждёт CRD `applications.argoproj.io`;
4. скачивает `apps/root.yaml` из ветки `main` `weigate-gitops` (`ROOT_URL`) и применяет его: AppProject и дочерние
   Application разворачивает ArgoCD сам;
5. создаёт RBAC агента.

## Порядок для пользователя

Нужны `kubectl`, `helm`, `curl`. Админский kubeconfig — результат `infra/terraform/talos` (`<config_dir>/kubeconfig-admin`).

```sh
export KUBECONFIG=<config_dir>/kubeconfig-admin
infra/bootstrap/bootstrap.sh
kubectl -n argocd get applications
```

Kubeconfig для агента (токен через TokenRequest, срок `TOKEN_DURATION`, по умолчанию 168h; токен печатается
только в файл):

```sh
infra/bootstrap/bootstrap.sh agent-kubeconfig > <путь вне репозитория>/agent.kubeconfig
KUBECONFIG=<путь вне репозитория>/agent.kubeconfig kubectl -n apps get pods
KUBECONFIG=<путь вне репозитория>/agent.kubeconfig kubectl -n apps get secrets   # должен ответить Forbidden
```

После истечения срока команду повторяют. Файл в git не попадает (`*kubeconfig` в `.gitignore`).

Доступ к UI ArgoCD — `kubectl -n argocd port-forward svc/argocd-server 8080:80`; пароль `admin` —
секрет `argocd-initial-admin-secret` в `argocd`.

Секреты стенда — отдельная задача (#109), local-path-provisioner — #110.

## Проверка в CI

`make bootstrap-check` (шаг в job `infra`): `shellcheck`, `bash -n`, `helm template` ArgoCD с `argocd-values.yaml`
(нужен доступ к репозиторию чартов, кластер не нужен), проверка, что в `argocd-cm` есть health-check Application,
dex выключен, неймспейсы совпадают с назначениями проекта, роль агента только читает и не видит Secrets и ConfigMaps.

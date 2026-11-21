# infra/talos: конфигурация минимального стенда

Конфигурация Talos для двух VM минимального стенда (SRS 6.4, INF-1.3): `talos-cp-1` и `talos-w-1`
(имена и размеры — `infra/terraform/hyperv/minimal.tfvars.example`). Агент только пишет код, все команды
`talosctl` запускает пользователь.

Сеть остаётся по умолчанию: flannel и kube-proxy. Cilium без kube-proxy и зеркало Zot — отдельная задача
(полный стенд), в эти патчи не входят.

| Файл | Назначение |
|---|---|
| `env.example` | параметры стенда: адреса, шлюз, диск установки, версии. Копия `env` в git не попадает |
| `patches/controlplane.yaml.tmpl` | `talos-cp-1`: hostname, статический адрес, диск установки |
| `patches/worker.yaml.tmpl` | `talos-w-1`: то же |
| `patches/worker-data-volume.yaml.tmpl` | `UserVolumeConfig` `data` на втором диске воркера |
| `gen.sh` | подставляет `env` в шаблоны (`envsubst`) и вызывает `talosctl gen config`; пишет только в `generated/` |

## Что настраивают патчи

- Статический адрес: интерфейс выбирается `deviceSelector: {physical: true}`, имя интерфейса знать не нужно;
  DHCP выключен, маршрут по умолчанию через `GATEWAY`. MAC-адреса Hyper-V динамические, адреса задаются
  здесь, а не в Hyper-V.
- Диск данных (только `talos-w-1`): `UserVolumeConfig` с именем `data` на диске, который не системный
  (`!system_disk`, минимум 40 ГБ — второй диск 50 ГБ). Talos монтирует том в `/var/mnt/data`.
  local-path-provisioner пишет в `/var/mnt/data/local-path` (путь задаётся в его настройках в `slopay-gitops`).
  Дополнительные `extraMounts` kubelet не нужны.
- Control plane: рабочие поды на нём не планируются (параметр `allowSchedulingOnControlPlanes` не меняется).

## Секреты

В `infra/talos/generated/` лежат `secrets.yaml`, `controlplane.yaml`, `worker.yaml` и `talosconfig`:
в них ключи CA кластера. Каталог и `env` в `.gitignore`, в git их не коммитить. `secrets.yaml` стоит
сохранить отдельно (менеджер паролей): с ним кластер пересобирается с теми же CA. Админский kubeconfig тоже
остаётся у пользователя; агент получает только kubeconfig на чтение в `apps` (runbook `docs/runbooks/`,
выдача — ручной шаг пользователя).

## Порядок для пользователя

1. VM созданы (`infra/terraform/hyperv`) и загружены с ISO Talos. Узел в режиме обслуживания отвечает на `--insecure`.
2. Проверить имена дисков (в `env` — `INSTALL_DISK`, второй диск воркера выбирается автоматически):
   ```sh
   talosctl get disks --insecure -n <CP_IP или W_IP>
   ```
   Адрес узла на этом этапе — тот, что выдал DHCP (виден в консоли VM).
3. Подготовить параметры и сгенерировать конфигурацию:
   ```sh
   cd infra/talos
   cp env.example env      # заполнить адреса, TALOS_VERSION и K8S_VERSION
   ./gen.sh
   ```
   `./gen.sh --render` только рендерит патчи в `generated/patches/`, `talosctl` не нужен.
4. Проверить конфигурацию (ручной шаг, в CI не выполняется):
   ```sh
   talosctl validate --mode metal --config generated/controlplane.yaml
   talosctl validate --mode metal --config generated/worker.yaml
   ```
5. Применить к узлам (по DHCP-адресам из шага 2) и выполнить bootstrap:
   ```sh
   talosctl apply-config --insecure -n <DHCP-адрес cp> -f generated/controlplane.yaml
   talosctl apply-config --insecure -n <DHCP-адрес w>  -f generated/worker.yaml
   talosctl --talosconfig generated/talosconfig -e <CP_IP> -n <CP_IP> bootstrap
   talosctl --talosconfig generated/talosconfig -e <CP_IP> -n <CP_IP> kubeconfig generated/kubeconfig-admin
   ```
   После применения узлы перезагружаются на статические адреса `CP_IP` и `W_IP`.
6. Проверка: `kubectl --kubeconfig generated/kubeconfig-admin get nodes` показывает оба узла `Ready`;
   `talosctl --talosconfig generated/talosconfig -e <CP_IP> -n <W_IP> get volumestatus` — том `u-data`
   в состоянии `ready`.

Дальше: bootstrap ArgoCD (`infra/` отдельной задачи) и шаги стенда в runbook.

## Проверка в CI

`make talos-check` (шаг в job `infra`): `bash -n gen.sh`, рендер патчей на значениях из `env.example` и поиск
ожидаемых полей. `talosctl` в CI не ставится, `talosctl validate` выполняет пользователь (шаг 4).

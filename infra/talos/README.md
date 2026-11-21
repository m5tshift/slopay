# infra/talos: патчи конфигурации минимального стенда

Патчи машинной конфигурации Talos для двух VM минимального стенда (SRS 6.4, INF-1.3): `talos-cp-1` и `talos-w-1`.
Их подставляет `infra/terraform/talos` (`templatefile`, переменные — `hostname`, `ip`, `prefixlen`, `gateway`,
`dns`, `install_disk`, `install_image`). Установку, bootstrap и выдачу kubeconfig ведёт Terraform; `talosctl`
нужен только для диагностики.

Сеть остаётся по умолчанию: flannel и kube-proxy. Cilium без kube-proxy и зеркало Zot — отдельная задача
(полный стенд), в эти патчи не входят.

| Файл | Назначение |
|---|---|
| `patches/controlplane.yaml.tmpl` | control plane: hostname, статический адрес, диск и образ установки |
| `patches/worker.yaml.tmpl` | воркер: то же |
| `patches/worker-data-volume.yaml.tmpl` | `UserVolumeConfig` `data` на втором диске воркера |

## Что настраивают патчи

- Статический адрес: интерфейс выбирается `deviceSelector: {physical: true}`, DHCP выключен, маршрут по
  умолчанию через `gateway`. Тот же адрес задан в параметре ядра `ip=` schematic узла.
- Диск данных (только воркер): `UserVolumeConfig` с именем `data` на диске, который не системный
  (`!system_disk`, минимум 40 ГБ — второй диск 50 ГБ). Talos монтирует том в `/var/mnt/data`.
  local-path-provisioner пишет в `/var/mnt/data/local-path` (путь задаётся в его настройках в `slopay-gitops`).
- Control plane: рабочие поды на нём не планируются (параметр `allowSchedulingOnControlPlanes` не меняется).

## Секреты

Секреты кластера лежат в state слоя `infra/terraform/talos` (локальный, вне Git) и в `config_dir`
(`kubeconfig-admin`, `talosconfig`). Агент получает только kubeconfig на чтение в `apps` (runbook, выдача —
ручной шаг пользователя).

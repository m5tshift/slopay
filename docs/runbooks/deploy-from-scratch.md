# Runbook: развёртывание минимального стенда с нуля

Минимальный стенд (SRS 6.4): две VM Hyper-V (`talos-cp-1`, `talos-w-1`), Talos со стандартной сетью, ArgoCD
с App of Apps из `slopay-gitops`, платформа и сервисы до статуса `PAID`. Все команды запускает оператор
стенда: агент пишет код и читает кластер через свой kubeconfig. Подробности каждого шага — в README
каталогов `infra/hyperv`, `infra/talos`, `infra/bootstrap`, `infra/stand-secrets`.

Команды выполняются из корня клона `slopay`, если не сказано иное. Шаги 1 и 2 — PowerShell от администратора
на ПК стенда, остальные — оболочка с `kubectl`, `talosctl`, `helm`, `curl`, `openssl`, `jq`, `cast` (Foundry),
`envsubst`.

## Требования

| Что | Значение |
|---|---|
| ПК | Windows Pro с Hyper-V, 12 логических CPU, 32 ГБ RAM, не менее 250 ГБ свободного места на SSD, Ethernet (SRS 6.1) |
| Ресурсы VM | `talos-cp-1`: 2 vCPU, 3 ГБ, диск 20 ГБ; `talos-w-1`: 6 vCPU, 14 ГБ, диски 20 и 50 ГБ (SRS 6.4) |
| Windows | обновления приостановлены, сон и гибернация отключены, WSL2 и Docker Desktop остановлены или ограничены `.wslconfig` |
| Сеть | два свободных адреса вне диапазона DHCP роутера (`CP_IP`, `W_IP`), шлюз, DNS; рабочая машина в той же подсети |
| ISO | образ Talos из Image Factory (`metal-amd64`), скачивается вручную; его версия — `TALOS_VERSION` в `infra/talos/env` |
| GitHub | один раз выполнены действия из раздела «Однократные действия на GitHub» |

## Однократные действия на GitHub

Нужны, чтобы CI собирал образы и обновлял теги в `slopay-gitops`, а кластер мог их скачать. Повторять при
пересборке кластера не нужно.

1. Deploy key для `slopay-gitops` (`.github/workflows/build.yml`, job `gitops`):
   ```sh
   ssh-keygen -t ed25519 -N '' -C slopay-gitops-deploy -f <каталог вне репозитория>/gitops_deploy_key
   ```
   Публичную часть (`.pub`) добавить в `slopay-gitops`: Settings, Deploy keys, с правом записи (Allow write access).
   Правило ветки `main` в `slopay-gitops` должно пропускать deploy key без PR.
2. Приватную часть сохранить секретом репозитория `slopay` с именем `GITOPS_DEPLOY_KEY`: Settings, Secrets and
   variables, Actions. Файл ключа удалить или оставить вне репозитория (шаблоны `*deploy_key*` в `.gitignore`).
3. Образы публикуются в `ghcr.io/m5tshift/slopay/<component>`. Пакет, созданный первой сборкой, приватный:
   после первой сборки сделать каждый пакет публичным (профиль или организация, Packages, пакет,
   Package settings, Change visibility, Public). У кластера нет `imagePullSecrets`.
4. Первая сборка: Actions, workflow `build`, Run workflow (пересобирает все компоненты) либо любой push в `main`.
   Job `gitops` пишет тег `<7 знаков SHA>` в `services/<component>/values.yaml` репозитория `slopay-gitops`.

Проверка: все job workflow `build` зелёные; в `slopay-gitops` есть коммит `chore(gitops): … image <тег>`;
в списке пакетов шесть: `chain-watcher`, `invoice-service`, `merchant-service`, `rate-service`, `payer-sim`,
`rate-mock`, все Public.

## Шаги

### 1. Параметры VM и создание VM (`infra/hyperv`)

```powershell
cd infra\hyperv
Copy-Item stand.minimal.json.example stand.json    # поправить switchName, netAdapterName, пути, isoPath
.\New-StandVMs.ps1 -ConfigPath .\stand.json -WhatIf
.\New-StandVMs.ps1 -ConfigPath .\stand.json
Start-VM talos-cp-1, talos-w-1
```

Проверка:
```powershell
Get-VM | Format-Table Name, Generation, ProcessorCount, MemoryStartup, DynamicMemoryEnabled, AutomaticStartAction
.\New-StandVMs.ps1 -ConfigPath .\stand.json     # повторный запуск: без предупреждений и изменений
```
Обе VM загрузились с ISO Talos, в консоли виден адрес от DHCP. Эти адреса понадобятся на шаге 3.

### 2. Параметры и конфигурация Talos (`infra/talos`)

```sh
talosctl get disks --insecure -n <DHCP-адрес cp>     # имя системного диска для INSTALL_DISK
cd infra/talos
cp env.example env      # CP_IP, W_IP, PREFIXLEN, GATEWAY, DNS, INSTALL_DISK, TALOS_VERSION, K8S_VERSION
./gen.sh
talosctl validate --mode metal --config generated/controlplane.yaml
talosctl validate --mode metal --config generated/worker.yaml
```

Проверка: `generated/` содержит `secrets.yaml`, `controlplane.yaml`, `worker.yaml`, `talosconfig`, обе проверки
`talosctl validate` проходят. Каталог и `env` в git не попадают; `secrets.yaml` сохранить отдельно.

### 3. Установка Talos и bootstrap Kubernetes

Из `infra/talos`, адреса `<DHCP-адрес …>` — из консолей VM (шаг 1):

```sh
talosctl apply-config --insecure -n <DHCP-адрес cp> -f generated/controlplane.yaml
talosctl apply-config --insecure -n <DHCP-адрес w>  -f generated/worker.yaml
talosctl --talosconfig generated/talosconfig -e <CP_IP> -n <CP_IP> bootstrap
talosctl --talosconfig generated/talosconfig -e <CP_IP> -n <CP_IP> kubeconfig generated/kubeconfig-admin
```

Узлы устанавливают Talos на диск и перезагружаются на `CP_IP` и `W_IP`. После установки на `talos-cp-1`
и `talos-w-1` убрать ISO или выставить загрузку с диска (PowerShell):
```powershell
Set-VMFirmware -VMName talos-w-1 -FirstBootDevice (Get-VMHardDiskDrive -VMName talos-w-1 | Select-Object -First 1)
Get-VMDvdDrive -VMName talos-w-1 | Set-VMDvdDrive -Path $null
```
То же для `talos-cp-1`.

Проверка:
```sh
kubectl --kubeconfig generated/kubeconfig-admin get nodes
talosctl --talosconfig generated/talosconfig -e <CP_IP> -n <W_IP> get volumestatus
```
Оба узла `Ready`; том `u-data` в состоянии `ready`.

### 4. Bootstrap: неймспейсы, ArgoCD, root Application (`infra/bootstrap`)

```sh
export KUBECONFIG=<путь к infra/talos/generated/kubeconfig-admin>
infra/bootstrap/bootstrap.sh
```

Скрипт идемпотентен. Он создаёт неймспейсы `argocd`, `local-path-storage`, `kafka`, `data`, `chain`, `apps`,
ставит ArgoCD, применяет `apps/root.yaml` из `main` репозитория `slopay-gitops` и RBAC агента. ArgoCD сразу
начинает синхронизацию по волнам (раздел «Проверка»).

Проверка:
```sh
kubectl -n argocd get pods
kubectl -n argocd get applications
```
Поды ArgoCD `Running`, Application `root` появился.

### 5. Секреты стенда (`infra/stand-secrets`)

Запустить как можно раньше, пока синхронизация не дошла до волны 2: CNPG и Percona читают секреты в `data`
при создании кластера, а поды Valkey, Anvil и сервисов ждут свои секреты.

```sh
infra/stand-secrets/stand-secrets.sh --dry-run
infra/stand-secrets/stand-secrets.sh
```

Проверка: повторный `--dry-run` выводит у всех секретов `exists`; секреты есть в `apps`, `chain`, `data`:
```sh
kubectl -n apps get secrets
kubectl -n chain get secret anvil-mnemonic
kubectl -n data get secrets
```
Таблица секретов — `infra/stand-secrets/README.md`. Значения скрипт не печатает.

### 6. Ожидание синхронизации ArgoCD

Порядок задаёт `argocd.argoproj.io/sync-wave` в `slopay-gitops`; следующая волна ждёт здоровья предыдущей
(Lua health-check Application из `argocd-values.yaml`):

| Волна | Application |
|---|---|
| 0 | `local-path-provisioner` |
| 1 | `strimzi-operator`, `cnpg-operator`, `percona-mongodb-operator` |
| 2 | `kafka`, `cnpg-clusters`, `percona-mongodb` |
| 3 | `valkey`, `anvil`, `rate-mock`, `payer-sim` |
| 4 | `merchant-service`, `invoice-service`, `chain-watcher`, `rate-service` |

Прогресс:
```sh
kubectl -n argocd get applications -w
```
Первая синхронизация образов и операторов занимает заметное время; зависший Application ArgoCD повторяет сам
(`retry` в `syncPolicy`). Руками в кластере ничего не правят, ресурсы меняются только PR в `slopay-gitops`.

### 7. Kubeconfig для агента

```sh
infra/bootstrap/bootstrap.sh agent-kubeconfig > <путь вне репозитория>/agent.kubeconfig
KUBECONFIG=<путь вне репозитория>/agent.kubeconfig kubectl -n apps get pods
KUBECONFIG=<путь вне репозитория>/agent.kubeconfig kubectl -n apps get secrets   # должен ответить Forbidden
```

Токен действует `TOKEN_DURATION` (по умолчанию 168 часов); после истечения команду повторяют. Файл в git не
попадает (`*kubeconfig` в `.gitignore`). Админский kubeconfig, `talosconfig` и `secrets.yaml` агенту не отдаются.

## Проверка стенда

1. Все Application `Synced` и `Healthy`:
   ```sh
   kubectl -n argocd get applications
   ```
   Ожидаются шестнадцать: `root` и пятнадцать Application из таблицы шага 6.
2. Поды в `apps` `Running` и `Ready` (kubeconfig агента подходит):
   ```sh
   kubectl -n apps get pods
   ```
3. `/health/ready` через port-forward (порт сервиса 8080, `slopay-lib`; Service называется как компонент):
   ```sh
   kubectl -n apps port-forward svc/merchant-service 8080:8080 &
   curl -fsS localhost:8080/health/ready
   ```
   Повторить для `invoice-service`, `chain-watcher`, `rate-service`, при необходимости с другим локальным
   портом. Ответ `200` у каждого.
4. UI ArgoCD: `kubectl -n argocd port-forward svc/argocd-server 8080:80`, пользователь `admin`, пароль —
   секрет `argocd-initial-admin-secret` в `argocd`.

Дальше срез до `PAID` проверяется сценарием из SRS 6.4.

## Пересборка кластера с нуля

Те же шаги, с отличиями:

1. Удалить VM вместе с дисками (PowerShell):
   ```powershell
   .\Remove-StandVMs.ps1 -ConfigPath .\stand.json -RemoveDisks
   ```
   Для одной VM — параметр `-Name talos-w-1`.
2. Шаги 1 и 3 заново (создание VM, `apply-config`, `bootstrap`). Шаг 2 повторять не нужно: `gen.sh` берёт
   существующий `generated/secrets.yaml`, CA кластера сохраняется, `generated/*.yaml` остаются годными.
   `gen.sh` запускают снова только при смене адресов или версий.
3. Шаги 4–7 заново: новый кластер пуст, скрипт стенда создаёт все секреты заново, kubeconfig агента выдаётся
   новый (токен привязан к старому кластеру).
4. Действия на GitHub не повторяются. Теги образов в `slopay-gitops` уже указывают на собранные образы.

Состояние Anvil, баз и Kafka лежит на `/var/mnt/data` воркера и пропадает вместе с его диском.

## Типичные проблемы

| Симптом | Причина | Что делать |
|---|---|---|
| `CreateContainerConfigError` у подов в `apps`, `chain`, `data` | не запущен `stand-secrets.sh` или нет нужного секрета (`kubectl -n apps describe pod <под>`: `secret "…" not found`) | запустить `infra/stand-secrets/stand-secrets.sh` (шаг 5); поды поднимутся при следующей попытке, зависший под можно удалить |
| `ImagePullBackOff` у сервисов или testkit | образ с тегом из `slopay-gitops` ещё не собран (первая сборка не прошла) или пакет GHCR приватный | выполнить первую сборку и сделать пакеты публичными (раздел «Однократные действия на GitHub»); `kubectl -n apps describe pod <под>` показывает `not found` или `unauthorized` |
| Job `gitops` workflow `build` падает на checkout или push | нет секрета `GITOPS_DEPLOY_KEY`, у ключа нет права записи, правило `main` не пропускает deploy key | шаги 1 и 2 раздела про GitHub |
| Application `OutOfSync` или `Degraded` на волне 2 | секреты `data` созданы после кластеров CNPG и Percona | выполнить шаг 5; если кластер уже создан с другим паролем, пароль меняют в самой БД |
| Application ждёт `Progressing` на волне 0 | local-path-provisioner не получил привилегированный неймспейс | проверить, что `bootstrap.sh` отработал (метка `privileged` у `local-path-storage`) |
| `kubectl -n apps get secrets` у агента не `Forbidden` | выдан админский kubeconfig | выдать `agent-kubeconfig` (шаг 7) |
| `bootstrap.sh`: `kubectl cluster-info` не отвечает | `KUBECONFIG` не указывает на `generated/kubeconfig-admin` или узлы не готовы | шаги 3 и 4 |

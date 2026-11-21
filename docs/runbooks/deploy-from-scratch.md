# Runbook: развёртывание минимального стенда с нуля

Минимальный стенд (SRS 6.4): две VM Hyper-V (`talos-cp-1`, `talos-w-1`), Talos со стандартной сетью, ArgoCD
с App of Apps из `slopay-gitops`, платформа и сервисы до статуса `PAID`. Все команды запускает оператор
стенда: агент пишет код и читает кластер через свой kubeconfig. Подробности каждого шага — в README
каталогов `infra/terraform/hyperv`, `infra/terraform/talos`, `infra/talos`, `infra/bootstrap`, `infra/stand-secrets`.

Команды выполняются из корня клона `slopay`, если не сказано иное. Шаг 0 — PowerShell от администратора
на ПК стенда, остальные — оболочка с `terraform`, `kubectl`, `helm`, `curl`, `openssl`, `jq`, `cast` (Foundry);
`talosctl` нужен только для диагностики.

## Требования

| Что | Значение |
|---|---|
| ПК | Windows Pro с Hyper-V, 12 логических CPU, 32 ГБ RAM, не менее 250 ГБ свободного места на SSD, Ethernet (SRS 6.1) |
| Ресурсы VM | `talos-cp-1`: 2 vCPU, 3 ГБ, диск 20 ГБ; `talos-w-1`: 6 vCPU, 14 ГБ, диски 20 и 50 ГБ (SRS 6.4) |
| Windows | обновления приостановлены, сон и гибернация отключены, WSL2 и Docker Desktop остановлены или ограничены `.wslconfig` |
| Сеть | два свободных адреса вне диапазона DHCP роутера (`nodes`), шлюз (`gateway`), DNS (`dns`); рабочая машина в той же подсети |
| ISO | ссылки на ISO Talos из Image Factory выдаёт слой `infra/terraform/talos` (вывод `iso_urls`), ISO на ПК скачивает Terraform слоя `hyperv`; версия — `talos_version` |
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
в списке пакетов девять: `chain-watcher`, `invoice-service`, `merchant-service`, `ledger-service`,
`reconciliation-service`, `rate-service`, `payer-sim`, `rate-mock`, `merchant-mock`, все Public.

## Шаги

### 0. Включение WinRM на ПК стенда (один раз)

PowerShell от администратора на ПК стенда (учётная запись — член группы Administrators, с паролем):

```powershell
Enable-PSRemoting -SkipNetworkProfileCheck -Force
$cert = New-SelfSignedCertificate -DnsName $env:COMPUTERNAME -CertStoreLocation Cert:\LocalMachine\My
New-Item -Path WSMan:\localhost\Listener -Transport HTTPS -Address * -CertificateThumbPrint $cert.Thumbprint -Force
New-NetFirewallRule -DisplayName "WinRM HTTPS" -Direction Inbound -Protocol TCP -LocalPort 5986 -Action Allow
```

Проверка: `winrm enumerate winrm/config/listener` показывает слушатель `Transport = HTTPS`, порт 5986. С рабочей
машины порт доступен: `nc -vz <адрес ПК> 5986`.

### 1. ISO Talos (`infra/terraform/talos`)

Адрес узла вшит в его ISO (`ip=` в параметрах ядра, ADR 0008), поэтому сначала слой Talos создаёт schematic
и ссылки на ISO в Image Factory (нужен доступ к `factory.talos.dev`). Остальное в этом слое на шаге 1 не
затрагивается.

```sh
cd infra/terraform/talos
cp minimal.tfvars.example terraform.tfvars    # поправить: nodes, gateway, dns, talos_version, kubernetes_version, config_dir
terraform init
terraform apply -target='talos_image_factory_schematic.node' -target='data.talos_image_factory_urls.node'
terraform output -json iso_urls | jq '{iso_urls: .}' > ../hyperv/iso_urls.auto.tfvars.json
```

Проверка: `iso_urls.auto.tfvars.json` содержит ссылку на ISO для каждого узла. Оба файла (`terraform.tfvars`,
`iso_urls.auto.tfvars.json`) в Git не попадают.

### 2. Создание VM (`infra/terraform/hyperv`)

```sh
cd infra/terraform/hyperv
cp minimal.tfvars.example terraform.tfvars    # поправить; добавить winrm_host и winrm_password (файл вне Git)
terraform init
terraform plan
terraform apply
terraform output vm_macs                      # MAC для резерва адресов в роутере
```

Для `full.tfvars.example` ISO Debian (`lb-*`) задаётся отдельно, переменной `extra_iso_urls`.

Проверка:
```powershell
Get-VM | Format-Table Name, Generation, ProcessorCount, MemoryStartup, DynamicMemoryEnabled, AutomaticStartAction
```
```sh
terraform plan                                # повторный запуск: без изменений
```
Обе VM загрузились с ISO Talos и отвечают на статических адресах из `nodes`. Необязательная диагностика
(имя диска установки для `install_disk`): `talosctl get disks --insecure -n <адрес узла>`.

### 3. Установка Talos и bootstrap Kubernetes (`infra/terraform/talos`)

```sh
cd infra/terraform/talos
terraform apply
```

Terraform создаёт секреты кластера, собирает машинные конфигурации из патчей `infra/talos/patches`, применяет
их к узлам, выполняет bootstrap и пишет `kubeconfig-admin` и `talosconfig` в `config_dir` (вне Git, права 0600).
Узлы устанавливают Talos на диск и перезагружаются на тех же адресах.
Порядок загрузки VM — диск, затем DVD: после установки ISO вынимать не нужно.
State слоя (`terraform.tfstate`) содержит ключи CA кластера: он локальный, вне Git, копию хранить отдельно.

Проверка:
```sh
terraform plan                                # повторный запуск: без изменений
kubectl --kubeconfig <config_dir>/kubeconfig-admin get nodes
talosctl --talosconfig <config_dir>/talosconfig -e <адрес cp> -n <адрес w> get volumestatus
```
Оба узла `Ready`; том `u-data` в состоянии `ready`.

### 4. Bootstrap: неймспейсы, ArgoCD, root Application (`infra/bootstrap`)

```sh
export KUBECONFIG=<config_dir>/kubeconfig-admin
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

1. Удалить VM вместе с дисками:
   ```sh
   terraform -chdir=infra/terraform/hyperv destroy
   ```
2. Шаги 2 и 3 заново. State слоя `talos` сохраняется, поэтому CA кластера остаётся прежним, а schematic и ISO
   не пересоздаются (шаг 1 повторять не нужно). Bootstrap уже записан в state и повторно не выполнится,
   его заменяют:
   ```sh
   terraform -chdir=infra/terraform/talos apply -replace=talos_machine_bootstrap.this
   ```
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
| `bootstrap.sh`: `kubectl cluster-info` не отвечает | `KUBECONFIG` не указывает на `<config_dir>/kubeconfig-admin` или узлы не готовы | шаги 3 и 4 |

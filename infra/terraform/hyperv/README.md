# infra/terraform/hyperv: VM Hyper-V

Слой «VM» (SRS 6.1, 6.4, ADR 0008): External vSwitch, динамические VHDX и VM второго поколения на ПК стенда
(провайдер `taliesins/hyperv` 1.2.1, WinRM по HTTPS). ISO на ПК стенда скачивает Terraform: `terraform_data`
с `remote-exec` выполняет `Invoke-WebRequest` по WinRM. Агент запускает только `init -backend=false`, `validate`
и `test`; применение запускает пользователь.

## Файлы

| Файл | Назначение |
|---|---|
| `versions.tf`, `providers.tf`, `.terraform.lock.hcl` | версия провайдера и подключение по WinRM |
| `variables.tf` | входные переменные и их проверки |
| `host.tf` | каталоги и скачивание ISO на хост |
| `network.tf`, `disks.tf`, `vms.tf` | vSwitch, VHDX, VM |
| `outputs.tf` | `vm_names`, `vm_macs`, `iso_files` |
| `minimal.tfvars.example`, `full.tfvars.example` | минимальный (SRS 6.4) и полный (SRS 6.1, с `lb-1`, `lb-2`) стенд |
| `tests/vms.tftest.hcl` | `terraform test` с `mock_provider`: хост не нужен |

## Переменные

`winrm_host` и `winrm_password` задаются только в `terraform.tfvars` (в Git не попадает). Остальное: `winrm_port`
(5986), `winrm_user`, `winrm_insecure` (по умолчанию `true`: самоподписанный сертификат), `switch_name`,
`net_adapter_name`, `vm_path`, `vhd_path`, `iso_dir`.

- `vms`: имя VM, `vcpu`, `ram_gb` (статическая память), `disks` (ГБ, первый системный), `mac` (12 символов
  A-F0-9 без разделителей, префикс `02`, уникальный).
- `iso_urls`: ссылка на ISO для каждой VM (ключи как у `vms`). Для Talos это ссылка Image Factory этого узла
  (адрес узла вшит в ISO, ADR 0008); до #157 вписывается вручную. Для `lb-*` это Debian netinst из архива cdimage.
- `iso_sha256`: необязательная контрольная сумма по URL; если задана, после скачивания проверяется.

ISO сохраняется как `<iso_dir>\<хеш URL>.iso`: одинаковые URL качаются один раз, смена URL даёт новый файл,
старые файлы не удаляются.

## Запуск (пользователь)

```sh
cd infra/terraform/hyperv
cp minimal.tfvars.example terraform.tfvars   # поправить; добавить winrm_host и winrm_password
terraform init
terraform plan
terraform apply
terraform output vm_macs                      # для резерва адресов в роутере
```

State локальный (`terraform.tfstate`), в Git не попадает; в нём есть пароль WinRM.

## Порядок загрузки

Диск, затем DVD. Пока диск пуст, VM грузится с ISO; после установки на диск загружается диск, ISO вынимать
не нужно.

## Особенности

- Память статическая (`static_memory`; `dynamic_memory` с ним несовместим). Secure Boot выключен, автозапуск при
  старте Windows (`Start`), остановка `ShutDown`. Если Talos не реагирует на сигнал Hyper-V и Windows ждёт при
  выключении, переключить `automatic_stop_action` на `Save`.
- `wait_for_ips = false`: у Talos нет интеграционных сервисов Hyper-V, адрес через провайдер не виден.
- vCPU, память и диски существующей VM меняются только при выключенной VM; размер готового VHDX ресурс не меняет.
- Провайдер последний раз выпущен в феврале 2024. Если повторный `terraform plan` сразу после применения
  показывает изменения в `vm_firmware` или `dvd_drives`, это дрейф провайдера.

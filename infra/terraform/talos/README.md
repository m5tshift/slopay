# infra/terraform/talos: установка и конфигурация Talos

Слой Terraform с провайдером `siderolabs/talos` 0.12.0 (ADR 0008; SRS 6.4, INF-1.1, INF-1.3). Состояние
локальное, вне Git: в нём ключи CA кластера, kubeconfig и talosconfig. Копию state хранить отдельно.
Агент только пишет код, `terraform apply` запускает пользователь.

| Файл | Назначение |
|---|---|
| `image.tf` | schematic Image Factory и ссылки на ISO и installer на каждый узел |
| `cluster.tf` | секреты кластера, машинные конфигурации из патчей `infra/talos/patches`, применение, bootstrap |
| `files.tf` | `kubeconfig-admin` и `talosconfig` в `config_dir` (вне репозитория, права 0600) |
| `outputs.tf` | `iso_urls` (вход `infra/terraform/hyperv`), `installer_images`, `cluster_endpoint`, пути файлов |
| `minimal.tfvars.example` | параметры минимального стенда |

## Как это работает

- Адрес узла. Для каждого узла свой schematic с `extraKernelArgs`: `net.ifnames=0` и
  `ip=<адрес>::<шлюз>:<маска>:<имя узла>:eth0:off:<DNS>`. Узел грузится с ISO уже на статическом адресе.
  Тот же адрес статикой стоит в патче (`dhcp: false`), `node` и `endpoint` ресурсов равны ему с первого apply.
- Патчи (`infra/talos/patches`) подставляются через `templatefile`. Каждый патч — отдельный элемент
  `config_patches`, `UserVolumeConfig` воркера (`data`, второй диск) не склеивается с другим документом.
  `machine.install.image` — installer из Image Factory того же schematic.
- Повторный apply на готовом кластере ничего не меняет.

## Порядок (пользователь)

1. `terraform apply -target='talos_image_factory_schematic.node' -target='data.talos_image_factory_urls.node'`,
   затем `terraform output -json iso_urls | jq '{iso_urls: .}' > ../hyperv/iso_urls.auto.tfvars.json`.
2. `terraform apply` в `infra/terraform/hyperv`: VM загружаются с ISO.
3. `terraform apply` здесь: конфигурация, bootstrap, файлы в `config_dir`.

Подробно, с проверками, — `docs/runbooks/deploy-from-scratch.md`. Проверка слоя в CI: `make tf-validate`
(`terraform test` с `mock_provider`).

## Переменные

`talos_version`, `kubernetes_version`, `nodes` (роль и адрес по имени VM), `prefixlen`, `gateway`, `dns`,
`install_disk` (по умолчанию `/dev/sda`), `config_dir`, `cluster_name` (по умолчанию `weigate`), `apply_mode`.
Реальные значения — в `terraform.tfvars` (в Git не попадает).

# infra/terraform/openbao: конфигурация OpenBao

Слой Terraform с провайдером `hashicorp/vault` 5.3.0 (SRS INF-2.1, INF-2.4, 4.1.10). Состояние локальное,
вне Git. Слой применяется после init и unseal OpenBao (runbook) и установки OpenBao и ESO через
`weigate-gitops`. Агент только пишет код, применяет слой пользователь.

| Что | Ресурсы |
|---|---|
| KV v2 | mount `secret`; при `seed_kv_secrets = true` — начальные секреты `garage/{core,keys}`, `grafana/admin`, `alertmanager/telegram`, `mongodb/users`, `valkey/users`, `clickhouse/users`, `ci/github`, `anvil/mnemonic`, `payer-keys/anvil` (с `ignore_changes` на последующие ротации) |
| Kubernetes auth | mount `kubernetes`; роли `external-secrets` (SA в `secrets`), `merchant-service`, `webhook-service` (SA в `apps`) |
| Политики | `eso-read`: `secret/data/{garage,grafana,alertmanager,clickhouse,mongodb,valkey,anvil,payer-keys,ci}/*`, `database/static-creds/*`; `merchant-service`: запись `secret/data/webhook/*`, подпись `transit/sign/merchant-jwt`; `webhook-service`: чтение `secret/data/webhook/*` |
| Transit | ключ `merchant-jwt` (ECDSA P-256, не экспортируется) |
| Database | подключения к `merchant-pg` и `invoice-pg` по умолчанию (плюс `ledger-pg` в `full.tfvars.example`), статические роли с ротацией 24 ч; при `manage_vault_admin_secrets = true` Terraform сам создаёт `<кластер>-vault-admin` Secret в `data` и ждёт 15 с синхронизации CNPG |

Значения секретов в Terraform не задаются: Transit-ключ и пароли статических ролей генерирует OpenBao,
`webhook/<merchantId>` записывает `merchant-service`. В state попадает только пароль роли `vault-admin`
(см. ниже), и после ротации он недействителен. Токен провайдера — только в окружении.

## Порядок (пользователь)

1. Применить в `weigate-gitops` managed role `vault-admin` (PR со ссылкой на #54) и создать для каждого
   кластера Secret с паролем (в Git его нет):
   `kubectl -n data create secret generic <кластер>-vault-admin --type=kubernetes.io/basic-auth --from-literal=username=vault-admin --from-literal=password=<пароль>`
   для `merchant-pg` и `invoice-pg`.
2. Пробросить OpenBao: `kubectl -n secrets port-forward svc/openbao 8200:8200`.
3. Задать окружение, не записывая значения в историю оболочки: токен провайдера (переменная `VAULT_TOKEN`)
   и `TF_VAR_vault_admin_passwords='{"merchant-pg":"...","invoice-pg":"..."}'`.
4. `cp terraform.tfvars.example terraform.tfvars`, затем `terraform init` и `terraform apply`.
5. Сразу после apply сменить пароль `vault-admin`, чтобы значение из state стало недействительным:
   `bao write -f database/rotate-root/merchant-pg` и `bao write -f database/rotate-root/invoice-pg`.
6. Проверка: `bao read database/static-creds/merchant-service`, `bao read transit/keys/merchant-jwt`.
7. Начальные значения KV (`garage/core`, `garage/keys`, `grafana/admin`, `alertmanager/telegram`) пользователь
   записывает сам: `bao kv put secret/<путь> ...`.

Проверка слоя в CI: `make tf-validate` (`terraform test` с `mock_provider`).

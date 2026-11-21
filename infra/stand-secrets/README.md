# infra/stand-secrets: секреты минимального стенда

Секреты стенда (SRS 6.4, 4.1.10) создаёт скрипт вне git. Скрипт запускает пользователь с админским
kubeconfig; агент его не запускает. После OpenBao + ESO (#63) секреты заменят ExternalSecret, скрипт уйдёт.

Нужны `kubectl`, `openssl`, `jq` и `cast` (Foundry). Неймспейсы `apps`, `chain`, `data` создаёт
`infra/bootstrap`; скрипт их не создаёт.

## Секреты

| Секрет | Неймспейс | Ключи | Значение |
|---|---|---|---|
| `merchant-service-jwt-signing` | `apps` | `private.pem` | ключ подписи JWT, ECDSA P-256, PKCS#8 PEM |
| `payer-sim-payer-keys` | `apps` | `keys` | 10 приватных ключей (`0x` + 64 hex), по одному в строке; выводятся из мнемоники по пути `m/44'/60'/0'/0/i`, как у Anvil |
| `anvil-mnemonic` | `chain` | `mnemonic` | мнемоника Anvil, 12 слов |
| `chain-watcher-mongodb` | `apps` | `password` | пароль пользователя MongoDB `chain-watcher` (клиентская сторона; серверная — в #113) |
| `invoice-service-valkey` | `apps` | `password` | пароль пользователя Valkey `invoice-service` |
| `chain-watcher-valkey` | `apps` | `password` | пароль пользователя Valkey `chain-watcher` |
| `rate-service-valkey` | `apps` | `password` | пароль пользователя Valkey `rate-service` |
| `valkey-users` | `data` | `invoice-service`, `chain-watcher`, `rate-service` | те же пароли; из них init-контейнер Valkey (`platform/valkey` в `slopay-gitops`) строит ACL |

Имена секретов — `<компонент>-<name>` из `charts/<сервис>/values.yaml` (правило `slopay-lib`).
Пароли PostgreSQL выдаёт CloudNativePG, скрипт их не создаёт. У `rate-mock` секретов нет.

Пароли — 24 случайных байта в hex (`openssl rand`). Группа Kafka `chain-watcher` читает `invoice.events`;
ACL Kafka на минимальном стенде нет, секрет не нужен.

## Запуск

```sh
export KUBECONFIG=<админский kubeconfig>
infra/stand-secrets/stand-secrets.sh --dry-run      # что будет создано
infra/stand-secrets/stand-secrets.sh                # создать недостающее
```

| Параметр | Действие |
|---|---|
| `--dry-run` | только перечень: `would create`, `exists`, `would replace` |
| `--rotate ГРУППА` | заменяет секреты группы: `jwt`, `anvil` (мнемоника и ключи), `mongodb`, `valkey` (все четыре секрета) |
| `--mnemonic-file ФАЙЛ` | взять мнемонику из файла вместо генерации |

Без `--rotate` существующие секреты не меняются; повторный запуск создаёт только недостающие.
Если нет `valkey-users` или одного из `<сервис>-valkey`, недостающее создаётся из значения, которое уже есть
в другом экземпляре, поэтому пароли совпадают. `payer-sim-payer-keys` без `anvil-mnemonic` — ошибка:
ключи должны соответствовать мнемонике; выход — `--rotate anvil`. Если нет только ключей,
они выводятся из существующей мнемоники.

Значения не печатаются и не попадают в аргументы процессов: они лежат в каталоге `mktemp -d` (umask 077),
который удаляется при выходе, и передаются в `kubectl create secret --from-file`. `--rotate` удаляет Secret
и создаёт заново.

## Порядок на стенде

1. `infra/bootstrap/bootstrap.sh` — неймспейсы и ArgoCD.
2. `stand-secrets.sh` — до синхронизации Anvil, Valkey и сервисов: поды ждут секреты (`valkey-users`
   читает init-контейнер Valkey, `anvil-mnemonic` — Anvil).
3. Синхронизация ArgoCD. Если Valkey или Anvil уже запущены без секретов, после скрипта их поды
   перезапускаются сами при следующей попытке; зависший под можно удалить.

`--rotate anvil` меняет мнемонику: состояние Anvil на PV (`--state`) относится к старой, его нужно сбросить.
`--rotate valkey` и `--rotate mongodb` требуют перезапуска Valkey и сервисов.

## Проверка

`make stand-secrets-check`: shellcheck и `test.sh` — скрипт на поддельных `kubectl` и `cast`: полный набор
секретов, P-256 и PKCS#8, ключи выведены из мнемоники, пароли Valkey совпадают в обоих местах, повторный запуск
ничего не меняет, `--rotate` меняет только свою группу, значения не попадают в вывод и аргументы.

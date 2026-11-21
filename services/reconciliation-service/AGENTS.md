# AGENTS.md — reconciliation-service

## Назначение

Раз в 10 минут сверяет балансы адресов счетов в цепочке на блоке `head − N` с переводами, учтёнными в счетах
(проекция `invoice.events`), и с проводками ledger-service. Расхождения публикует в `reconciliation.events`
и сохраняет отчёт в MongoDB. Требования — SRS 4.1.3, 4.1.4, 4.1.8, 4.1.9, UC-08, FR-15.

## API и топики

| Вид | Что | Контракт |
|---|---|---|
| HTTP | только `/health/live`, `/health/ready`, `/metrics` | — |
| Публикует | `reconciliation.events`: `reconciliation.mismatch` на каждое расхождение, затем `reconciliation.completed`; ключ — `runId` | [`contracts/asyncapi/reconciliation-service.yaml`](../../contracts/asyncapi/reconciliation-service.yaml) |
| Потребляет | `invoice.events`: `invoice.created`, `invoice.paid`, `invoice.underpaid`, `invoice.late_paid`, `invoice.late_transfer`, `invoice.reverted`; группа `KAFKA_CONSUMER_GROUP` | [`contracts/asyncapi/invoice-service.yaml`](../../contracts/asyncapi/invoice-service.yaml) |

Синхронные вызовы:
- нода EVM по JSON-RPC через Circuit Breaker: `eth_chainId`, `eth_blockNumber`, `eth_getBlockByNumber`
  и пачки по `RPC_BATCH_SIZE` из `eth_getBalance` (ETH) и `eth_call` `balanceOf` (ERC-20) на блоке сверки;
- ledger-service: `GET /internal/addresses/statement` (до 100 адресов за запрос) и, если суммы не совпали,
  `GET /internal/addresses/{address}/postings` — [`contracts/openapi/ledger-service.yaml`](../../contracts/openapi/ledger-service.yaml).
  Таймаут 2 с, один повтор и outlier detection задают ресурсы Istio чарта ledger-service (SRS 4.1.9);
  `LEDGER_TIMEOUT` ограничивает вызов без sidecar.

`event.id` детерминирован: `<runId>:<address>:<ETH | адрес контракта>` у `reconciliation.mismatch`,
`<runId>:completed` у `reconciliation.completed`. Повторная публикация после сбоя даёт те же события.

## Хранилища

| Хранилище | Что хранит | Схема |
|---|---|---|
| MongoDB | проекция `invoices`: `_id` = `invoiceId`, `merchantId`, `address`, `asset`, `createdAt`, `expiresAt`, `paidAt`, `transfers.<blockHash:txHash:logIndex>` (`blockNumber`, `amount`), `sequence`, `expireAt` (TTL `PROJECTION_TTL`) | [`internal/projection`](internal/projection/store.go) |
| MongoDB | отчёты `reports`: `_id` = `runId`, блок, время, число адресов, строки (расхождения и «после окна отслеживания»), `published`, `expireAt` (TTL `REPORT_TTL`) | [`internal/report`](internal/report/store.go) |

## Конфигурация

| Переменная | Назначение | Значение по SRS |
|---|---|---|
| `HTTP_ADDR` | адрес HTTP | `:8080` |
| `LOG_LEVEL` | уровень логов | `info` |
| `KAFKA_BROKERS` | брокеры через запятую, обязательно | — |
| `KAFKA_CONSUMER_GROUP` | группа потребителя `invoice.events` | `reconciliation-service` |
| `MONGODB_URI`, `MONGODB_USERNAME`, `MONGODB_PASSWORD_FILE`, `MONGODB_DATABASE`, `MONGODB_AUTH_SOURCE` | MongoDB (`storage.MongoConfigFromEnv`) | — |
| `NODE_URL` | JSON-RPC ноды, `http` или `https`, обязательно | — |
| `CHAIN_ID` | ожидаемая цепочка, обязательно; с нодой другой цепочки сверка не выполняется | 31337 (Anvil) |
| `CONFIRMATIONS` | N: балансы читаются на блоке `head − N` | 7 (4.1.9) |
| `RPC_BATCH_SIZE` | вызовов в одной пачке JSON-RPC | 100 |
| `NODE_TIMEOUT` | таймаут вызова ноды | 5 с |
| `NODE_BREAKER_FAILURE_THRESHOLD`, `NODE_BREAKER_OPEN_TIMEOUT`, `NODE_BREAKER_HALF_OPEN_MAX` | Circuit Breaker к ноде | 5, 15 с, 1 |
| `LEDGER_URL` | базовый URL ledger-service, обязательно | — |
| `LEDGER_TIMEOUT` | таймаут вызова ledger-service | 2 с (4.1.9) |
| `RUN_INTERVAL` | период сверки | 10 мин (UC-08) |
| `RUN_TIMEOUT` | наибольшая длительность одного запуска, больше `SETTLE_DELAY` | 5 мин |
| `SETTLE_DELAY` | пауза после выбора блока, чтобы события его переводов дошли до проекции и ledger | 15 с |
| `ADDRESS_LOOKBACK` | сверяются адреса счетов, созданных за этот период до времени блока | 7 сут (UC-08) |
| `TRACKING_WINDOW` | окно отслеживания адреса после `expiresAt` или `paidAt` | 24 ч (4.1.4) |
| `PROJECTION_TTL` | срок хранения счёта в проекции, больше `ADDRESS_LOOKBACK` | 8 сут |
| `REPORT_TTL` | срок хранения отчёта | 30 сут |
| `LEADER_LEASE_NAME`, `POD_NAME`, `POD_NAMESPACE` | выбор лидера через Lease; без имени Lease реплика всегда лидер | задаёт weigate-lib |

Секреты читаются из файлов: `/var/run/secrets/weigate/mongodb/password`.

## Команды

```sh
go build ./services/reconciliation-service/...
go test -race ./services/reconciliation-service/...
go test -race -tags integration ./services/reconciliation-service/...   # Testcontainers: Anvil, Kafka, MongoDB
```

Общие проверки — цели `make` из корневого AGENTS.md. Чарт — [`charts/reconciliation-service`](../../charts/reconciliation-service).

## Особые правила

- Сверку выполняет только держатель Lease (`pkg/leader`); проекцию `invoice.events` ведут все реплики.
  Запуски выровнены по `RUN_INTERVAL` от начала эпохи. При получении лидерства метрики берутся из последнего
  отчёта.
- Запуск: блок `head − N`, пауза `SETTLE_DELAY`, счета с `createdAt` ≥ время блока − `ADDRESS_LOOKBACK`.
  Счета одного адреса и актива суммируются; учитываются переводы из блоков не выше блока сверки, перевод
  в нескольких счетах считается один раз.
- Сумма ledger — проводки поступлений по адресу и активу. Если число и сумма проводок выписки совпали с
  переводами счетов, берётся выписка; иначе читаются проводки адреса и суммируются те, что не выше блока сверки.
- В окне отслеживания любое расхождение баланса цепочки и счетов — `chain_vs_invoices`. После окна баланс
  больше учтённого — не расхождение, а строка отчёта `after_window`; меньше — по-прежнему расхождение.
  Независимо счета сверяются с ledger — `invoices_vs_ledger`. На адрес и актив — не больше одного события.
- Ledger-service недоступен или ответил ошибкой — запуск откладывается до следующего (`result=postponed`),
  отчёт не сохраняется. Ошибка ноды или MongoDB — `result=failed`.
- Отчёт сохраняется до публикации, после неё отмечается `published`. Неопубликованные отчёты публикуются
  повторно в начале следующего запуска.
- Проекция: `invoice.reverted` убирает перевод, повторное применение события даёт тот же документ; события счёта идут по порядку из его партиции.
  При пустой группе потребителя топик читается с `now − ADDRESS_LOOKBACK`, поэтому хранение
  `invoice.events` должно быть не меньше `ADDRESS_LOOKBACK`.
- Метрики: `weigate_reconciliation_runs{result}` (`ok`, `mismatch`, `postponed`, `failed`),
  `weigate_reconciliation_run_duration`, `weigate_reconciliation_node_requests{method,result}`; у лидера —
  `weigate_reconciliation_mismatches` (алерт `ReconciliationMismatch` при > 0), `weigate_reconciliation_after_window`,
  `weigate_reconciliation_addresses_checked`, `weigate_reconciliation_block`, `weigate_reconciliation_last_run_timestamp`.

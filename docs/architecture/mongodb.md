# Схемы документов MongoDB

Коллекции по коду сервисов. Суммы в документах — десятичные строки целых чисел (минимальные единицы актива).
Поля `_id` и TTL-индексы указаны как в коде; у каждого сервиса своя база.

## chain-watcher

`internal/store`, `internal/projection`.

| Коллекция | `_id` | Поля | Индексы |
|---|---|---|---|
| `chain_state` | `chainId` (int64) | `cursor` — номер последнего обработанного блока (null до первого); `window` — до `WINDOW_SIZE` заголовков `{number, hash, parentHash, timestamp}`; `version`; `updatedAt` | — |
| `tracked_addresses` | адрес получателя | `invoiceId`, `createdAt` (время `invoice.created`), `expiresAt`, `trackUntil` (конец окна), `rescanned` | `(rescanned, trackUntil)`, `(trackUntil)` |
| `projection_state` | идентификатор проекции | `rebuiltAt` — отметка о построении проекции | — |

Запись `chain_state` условная по `version`: курсор и окно меняются одним документом, чужая запись отклоняется.
`trackUntil` растёт только вверх (`$max`).

## webhook-service

`internal/merchants`, `internal/journal`.

| Коллекция | `_id` | Поля | Индексы |
|---|---|---|---|
| `merchants` | `merchantId` | `version`, `webhookUrl`, `secretVersion`; применяется только документ с большей `version` | — |
| `deliveries` | `<eventId>:<попытка>` | `eventId`, `eventType`, `invoiceId`, `merchantId`, `attempt`, `result` (`delivered`, `failed`, `skipped`), `statusCode`, `reason`, `secretVersion`, `at`, `durationMs`, `expireAt` | `(eventId, at)`, TTL `expireAt` (`JOURNAL_TTL`, 30 сут) |

## reconciliation-service

`internal/projection`, `internal/report`.

| Коллекция | `_id` | Поля | Индексы |
|---|---|---|---|
| `invoices` | `invoiceId` | `merchantId`, `address`, `asset {kind, contract, decimals}`, `createdAt`, `expiresAt`, `paidAt`, `transfers` — карта `<blockHash:txHash:logIndex>` → `{blockNumber, amount}`, `sequence`, `expireAt` | `(createdAt)`, TTL `expireAt` (`PROJECTION_TTL`, 8 сут) |
| `reports` | `runId` | `chainId`, `blockNumber`, `blockTime`, `startedAt`, `finishedAt`, `addressesChecked`, `mismatches`, `afterWindow`, `result`, `rows`, `published`, `expireAt` | `(startedAt -1)`, `(published, startedAt)`, TTL `expireAt` (`REPORT_TTL`, 30 сут) |

Строка `rows`: `kind` (расхождение `chain_vs_invoices`, `invoices_vs_ledger` или строка «после окна отслеживания»
`after_window`), `address`, `merchantId`, `invoiceIds`, `asset`, `chainBalance`, `invoicesTotal`, `ledgerTotal`,
`excess`, `trackUntil`.

## Valkey и ClickHouse

Не документы MongoDB, но входят в модель хранилищ:

| Хранилище | Что | Ключ |
|---|---|---|
| Valkey, chain-watcher | `SET` отслеживаемых адресов | `chain-watcher:<chainId>:tracked` |
| Valkey, webhook-service | дедупликация (`SET NX`, TTL 24 ч) | `webhook-service:dedup:<event.id>` |
| Valkey, invoice-service | кэш страницы оплаты (TTL 30 с) | `invoice:status:<id>` |
| ClickHouse | витрины `merchant_turnover_daily`, `merchant_conversion_daily`, наполняются из `invoice.events` движком Kafka; читает `reporting-service` | схема — `weigate-gitops` |

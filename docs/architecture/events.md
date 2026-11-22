# Каталог событий

Топики и события по [`contracts/asyncapi/`](../../contracts/asyncapi) и [`contracts/schemas/`](../../contracts/schemas).
Формат — CloudEvents (JSON), `dataschema` ведёт на версионированную схему, Kafka-ключ равен `subject` события.
Заголовки: `content-type: application/cloudevents+json; charset=UTF-8`, `traceparent`, `tracestate`.
Доставка at-least-once, потребители дедуплицируют по `event.id`. Партиции и ключи — SRS 4.1.5, RF = 3.
`clickhouse` — движок Kafka витрин (не сервис).

| Топик | Ключ | Партиции | Тип события | Схема | Производитель | Потребители |
|---|---|---|---|---|---|---|
| `chain.blocks` | `chainId` | 1 | `chain.new_head` | `chain.new_head.v1.json` | chain-watcher | invoice-service |
| `chain.blocks` | `chainId` | 1 | `chain.reorg` | `chain.reorg.v1.json` | chain-watcher | invoice-service |
| `chain.transfers` | адрес получателя | 6 | `chain.transfer` | `chain.transfer.v1.json` | chain-watcher | invoice-service, clickhouse |
| `invoice.events` | `invoiceId` | 6 | `invoice.created` | `invoice.created.v1.json` | invoice-service | chain-watcher, webhook-service, ledger-service, reconciliation-service, clickhouse |
| `invoice.events` | `invoiceId` | 6 | `invoice.detected` | `invoice.detected.v1.json` | invoice-service | chain-watcher, webhook-service, ledger-service, reconciliation-service, clickhouse |
| `invoice.events` | `invoiceId` | 6 | `invoice.confirming` | `invoice.confirming.v1.json` | invoice-service | chain-watcher, webhook-service, ledger-service, reconciliation-service, clickhouse |
| `invoice.events` | `invoiceId` | 6 | `invoice.paid` | `invoice.paid.v1.json` | invoice-service | chain-watcher, webhook-service, ledger-service, reconciliation-service, clickhouse |
| `invoice.events` | `invoiceId` | 6 | `invoice.underpaid` | `invoice.underpaid.v1.json` | invoice-service | chain-watcher, webhook-service, ledger-service, reconciliation-service, clickhouse |
| `invoice.events` | `invoiceId` | 6 | `invoice.expired` | `invoice.expired.v1.json` | invoice-service | chain-watcher, webhook-service, ledger-service, reconciliation-service, clickhouse |
| `invoice.events` | `invoiceId` | 6 | `invoice.late_paid` | `invoice.late_paid.v1.json` | invoice-service | chain-watcher, webhook-service, ledger-service, reconciliation-service, clickhouse |
| `invoice.events` | `invoiceId` | 6 | `invoice.reverted` | `invoice.reverted.v1.json` | invoice-service | chain-watcher, webhook-service, ledger-service, reconciliation-service, clickhouse |
| `invoice.events` | `invoiceId` | 6 | `invoice.late_transfer` | `invoice.late_transfer.v1.json` | invoice-service | chain-watcher, webhook-service, ledger-service, reconciliation-service, clickhouse |
| `merchant.events` | `merchantId` | 3, compacted | `merchant.updated` | `merchant.updated.v1.json` | merchant-service | invoice-service, webhook-service |
| `ledger.events` | `merchantId` | 3 | `ledger.receipt_posted` | `ledger.receipt_posted.v1.json` | ledger-service | — |
| `ledger.events` | `merchantId` | 3 | `ledger.fee_accrued` | `ledger.fee_accrued.v1.json` | ledger-service | — |
| `reconciliation.events` | `runId` | 1 | `reconciliation.mismatch` | `reconciliation.mismatch.v1.json` | reconciliation-service | — |
| `reconciliation.events` | `runId` | 1 | `reconciliation.completed` | `reconciliation.completed.v1.json` | reconciliation-service | — |
| `rate.ticks` | `источник:пара` | 3 | `rate.tick` | `rate.tick.v1.json` | rate-service | clickhouse |
| `rate.latest` | пара | 1, compacted | `rate.latest` | `rate.latest.v1.json` | rate-service | rate-service |
| `webhook.retry.1m` | `invoiceId` | 3 | `webhook.retry` | `webhook.retry.v1.json` | webhook-service | webhook-service |
| `webhook.retry.5m` | `invoiceId` | 3 | `webhook.retry` | `webhook.retry.v1.json` | webhook-service | webhook-service |
| `webhook.retry.30m` | `invoiceId` | 3 | `webhook.retry` | `webhook.retry.v1.json` | webhook-service | webhook-service |
| `webhook.dlq` | `invoiceId` | 1 | `webhook.dead_lettered` | `webhook.dead_lettered.v1.json` | webhook-service | — |

Общие схемы: `cloudevent.v1.json` (конверт) и `defs.v1.json` (типы и заголовки Kafka).

## Идентификатор события

| Событие | `event.id` |
|---|---|
| `chain.new_head` | `<chainId>:<hash>` |
| `chain.reorg` | `<chainId>:reorg:<fromBlock>:<hash первого отменённого блока>` |
| `chain.transfer` | `<blockHash>:<txHash>:<logIndex>`, у ETH `logIndex` пуст |
| `ledger.receipt_posted` | `receipt:<blockHash>:<txHash>:<logIndex>` |
| `ledger.fee_accrued` | `fee:<blockHash>:<txHash>:<logIndex>` |
| `reconciliation.mismatch` | `<runId>:<address>:<ETH или адрес контракта>` |
| `reconciliation.completed` | `<runId>:completed` |
| `webhook.retry` | `<id исходного события>:<число неудачных попыток>` |
| `webhook.dead_lettered` | `<id исходного события>:dlq` |

## Проверка

`go test ./docs/architecture/` сверяет этот каталог с AsyncAPI (топик, производитель, потребители, схема каждого типа),
таблицы ER-схемы — с миграциями, коллекции MongoDB — с кодом сервисов, а типы событий и топики на диаграммах —
с каталогом.

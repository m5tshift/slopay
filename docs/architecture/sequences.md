# Sequence-диаграммы

Сценарии SRS 9 по коду и контрактам. Топики и типы событий — в [`events.md`](events.md), статусы — SRS 4.1.4.
N — число подтверждений (`CONFIRMATIONS`, по умолчанию 7). Все сроки считаются по `timestamp` блоков (время цепочки).
Стрелка `-)` — асинхронное сообщение Kafka, `->>` — синхронный вызов.

## Успешная оплата

```mermaid
sequenceDiagram
    autonumber
    actor P as Покупатель
    participant M as Мерчант
    participant INV as invoice-service
    participant K as Kafka
    participant CW as chain-watcher
    participant N as Anvil
    participant LED as ledger-service
    participant WH as webhook-service
    M->>INV: POST /api/v1/invoices (Idempotency-Key)
    INV->>INV: курс, адрес из xpub, сумма в wei, счёт и outbox в одной транзакции
    INV-->>M: 201 {address, amount, expiresAt}
    INV-)K: invoice.events: invoice.created
    K-)CW: invoice.created: адрес в tracked_addresses и в Valkey
    K-)WH: invoice.created
    WH->>M: POST webhookUrl (HMAC)
    P->>N: перевод на address
    CW->>N: eth_getBlockByNumber, eth_getLogs
    CW-)K: chain.blocks: chain.new_head
    CW-)K: chain.transfers: chain.transfer (blockHash, txHash, logIndex)
    K-)INV: chain.new_head: каноническая цепочка, время цепочки
    K-)INV: chain.transfer: блок в канонической цепочке, перевод учтён
    INV-)K: invoice.detected (NEW → DETECTED)
    loop каждый новый блок
        CW-)K: chain.new_head
        K-)INV: пересчёт подтверждений
        INV-)K: invoice.confirming (2..N-1)
    end
    INV-)K: invoice.paid (CONFIRMING → PAID, сумма в допуске)
    K-)CW: invoice.paid: окно отслеживания продлено
    K-)LED: invoice.paid: проводка поступления и комиссии, outbox
    LED-)K: ledger.events: ledger.receipt_posted, ledger.fee_accrued
    K-)WH: invoice.paid
    WH->>M: POST webhookUrl (HMAC)
    M-->>WH: 200
```

## Недоплата

```mermaid
sequenceDiagram
    autonumber
    actor P as Покупатель
    participant CW as chain-watcher
    participant K as Kafka
    participant INV as invoice-service
    participant WH as webhook-service
    participant M as Мерчант
    P->>CW: перевод меньше суммы счёта за вычетом допуска
    CW-)K: chain.transfer
    K-)INV: перевод учтён
    INV-)K: invoice.detected, затем invoice.confirming
    Note over INV: N подтверждений, получено меньше допуска
    INV-)K: invoice.underpaid (CONFIRMING → UNDERPAID)
    K-)WH: invoice.underpaid
    WH->>M: POST webhookUrl
    alt доплата до expiresAt
        P->>CW: второй перевод на тот же адрес
        CW-)K: chain.transfer
        K-)INV: суммы складываются, подтверждения по последнему переводу
        INV-)K: invoice.confirming (UNDERPAID → CONFIRMING)
        INV-)K: invoice.paid, если сумма в допуске, иначе снова invoice.underpaid
    else блок с timestamp > expiresAt обработан
        Note over INV: UNDERPAID final
        P->>CW: перевод позже
        CW-)K: chain.transfer
        K-)INV: статус не меняется
        INV-)K: invoice.late_transfer
        K-)WH: invoice.late_transfer
        WH->>M: POST webhookUrl
    end
```

## Истечение и поздний платёж

```mermaid
sequenceDiagram
    autonumber
    actor P as Покупатель
    participant CW as chain-watcher
    participant K as Kafka
    participant INV as invoice-service
    participant WH as webhook-service
    participant M as Мерчант
    CW-)K: chain.new_head (timestamp > expiresAt)
    K-)INV: время цепочки прошло expiresAt, переводов в блоках до expiresAt нет
    INV-)K: invoice.expired (NEW → EXPIRED)
    K-)WH: invoice.expired
    WH->>M: POST webhookUrl
    P->>CW: перевод в окне отслеживания (24 ч после expiresAt)
    CW-)K: chain.transfer (адрес ещё в tracked_addresses)
    K-)INV: перевод учтён как поздний
    Note over INV: ждёт N подтверждений по chain.new_head
    INV-)K: invoice.late_paid (EXPIRED → LATE_PAID, фактическая сумма)
    K-)WH: invoice.late_paid
    WH->>M: POST webhookUrl: решение за мерчантом
    Note over CW: время цепочки прошло trackUntil: адрес удаляется из проекции и Valkey, переводы дальше не публикуются
```

Переход в `LATE_PAID` окно не продлевает. Перевод на адрес счёта в финальном статусе внутри окна даёт `invoice.late_transfer`.

## Реорганизация

```mermaid
sequenceDiagram
    autonumber
    participant N as Anvil
    participant CW as chain-watcher
    participant K as Kafka
    participant INV as invoice-service
    participant WH as webhook-service
    participant M as Мерчант
    CW->>N: eth_getBlockByNumber: parentHash не совпал с окном заголовков
    CW->>CW: глубина не больше REORG_MAX_DEPTH, поиск общего предка
    CW-)K: chain.blocks: chain.reorg (fromBlock)
    CW-)K: chain.blocks: chain.new_head новой цепочки
    K-)INV: chain.reorg: блоки от fromBlock убраны из канонической цепочки
    Note over INV: перевод из убранного блока не канонический, сумма счёта пересчитана
    INV-)K: invoice.reverted (DETECTED или CONFIRMING → NEW)
    K-)WH: invoice.reverted
    WH->>M: POST webhookUrl
    alt тот же txHash включён в новую цепочку
        CW-)K: chain.transfer (новый blockHash, тот же txHash)
        K-)INV: перевод учтён как поданный вовремя
        INV-)K: invoice.detected (NEW → DETECTED)
    else expiresAt уже прошёл
        Note over INV: ожидание 10 мин времени цепочки (grace_until) повторного включения txHash
        INV-)K: invoice.expired (NEW → EXPIRED), если перевод не вернулся
    end
```

Проводка ledger создаётся только после N подтверждений, сторнирующих проводок нет (журнал только добавляется).

## Повторная доставка вебхука

```mermaid
sequenceDiagram
    autonumber
    participant K as Kafka
    participant WH as webhook-service
    participant V as Valkey
    participant B as OpenBao
    participant J as MongoDB deliveries
    participant M as Мерчант
    K-)WH: invoice.events: invoice.paid (event.id = E)
    WH->>V: SET webhook-service:dedup:E NX (TTL 24 ч)
    V-->>WH: OK, ключ новый
    WH->>B: KV v2 webhook/merchantId, версия секрета
    WH->>M: POST webhookUrl (X-Weigate-Signature, X-Weigate-Timestamp)
    M--xWH: 500 или таймаут (попытка 1)
    WH->>J: deliveries E:1, result failed
    WH-)K: webhook.retry.1m (event.id E:1, nextAttemptAt)
    K-)WH: retry-потребитель ждёт nextAttemptAt
    WH->>M: POST webhookUrl: подпись и timestamp новые, тело то же
    M--xWH: ошибка (попытка 2)
    WH-)K: webhook.retry.5m (E:2)
    WH->>M: попытка 3 после 5 мин
    M--xWH: ошибка
    WH-)K: webhook.retry.30m (E:3)
    WH->>M: попытка 4 после 30 мин
    alt 2xx
        M-->>WH: 200
        WH->>J: deliveries E:4, result delivered
    else снова ошибка
        WH-)K: webhook.dlq: webhook.dead_lettered (event.id E:dlq)
    end
    Note over K,WH: Kafka доставила invoice.paid повторно (at-least-once)
    K-)WH: invoice.paid (event.id = E)
    WH->>V: SET webhook-service:dedup:E NX
    V-->>WH: ключ есть: доставка пропущена
```

Повтор из DLQ — `POST /internal/dlq/replay` на `ADMIN_ADDR`; запись «ушла из DLQ», когда в журнале есть доставленная
переотправка `<event.id>:dlq`.

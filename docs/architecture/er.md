# ER-схема PostgreSQL

Схемы по миграциям `services/<service>/migrations/`. У каждого сервиса своя база, внешних ключей между базами нет:
связи между сервисами логические, через события Kafka. Суммы — `NUMERIC(78,0)`, USD — целые центы.
Таблица `outbox` одинакова в `invoice-service`, `merchant-service` и `ledger-service` (миграция `00001_outbox.sql`).

## merchant-service

Миграции `00001_outbox.sql`, `00002_merchants.sql`.

```mermaid
erDiagram
    merchants ||--o{ api_keys : "merchant_id"
    merchants {
        uuid id PK
        text name
        text email
        text xpub "только xpub"
        text webhook_url
        int tolerance_bps "0..500"
        int webhook_secret_version
        bigint version "версия merchant.updated"
        timestamptz created_at
        timestamptz updated_at
    }
    api_keys {
        uuid id PK
        uuid merchant_id FK
        bytea key_hash UK "SHA-256"
        timestamptz created_at
        timestamptz expires_at "плановая ротация"
        timestamptz revoked_at
    }
    outbox {
        bigserial id PK
        text event_id UK
        text topic
        text key
        jsonb event
        text traceparent
        text tracestate
        timestamptz created_at
        timestamptz published_at
    }
```

## invoice-service

Миграции `00001_outbox.sql`, `00002_invoices.sql`, `00003_lifecycle.sql`. `merchants` — проекция `merchant.events`.

```mermaid
erDiagram
    merchants ||--o{ invoices : "merchant_id"
    invoices ||--o{ transfers : "invoice_id"
    chain_state ||--o{ chain_blocks : "chain_id"
    merchants {
        uuid id PK
        bigint version "применяется только более новая"
        text xpub
        int tolerance_bps
        bigint next_derivation_index "под блокировкой строки"
        timestamptz updated_at
    }
    invoices {
        uuid id PK
        uuid merchant_id FK
        text idempotency_key "UK (merchant_id, idempotency_key)"
        bytea request_hash
        text order_id
        bigint amount_usd_cents
        text asset_kind "ETH или ERC20"
        text asset_contract
        int asset_decimals
        numeric rate
        timestamptz rate_quoted_at
        numeric amount "NUMERIC(78,0)"
        text address
        bigint derivation_index "UK (merchant_id, derivation_index)"
        text status "NEW DETECTED CONFIRMING PAID UNDERPAID EXPIRED LATE_PAID"
        int sequence
        int confirmations
        numeric received_amount
        bool overpaid
        timestamptz created_at
        timestamptz expires_at
        timestamptz updated_at
        timestamptz paid_at
        bool watch "частичный индекс"
        timestamptz deadline "частичный индекс"
    }
    transfers {
        text block_hash PK
        text tx_hash PK
        int log_index PK "-1 для ETH"
        uuid invoice_id FK
        bigint block_number
        timestamptz block_time
        numeric amount
        text asset_kind
        text asset_contract
        int asset_decimals
        bool canonical
        text role "пусто, counted, late"
        timestamptz grace_until
        timestamptz created_at
    }
    chain_state {
        bigint chain_id PK
        bool known
        bigint head_number
        text head_hash
        timestamptz chain_time "наибольший timestamp блока"
        bigint blocks_offset
        timestamptz updated_at
    }
    chain_blocks {
        bigint chain_id PK
        bigint number PK
        text hash
        text parent_hash
        timestamptz block_time
    }
    outbox {
        bigserial id PK
        text event_id UK
        text topic
        text key
        jsonb event
        text traceparent
        text tracestate
        timestamptz created_at
        timestamptz published_at
    }
```

Индексы `invoices`: `(merchant_id, idempotency_key)` и `(merchant_id, derivation_index)` уникальные, список мерчанта
`(merchant_id, created_at DESC, id DESC)`, поиск перевода `(address)`, `(id) WHERE watch`, `(deadline) WHERE deadline IS NOT NULL`.
`transfers`: `(invoice_id)`, `(block_number)`.

## ledger-service

Миграции `00001_outbox.sql`, `00002_ledger.sql`. Журнал только добавляется: триггеры запрещают `UPDATE`, `DELETE`
и `TRUNCATE`, отложенные триггеры проверяют, что у проводки не меньше двух строк и дебет равен кредиту.

```mermaid
erDiagram
    merchants ||--o{ postings : "merchant_id"
    postings ||--|{ entries : "posting_id"
    merchants {
        uuid id PK
        timestamptz first_seen_at
    }
    postings {
        text id PK "receipt:blockHash:txHash:logIndex"
        uuid merchant_id FK
        uuid invoice_id
        text address
        bigint block_number
        text block_hash
        text tx_hash
        int log_index "NULL для ETH"
        text asset_kind
        text asset_contract
        int asset_decimals
        numeric amount
        int fee_bps
        numeric fee
        timestamptz posted_at
    }
    entries {
        text posting_id PK
        smallint line PK
        text account
        char side "D или C"
        numeric amount
    }
    outbox {
        bigserial id PK
        text event_id UK
        text topic
        text key
        jsonb event
        text traceparent
        text tracestate
        timestamptz created_at
        timestamptz published_at
    }
```

Индексы: `postings (merchant_id, posted_at, id)`, `postings (address, posted_at, id)`, `entries (account)`.
`postings.invoice_id` внешнего ключа не имеет: счета живут в базе `invoice-service`.

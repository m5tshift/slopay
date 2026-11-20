# AGENTS.md — invoice-service

## Назначение

Выставляет счета: фиксирует курс через `rate-service`, считает сумму в wei, выдаёт адрес из xpub мерчанта,
отдаёт статус и список счетов мерчанту и публичную страницу оплаты покупателю. Ведёт автомат статусов счёта
по `chain.blocks` и `chain.transfers`. Требования — SRS 4.1.3, 4.1.4, 4.1.5, 4.1.6, 4.1.7,
FR: FR-01, FR-02, FR-03, FR-04, FR-05, FR-06, FR-08, NFR: NFR-02, NFR-04, NFR-06, NFR-07, NFR-08.

## API и топики

| Вид | Что | Контракт |
|---|---|---|
| HTTP | `POST /api/v1/invoices`, `GET /api/v1/invoices`, `GET /api/v1/invoices/{id}`, `GET /pay/{id}`, `GET /pay/{id}/status` | [`contracts/openapi/invoice-service.yaml`](../../contracts/openapi/invoice-service.yaml) |
| Публикует | `invoice.events` (все `invoice.*`), только через outbox | [`contracts/asyncapi/invoice-service.yaml`](../../contracts/asyncapi/invoice-service.yaml) |
| Потребляет | `merchant.events` (`merchant.updated`) — проекция мерчантов | [`contracts/asyncapi/merchant-service.yaml`](../../contracts/asyncapi/merchant-service.yaml) |
| Потребляет | `chain.blocks` (`chain.new_head`, `chain.reorg`), группа `<KAFKA_CONSUMER_GROUP>-chain-blocks`; `chain.transfers` (`chain.transfer`), группа `<KAFKA_CONSUMER_GROUP>-chain-transfers` | [`contracts/asyncapi/chain-watcher.yaml`](../../contracts/asyncapi/chain-watcher.yaml) |

Синхронные вызовы других сервисов: `rate-service` `GET /rates/{pair}`
([`contracts/openapi/rate-service.yaml`](../../contracts/openapi/rate-service.yaml)) при создании счёта.
Таймаут и повтор этого вызова, outlier detection — в Istio (SRS 4.1.9, ADR 0003); в коде только таймаут клиента.

Каждый запрос сверяется с контрактом (`internal/api`, kin-openapi): операциям `/api/v1/*` нужен JWT
мерчанта (`401` с `WWW-Authenticate: Bearer`), затем проверяется запрос (`400`). `/pay/*` публичные
и не раскрывают данных мерчанта, кроме `orderId`.

## Хранилища

| Хранилище | Что хранит | Схема |
|---|---|---|
| PostgreSQL | счета, проекция мерчантов со счётчиком деривации, outbox; канонические блоки (`chain_state`, `chain_blocks`) и переводы на адреса счетов (`transfers`) | [`migrations/`](migrations/), применяются при старте (goose) |
| Valkey | кэш статуса страницы оплаты: `invoice:status:<id>`, `STRING` с JSON страницы, TTL 30 с | — |

## Конфигурация

| Переменная | Назначение | Значение по SRS |
|---|---|---|
| `HTTP_ADDR` | адрес HTTP | `:8080` |
| `LOG_LEVEL` | уровень логов | `info` |
| `KAFKA_BROKERS` | брокеры через запятую, обязательно | — |
| `KAFKA_CONSUMER_GROUP` | группа потребителя `merchant.events` и префикс групп `chain.*` | `invoice-service` |
| `POSTGRES_URL`, `POSTGRES_PASSWORD_FILE`, `POSTGRES_MAX_CONNS` | PostgreSQL (`storage.PostgresConfigFromEnv`) | — |
| `VALKEY_ADDRS`, `VALKEY_USERNAME`, `VALKEY_PASSWORD_FILE` | Valkey (`storage.ValkeyConfigFromEnv`) | — |
| `VALKEY_TIMEOUT` | таймаут операции с кэшем | 100 мс |
| `STATUS_CACHE_TTL` | TTL кэша статуса | 30 с (4.1.7) |
| `RATE_SERVICE_URL` | адрес `rate-service`, обязательно | — |
| `RATE_SERVICE_TIMEOUT` | таймаут клиента `rate-service` | 1 с |
| `RATE_MAX_AGE` | максимальный возраст курса, не больше 60 с | 60 с (4.1.3) |
| `RATE_RETRY_AFTER` | `Retry-After` ответа `503`, если `rate-service` его не прислал | 5 с |
| `INVOICE_TTL` | срок жизни счёта | 15 мин (4.1.4) |
| `JWT_ISSUER` | ожидаемый `iss` JWT; пусто — не проверяется | `slopay-merchant-service` |
| `CHAIN_ID` | цепочка, события других цепочек пропускаются | `31337` (Anvil) |
| `CONFIRMATIONS` | N подтверждений, не меньше 2 и больше глубины реорганизации | 7 (3.1) |
| `REVERT_WAIT` | ожидание того же `txHash` после отката после `expiresAt` | 10 мин (4.1.4) |
| `TRACKING_WINDOW` | окно отслеживания адреса | 24 ч (4.1.4) |
| `CHAIN_BLOCKS` | сколько последних канонических блоков хранится, больше `CONFIRMATIONS` | 256 |
| `BARRIER_POLL` | период опроса закоммиченных смещений `chain.transfers` перед блоком | 50 мс |

Секреты читаются из файлов: `/var/run/secrets/slopay/postgres/password`, `/var/run/secrets/slopay/valkey/password`.

## Команды

```sh
go build ./services/invoice-service/...
go test -race ./services/invoice-service/...
go test -race -tags integration ./services/invoice-service/...   # Testcontainers: PostgreSQL, Kafka, Valkey
go generate ./services/invoice-service/...                       # internal/api/api.gen.go из контракта
```

Общие проверки — цели `make` из корневого AGENTS.md. Чарт — [`charts/invoice-service`](../../charts/invoice-service).

## Особые правила

- **Идемпотентность** (FR-01, NFR-06): ключ ищется в PostgreSQL, а не в Valkey. Тот же ключ с тем же телом
  (SHA-256 канонического JSON) — тот же счёт и `200` без вызова `rate-service`; с другим телом — `422`.
  Гонку одинаковых ключей разрешает блокировка строки мерчанта и уникальный индекс
  `(merchant_id, idempotency_key)`.
- **Адрес** (4.1.3): индекс деривации берётся из `merchants.next_derivation_index` под `SELECT … FOR UPDATE`
  в той же транзакции, что вставка счёта и запись outbox; уникальный индекс `(merchant_id, derivation_index)`.
  Индексы, на которых BIP-32 не даёт ключа, пропускаются. Счётчик меняет только создание счёта,
  событие мерчанта его не трогает.
- **Курс** (4.1.3): курс старше `RATE_MAX_AGE` или `503` от `rate-service` — `503 rate_unavailable`
  с `reason` и `Retry-After`. Сумма — `pkg/money`, округление вверх; курс и сумма фиксируются в счёте.
- **Проекция мерчантов**: событие с версией не выше сохранённой игнорируется. Ошибка записи повторяется
  с backoff, партиция не продвигается, событие не теряется. Мерчант без проекции — `409 xpub_not_set`.
- **JWT**: подпись проверяет Istio Ingress Gateway по JWKS `merchant-service` (SRS 4.1.10). Сервис только
  разбирает токен: не `alg: none`, подпись не пустая, `exp` в будущем, `iss` = `JWT_ISSUER`, `sub` — UUID
  мерчанта. Без Istio (минимальный стенд) подпись не проверяется никем. Токены в логи не попадают.
- **Кэш** — не часть readiness: без Valkey сервис работает, статус читается из PostgreSQL.
  `GET /pay/{id}/status` читает кэш, `GET /pay/{id}` — PostgreSQL и обновляет кэш.
- **Автомат счёта** (4.1.4) — `internal/lifecycle`. `Evaluate` — чистая функция: по счёту, его переводам
  и канонической цепочке пересчитывает статус, роли переводов и переходы; каждый переход — событие
  `invoice.*` со следующим `sequence` и детерминированным `id` (UUIDv5 от счёта и `sequence`). Новый
  переход сначала появляется в SRS и контракте, затем в `Evaluate`, в эталонной модели property-теста
  (`property_test.go`) и в unit-тестах.
- **Время цепочки**: TTL, ожидание после отката и окно считаются от максимального `timestamp` обработанных
  блоков (`chain_state.chain_time`), часы сервиса не используются. Счёт в `DETECTED`/`CONFIRMING` по TTL
  не истекает.
- **Цепочка** (4.1.5): `chain_blocks` — номер → хеш канонических блоков, `transfers.canonical` — блок
  перевода в канонической цепочке. Перевод учитывается только в каноническом блоке; `chain.reorg`
  снимает флаг с переводов от `fromBlock` и пересчитывает счета, `chain.new_head`, заменивший блок без
  `chain.reorg`, обрабатывается как реорганизация. Статус после N подтверждений реорганизация не отменяет,
  поэтому N больше поддерживаемой глубины.
- **Ровно один раз**: `chain.blocks` — одна партиция; смещение последней применённой записи хранится
  в `chain_state.blocks_offset` в той же транзакции, повтор пропускается. Перевод дедуплицируется по
  `blockHash + txHash + logIndex` (первичный ключ `transfers`, `logIndex` = −1 у ETH).
- **Порядок**: перед `chain.new_head` обработчик ждёт, пока группа `chain.transfers` закоммитит конец
  топика на момент ожидания (`Barrier`); `chain-watcher` публикует переводы блока до его `chain.new_head`
  (раздел «Publication order» контракта). Блокировка строки `chain_state`: события блоков — `FOR UPDATE`,
  переводы — `FOR SHARE`, счёт — `FOR UPDATE`.
- **Допуск** мерчанта читается при каждом пересчёте счёта из проекции мерчантов.
- **Кэш статуса** обновляется после коммита каждого перехода.
- **USDT** пока не поддерживается (`400` на `asset: USDT`): курс USDT и адрес контракта — отдельная задача.

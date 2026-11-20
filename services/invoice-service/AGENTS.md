# AGENTS.md — invoice-service

## Назначение

Выставляет счета: фиксирует курс через `rate-service`, считает сумму в wei, выдаёт адрес из xpub мерчанта,
отдаёт статус и список счетов мерчанту и публичную страницу оплаты покупателю. Требования — SRS 4.1.3,
4.1.4, 4.1.6, 4.1.7, FR: FR-01, FR-02, FR-03, FR-08, NFR: NFR-02, NFR-06, NFR-08.

## API и топики

| Вид | Что | Контракт |
|---|---|---|
| HTTP | `POST /api/v1/invoices`, `GET /api/v1/invoices`, `GET /api/v1/invoices/{id}`, `GET /pay/{id}`, `GET /pay/{id}/status` | [`contracts/openapi/invoice-service.yaml`](../../contracts/openapi/invoice-service.yaml) |
| Публикует | `invoice.events` (`invoice.created`), только через outbox | [`contracts/asyncapi/invoice-service.yaml`](../../contracts/asyncapi/invoice-service.yaml) |
| Потребляет | `merchant.events` (`merchant.updated`) — проекция мерчантов | [`contracts/asyncapi/merchant-service.yaml`](../../contracts/asyncapi/merchant-service.yaml) |

Синхронные вызовы других сервисов: `rate-service` `GET /rates/{pair}`
([`contracts/openapi/rate-service.yaml`](../../contracts/openapi/rate-service.yaml)) при создании счёта.
Таймаут и повтор этого вызова, outlier detection — в Istio (SRS 4.1.9, ADR 0003); в коде только таймаут клиента.

Каждый запрос сверяется с контрактом (`internal/api`, kin-openapi): операциям `/api/v1/*` нужен JWT
мерчанта (`401` с `WWW-Authenticate: Bearer`), затем проверяется запрос (`400`). `/pay/*` публичные
и не раскрывают данных мерчанта, кроме `orderId`.

## Хранилища

| Хранилище | Что хранит | Схема |
|---|---|---|
| PostgreSQL | счета, проекция мерчантов со счётчиком деривации, outbox | [`migrations/`](migrations/), применяются при старте (goose) |
| Valkey | кэш статуса страницы оплаты: `invoice:status:<id>`, `STRING` с JSON страницы, TTL 30 с | — |

## Конфигурация

| Переменная | Назначение | Значение по SRS |
|---|---|---|
| `HTTP_ADDR` | адрес HTTP | `:8080` |
| `LOG_LEVEL` | уровень логов | `info` |
| `KAFKA_BROKERS` | брокеры через запятую, обязательно | — |
| `KAFKA_CONSUMER_GROUP` | группа потребителя `merchant.events` | `invoice-service` |
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
- **USDT** пока не поддерживается (`400` на `asset: USDT`): курс USDT и адрес контракта — отдельная задача.

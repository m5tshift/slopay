# AGENTS.md — ledger-service

## Назначение

Ведёт учёт поступлений и комиссии платформы по двойной записи: на каждый перевод с N подтверждениями —
одна проводка и комиссия 0,5 %. Публикует проводки в `ledger.events` и отдаёт выписки для сверки.
Требования — SRS 4.1.3, FR-12, NFR-06.

## API и топики

| Вид | Что | Контракт |
|---|---|---|
| HTTP | `GET /internal/merchants/{merchantId}/statement`, `GET /internal/addresses/statement`, `GET /internal/addresses/{address}/postings` | [`contracts/openapi/ledger-service.yaml`](../../contracts/openapi/ledger-service.yaml) |
| Публикует | `ledger.events` (`ledger.receipt_posted`, `ledger.fee_accrued`), только через outbox | [`contracts/asyncapi/ledger-service.yaml`](../../contracts/asyncapi/ledger-service.yaml) |
| Потребляет | `invoice.events` (группа `KAFKA_CONSUMER_GROUP`) | [`contracts/asyncapi/invoice-service.yaml`](../../contracts/asyncapi/invoice-service.yaml) |

Синхронные вызовы других сервисов и внешних систем: нет.

API внутреннее (вызывает reconciliation-service), вызывающие аутентифицируются mTLS mesh, поэтому
аутентификации на уровне приложения нет. Каждый запрос сверяется с контрактом (`internal/api`,
kin-openapi), ошибка — `400` с именами параметров. Страницы — keyset по `(postedAt, postingId)`,
курсор непрозрачный, `limit` по умолчанию 200, максимум 1000.

## Хранилища

| Хранилище | Что хранит | Схема |
|---|---|---|
| PostgreSQL | мерчанты (по событиям счетов), проводки (`postings`), строки проводок (`entries`), outbox | [`migrations/`](migrations/), применяются при старте (goose) |

## Конфигурация

| Переменная | Назначение | Значение по SRS |
|---|---|---|
| `HTTP_ADDR` | адрес HTTP | `:8080` |
| `LOG_LEVEL` | уровень логов | `info` |
| `KAFKA_BROKERS` | брокеры через запятую, обязательно | — |
| `KAFKA_CONSUMER_GROUP` | группа потребителя `invoice.events` | `ledger-service` |
| `POSTGRES_URL`, `POSTGRES_PASSWORD_FILE`, `POSTGRES_MAX_CONNS` | PostgreSQL (`storage.PostgresConfigFromEnv`) | — |
| `FEE_BPS` | комиссия платформы, б. п. (0..10000) | 50 = 0,5 % (4.1.3) |

Секреты читаются из файлов: `/var/run/secrets/weigate/postgres/password`.

## Команды

```sh
go build ./services/ledger-service/...
go test -race ./services/ledger-service/...
go test -race -tags integration ./services/ledger-service/...   # Testcontainers: PostgreSQL, Kafka
go generate ./services/ledger-service/...                       # internal/api/api.gen.go и копия контракта internal/api/openapi.yaml
```

Общие проверки — цели `make` из корневого AGENTS.md. Чарт — [`charts/ledger-service`](../../charts/ledger-service).

## Особые правила

- Проводятся переводы из `invoice.paid`, `invoice.underpaid`, `invoice.late_paid` и `invoice.late_transfer`
  (поле `transfers`). Остальные события счёта ничего не проводят, только делают мерчанта известным:
  выписка неизвестного мерчанта — `404`. События других типов в топике пропускаются.
- Ключ идемпотентности — `blockHash + txHash + logIndex` (у ETH `logIndex` пустой): ID проводки
  `receipt:<ключ>`, события комиссии — `fee:<ключ>`. Повторное событие и перевод, пришедший в нескольких
  событиях, проводятся один раз (`ON CONFLICT DO NOTHING` по ID). Тот же перевод в другом блоке после
  реорганизации — другой перевод.
- Проводка — четыре строки: Д `address:<адрес>` / К `merchant:<id>:received` на сумму перевода
  и Д `merchant:<id>:fee_due` / К `platform:fee_income` на комиссию. Комиссия — `FEE_BPS` от суммы
  в активе перевода с округлением вниз (`pkg/money.Fee`); нулевая комиссия тоже публикуется.
- Сторно нет. Схема БД запрещает `UPDATE`, `DELETE` и `TRUNCATE` проводок и строк и отклоняет при commit
  проводку, у которой меньше двух строк или дебет не равен кредиту (отложенные constraint-триггеры).
- Проводки, их строки и события пишутся в одной транзакции; строка мерчанта блокируется
  (`SELECT … FOR UPDATE`), чтобы события одного ключа Kafka попадали в outbox по порядку.
- `postedAt` — время обработки события. Ошибка записи в PostgreSQL повторяется с backoff (0,5 с … 30 с),
  партиция ждёт; событие, которое нельзя провести, возвращается потребителю ошибкой, а не пропускается.

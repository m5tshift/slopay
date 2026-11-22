# AGENTS.md — webhook-service

## Назначение

Доставляет мерчанту подписанный вебхук на каждое событие `invoice.*`. Неудачная попытка повторяется через
топики `webhook.retry.1m`, `.5m`, `.30m`, затем доставка уходит в `webhook.dlq`. У каждого endpoint мерчанта
свой Circuit Breaker. Требования — SRS 4.1.3, 4.1.6, 4.1.7, 4.1.9, 4.1.10, FR-07, NFR-04, NFR-09.

## API и топики

| Вид | Что | Контракт |
|---|---|---|
| HTTP | `HTTP_ADDR`: `/health/live`, `/health/ready`, `/metrics`; `ADMIN_ADDR`: `POST /internal/dlq/replay` (повтор из DLQ) | [`contracts/openapi/webhook-service.yaml`](../../contracts/openapi/webhook-service.yaml) |
| Публикует | `webhook.retry.1m`, `webhook.retry.5m`, `webhook.retry.30m` (`webhook.retry`), `webhook.dlq` (`webhook.dead_lettered`); ключ — `invoiceId` | [`contracts/asyncapi/webhook-service.yaml`](../../contracts/asyncapi/webhook-service.yaml) |
| Потребляет | `invoice.events`: все `invoice.*`; группа `KAFKA_CONSUMER_GROUP` | [`contracts/asyncapi/invoice-service.yaml`](../../contracts/asyncapi/invoice-service.yaml) |
| Потребляет | `merchant.events`: `merchant.updated`; группа `<KAFKA_CONSUMER_GROUP>-merchants` | [`contracts/asyncapi/merchant-service.yaml`](../../contracts/asyncapi/merchant-service.yaml) |
| Потребляет | свои `webhook.retry.*`; группа `<KAFKA_CONSUMER_GROUP>-<топик>` | [`contracts/asyncapi/webhook-service.yaml`](../../contracts/asyncapi/webhook-service.yaml) |

Синхронные вызовы:
- endpoint мерчанта: `POST` на `webhookUrl` из проекции `merchant.events` — [`contracts/openapi/webhook.yaml`](../../contracts/openapi/webhook.yaml).
  Тело — CloudEvent из `invoice.events` без изменений, заголовки `X-Weigate-Timestamp`,
  `X-Weigate-Signature: v1=<hex HMAC-SHA256("<timestamp>.<тело>")>`, `X-Weigate-Secret-Version`;
- OpenBao: чтение KV v2 `<OPENBAO_KV_MOUNT>/data/webhook/<merchantId>?version=<webhookSecretVersion>`
  с токеном из `OPENBAO_TOKEN_FILE` (bao agent, роль `webhook-service`).

`event.id`: `<id исходного события>:<номер попытки>` у `webhook.retry`, `<id исходного события>:dlq`
у `webhook.dead_lettered`.

## Хранилища

| Хранилище | Что хранит | Схема |
|---|---|---|
| MongoDB | проекция `merchants`: `_id` = `merchantId`, `version`, `webhookUrl`, `secretVersion`; применяется только более новая `version` | [`internal/merchants`](internal/merchants/merchants.go) |
| MongoDB | журнал `deliveries`: `_id` = `<eventId>:<попытка>`, тип события, счёт, мерчант, результат (`delivered`, `failed`, `skipped`), код ответа, причина, версия секрета, время, длительность, `expireAt` (TTL `JOURNAL_TTL`) | [`internal/journal`](internal/journal/journal.go) |
| Valkey | `webhook-service:dedup:<event.id>` (`SET NX`, TTL `DEDUP_TTL`) | `storage.ValkeyDedup` |

## Конфигурация

| Переменная | Назначение | Значение по SRS |
|---|---|---|
| `HTTP_ADDR` | адрес HTTP | `:8080` |
| `ADMIN_ADDR` | адрес внутреннего API оператора (`/internal/dlq/replay`), отдельный порт | `:8081` |
| `LOG_LEVEL` | уровень логов | `info` |
| `KAFKA_BROKERS` | брокеры через запятую, обязательно | — |
| `KAFKA_CONSUMER_GROUP` | группа `invoice.events` и префикс остальных групп | `webhook-service` |
| `MONGODB_URI`, `MONGODB_USERNAME`, `MONGODB_PASSWORD_FILE`, `MONGODB_DATABASE`, `MONGODB_AUTH_SOURCE` | MongoDB (`storage.MongoConfigFromEnv`) | — |
| `VALKEY_ADDRS`, `VALKEY_USERNAME`, `VALKEY_PASSWORD_FILE` | Valkey (`storage.ValkeyConfigFromEnv`) | — |
| `VALKEY_TIMEOUT` | таймаут вызова Valkey | 100 мс |
| `DEDUP_TTL` | срок ключа дедупликации | 24 ч (4.1.7) |
| `OPENBAO_ADDR` | адрес OpenBao, `http` или `https`, обязательно | задаёт weigate-lib |
| `OPENBAO_TOKEN_FILE` | файл токена bao agent, обязательно | задаёт weigate-lib |
| `OPENBAO_KV_MOUNT` | mount KV v2 | `secret` |
| `DELIVERY_TIMEOUT` | таймаут одной попытки | 5 с |
| `ENDPOINT_BREAKER_FAILURE_THRESHOLD`, `ENDPOINT_BREAKER_OPEN_TIMEOUT`, `ENDPOINT_BREAKER_HALF_OPEN_MAX` | Circuit Breaker на endpoint мерчанта | 5, 30 с, 1 (4.1.9) |
| `DELIVERY_CONCURRENCY` | сколько счетов одного топика доставляются одновременно | 64 |
| `JOURNAL_TTL` | срок хранения журнала доставок | 30 сут (4.1.6) |

Задержки 1, 5 и 30 мин заданы именами топиков в контракте, поэтому в коде, а не в конфигурации.

Секреты читаются из файлов: `/var/run/secrets/weigate/mongodb/password`, `/var/run/secrets/weigate/valkey/password`,
`/var/run/secrets/weigate/openbao/token`.

## Команды

```sh
go build ./services/webhook-service/...
go test -race ./services/webhook-service/...
go test -race -tags integration ./services/webhook-service/...   # Testcontainers: Kafka, MongoDB, Valkey, OpenBao (dev)
```

Общие проверки — цели `make` из корневого AGENTS.md. Чарт — [`charts/webhook-service`](../../charts/webhook-service).

## Особые правила

- URL вебхука берётся только из настроек мерчанта (проекция `merchant.updated`), из события счёта — никогда.
  Без URL доставка не выполняется и пишется в журнал как `skipped`; неизвестный мерчант — неудачная попытка.
- Подпись и `X-Weigate-Timestamp` создаются заново на каждую попытку, секрет — версии `webhookSecretVersion`
  из проекции на момент попытки. Тело одинаково во всех попытках. Секрет и подпись в логи не попадают.
- Попытка неудачна при ответе не 2xx (редиректы не выполняются), таймауте, открытом Circuit Breaker,
  неизвестном мерчанте и недоступном секрете. Попытки 1–3 публикуются в `webhook.retry.<стадия>`
  с `nextAttemptAt`, после 4-й — `webhook.dead_lettered` в `webhook.dlq`. Потребитель retry-топика ждёт
  `nextAttemptAt` и только потом повторяет.
- Circuit Breaker — на мерчанта (`merchant:<merchantId>`, у мерчанта один URL). Недоступный секрет его
  не открывает.
- Записи одного счёта обрабатываются по порядку, разных счетов — параллельно, до `DELIVERY_CONCURRENCY`;
  у каждого retry-топика своя группа. Поэтому медленный endpoint и ожидание 30 мин не задерживают остальных.
- Дедупликация по `event.id` занимается один раз до доставки; при ошибке хранилища (MongoDB, Kafka) обработчик
  повторяется без новой проверки. При остановке ключ снимается. Без Valkey вебхуки доставляются без дедупликации,
  он не входит в readiness; OpenBao тоже не входит: без него доставки идут через retry-топики.
- При ответе OpenBao 403 токен перечитывается из файла и запрос повторяется с backoff до 2 мин (SRS INF-2.4).
- Метрики: `weigate_webhook_deliveries{result,event_type}`, `weigate_webhook_delivery_duration{result}`,
  `weigate_webhook_scheduled{topic}`, `weigate_webhook_paid_delay` — от `paidAt` (блок N-го подтверждения)
  до доставленного `invoice.paid` (NFR-04).

## Переотправка из DLQ

`POST /internal/dlq/replay` слушает `ADMIN_ADDR`, на gateway не публикуется. Оператор вызывает его через port-forward:

```sh
kubectl -n apps port-forward deploy/webhook-service 8081:8081
curl -X POST localhost:8081/internal/dlq/replay -H 'Content-Type: application/json' -d '{"merchantId":"<uuid>"}'
```

Тело — фильтры `invoiceId`, `merchantId` (пересечение) и `limit` (1–1000, по умолчанию 100); без фильтров берутся самые
старые записи. Сервис читает `webhook.dlq` целиком без группы потребителей и для каждой записи делает одну новую
попытку (подпись, `X-Weigate-Timestamp` и секрет текущие, тело и `event.id` прежние). Из топика Kafka запись удалить
нельзя, поэтому «ушла из DLQ» значит: в журнале есть доставленная переотправка `<event.id>:dlq`, и следующий вызов её
пропускает. Неудачная переотправка в журнал пишется как `failed`, запись остаётся в DLQ и не публикуется повторно,
в retry-топики не попадает. Ответ — `selected`, `delivered`, `failed`; `503` с `Retry-After` — Kafka или MongoDB недоступны.

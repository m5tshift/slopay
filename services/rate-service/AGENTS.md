# AGENTS.md — rate-service

## Назначение

Опрашивает источники курса, агрегирует ETH/USD и хранит последний курс ETH/USD и USDT/USD; отдаёт его
`invoice-service` при создании счёта. Требования — SRS 4.1.3, 4.1.7, 4.1.9, испытания 8.2а и 8.2д.

## API и топики

| Вид | Что | Контракт |
|---|---|---|
| HTTP | `GET /rates/{pair}` (`ETH-USD`, `USDT-USD`); `GET`, `PUT /test/fault` — только при `FAULT_INJECTION_ENABLED` | [`contracts/openapi/rate-service.yaml`](../../contracts/openapi/rate-service.yaml) |
| Публикует | `rate.ticks` (`rate.tick`, ключ `<source>:<pair>`), `rate.latest` (`rate.latest`, ключ — пара, compact) | [`contracts/asyncapi/rate-service.yaml`](../../contracts/asyncapi/rate-service.yaml) |
| Потребляет | `rate.latest` — без consumer group, с начала топика, в память реплики | [`contracts/asyncapi/rate-service.yaml`](../../contracts/asyncapi/rate-service.yaml) |

Синхронные вызовы: источники курса. В режиме `mock` — `rate-mock`
(`GET /sources/{source}/quotes/{pair}`, [`contracts/openapi/rate-mock.yaml`](../../contracts/openapi/rate-mock.yaml)):
Coinbase даёт ETH-USD и USDT-USD, OKX — ETH-USDT. В режиме `exchanges` — публичные API бирж без ключей:
Coinbase Exchange `GET /products/{pair}/ticker`, OKX `GET /api/v5/market/ticker?instId=ETH-USDT`.

## Хранилища

| Хранилище | Что хранит | Схема |
|---|---|---|
| Valkey | исходящий лимит к биржам: token bucket на источник, HASH `ratelimit:out:<source>` | поля `tokens`, `ts`; Lua-скрипт на часах Valkey ([`internal/limiter`](internal/limiter/limiter.go)) |
| Valkey | последний снимок пары: HASH `rate:latest:<pair>` | поля `status` (`ok`/`diverged`), `updatedAt`, `price`, `quotedAt`, `sources` ([`internal/store`](internal/store/store.go)) |
| память реплики | тот же снимок, восстановленный из `rate.latest` | [`internal/rate`](internal/rate/rate.go) |

Ответ строится по более новому (`updatedAt`) из снимков Valkey и памяти. Без Valkey реплика стартует
и отвечает из памяти, подключение повторяется в фоне.

## Конфигурация

| Переменная | Назначение | Значение по SRS |
|---|---|---|
| `HTTP_ADDR` | адрес HTTP | `:8080` |
| `LOG_LEVEL` | уровень логов | `info` |
| `KAFKA_BROKERS` | брокеры через запятую, обязательно | — |
| `VALKEY_ADDRS`, `VALKEY_USERNAME`, `VALKEY_PASSWORD_FILE` | Valkey (`storage.ValkeyConfigFromEnv`) | — |
| `VALKEY_TIMEOUT` | таймаут операции Valkey | 200 мс |
| `RATE_SOURCE_MODE` | источник курса: `mock` или `exchanges`; автоматического переключения нет | `mock` |
| `RATE_MOCK_URL` | адрес `rate-mock`, обязательно в режиме `mock` | — |
| `COINBASE_URL`, `OKX_URL` | базовые URL бирж в режиме `exchanges` | `https://api.exchange.coinbase.com`, `https://www.okx.com` |
| `SOURCE_LIMIT_COINBASE_REQUESTS`, `SOURCE_LIMIT_COINBASE_WINDOW` | исходящий лимит к Coinbase: запросов за окно (режим `exchanges`) | 10 за 1 с (публичный лимит биржи) |
| `SOURCE_LIMIT_OKX_REQUESTS`, `SOURCE_LIMIT_OKX_WINDOW` | то же для OKX | 20 за 2 с (публичный лимит биржи) |
| `SOURCE_TIMEOUT` | таймаут запроса к источнику | 2 с |
| `SOURCE_BREAKER_FAILURE_THRESHOLD`, `SOURCE_BREAKER_OPEN_TIMEOUT`, `SOURCE_BREAKER_HALF_OPEN_MAX` | Circuit Breaker, свой на каждый источник | 3, 30 с, 1 (4.1.9) |
| `POLL_INTERVAL` | период опроса источников лидером | 5 с (4.1.3) |
| `RATE_MAX_AGE` | возраст, после которого курс устарел (не больше 60 с) | 60 с (4.1.3) |
| `USDT_MAX_AGE` | сколько используется последний USDT/USD для пересчёта ETH-USDT | 1 ч (4.1.3) |
| `DIVERGENCE_MAX_BPS` | допустимое расхождение источников, б. п. | 100 (4.1.3) |
| `RETRY_AFTER` | `Retry-After` в ответе `503` | 5 с |
| `FAULT_INJECTION_ENABLED` | включает `/test/fault` | `false`, на стенде для 8.2а — `true` |
| `LEADER_LEASE_NAME`, `POD_NAME`, `POD_NAMESPACE` | выбор лидера через Lease; без имени Lease реплика всегда лидер | задаёт slopay-lib |

Секреты читаются из файлов: `/var/run/secrets/slopay/valkey/password`.

## Команды

```sh
go build ./services/rate-service/...
go test -race ./services/rate-service/...
go test -race -tags integration ./services/rate-service/...   # Testcontainers: Kafka, Valkey; rate-mock — httptest
go generate ./services/rate-service/...                       # internal/api/api.gen.go из контракта
```

Общие проверки — цели `make` из корневого AGENTS.md. Чарт — [`charts/rate-service`](../../charts/rate-service).

## Особые правила

- Источники опрашивает только держатель Lease (`pkg/leader`), раз в `POLL_INTERVAL`; на каждый источник —
  свой Circuit Breaker. `GET /rates/{pair}` обслуживают все реплики.
- ETH-USDT от OKX пересчитывается в ETH-USD через USDT/USD не старше `USDT_MAX_AGE`. Курс ETH/USD — среднее
  котировок при расхождении не больше `DIVERGENCE_MAX_BPS`, при одной котировке — она сама.
- При большем расхождении в Valkey пишется отметка `diverged`, и все реплики отвечают `503` с `reason: diverged`
  до следующей согласованной агрегации. В `rate.latest` публикуется только достоверный курс.
- Курс старше `RATE_MAX_AGE` — `503` с `reason: stale`; курса нет — `503` с `reason: unavailable`.
- Метрики: `slopay_rate_age_seconds{pair}` во всех репликах; у лидера `slopay_rate_diverged{pair}`,
  `slopay_rate_divergence_bps{pair}` (алерт `RateDivergence`) и `slopay_rate_source_requests_total`;
  состояние CB — `slopay_circuit_breaker_state`.
- `PUT /test/fault` с `{"enabled": true}` заставляет эту реплику отвечать на `GET /rates/{pair}` `503`
  (`fault_injected`); флаг живёт в памяти реплики. Без `FAULT_INJECTION_ENABLED` путь отвечает `404`.
- Исходящий лимит к биржам (SRS 4.1.7, 4.1.8) — token bucket на источник, только в режиме `exchanges`: ёмкость —
  число запросов, один токен возвращается каждые `окно/ёмкость`. Токен берётся до Circuit Breaker; запрос без
  токена не отправляется, считается `slopay_rate_source_requests_total{result="limited"}` и не открывает CB.
  Без Valkey (не подключён или ошибка вызова) лидер считает тот же бакет в памяти процесса.
- Геоблокировки не обходятся: клиент бирж не использует `HTTP(S)_PROXY` окружения, ответ `403`/`451` — обычная
  ошибка источника для его CB. При блокировке стенд переводят в режим `mock` конфигурацией (SRS, таблица рисков).
- У Coinbase время котировки — поле `time` тикера, у OKX — `ts` (мс); цена разбирается как десятичная строка,
  `float` не используется.

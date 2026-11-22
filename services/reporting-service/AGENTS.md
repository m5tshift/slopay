# AGENTS.md — reporting-service

## Назначение

Отдаёт мерчанту отчёты по оборотам и конверсии за период из витрин ClickHouse. Сервис только читает.
Требования — SRS 4.1.3, FR-11.

## API и топики

| Вид | Что | Контракт |
|---|---|---|
| HTTP | `GET /api/v1/reports/turnover?from&to`, `GET /api/v1/reports/conversion?from&to`, `/health/live`, `/health/ready`, `/metrics` | [`contracts/openapi/reporting-service.yaml`](../../contracts/openapi/reporting-service.yaml) |

Топиков нет: данные в ClickHouse попадают из `invoice.events` движком Kafka (SRS 4.1.5), это не часть сервиса.

Синхронные вызовы:
- ClickHouse, нативный протокол: `merchant_turnover_daily` и `merchant_conversion_daily` базы `weigate`;
- JWKS merchant-service (`JWKS_URL`): подпись JWT проверяется в сервисе (SRS 4.1.10), как в invoice-service.

## Хранилища

| Хранилище | Что хранит | Схема |
|---|---|---|
| ClickHouse | только чтение витрин; схема — `weigate-gitops`, `platform/clickhouse/full/ddl/05-reports.sql` (#69) | [`internal/store`](internal/store/store.go) |

## Конфигурация

| Переменная | Назначение | Значение по SRS |
|---|---|---|
| `HTTP_ADDR` | адрес HTTP | `:8080` |
| `LOG_LEVEL` | уровень логов | `info` |
| `CLICKHOUSE_ADDR`, `CLICKHOUSE_USERNAME`, `CLICKHOUSE_PASSWORD_FILE`, `CLICKHOUSE_DATABASE` | ClickHouse, все обязательны | `reporting`, `weigate` на стенде |
| `JWKS_URL` | JWKS merchant-service, обязательно | — |
| `JWT_ISSUER` | ожидаемый `iss` токена | `weigate-merchant-service` |
| `JWKS_TIMEOUT`, `JWKS_REFRESH`, `JWKS_MIN_INTERVAL` | запрос JWKS, обновление кэша, пауза между запросами | 2 с, 5 мин, 10 с |
| `REPORT_MAX_DAYS` | самый длинный период отчёта | 92 |
| `QUERY_TIMEOUT` | таймаут одного запроса к ClickHouse | 10 с |
| `RETRY_AFTER` | `Retry-After` ответа `503` | 5 с |

Секрет читается из файла `/var/run/secrets/weigate/clickhouse/password` (пароль пользователя `reporting`).

## Команды

```sh
go build ./services/reporting-service/...
go test -race ./services/reporting-service/...
go test -race -tags integration ./services/reporting-service/...   # Testcontainers: ClickHouse
```

Общие проверки — цели `make` из корневого AGENTS.md. Чарт — [`charts/reporting-service`](../../charts/reporting-service).

## Особые правила

- Мерчант — `sub` JWT (ES256, подпись проверяется по JWKS). Запрос к ClickHouse всегда с `merchant_id` из токена,
  чужие данные недоступны. Без токена — `401` до разбора параметров.
- Период — даты UTC, обе границы включены, не длиннее `REPORT_MAX_DAYS`, иначе `400`. Дни без счетов в отчёте
  отсутствуют. Конверсия считается по дню создания счёта, оборот — по дню первой оплаты (витрины).
- Недоступность ClickHouse — `503` с `Retry-After`; `/health/ready` зависит от ClickHouse.
- Пользователь `reporting` работает в профиле `readonly = 1`: он не может менять настройки запроса. Драйвер
  превращает дедлайн контекста в `max_execution_time`, поэтому в драйвер контекст передаётся без дедлайна
  (`store.noDeadline`), а таймаут держит сервис. Не передавать в драйвер `clickhouse.WithSettings`.
- Пароль перечитывается из файла при ошибке аутентификации (код 516) с backoff до 2 мин, без рестарта (SRS INF-2.4).
- `internal/jwks` и `internal/api/auth.go` повторяют код invoice-service: `internal` другого сервиса импортировать
  нельзя, а `pkg/` в этой задаче не менялся. При изменении алгоритма токена правятся оба места.

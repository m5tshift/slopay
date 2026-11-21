# merchant-mock

Приёмник вебхуков (SRS 4.1.3, 4.1.10, 8.2б): играет мерчанта для `webhook-service`. Принимает вебхуки по
[`contracts/openapi/webhook.yaml`](../../contracts/openapi/webhook.yaml), проверяет их так, как обязан проверять
мерчант, ведёт журнал и по команде отвечает ошибкой или с задержкой. Контракт компонента:
[`contracts/openapi/merchant-mock.yaml`](../../contracts/openapi/merchant-mock.yaml).

Один экземпляр обслуживает много виртуальных мерчантов: имя endpoint — последний сегмент URL вебхука,
`http://merchant-mock.apps.svc.cluster.local/webhook/<endpoint>`.

## Проверка вебхука

Порядок: задержка, внесённая ошибка, заголовки, версия секрета, метка времени, подпись, тело, дубликат, порядок.

| Исход (`outcome`) | Ответ | Когда |
|---|---|---|
| `injected_error` | статус команды (`500`) | на endpoint включена ошибка; подпись не проверяется |
| `malformed` | `400` | нет или неверны заголовки `X-Weigate-*`; тело не событие счёта |
| `unknown_secret_version` | `401` | версии нет среди текущей и предыдущей, либо предыдущая старше `PREVIOUS_SECRET_TTL` |
| `invalid_timestamp` | `401` | метка отличается от часов компонента больше чем на `TIMESTAMP_TOLERANCE` |
| `invalid_signature` | `401` | `v1=hex(HMAC-SHA256(секрет, "<метка>.<сырое тело>"))` не совпала (сравнение за постоянное время) |
| `duplicate` | `200` | такой `id` события уже принят |
| `stale` | `200` | `data.sequence` меньше принятого для этого `invoiceId` |
| `accepted` | `200` | вебхук принят |

Дубликат и устаревший вебхук подтверждаются `200`, чтобы отправитель прекратил ретраи; они только считаются
и попадают в журнал.

## Команды

| Запрос | Действие |
|---|---|
| `PUT /control/endpoints/{endpoint}/secret` `{"version": 2, "secret": "..."}` | задать секрет; версия выше текущей делает прежнюю «предыдущей» на `PREVIOUS_SECRET_TTL` (ротация) |
| `PUT /control/endpoints/{endpoint}/fault` `{"status": 500, "delayMs": 3000}` | отвечать ошибкой `5xx` и/или с задержкой; `{}` снимает |
| `GET /control/endpoints/{endpoint}` | версии секрета (сами секреты не возвращаются), неисправность, счётчики исходов |
| `GET /control/endpoints/{endpoint}/journal?limit=100` | последние записи журнала, старые первыми |
| `POST /control/reset` | забыть все endpoint |

Endpoint создаётся первой командой `secret` или `fault`; вебхук на неизвестный endpoint получает `404`.
Состояние хранится в памяти, рестарт его стирает. Аутентификации нет: компонент доступен только внутри mesh.
Секреты и подписи в логи и журнал не попадают.

## Метрики

`merchant_mock_webhooks_total{endpoint,outcome}` — счётчик по исходам; дубликаты — `outcome="duplicate"`, неверные
подписи — `outcome="invalid_signature"`. Счётчики не сбрасываются командой `reset`. RED-метрики и `/health/*` —
общие для компонентов.

## Конфигурация

| Переменная | Назначение | По умолчанию |
|---|---|---|
| `HTTP_ADDR` | адрес HTTP | `:8080` |
| `LOG_LEVEL` | уровень логов | `info` |
| `TIMESTAMP_TOLERANCE` | допуск метки времени | `5m` |
| `PREVIOUS_SECRET_TTL` | срок действия предыдущей версии секрета | `24h` |
| `JOURNAL_SIZE` | записей журнала на endpoint | `1000` |
| `SEEN_EVENTS` | запомненных `id` событий на endpoint (окно дедупликации) | `100000` |

## Сборка и тесты

```sh
docker build -f testkit/merchant-mock/Dockerfile -t weigate/merchant-mock .
go test -race ./testkit/merchant-mock/...
```

Тесты API сверяют каждый ответ с контрактом (kin-openapi).

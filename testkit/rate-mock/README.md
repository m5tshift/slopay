# rate-mock

Генератор курса (SRS 4.1.3, 8.2д): два независимых источника вместо Coinbase и OKX для `rate-service`
в режиме `mock`. Контракт: [`contracts/openapi/rate-mock.yaml`](../../contracts/openapi/rate-mock.yaml).

| Источник | Пары |
|---|---|
| `coinbase` | `ETH-USD`, `USDT-USD` |
| `okx` | `ETH-USDT` |

Цены — целые миллионные доли (шесть знаков), без `float`. Каждая цена каждый тик (`TICK_INTERVAL`) случайно
смещается не более чем на `WALK_STEP_BPS` и удерживается в полосе `WALK_BAND_BPS` вокруг центра. Центр
`ETH-USD` и `USDT-USD` задан конфигурацией, центр `ETH-USDT` — их отношение. В норме источники расходятся
меньше 1 %: полоса не больше 33 б. п. (три цены, 3 × 33 < 100).

## API

| Запрос | Действие |
|---|---|
| `GET /sources/{source}/quotes/{pair}` | котировка; `503`, пока источник отключён |
| `POST /control/sources/{source}/disable`, `.../enable` | отключить и вернуть источник |
| `PUT /control/divergence` `{"bps": 150}` | сместить `okx` относительно `coinbase`; больше 100 б. п. плюс полоса (от 150 при настройках по умолчанию) — расхождение больше 1 % |
| `POST /control/reset` | вернуть норму: оба источника включены, смещения нет |
| `GET /control` | текущее состояние |

Состояние хранится в памяти, рестарт возвращает норму. Аутентификации нет: компонент доступен только внутри mesh.

## Конфигурация

| Переменная | Назначение | По умолчанию |
|---|---|---|
| `HTTP_ADDR` | адрес HTTP | `:8080` |
| `LOG_LEVEL` | уровень логов | `info` |
| `BASE_PRICE_ETH_USD` | цена ETH в USD, вокруг которой идёт блуждание | `3700` |
| `BASE_PRICE_USDT_USD` | цена USDT в USD | `1` |
| `TICK_INTERVAL` | период шага | `1s` |
| `WALK_STEP_BPS` | наибольший шаг за тик, б. п. | `2` |
| `WALK_BAND_BPS` | полоса вокруг центра, б. п. (0–33) | `10` |

## Сборка и тесты

```sh
docker build -f testkit/rate-mock/Dockerfile -t slopay/rate-mock .
go test -race ./testkit/rate-mock/...
```

Тесты API сверяют каждый ответ с контрактом (kin-openapi).

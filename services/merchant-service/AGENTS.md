# AGENTS.md — merchant-service

## Назначение

Регистрирует мерчантов, хранит xpub, URL вебхука и допуск оплаты, выпускает API-ключи и обменивает их
на JWT, публикует состояние мерчанта в `merchant.events`. Требования — SRS 4.1.2, 4.1.3, 4.1.10, UC-06, UC-07.

## API и топики

| Вид | Что | Контракт |
|---|---|---|
| HTTP | `POST /api/v1/merchants`, `POST /api/v1/auth/token`, `GET /.well-known/jwks.json`, `/api/v1/merchants/me/*` | [`contracts/openapi/merchant-service.yaml`](../../contracts/openapi/merchant-service.yaml) |
| Публикует | `merchant.events` (`merchant.updated`), только через outbox | [`contracts/asyncapi/merchant-service.yaml`](../../contracts/asyncapi/merchant-service.yaml) |
| Потребляет | — | — |

Синхронные вызовы других сервисов и внешних систем: нет.

Каждый запрос сверяется с контрактом (`internal/api`, kin-openapi): операциям с `security` нужен JWT
этого сервиса (`401` с `WWW-Authenticate: Bearer`), затем проверяется запрос (`400`, значения полей
в ответ не попадают). Лимиты запросов (`429`) ставит Istio Ingress Gateway, сервис их не считает.

## Хранилища

| Хранилище | Что хранит | Схема |
|---|---|---|
| PostgreSQL | мерчанты, API-ключи (только SHA-256), outbox | [`migrations/`](migrations/), применяются при старте (goose) |
| OpenBao KV v2 | секреты подписи вебхуков, версия секрета — версия KV | `webhook/<merchantId>` |
| OpenBao Transit | подпись JWT (ключ `merchant-jwt`, ECDSA P-256), приватный ключ не покидает OpenBao | `transit/sign/merchant-jwt`, `transit/keys/merchant-jwt` |

## Конфигурация

| Переменная | Назначение | Значение по SRS |
|---|---|---|
| `HTTP_ADDR` | адрес HTTP | `:8080` |
| `LOG_LEVEL` | уровень логов | `info` |
| `KAFKA_BROKERS` | брокеры через запятую, обязательно | — |
| `POSTGRES_URL`, `POSTGRES_PASSWORD_FILE`, `POSTGRES_MAX_CONNS` | PostgreSQL (`storage.PostgresConfigFromEnv`) | — |
| `JWT_SIGNING_KEY_FILE` | приватный ключ ECDSA P-256 в PEM (PKCS#8 или SEC1): режим минимального стенда; обязателен, если не задан `JWT_TRANSIT_KEY` | — |
| `JWT_TRANSIT_KEY` | имя ключа Transit (`merchant-jwt`): JWT подписывает OpenBao; исключает `JWT_SIGNING_KEY_FILE`, нужен `OPENBAO_ADDR` | не задан |
| `OPENBAO_TRANSIT_MOUNT`, `TRANSIT_KEYS_REFRESH` | монтирование Transit; как долго кэшируются публичные ключи Transit | `transit`, 1 мин |
| `JWT_ISSUER` | `iss` токена | `weigate-merchant-service` |
| `JWT_TTL` | срок жизни JWT | 15 мин (4.1.10) |
| `JWKS_MAX_AGE` | `Cache-Control: max-age` у JWKS | 5 мин |
| `API_KEY_ROTATION_GRACE` | сколько работает старый ключ после выпуска нового | 24 ч (4.1.10) |
| `API_KEY_MAX_ACTIVE` | активных ключей у мерчанта | 2 (4.1.10) |
| `OPENBAO_ADDR`, `OPENBAO_TOKEN_FILE`, `OPENBAO_KV_MOUNT` | OpenBao для секретов вебхуков; файл токена читается при каждом запросе | не задан, `secret` |
| `TOLERANCE_DEFAULT_BPS`, `TOLERANCE_MAX_BPS` | допуск оплаты по умолчанию и максимум, б. п. | 100 и 500 (4.1.4) |

Секреты читаются из файлов: `/var/run/secrets/weigate/postgres/password`, `/var/run/secrets/weigate/openbao/token`,
`/var/run/secrets/weigate/jwt-signing/private.pem`. Файл ключа подписи перечитывается: после ротации
новые токены подписываются новым ключом, прежний публичный ключ остаётся в JWKS на время `JWT_TTL`.
В режиме Transit JWKS строится из всех доступных версий публичного ключа Transit (текущая первой);
`kid` — отпечаток ключа (RFC 7638). Новая версия ключа подхватывается при первой подписи ею или через
`TRANSIT_KEYS_REFRESH`; лишние версии убирает оператор (`min_available_version` ключа).

## Команды

```sh
go build ./services/merchant-service/...
go test -race ./services/merchant-service/...
go test -race -tags integration ./services/merchant-service/...   # Testcontainers: PostgreSQL, Kafka
go generate ./services/merchant-service/...                       # internal/api/api.gen.go из контракта
```

Общие проверки — цели `make` из корневого AGENTS.md. Чарт — [`charts/merchant-service`](../../charts/merchant-service).

## Особые правила

- Подпись JWT скрыта за интерфейсом `jwt.Signer`: `FileSigner` (ключ из смонтированного файла, минимальный
  стенд) или `TransitSigner` (OpenBao Transit, `JWT_TRANSIT_KEY`). Токен OpenBao читается из файла при каждом
  запросе; политика сервиса — подпись `merchant-jwt`, чтение его публичных ключей и запись `webhook/*`.
- Секрет вебхука генерирует сервис (`whsec_…`, 256 бит) и пишет в OpenBao KV v2 по пути
  `<mount>/data/webhook/<merchantId>` (поле `secret`); версия секрета — версия KV из ответа записи, она же
  `webhookSecretVersion` в `merchant.updated`. Первая версия пишется при регистрации (мерчант её не видит,
  секрет он получает при первой ротации); без OpenBao регистрация остаётся с версией 1 без секрета.
  Ротация (`POST /api/v1/merchants/me/webhook-secret/rotate`): под блокировкой строки мерчанта сервис пишет
  новый секрет, затем ставит `webhookSecretVersion` равным версии KV, событие уходит через outbox. Если
  транзакция упала после записи, версия KV остаётся неопубликованной, следующая ротация даст большую
  (пропуск допустим). Предыдущие версии читаемы (24 ч). Сбой OpenBao — `500`, версия не меняется. Секрет
  показывается один раз, в событие, логи и ошибки не попадает. Без `OPENBAO_ADDR` эндпоинт отвечает `501`.
- API-ключ показывается один раз (регистрация, выпуск); в базе только SHA-256. Ключи, JWT и присланные
  xpub/xprv в логи и ответы об ошибках не попадают.
- Принимается только расширенный публичный ключ (`xpub`/`tpub`); приватный ключ отклоняется с `400`
  и нигде не сохраняется.
- Изменения мерчанта сериализуются блокировкой строки (`SELECT … FOR UPDATE`). `version` растёт при каждом
  изменении публикуемого состояния; запрос без изменений версию не меняет и событие не публикует.
- `merchant.updated` публикуется только у мерчанта с xpub: схема события требует xpub, а без него
  счёт выставить нельзя. Регистрация события не публикует.
- Ротация ключей: новый ключ ставит старому срок `API_KEY_ROTATION_GRACE`; при лимите активных ключей
  выпуск — `409`. Экстренный отзыв действует сразу; последний активный ключ отозвать нельзя (`409`).
  Если отозван новый ключ, оставшийся снова работает бессрочно.

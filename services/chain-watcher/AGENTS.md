# AGENTS.md — chain-watcher

## Назначение

Читает блоки EVM-цепочки (Anvil, опционально Sepolia) с курсора, публикует новые блоки, обнаруживает
реорганизации и публикует переводы нативного ETH на отслеживаемые адреса. Адреса берёт из проекции
`invoice.events`. Требования — SRS 4.1.3, 4.1.6, 4.1.7, 4.1.9, FR-04, FR-06, NFR-04, NFR-05, NFR-07,
испытания 8.2в и 8.5. Переводы токенов (ERC-20) — отдельная задача.

## API и топики

| Вид | Что | Контракт |
|---|---|---|
| HTTP | только `/health/live`, `/health/ready`, `/metrics` | — |
| Публикует | `chain.blocks`: `chain.new_head` (`number`, `hash`, `parentHash`, `timestamp`), `chain.reorg` (`fromBlock`); ключ — `chainId` | [`contracts/asyncapi/chain-watcher.yaml`](../../contracts/asyncapi/chain-watcher.yaml) |
| Публикует | `chain.transfers`: `chain.transfer` — перевод ETH (`blockNumber`, `blockHash`, `txHash`, `from`, `to`, `amount`, `logIndex` = null); ключ — адрес получателя | там же |
| Потребляет | `invoice.events`: `invoice.created` (адрес и окно отслеживания), `invoice.paid` (продление окна); группа `KAFKA_CONSUMER_GROUP` | [`contracts/asyncapi/invoice-service.yaml`](../../contracts/asyncapi/invoice-service.yaml) |

Синхронные вызовы: нода EVM по JSON-RPC (`eth_chainId`, `eth_blockNumber`, `eth_getBlockByNumber`,
`eth_getTransactionReceipt`) через Circuit Breaker.

`event.id` детерминирован: `<chainId>:<hash>` у `chain.new_head`, `<chainId>:reorg:<fromBlock>:<hash первого
отменённого блока>` у `chain.reorg`, `<blockHash>:<txHash>:` у `chain.transfer`. Повтор шага после сбоя публикует то же событие, потребители его
дедуплицируют.

## Хранилища

| Хранилище | Что хранит | Схема |
|---|---|---|
| MongoDB | один документ на цепочку в `chain_state`: `_id` = `chainId`, `cursor` — номер последнего обработанного блока, `window` — до `WINDOW_SIZE` последних заголовков (`number`, `hash`, `parentHash`, `timestamp`), `version`, `updatedAt` | [`internal/store`](internal/store/store.go) |
| MongoDB | проекция отслеживаемых адресов `tracked_addresses`: `_id` = адрес, `invoiceId`, `createdAt` (время `invoice.created`), `expiresAt`, `trackUntil` — конец окна, `rescanned` — обработанные блоки уже просмотрены; `projection_state` — отметка о построении проекции | [`internal/projection`](internal/projection/store.go) |
| Valkey | `SET` `chain-watcher:<chainId>:tracked` — кэш адресов с `rescanned` = true и служебный элемент `token:<uuid>` полной загрузки; проверка — `SISMEMBER` | [`internal/tracked`](internal/tracked/cache.go) |

Курсор и окно меняются одной записью документа. Запись условная по `version`: если состояние успела изменить
другая реплика, запись отклоняется, и лидер перечитывает состояние.

## Конфигурация

| Переменная | Назначение | Значение по SRS |
|---|---|---|
| `HTTP_ADDR` | адрес HTTP | `:8080` |
| `LOG_LEVEL` | уровень логов | `info` |
| `KAFKA_BROKERS` | брокеры через запятую, обязательно | — |
| `MONGODB_URI`, `MONGODB_USERNAME`, `MONGODB_PASSWORD_FILE`, `MONGODB_DATABASE`, `MONGODB_AUTH_SOURCE` | MongoDB (`storage.MongoConfigFromEnv`) | — |
| `NODE_URL` | JSON-RPC ноды, `http` или `https`, обязательно | — |
| `NODE_TIMEOUT` | таймаут вызова ноды | 5 с |
| `NODE_BREAKER_FAILURE_THRESHOLD`, `NODE_BREAKER_OPEN_TIMEOUT`, `NODE_BREAKER_HALF_OPEN_MAX` | Circuit Breaker к ноде | 5, 15 с, 1 |
| `BACKOFF_INITIAL`, `BACKOFF_MAX` | пауза после ошибки, удваивается до максимума | 500 мс, 30 с |
| `POLL_INTERVAL` | пауза, когда курсор дошёл до головы ноды | 1 с |
| `CHAIN_ID` | ожидаемая цепочка, обязательно; нода другой цепочки не читается | 31337 (Anvil) |
| `CHAIN_START_BLOCK` | первый блок, если курсора ещё нет; без значения — голова ноды на момент старта | — |
| `WINDOW_SIZE` | число хранимых заголовков | 64 (4.1.6) |
| `REORG_MAX_DEPTH` | наибольшая обрабатываемая глубина реорганизации, меньше `WINDOW_SIZE` | 6 (4.1.9) |
| `KAFKA_CONSUMER_GROUP` | группа потребителя `invoice.events` | `chain-watcher` |
| `TRACKING_WINDOW` | окно отслеживания адреса после `expiresAt` или `paidAt` | 24 ч (4.1.4) |
| `PROJECTION_REBUILD_WINDOW` | сколько `invoice.events` перечитывается при потере проекции; глубина повторного просмотра блоков | 26 ч (4.1.7) |
| `RESCAN_CLOCK_SKEW` | запас к времени счёта при повторном просмотре блоков | 30 с |
| `VALKEY_ADDRS`, `VALKEY_USERNAME`, `VALKEY_PASSWORD_FILE` | Valkey (`storage.ValkeyConfigFromEnv`), адреса обязательны | — |
| `VALKEY_TIMEOUT` | таймаут запроса к Valkey; дольше — проверка в MongoDB | 100 мс |
| `LEADER_LEASE_NAME`, `POD_NAME`, `POD_NAMESPACE` | выбор лидера через Lease; без имени Lease реплика всегда лидер | задаёт weigate-lib |

Секреты читаются из файлов: `/var/run/secrets/weigate/mongodb/password`, `/var/run/secrets/weigate/valkey/password`.

## Команды

```sh
go build ./services/chain-watcher/...
go test -race ./services/chain-watcher/...
go test -race -tags integration ./services/chain-watcher/...   # Testcontainers: Anvil, Kafka, MongoDB, Valkey
```

Общие проверки — цели `make` из корневого AGENTS.md. Чарт — [`charts/chain-watcher`](../../charts/chain-watcher).

## Особые правила

- Блоки читает только держатель Lease (`pkg/leader`); при получении лидерства состояние читается из MongoDB.
- Блоки обрабатываются строго по одному: `cursor + 1`, затем следующий. Каждый шаг сначала публикует событие,
  затем сохраняет состояние, поэтому после сбоя или рестарта чтение продолжается с курсора без пропусков.
- `parentHash` блока `cursor + 1` сверяется с хэшем курсора. При расхождении сервис ищет общий предок
  в окне не глубже `REORG_MAX_DEPTH`, публикует `chain.reorg` с `fromBlock` = предок + 1, отбрасывает
  заголовки после предка и читает новую ветку с `fromBlock`.
- Реорганизация глубже `REORG_MAX_DEPTH` или ниже начала окна останавливает чтение до потери лидерства или
  рестарта: `weigate_chain_reorg_too_deep` = 1 (алерт `ChainReorgTooDeep`), в лог — ошибка с
  `alert=ChainReorgTooDeep`. Состояние в MongoDB не меняется.
- Переводы блока публикуются в `chain.transfers` до `chain.new_head` этого блока. Получатели блока сверяются
  с кэшем в Valkey; перевод публикуется, если квитанция есть, `status` = 1 и она из того же блока.
- Новый адрес (`rescanned` = false) перед следующим блоком ищется в уже обработанных блоках от `createdAt` −
  `RESCAN_CLOCK_SKEW`, но не глубже `PROJECTION_REBUILD_WINDOW`. Найденные переводы публикуются до следующего
  `chain.new_head`, затем адрес отмечается `rescanned` и попадает в кэш. Если блок за это время заменён
  реорганизацией, повторный просмотр откладывается до её обработки.
- Адрес удаляется из проекции и кэша, когда время головы ноды прошло `trackUntil`; `invoice.paid`
  продлевает окно.
- Valkey — только кэш. Пока он недоступен или не загружен, адреса проверяются в MongoDB, кэш подключается
  в фоне. Процесс при старте загружает кэш заново: `DEL`, адреса пачками, в конце свой элемент `token:<uuid>`.
  Без своего элемента кэш считается неполным (его перезагрузил другой процесс) и загружается снова.
- Если `projection_state` пуст (первый старт или потеря данных), до потребления `invoice.events` проекция
  строится заново чтением топика за `PROJECTION_REBUILD_WINDOW`. Потеря проекции обнаруживается только при
  старте.
- Внутренние переводы ETH из контрактов не видны: ищутся только транзакции с `value` > 0.
- Ошибка ноды, Kafka или MongoDB приостанавливает чтение с backoff; блок, который не удалось опубликовать или
  сохранить, читается заново.
- Метрики: у лидера `weigate_chain_head_block`, `weigate_chain_cursor_block`, `weigate_chain_reorg_too_deep`
  (атрибут `chain_id`); `weigate_chain_blocks_processed`, `weigate_chain_reorgs{depth}`,
  `weigate_chain_node_requests{method,result}` (`ok`, `error`, `rejected` при открытом CB),
  `weigate_chain_node_request_duration`, `weigate_chain_transfers_published{mode}` (`live`, `rescan`),
  `weigate_chain_transfer_delay` (от времени блока до публикации перевода, NFR-04 p95 ≤ 3 с),
  `weigate_chain_rescan_blocks`, `weigate_chain_address_lookups{source}` (`cache`, `mongodb`); состояние CB — `weigate_circuit_breaker_state`.
- Интеграционные тесты делают реорганизацию через `evm_snapshot` и `evm_revert` и останавливают контейнер
  Anvil; Anvil запущен с `--state`, поэтому после рестарта цепочка и хэши блоков те же.

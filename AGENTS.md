# AGENTS.md — SLOPay

Шлюз криптоплатежей: мерчант выставляет счёт в USD, покупатель платит в ETH, мерчант получает подписанный
вебхук после N подтверждений. Только тестовые сети (Anvil, опционально Sepolia).

**Источник правды — [`docs/SRS.md`](docs/SRS.md).** Ссылки вида «SRS 4.1.4» ведут на его разделы.
Если задача, код или контракт расходятся с SRS, остановись и опиши расхождение в issue или PR.
SRS самостоятельно не меняй.

## Структура

```
docs/
  SRS.md                 требования
  adr/NNNN-<slug>.md     архитектурные решения
  architecture/          модель C4 (Structurizr DSL), sequence-диаграммы, ER-схема, схемы MongoDB
  runbooks/              развёртывание с нуля, реакция на алерты
contracts/
  openapi/<service>.yaml       HTTP API (OpenAPI 3.0.3)
  asyncapi/<service>.yaml      топики Kafka (AsyncAPI 3.0), он же каталог событий
  schemas/<event>.json         JSON Schema событий
services/<service>/      микросервисы: cmd/, internal/, migrations/, AGENTS.md
pkg/                     общий код: деньги, CloudEvents, outbox, телеметрия
testkit/
  merchant-mock/, rate-mock/, payer-sim/   тестовые компоненты на Go, разворачиваются в кластере
  stand-setup/           подготовка стенда на Go: регистрация мерчантов, деплой тестового USDT
  erc20/                 контракт тестового USDT (Solidity, Foundry)
  locust/                профили нагрузки на Python; запускается с рабочей машины, образа нет
tools/holmes-bridge/     приёмник вебхуков Alertmanager для HolmesGPT (Go, SRS 4.5.5)
charts/
  slopay-lib/            library chart
  <component>/           чарт сервиса, тестового компонента или holmes-bridge поверх slopay-lib
infra/
  hyperv/                PowerShell-скрипты VM
  talos/                 патчи конфигурации (generated/ — вне git)
  terraform/             bootstrap-слой (SRS INF-2.1)
  ansible/roles/strimzi/ генерация оператора и CR Kafka в slopay-gitops (SRS INF-2.3)
  ansible/roles/lb/      HAProxy + Keepalived на VM lb-1, lb-2 (SRS 4.1.8)
  karpenter-kwok/        сборка Karpenter с провайдером KWOK (SRS INF-1.2)
tests/acceptance/        приёмочные испытания (SRS раздел 8)
  results/<id>.json      результаты прогонов
  protocol.md            протокол испытаний: отчёт Locust, результат сверки, скриншоты в screenshots/
.github/workflows/       CI
```

Манифесты и values для кластера живут в отдельном репозитории `slopay-gitops`, его правила — в его `AGENTS.md`.

## Стек

| Область | Выбор |
|---|---|
| Язык | Go, один модуль `github.com/m5tshift/slopay` на весь репозиторий, версия — в `go.mod` |
| HTTP | `net/http` (маршрутизация стандартной библиотеки), код из OpenAPI 3.0.3 — `oapi-codegen` |
| Kafka | `franz-go` |
| EVM | `go-ethereum` |
| PostgreSQL | `pgx`, миграции — `goose` в `services/<service>/migrations/` |
| MongoDB | официальный драйвер `mongo-go-driver` v2 |
| Valkey | `valkey-go` |
| Логи, метрики, трейсы | `log/slog` (JSON, с `trace_id`), OpenTelemetry SDK |
| Тесты | `testing`, `pgregory.net/rapid` (property-based), `testcontainers-go` |
| Линтер | `golangci-lint` |
| Конфигурация | переменные окружения, секреты — из файлов, смонтированных External Secrets |

Новую зависимость добавляй только с обоснованием в PR. Образы и чарты Bitnami запрещены.

Код из `contracts/` генерируется через `go generate` и коммитится. CI проверяет, что он актуален.

## Команды

```sh
make build              # go build ./...
make generate-check     # go generate ./... не меняет дерево: сгенерированный код совпадает с контрактами
make lint               # golangci-lint run
make test               # go test -race ./...: unit и property-based
make test-integration   # go test -race -tags integration ./...: Testcontainers, нужен Docker
make helm-lint          # helm lint и helm template для charts/*
make tf-validate        # terraform validate в infra/terraform
make ansible-lint       # ansible-lint infra/ansible
make precommit          # gitleaks и базовые проверки; один раз: pre-commit install
```

Цели для каталогов, которых ещё нет, пропускаются без ошибки. Те же проверки идут в CI
(`.github/workflows/pr.yml`).

## Сборка образов

Модуль Go один, а образ у каждого компонента свой: в него попадает только бинарник этого компонента.

- `Dockerfile` лежит в каталоге компонента: `services/<service>/`, `testkit/<name>/`, `tools/holmes-bridge/`.
  Контекст сборки — корень репозитория. `.dockerignore` в корне исключает всё, кроме `go.mod`, `go.sum`,
  `pkg/`, `services/`, `testkit/`, `tools/` и схем событий (`contracts/schemas.go`, `contracts/schemas/`):
  их встраивает `pkg/kafka`, поэтому образ сервиса с Kafka копирует их тоже.
- Сборка двухэтапная: статический бинарник (`CGO_ENABLED=0`, `-trimpath`) на образе Go той версии,
  что в `go.mod`, затем финальный образ distroless static, пользователь `nonroot`. Shell и пакетного
  менеджера в финальном образе нет.
- Базовые образы закреплены по digest и скачиваются через Zot.
- Имя образа — `slopay/<component>`, тег — короткий SHA коммита.
- Образа нет у `testkit/locust/`, `testkit/stand-setup/` и `testkit/erc20/`: они запускаются с рабочей машины.

```dockerfile
FROM golang:<версия из go.mod>@sha256:<digest> AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY pkg/ pkg/
COPY services/invoice-service/ services/invoice-service/
RUN CGO_ENABLED=0 go build -trimpath -o /app ./services/invoice-service/cmd/invoice-service

FROM gcr.io/distroless/static:nonroot@sha256:<digest>
COPY --from=build /app /app
ENTRYPOINT ["/app"]
```

CI пересобирает только то, что изменилось:

| Изменён путь | Что пересобирается |
|---|---|
| `services/<service>/**`, `testkit/<name>/**`, `tools/<name>/**` | только этот компонент |
| `pkg/**`, `go.mod`, `go.sum` | все компоненты на Go |
| `contracts/**` | компоненты, у которых код сгенерирован из изменённого контракта |
| `charts/<component>/**`, `charts/slopay-lib/**` | чарт компонента или все чарты |

Чарты публикуются в Zot как OCI-артефакты (SRS CI-5.3). Новая версия чарта выходит, когда в PR меняется
`version` в `Chart.yaml`. После сборки CI обновляет в `slopay-gitops` тег образа, а при выходе новой версии
чарта — и её.

## Порядок работы

1. **Одна задача — один issue** и один компонент или один инфраструктурный компонент. В issue: ссылки на SRS,
   критерии приёмки (FR, NFR), список обязательных тестов.
2. **Сначала контракт.** Изменение API или события начинается с `contracts/`, код пишется под утверждённый
   контракт. Ломающее изменение контракта — новая версия, а не правка существующей.
3. **Чужое не трогай.** Код другого сервиса, `pkg/` и инфраструктура меняются только в своей задаче.
4. **Ветка** `<type>/<issue>-<slug>`, коммиты по Conventional Commits: `feat(invoice-service): …`.
5. **PR** ссылается на issue (`Closes #<N>`). Без зелёного CI merge невозможен.
6. **Порядок реализации** — SRS раздел 7, п. 8: сначала вертикальный срез до `PAID`.

Язык: код, комментарии и сообщения коммитов — английский; документация — русский.

## Обязательно для каждого сервиса

- `/health/live`, `/health/ready`, `/metrics`; метрики RED, трейсы OTel (SRS 5, NFR-11).
- Логи — JSON через `slog` с `trace_id`. Секреты, API-ключи, JWT и подписи в логи не попадают (SRS 4.5.5).
- Трейс-контекст передаётся в заголовках Kafka-сообщений и читается из них (SRS 4.5.1).
- Секреты читаются из смонтированных файлов. При ошибке аутентификации в хранилище сервис перечитывает файл
  и повторяет подключение с backoff до 2 мин, без рестарта (SRS INF-2.4).
- Числа из SRS — конфигурация, а не константы в коде: N подтверждений, TTL, допуск, лимиты, комиссия.

## Правила предметной области

- **Деньги — только целые числа.** Суммы в wei и минимальных единицах токена — `*big.Int` в коде,
  `NUMERIC(78,0)` в PostgreSQL. USD — целые центы `int64`. `float` для денег запрещён.
- **Округление** (SRS 4.1.3): USD → wei вверх, комиссия 0,5 % вниз.
- **Ключи никогда не приватные.** Система хранит только xpub; приватных ключей и мнемоник мерчантов
  в коде, тестах и логах нет. Тестовая мнемоника Anvil — только в секрете стенда.
- **API-ключи мерчантов хранятся в виде хэшей** (SRS 4.1.10).
- **Сроки — по времени цепочки** (SRS 4.1.4): TTL, ожидание после отката, окно отслеживания считаются
  по `timestamp` обработанных блоков, а не по часам сервиса.
- **Автомат статусов счёта** — строго по SRS 4.1.4. Любой новый переход сначала появляется в SRS.
- **Идемпотентность везде** (SRS 4.1.5, 4.1.6): события дедуплицируются по `event.id`, переводы —
  по `blockHash + txHash + logIndex`. Создание счёта защищено уникальным индексом
  `(merchant_id, idempotency_key)`.
- **Transactional outbox** (SRS 4.1.5): `invoice-service`, `merchant-service` и `ledger-service` публикуют
  события только через outbox, прямая запись в Kafka из обработчика запроса запрещена. Остальные сервисы
  не хранят состояние в PostgreSQL и публикуют в Kafka напрямую, идемпотентно.
- **События** — CloudEvents (JSON) по схемам из `contracts/schemas/`, каждое событие счёта несёт `sequence`.

## Тесты

Готовность определяют тесты, а не описание в PR.

- **unit** — для каждой ветви логики;
- **property-based** (`rapid`) — для автомата счёта: любая последовательность событий, включая повторы,
  перестановки и реорганизации, даёт корректный статус;
- **контрактные** — реализация соответствует `contracts/`;
- **интеграционные** — Testcontainers: PostgreSQL, Kafka, Valkey, MongoDB, Anvil;
- `go test -race` для всего, что запускается конкурентно.

Все проверки PR идут на раннерах GitHub (SRS 4.1.11).

## Безопасность: репозитории публичные

Никогда не коммить (SRS 4.1.11): значения секретов, `kubeconfig`, `talosconfig`, машинные конфигурации
Talos (`controlplane.yaml`, `worker.yaml`, `secrets.yaml`), `.env`, `terraform.tfstate`, `*.tfvars`,
SSH-ключи (в том числе deploy key), вывод `bao operator init`, ключи API, токены. В репозитории — только
шаблоны (`*.example`) и ссылки на секреты OpenBao.

- Перед коммитом проверяй `git diff --staged`; gitleaks в pre-commit и CI.
- Workflow GitHub Actions: явные минимальные `permissions:`, сторонние actions закреплены по SHA коммита,
  `pull_request_target` не используется.
- На self-hosted раннерах идут только сборка BuildKit (rootless), push в Zot и обновление `slopay-gitops`,
  и только для push в `main`. Проверки PR — только на раннерах GitHub.
- Self-hosted раннеры эфемерные, работают в неймспейсе `ci` без прав на кластер, сеть ограничена GitHub,
  Zot, `proxy.golang.org` и `sum.golang.org`.
- `slopay-gitops` обновляется deploy key из OpenBao с правом записи только в этот репозиторий,
  а не через `GITHUB_TOKEN`.

## Кластер

- Изменения выше bootstrap-слоя — только через Git и ArgoCD. `kubectl apply`, `helm install` и правка
  ресурсов вручную запрещены.
- Bootstrap-слой (VM Hyper-V, Talos, Terraform, init и unseal OpenBao, VM `lb-1` и `lb-2`) агент только
  описывает кодом и runbook. Применяет его пользователь.
- У агента kubeconfig только на чтение в неймспейсе `apps`: логи, события, describe. `talosconfig`
  и админский kubeconfig агенту не нужны.
- Во время окна заморозки (SRS 6.3) изменения идут только в ветки, без выкатки.

## Приёмочные испытания

Результат каждого прогона — файл `tests/acceptance/results/<id>.json`, один на испытание, имя файла
совпадает с `id`:

```json
{"id": "8.1", "status": "passed", "date": "28.11.2026", "commit": "abc1234",
 "summary": "Профиль выдержан, все счета дошли до финальных статусов",
 "metrics": {"rps": 231.4, "p95_create_ms": 182, "p95_status_ms": 41, "error_rate_5xx": 0.0012,
             "detect_p95_s": 1.4, "webhook_p95_s": 2.2, "rate_age_max_s": 31,
             "missed_transfers": 0, "duplicates": 0, "reconciliation_mismatches": 0}}
```

- `id`: `8.1`, `8.2а`–`8.2д`, `8.3`–`8.10` (SRS раздел 8) или `deploy` (развёртывание с нуля, NFR-12);
- `status`: `passed` или `failed` по критерию приёмки из SRS. `passed` допустим, только если есть все ключи
  испытания из таблицы ниже и каждый укладывается в порог SRS;
- `date` — `ДД.ММ.ГГГГ`, `commit` — SHA (от 7 знаков), на котором шёл прогон;
- `summary` — вывод без чисел: числа только в `metrics`;
- `metrics` — только измеренные числа, без округления «на глаз», и только ключи из таблицы ниже. Доли — от 0
  до 1. У испытаний без ключей (8.2г, 8.3, 8.5, 8.6, 8.8, 8.10) — `{}`.

Все поля обязательны.

| Испытание | Ключи `metrics` |
|---|---|
| 8.1 | `rps`, `p95_create_ms`, `p95_status_ms`, `error_rate_5xx` (доля, не проценты), `detect_p95_s`, `webhook_p95_s`, `rate_age_max_s`, `missed_transfers`, `duplicates`, `reconciliation_mismatches` |
| 8.2а | `cb_seconds`, `error_rate_5xx` |
| 8.2б, 8.2д | `cb_seconds` |
| 8.2в | `cb_seconds`, `missed_transfers` |
| 8.4 | `vip_failover_s` |
| 8.7 | `missed_transfers`, `duplicates` |
| 8.9 | `analysis_delay_s` |
| `deploy` | `minutes` |

Испытание из нескольких прогонов (8.5 — глубины 3 и 6; 8.9 — во время 8.2а и 8.2г) записывается одним
файлом: `passed` — только если пройдены все прогоны, в `metrics` — худшее значение.

Результат не подгоняется: проваленное испытание фиксируется как `failed`.

## Документация

- **ADR** — `docs/adr/NNNN-<slug>.md`: контекст, решение, рассмотренные варианты, последствия, статус.
  Старые ADR не переписываются: изменённое решение — новый ADR со ссылкой на прежний.
- **C4** — `docs/architecture/`, Structurizr DSL.
- **Runbook** — `docs/runbooks/`, по одному на каждый алерт из SRS 4.5.4, плюс развёртывание с нуля
  и unseal OpenBao.

## AGENTS.md сервиса

В каждом `services/<service>/AGENTS.md`: назначение и ссылка на раздел SRS, API и топики (со ссылками
на `contracts/`), хранилища, команды сборки и тестов, особые правила сервиса.

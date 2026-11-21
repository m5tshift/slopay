# weigate-lib

Library chart компонентов Weigate (SRS CI-5.3). Чарт сервиса, тестового компонента или `holmes-bridge`
подключает библиотеку зависимостью и рендерит все ресурсы одним шаблоном. Образов и чартов Bitnami нет.

## Подключение

`charts/<component>/Chart.yaml`:

```yaml
apiVersion: v2
name: invoice-service
type: application
version: 0.1.0
dependencies:
  - name: weigate-lib
    version: 0.1.0
    repository: file://../weigate-lib
```

`charts/<component>/templates/all.yaml`:

```yaml
{{ include "weigate-lib.all" . }}
```

Рабочий пример — [`example/`](example/): `values.yaml` — минимальный компонент, `values-all.yaml` — все опции.
`make helm-lint` запускает [`example/test.sh`](example/test.sh): `helm lint` и `helm template` примера
с обоими файлами, проверку набора ресурсов и отказ рендера при неверных values.

Значения по умолчанию — в [`templates/_defaults.tpl`](templates/_defaults.tpl): `values.yaml` библиотеки
потребителю не передаётся, поэтому умолчания сливаются с values потребителя в шаблоне.

## Ресурсы

| Ресурс | Когда | Values |
|---|---|---|
| Deployment | всегда | `image`, `replicaCount`, `resources`, `probes`, `command`, `args`, `env`, `extraEnv` |
| Service | всегда | `service.port` (8080) |
| ServiceAccount | `serviceAccount.create` (по умолчанию да) | `serviceAccount.name` |
| PodDisruptionBudget | `pdb.enabled` (по умолчанию да) | `pdb.minAvailable` (1) |
| HorizontalPodAutoscaler | `hpa.enabled` | `hpa.minReplicas`, `maxReplicas`, `targetCPUUtilizationPercentage`, `targetMemoryUtilizationPercentage` |
| ServiceMonitor | `serviceMonitor.enabled` | `interval`, `scrapeTimeout`, `labels` |
| Role, RoleBinding на Lease | `leader.enabled` | — |
| ExternalSecret | `secrets[].externalSecret.enabled` | `secretStore`, `secrets[].externalSecret` |
| DestinationRule | `istio.destinationRule.enabled` | `outlierDetection`, `connectionPool` |
| VirtualService | `istio.virtualService.enabled` | `timeout`, `retries`, `fault` |

Istio, ExternalSecret и ServiceMonitor по умолчанию выключены: на минимальном стенде нет их CRD.

## Pod

- API, `/health/live`, `/health/ready` и `/metrics` — на одном порту `http` (как `telemetry.Register`).
  Startup и liveness — `/health/live`, readiness — `/health/ready`, ServiceMonitor — `/metrics`.
- `resources.requests` и `resources.limits` для `cpu` и `memory` обязательны (SRS 6.2): без них рендер падает.
- При включённом HPA поле `replicas` не задаётся, по умолчанию 2 реплики (SRS 6.3).
- Пользователь 65532 (distroless `nonroot`), корневая ФС только на чтение, запись — в `/tmp` (emptyDir).
- Токен ServiceAccount монтируется только при `leader.enabled`: API Kubernetes нужен лишь для Lease.
- Реплики разносятся по узлам (`topologySpread.enabled`, мягкое ограничение).

## Переменные окружения

| Переменная | Источник |
|---|---|
| `POD_NAME`, `POD_NAMESPACE` | downward API, всегда; `Identity` и `Namespace` для `pkg/leader` |
| `LEADER_LEASE_NAME` | имя компонента, при `leader.enabled`; Role даёт доступ только к этой Lease |
| `OTEL_SERVICE_NAME` | имя компонента |
| `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_EXPORTER_OTLP_INSECURE` | `telemetry.otlpEndpoint`, `telemetry.insecure`; без адреса экспорт OTLP выключен |
| `OTEL_TRACES_SAMPLER_ARG` | `telemetry.traceSampleRatio`, если задан |
| `<P>_BROKERS` | `kafka.enabled`, `kafka.brokers` через запятую; `P` = `kafka.prefix` (`KAFKA`) |
| `<P>_URL`, `<P>_PASSWORD_FILE`, `<P>_MAX_CONNS` | `postgres.*`, `P` = `POSTGRES` (`storage.PostgresConfigFromEnv`) |
| `<P>_URI`, `<P>_USERNAME`, `<P>_PASSWORD_FILE`, `<P>_DATABASE`, `<P>_AUTH_SOURCE` | `mongodb.*`, `P` = `MONGODB` (`storage.MongoConfigFromEnv`) |
| `<P>_ADDRS`, `<P>_USERNAME`, `<P>_PASSWORD_FILE` | `valkey.*`, `P` = `VALKEY` (`storage.ValkeyConfigFromEnv`) |
| произвольные | `env` (словарь имя → значение), `extraEnv` (список в формате Kubernetes) |

Пароли в переменные окружения не попадают: `*_PASSWORD_FILE` указывает на файл смонтированного секрета.

## Секреты

```yaml
secretStore:            # хранилище по умолчанию для ExternalSecret
  kind: ClusterSecretStore
  name: openbao
secrets:
  - name: postgres      # логическое имя: каталог /var/run/secrets/weigate/postgres
    externalSecret:
      enabled: true     # иначе Secret должен уже существовать
      refreshInterval: 1m
      data:
        - secretKey: password
          remoteRef: {key: database/static-creds/invoice-service, property: password}
  - name: valkey
    secretName: valkey-auth   # готовый Secret; по умолчанию <компонент>-<name>
postgres:
  enabled: true
  url: postgres://invoice-service@pg-rw.db.svc:5432/invoice
  password: {secret: postgres, key: password}   # -> POSTGRES_PASSWORD_FILE=/var/run/secrets/weigate/postgres/password
```

Секрет монтируется каталогом, без `subPath`: kubelet обновляет файл после ротации пароля, и сервис
перечитывает его без рестарта (SRS INF-2.4). `refreshInterval` ExternalSecret по умолчанию — 1 мин.
Ссылка `password.secret` на секрет, которого нет в `secrets`, — ошибка рендера.

## Istio (SRS 4.1.9, INF-3.1)

Ресурсы описывают вызовы этого компонента: их применяет sidecar вызывающего сервиса. Например, для
`invoice-service → rate-service` в values `rate-service`:

```yaml
istio:
  destinationRule:
    enabled: true            # outlier detection: consecutive5xxErrors 5, interval 10s, baseEjectionTime 30s
  virtualService:
    enabled: true
    timeout: 500ms
    retries: {attempts: 1, perTryTimeout: 500ms}
```

Fault injection для испытания 8.2г включается значениями:

```yaml
istio:
  virtualService:
    fault:
      enabled: true
      abort: {percentage: 100, httpStatus: 503}   # или delay: {percentage, fixedDelay}
```

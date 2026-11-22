# Модель C4

Модель Weigate в Structurizr DSL: [`workspace.dsl`](workspace.dsl). Требования — SRS раздел 9.

Модель описывает код `weigate` и `weigate-gitops`. Компоненты, которых в коде ещё нет (`webhook-service`,
`reporting-service`, ClickHouse), помечены тегом `Planned` и показаны пунктирной рамкой без деталей сверх SRS.

| Вид | Ключ представления |
|---|---|
| System Landscape | `L0-SystemLandscape` |
| L1 System Context | `L1-SystemContext` |
| L2 Container | `L2-Container` |
| L3 Component, `invoice-service` | `L3-InvoiceService` |
| L3 Component, `chain-watcher` | `L3-ChainWatcher` |
| L4 Code, автомат статусов счёта (`internal/lifecycle`) | `L4-InvoiceLifecycle` |
| Dynamic, оплата | `Dynamic-Payment` |
| Deployment, Kubernetes | `Deployment-Kubernetes` |

В Structurizr DSL нет уровня Code, поэтому L4 описан компонентами с тегом `Code`: функции и типы пакета
`lifecycle` (`Evaluate`, `Engine`, `Barrier`, `Config`, `NewEvent`).

## Проверка

Валидность DSL проверяет job `c4` в `.github/workflows/pr.yml` (образ `structurizr/cli`, закреплён по digest).
Локально, из корня репозитория:

```sh
docker run --rm -v "$PWD:/work" -w /work structurizr/cli validate -workspace docs/architecture/workspace.dsl
```

Просмотр: `docker run -it --rm -p 8080:8080 -v "$PWD/docs/architecture:/usr/local/structurizr" structurizr/lite`.

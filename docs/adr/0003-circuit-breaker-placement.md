# 0003. Circuit Breaker: в mesh или в приложении

## Контекст

Синхронных внутренних вызовов два: `invoice-service → rate-service` и `reconciliation-service →
ledger-service` (SRS 4.1.2). Синхронны также внешние вызовы к ноде, биржам и мерчантам. Отказ одного
мерчанта, биржи или ноды не должен влиять на независимые сценарии, Circuit Breaker срабатывает не дольше
чем за 30 с (SRS NFR-09). В кластере работает Istio с mTLS `STRICT` (SRS 4.1.10, INF-3.1).

## Решение

Уровень защиты выбирается для каждой зависимости (SRS 4.1.9):

| Зависимость | Уровень | Механизм | Поведение при отказе |
|---|---|---|---|
| `invoice-service → rate-service` | Mesh (Istio) | `DestinationRule` outlier detection, timeout 500 мс, 1 retry | Новые счета → `503`, существующие обрабатываются |
| `rate-service → биржи` | Приложение | Circuit Breaker на каждую биржу | Курс берётся с оставшейся биржи. Если недоступны обе, используется последний курс, пока он не старше 60 с |
| `chain-watcher → нода` | Mesh (Service для Anvil, `ServiceEntry` для Sepolia) + приложение | Outlier detection, backoff | Чтение приостанавливается. После восстановления — продолжение с курсора без пропуска блоков |
| `webhook-service → мерчант` | Приложение + mesh для `merchant-mock` | Circuit Breaker на endpoint, retry-топики, DLQ | Остальные мерчанты не затронуты |
| `reconciliation-service → ledger-service` | Mesh (Istio) | Outlier detection, timeout 2 с, 1 retry | Запуск сверки откладывается до следующего по расписанию, обработка платежей не затронута |

Вызов `reconciliation-service → нода` (балансы адресов, SRS UC-08) на схеме SRS 4.1 защищён Circuit Breaker,
но в таблице SRS 4.1.9 его нет: уровень защиты для него SRS не задаёт.

## Рассмотренные варианты

- **Всё в mesh.** Поведение при отказе бирж зависит от правил агрегации курса (SRS 4.1.9): при отказе одной
  биржи курс берётся с другой, при отказе обеих — последний курс не старше 60 с. Для доставки вебхуков mesh
  в SRS 4.1.9 указан только у `merchant-mock`. Повторы вебхуков идут по расписанию 1 м / 5 м / 30 м,
  затем в DLQ, через retry-топики Kafka (SRS 4.1.5).
- **Всё в приложении.** Отказ одной реплики `rate-service` исключает outlier detection Istio, запросы
  обслуживает вторая реплика (SRS 8.2а). Ejections outlier detection выводятся на дашборд, по ним срабатывает
  алерт `UpstreamEjected` (SRS 4.5.3, 4.5.4). Полный отказ `rate-service` проверяется fault injection
  в Istio (SRS 8.2г).
- **По зависимости** (выбран) — таблица выше.

## Последствия

- Состояние защиты видно в двух источниках: ejections outlier detection из Istio и состояние Circuit Breaker
  в метриках приложений; оба выводятся на дашборд «Mesh и устойчивость» (SRS 4.5.3).
- Алерты разделены так же: `UpstreamEjected` — по mesh; `RateSourceDown` и `ChainNodeUnavailable` — по
  Circuit Breaker в приложениях (SRS 4.5.4).
- Таймауты и retry внутренних вызовов задаются ресурсами Istio в чартах (SRS CI-5.3).
- `rate-service`, `chain-watcher` и `webhook-service` реализуют Circuit Breaker сами и экспортируют его
  состояние метриками.

## Статус

Принято, 15.11.2026.

# InvoiceExpiredRatioHigh

Уровень: warning. Условие алерта — SRS 4.5.4. Назад: [список runbook](README.md).

## Признаки

Доля счетов `EXPIRED` выше 30 % за 15 минут.

## Диагностика

Только чтение.

- Дашборд Grafana: Бизнес (доля EXPIRED, конверсия NEW → PAID).
- Логи компонента: `kubectl -n apps logs deploy/invoice-service --since=15m`.
- События и состояние подов: `kubectl -n apps get pods`, `kubectl -n apps describe pod <pod>`, `kubectl -n apps get events --sort-by=.lastTimestamp`.
- Связанные алерты: [ChainWatcherLagging](ChainWatcherLagging.md), [ChainNodeUnavailable](ChainNodeUnavailable.md), [RateStale](RateStale.md).

## Действия

1. Проверьте, что платежи обнаруживаются: отставание `chain-watcher`, доступность ноды.
2. Проверьте, что `payer-sim` (на стенде) работает и платит.
3. Проверьте TTL счетов в конфигурации: он не должен быть укорочен.

## Эскалация

Если за 30 минут причина не найдена или действие не помогло, передайте алерт дежурному владельцу компонента.

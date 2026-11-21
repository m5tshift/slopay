# ReconciliationMismatch

Уровень: critical. Условие алерта — SRS 4.5.4. Назад: [список runbook](README.md).

## Признаки

Сверка нашла расхождение между блокчейном, счетами и проводками.

## Диагностика

Только чтение.

- Дашборд Grafana: Бизнес.
- Логи компонента: `kubectl -n apps logs deploy/reconciliation-service --since=15m`.
- События и состояние подов: `kubectl -n apps get pods`, `kubectl -n apps describe pod <pod>`, `kubectl -n apps get events --sort-by=.lastTimestamp`.
- Отчёт сверки (`reconciliation.events`, ключ `runId`), детали в логах `reconciliation-service`.

## Действия

1. Определите тип расхождения: недостающий или лишний перевод, неверная сумма.
2. Проверьте, нет ли недавней реорганизации ([ChainReorgDetected](ChainReorgDetected.md)) или пропуска блоков ([ChainWatcherLagging](ChainWatcherLagging.md)).
3. Исправление данных выполняется только через сервисы, не вручную в БД.

## Эскалация

Немедленно владельцу ledger-service и reconciliation-service: расхождение означает риск потери или дублирования платежа.

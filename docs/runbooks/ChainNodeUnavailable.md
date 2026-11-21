# ChainNodeUnavailable

Уровень: critical. Условие алерта — SRS 4.5.4. Назад: [список runbook](README.md).

## Признаки

Circuit Breaker ноды открыт более 1 минуты, блоки не читаются, счета не переходят в `PAID`.

## Диагностика

Только чтение.

- Дашборд Grafana: Mesh и устойчивость (состояние Circuit Breaker), Блокчейн.
- Логи компонента: `kubectl -n apps logs deploy/chain-watcher --since=15m`.
- События и состояние подов: `kubectl -n apps get pods`, `kubectl -n apps describe pod <pod>`, `kubectl -n apps get events --sort-by=.lastTimestamp`.
- Какие сервисы видят открытый CB: `chain-watcher`, `reconciliation-service`.
- Состояние ноды: `kubectl -n apps get pods`, логи пода ноды (Anvil/Sepolia RPC).

## Действия

1. Выясните, недоступна нода или сеть до неё (логи, события пода).
2. Восстановление ноды — через ArgoCD/Git; после возврата CB закроется сам, затем проверьте [ChainWatcherLagging](ChainWatcherLagging.md).
3. Для Sepolia проверьте доступность внешнего RPC и лимиты провайдера.

## Эскалация

Если за 30 минут причина не найдена или действие не помогло, передайте алерт дежурному владельцу компонента. Если нода недоступна более 15 минут, сообщите владельцу стенда.

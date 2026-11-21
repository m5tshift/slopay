# ChainWatcherLagging

Уровень: warning. Условие алерта — SRS 4.5.4. Назад: [список runbook](README.md).

## Признаки

Отставание `chain-watcher` от головы ноды больше 10 блоков в течение 2 минут. Растёт время до N подтверждений.

## Диагностика

Только чтение.

- Дашборд Grafana: Блокчейн (высота головы ноды и `chain-watcher`, отставание в блоках).
- Логи компонента: `kubectl -n apps logs deploy/chain-watcher --since=15m`.
- События и состояние подов: `kubectl -n apps get pods`, `kubectl -n apps describe pod <pod>`, `kubectl -n apps get events --sort-by=.lastTimestamp`.
- Лидер читает блоки: `kubectl -n apps get lease`; в логах реплики-лидера ищите ошибки RPC.
- Связанные алерты: [ChainNodeUnavailable](ChainNodeUnavailable.md), [KafkaConsumerLagHigh](KafkaConsumerLagHigh.md).

## Действия

1. Если нода недоступна или Circuit Breaker открыт — действуйте по [ChainNodeUnavailable](ChainNodeUnavailable.md).
2. Если лидер сменился и новый не читает блоки — дождитесь окончания Lease; перезапуск пода выполняется через ArgoCD (изменением в `weigate-gitops`), не вручную.
3. Если отставание сохраняется при здоровой ноде — проверьте нагрузку (CPU, троттлинг) на дашборде инфраструктуры.

## Эскалация

Если за 30 минут причина не найдена или действие не помогло, передайте алерт дежурному владельцу компонента.

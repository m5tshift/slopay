# KafkaConsumerLagHigh

Уровень: warning. Условие алерта — SRS 4.5.4. Назад: [список runbook](README.md).

## Признаки

Lag consumer-группы растёт 5 минут подряд, события обрабатываются с задержкой.

## Диагностика

Только чтение.

- Дашборд Grafana: Kafka (consumer lag, latency produce/consume, under-replicated partitions).
- Логи компонента: `kubectl -n apps logs deploy/invoice-service --since=15m`.
- События и состояние подов: `kubectl -n apps get pods`, `kubectl -n apps describe pod <pod>`, `kubectl -n apps get events --sort-by=.lastTimestamp`.
- Какая группа и топик: метки алерта.
- Состояние брокеров Strimzi: `kubectl -n apps get kafka,kafkatopic`.

## Действия

1. Проверьте, жив ли consumer: поды, рестарты, ошибки в логах.
2. Проверьте под-реплицированные партиции и брокеры.
3. Если consumer медленный (БД, внешний вызов) — найдите узкое место по трейсам Tempo.

## Эскалация

Если за 30 минут причина не найдена или действие не помогло, передайте алерт дежурному владельцу компонента.

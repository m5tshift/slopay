# RateStale

Уровень: warning. Условие алерта — SRS 4.5.4. Назад: [список runbook](README.md).

## Признаки

Возраст курса больше 60 секунд, создание счетов может отклоняться.

## Диагностика

Только чтение.

- Дашборд Grafana: Бизнес (возраст курса).
- Логи компонента: `kubectl -n apps logs deploy/rate-service --since=15m`.
- События и состояние подов: `kubectl -n apps get pods`, `kubectl -n apps describe pod <pod>`, `kubectl -n apps get events --sort-by=.lastTimestamp`.
- Состояние источников: [RateSourceDown](RateSourceDown.md).
- Лидер опроса бирж: `kubectl -n apps get lease`.

## Действия

1. Проверьте, опрашивает ли лидер биржи и нет ли ошибок в логах.
2. При открытом CB биржи — см. [RateSourceDown](RateSourceDown.md).
3. На стенде с `rate-mock` проверьте, что мок отвечает: `kubectl -n apps get pods`.

## Эскалация

Если за 30 минут причина не найдена или действие не помогло, передайте алерт дежурному владельцу компонента и приложите разбор HolmesGPT (подсказка, не решение).

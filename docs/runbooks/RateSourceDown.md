# RateSourceDown

Уровень: warning. Условие алерта — SRS 4.5.4. Назад: [список runbook](README.md).

## Признаки

Circuit Breaker одной из бирж открыт более 5 минут.

## Диагностика

Только чтение.

- Дашборд Grafana: Mesh и устойчивость (Circuit Breaker), Бизнес (возраст курса).
- Логи компонента: `kubectl -n apps logs deploy/rate-service --since=15m`.
- События и состояние подов: `kubectl -n apps get pods`, `kubectl -n apps describe pod <pod>`, `kubectl -n apps get events --sort-by=.lastTimestamp`.
- Какая биржа недоступна: метки алерта, логи `rate-service`.

## Действия

1. Если вторая биржа отвечает, курс считается по ней; следите за [RateStale](RateStale.md) и [RateDivergence](RateDivergence.md).
2. Проверьте внешнюю доступность и лимиты API биржи (сеть, ответы 429/5xx в логах).
3. Если недоступны обе биржи — действуйте как при [RateStale](RateStale.md).

## Эскалация

Если за 30 минут причина не найдена или действие не помогло, передайте алерт дежурному владельцу компонента.

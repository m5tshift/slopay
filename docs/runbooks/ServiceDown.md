# ServiceDown

Уровень: critical. Условие алерта — SRS 4.5.4. Назад: [список runbook](README.md).

## Признаки

Сервис недоступен: нет ни одной готовой реплики или метрики не собираются.

## Диагностика

Только чтение.

- Дашборд Grafana: Golden signals сервиса, Инфраструктура.
- Логи компонента: `kubectl -n apps logs deploy/<сервис из меток алерта> --since=15m`.
- События и состояние подов: `kubectl -n apps get pods`, `kubectl -n apps describe pod <pod>`, `kubectl -n apps get events --sort-by=.lastTimestamp`.
- `kubectl -n apps get deploy,pods`; `/health/ready` и `/metrics` пода через логи проб в `describe`.

## Действия

1. Найдите причину: падение, не пройден readiness, нет ресурсов, недоступна зависимость.
2. Связанные алерты: [PodCrashLooping](PodCrashLooping.md), [PVCAlmostFull](PVCAlmostFull.md).
3. Восстановление — через Git и ArgoCD.

## Эскалация

Немедленно владельцу сервиса. Если за 30 минут причина не найдена или действие не помогло, передайте алерт дежурному владельцу компонента и приложите разбор HolmesGPT (подсказка, не решение).

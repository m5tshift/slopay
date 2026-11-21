# PodCrashLooping

Уровень: critical. Условие алерта — SRS 4.5.4. Назад: [список runbook](README.md).

## Признаки

Под перезапускается в цикле (CrashLoopBackOff).

## Диагностика

Только чтение.

- Дашборд Grafana: Инфраструктура, Golden signals сервиса.
- Логи компонента: `kubectl -n apps logs deploy/<сервис из меток алерта> --since=15m`.
- События и состояние подов: `kubectl -n apps get pods`, `kubectl -n apps describe pod <pod>`, `kubectl -n apps get events --sort-by=.lastTimestamp`.
- `kubectl -n apps describe pod <pod>` (причина и код выхода), `kubectl -n apps logs <pod> --previous`.

## Действия

1. Определите причину: ошибка конфигурации, нет секрета, OOMKilled, недоступная зависимость.
2. Исправление — изменением в Git (ArgoCD); ручная правка ресурсов запрещена.

## Эскалация

Если за 30 минут причина не найдена или действие не помогло, передайте алерт дежурному владельцу компонента и приложите разбор HolmesGPT (подсказка, не решение).

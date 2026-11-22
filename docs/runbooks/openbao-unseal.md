# Runbook: unseal OpenBao после рестарта

OpenBao на стенде работает в одном поде `openbao-0` (неймспейс `secrets`) с данными на PV и после каждого
рестарта пода или узла запечатан. Unseal ручной (SRS INF-2.4). Первичная инициализация —
[deploy-from-scratch.md](deploy-from-scratch.md), шаг 5а.

## Симптомы

- `kubectl -n secrets exec openbao-0 -- bao status` показывает `Sealed true`.
- ExternalSecret не обновляются, ClusterSecretStore `openbao` не готов (`kubectl get clustersecretstore openbao`).
- Не идёт ротация паролей PostgreSQL. Уже синхронизированные секреты остаются в Kubernetes, пароли статических
  ролей действуют до следующей ротации; `webhook-service` не прочитает новую версию секрета подписи.

## Unseal

Нужны три из пяти ключей из файла `openbao-init.txt` (вне репозитория). Каждая команда запрашивает один ключ
скрытым вводом:

```sh
kubectl -n secrets exec -it openbao-0 -- bao operator unseal
kubectl -n secrets exec -it openbao-0 -- bao operator unseal
kubectl -n secrets exec -it openbao-0 -- bao operator unseal
```

Проверка:

```sh
kubectl -n secrets exec openbao-0 -- bao status        # Sealed false
kubectl get clustersecretstore openbao                 # Ready
kubectl -n secrets get pod openbao-0                   # Running
```

ESO читает OpenBao сам (интервал обновления не больше 1 мин), перезапускать ничего не нужно. Ротация паролей
PostgreSQL возобновляется со следующего срока.

## Если не получается

| Симптом | Что делать |
|---|---|
| `Initialized false` | PV пуст: init по шагу 5а развёртывания, затем фаза 2 заново |
| ключ не принимается | опечатка или ключ от другого экземпляра; взять ключи из актуального `openbao-init.txt` |
| под не `Running` | `kubectl -n secrets describe pod openbao-0`, `kubectl -n argocd get application openbao` |
| ключи потеряны | данные недоступны: удалить PVC `data-openbao-0` и под, дать ArgoCD пересоздать, выполнить init и фазу 2 заново |

# 0004. Граница Terraform, ArgoCD и Ansible

## Контекст

Платформу разворачивают три инструмента. Нужно, чтобы каждым ресурсом управлял ровно один из них:

- ArgoCD не может развернуть сам себя и сетевой плагин кластера, без которых он не работает.
- Репозитории публичные: значений секретов нет ни в Git, ни в state Terraform (SRS 4.1.11).
- Риски SRS раздел 10: агент меняет платформу в обход GitOps, и стенд дрейфует; двойное управление Kafka
  (Ansible и ArgoCD) приводит к дрейфу конфигурации.
- Стенд разворачивается с нуля не дольше чем за 60 мин (SRS NFR-12).

## Решение

| Слой | Инструмент | Что делает |
|---|---|---|
| VM и ОС узлов | PowerShell (Hyper-V), `talosctl` | VM `talos-*`, `lb-1`, `lb-2`; установка и конфигурация Talos (SRS 6.1, INF-1.1) |
| Bootstrap кластера, фаза 1 | Terraform | Cilium, неймспейсы, ServiceAccount, ArgoCD (SRS INF-2.1) |
| Init и unseal OpenBao | Оператор, по runbook | После того как ArgoCD развернул OpenBao |
| Bootstrap кластера, фаза 2 | Terraform | Движки, политики и Kubernetes auth в OpenBao |
| Всё выше bootstrap-слоя | ArgoCD | App of Apps, auto-sync и self-heal; единственный источник правды (SRS INF-2.2) |
| Kafka | Ansible-роль | Генерирует манифесты оператора Strimzi и CR `Kafka`, `KafkaNodePool`, `KafkaTopic` в GitOps-репозиторий; применяет их ArgoCD. Напрямую в кластер роль ничего не устанавливает (SRS INF-2.3) |

Terraform использует провайдеры `kubernetes`, `helm` и `vault` (для OpenBao). State хранится локально, вне Git.
Значения секретов генерирует OpenBao, а не Terraform, поэтому в state они не попадают (SRS INF-2.1).

Агент описывает bootstrap-слой кодом и runbook, применяет его пользователь. Изменения выше bootstrap-слоя —
только через Git и ArgoCD (SRS раздел 7, п. 7).

## Рассмотренные варианты

SRS отдельно отклоняет два варианта:

- **Ansible применяет Strimzi и Kafka напрямую в кластер.** Kafka управлялась бы двумя инструментами,
  что ведёт к дрейфу конфигурации (SRS раздел 10). Поэтому роль только генерирует манифесты в GitOps.
- **Terraform генерирует значения секретов.** Значения попали бы в state Terraform. Поэтому Terraform
  настраивает только движки, политики и auth, а значения генерирует OpenBao (SRS INF-2.1).

## Последствия

- Развёртывание с нуля включает ручной шаг между фазами Terraform: init и unseal OpenBao по runbook.
  Этот шаг входит в 60 мин NFR-12.
- Bootstrap-слой (Talos, Cilium, неймспейсы, ArgoCD и конфигурация OpenBao) ArgoCD не управляет;
  его изменения применяются вручную тем же кодом Terraform и `talosctl`.
- Изменение Kafka — правка Ansible-роли, генерация манифестов и коммит в GitOps-репозиторий; ArgoCD
  применяет результат.
- `kubectl apply`, `helm install` и ручная правка ресурсов выше bootstrap-слоя запрещены: ArgoCD с self-heal
  вернёт их к состоянию из Git.
- State Terraform лежит только на машине оператора, вне Git.

## Статус

Принято, 15.11.2026.

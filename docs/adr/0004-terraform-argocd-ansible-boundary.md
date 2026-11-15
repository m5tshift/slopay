# 0004. Граница Terraform, ArgoCD и Ansible

## Контекст

Платформу разворачивают несколько инструментов, и каждым ресурсом должен управлять ровно один из них:

- Bootstrap-слой — Talos, Cilium, неймспейсы, ArgoCD и конфигурация OpenBao; всё выше него — ArgoCD
  (SRS INF-2.1, INF-2.2).
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
| Kafka | Ansible-роль `strimzi` | Генерирует манифесты оператора Strimzi и CR `Kafka`, `KafkaNodePool`, `KafkaTopic` в GitOps-репозиторий; применяет их ArgoCD. Напрямую в кластер роль ничего не устанавливает (SRS INF-2.3) |
| HAProxy и Keepalived на `lb-1`, `lb-2` | Ansible-роль `lb` | Настраивает VM вне кластера (SRS 4.1.8, INF-3.2). Инструмент в SRS не задан, роль `infra/ansible/roles/lb/` определена структурой репозитория в `AGENTS.md` |

Terraform использует провайдеры `kubernetes`, `helm` и `vault` (для OpenBao). State хранится локально, вне Git.
Значения секретов генерирует OpenBao, а не Terraform, поэтому в state они не попадают (SRS INF-2.1).

## Рассмотренные варианты

SRS отдельно отклоняет два варианта:

- **Ansible применяет Strimzi и Kafka напрямую в кластер.** Kafka управлялась бы двумя инструментами,
  что ведёт к дрейфу конфигурации (SRS раздел 10). Поэтому роль только генерирует манифесты в GitOps.
- **Terraform генерирует значения секретов.** Значения попали бы в state Terraform. Поэтому Terraform
  настраивает только движки, политики и auth, а значения генерирует OpenBao (SRS INF-2.1).

## Последствия

- Развёртывание с нуля включает ручной шаг между фазами Terraform: init и unseal OpenBao по runbook.
  Этот шаг входит в 60 мин NFR-12.
- Bootstrap-слой ArgoCD не управляет: его изменения применяются тем же кодом Terraform и `talosctl`.
- Изменение Kafka — правка Ansible-роли, генерация манифестов и коммит в GitOps-репозиторий; ArgoCD
  применяет результат.
- Ansible работает в двух режимах: генерирует манифесты для GitOps (Kafka) и настраивает VM балансировщиков
  напрямую (вне кластера).
- Любые изменения кластера выше bootstrap-слоя — только через Git и ArgoCD (SRS раздел 7, п. 7).
- State Terraform лежит только на машине оператора, вне Git.

## Статус

Принято, 15.11.2026.

# 0002. Стек наблюдаемости: VictoriaMetrics, VictoriaLogs, Tempo, Grafana

## Контекст

Все сервисы отдают метрики RED, JSON-логи с `trace_id` и трейсы OTel, трейс проходит сквозь Kafka (SRS NFR-11,
4.5.1). Нужны дашборды SRS 4.5.3 и алерты SRS 4.5.4 с доставкой в Telegram и email. Стек должен принимать
метрики Strimzi, Istio, Cilium и kube-state-metrics. На наблюдаемость в бюджете памяти отведено около 2,5 ГБ
(SRS 6.2).

## Решение

Вариант B: VictoriaMetrics, VictoriaLogs, Grafana Tempo и Grafana; алерты — vmalert и Alertmanager
(SRS 4.5.2). Все сигналы проходят через OpenTelemetry Collector (SRS 4.5.1). Трейсы Tempo хранятся в S3
(SRS 4.1.6). Каналы алертов — Telegram и email через Mailpit (SRS 4.5.4).

## Рассмотренные варианты

Сравнение — SRS Приложение Б.2.

| Вариант | Состав | Плюсы | Минусы |
|---|---|---|---|
| A. LGTM | Prometheus / Loki / Tempo / Grafana / Alertmanager | Стандарт, много готовых дашбордов | Prometheus и Loki заметно тяжелее по ресурсам; Loki требует дисциплины с лейблами |
| **B. Victoria + Tempo** (выбран) | VictoriaMetrics / VictoriaLogs / Tempo / Grafana / vmalert + Alertmanager | Экономия RAM и диска в разы; совместимость с PromQL и ServiceMonitor; VictoriaLogs без настройки лейблов | Меньше сообщество; VictoriaLogs моложе Loki |
| C. ClickHouse-центричный | OTel → ClickHouse + SigNoz или Uptrace | ClickHouse уже есть в Системе, одно хранилище для всех сигналов | Смешение бизнес-данных и телеметрии в одном ClickHouse; SigNoz сам по себе тяжёлый |
| D. ELK + Prometheus + Jaeger | Elasticsearch / Logstash / Kibana + … | Полнотекстовый поиск, зрелость | Самый тяжёлый; три разных интерфейса |
| InfluxDB | — | Удобен для push-метрик | Нет pull-модели Kubernetes и PromQL |

Вариант B минимален по ресурсам для одного стенда и совместим с метриками Strimzi, Istio, Cilium
и kube-state-metrics. Вариант C — обоснованная альтернатива, раз ClickHouse уже есть в Системе. Он отклонён,
чтобы не смешивать телеметрию и бизнес-данные.

## Последствия

- Всё собирается через OTel Collector, поэтому переход на вариант A или C не требует правки сервисов
  (SRS Б.2).
- Совместимость с PromQL и ServiceMonitor: ServiceMonitor из чартов сервисов (SRS CI-5.3) работают
  с VictoriaMetrics.
- Сообщество меньше, VictoriaLogs моложе Loki.
- Шаги 2 и 3 порядка разгрузки при нехватке памяти — сэмплирование трейсов и меньший retention метрик
  и логов (SRS 6.2).

## Статус

Принято, 15.11.2026.

## Изменения

Каналы алертов — только Telegram, HolmesGPT исключён (#170).

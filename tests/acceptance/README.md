# tests/acceptance

Сбор результатов испытаний SRS раздела 8 в `results/<id>.json` (формат — AGENTS.md, «Приёмочные испытания»).
Только стандартная библиотека Python.

```sh
python3 tests/acceptance/collect.py 8.1 --date ДД.ММ.ГГГГ --commit <sha> --summary "<вывод без чисел>" \
  --locust-csv <вне репозитория>/locust-8.1_stats.csv --vm-url http://<VictoriaMetrics>:8428 --window 43m \
  --value webhook_p95_s=<число> --value missed_transfers=<число> --value duplicates=<число>
```

- Locust (`--csv` прогона): `rps`, `p95_create_ms`, `p95_status_ms`, `error_rate_5xx` (доля неуспешных ответов
  Locust: всё, кроме `201` и `200`).
- VictoriaMetrics: запросы в `queries.json`, `{window}` — длительность прогона; запрос обязан вернуть один ряд.
- Остальные ключи (`--value КЛЮЧ=ЧИСЛО`): значения, измеренные вне этих источников, например `cb_seconds`,
  `vip_failover_s`, `minutes`, `missed_transfers`.
- Числа записываются как получены, без округления. `status` считается по порогам SRS (`THRESHOLDS` в `collect.py`):
  `passed` только если есть все ключи испытания и каждый в пороге. У испытаний без ключей статус задаёт `--status`.

Проверка: `make acceptance-test`.

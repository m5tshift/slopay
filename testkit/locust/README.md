# locust

Профиль нагрузки SRS 8.1 и 8.3. Запускается с рабочей машины через VIP, образа нет. Контракты:
`contracts/openapi/merchant-service.yaml` (`POST /api/v1/auth/token`) и `contracts/openapi/invoice-service.yaml`
(`POST /api/v1/invoices`, `GET /api/v1/invoices/{id}`).

```sh
python3 -m venv .venv && .venv/bin/pip install -r testkit/locust/requirements.txt
export WEIGATE_KEYS_FILE=<вне репозитория>/merchant-keys.json   # файл stand-setup
```

Файл ключей создаёт `stand-setup` (`testkit/stand-setup/README.md`); он лежит вне git.
Если шлюз с самоподписанным сертификатом, задайте `REQUESTS_CA_BUNDLE=<CA шлюза>`.

## 8.1: нагрузка

```sh
.venv/bin/locust -f testkit/locust/locustfile.py --headless --host https://<VIP> \
  -u 60 -r 0.3333 -t 23m --csv <вне репозитория>/locust-8.1 Merchant
```

- 60 мерчантов, разгон 3 мин (`-r` = 60/180), затем 20 мин нагрузки: `-t` считается с начала разгона.
- Мерчант — цикл из пяти запросов: один `POST /api/v1/invoices` и четыре `GET /api/v1/invoices/{id}`;
  это ~0,75 счёта/с и ~3 опроса/с на мерчанта, ~225 RPS всего. Опрашиваются счета, созданные этим же мерчантом.
- Токен получается при старте мерчанта (разгон разносит 60 запросов на 3 мин, ~20/мин) и обновляется через
  13 мин плюс до 60 с случайной задержки: в пределах 30 запросов/мин на IP.
- Любой ответ кроме `201` и `200` считается ошибкой, в том числе `429` и `503`.
- Прогон 8.2а и 8.2г — тот же запуск.

## 8.3: мерчант-нарушитель

Во время 8.2а, отдельным процессом, нужен мерчант `*-extra-*` из файла ключей (`stand-setup -extra 1`):

```sh
.venv/bin/locust -f testkit/locust/locustfile.py --headless --host https://<VIP> \
  -u 1 -r 1 -t 70s Violator
```

3 счёта/с в течение 1 мин, затем мерчант останавливает прогон. Ответ `429` с `Retry-After` ожидаем и ошибкой не
считается; `429` без `Retry-After` — ошибка.

## Проверка без стенда

```sh
make locust-test
```

Короткий прогон обоих профилей против заглушки API (`test_locustfile.py`): частоты запросов, один токен на
мерчанта, `429` с `Retry-After` у нарушителя.

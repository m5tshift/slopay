# stand-setup

Подготовка стенда (SRS 8.1): регистрация мерчантов в `merchant-service`, загрузка xpub, URL вебхука
`merchant-mock` и допуск. Запускается с рабочей машины, образа нет. Контракт:
`contracts/openapi/merchant-service.yaml`.

```sh
go run ./testkit/stand-setup/cmd/stand-setup \
  -base-url https://<gateway> \
  -mnemonic-file <вне репозитория>/mnemonic.txt \
  -keys-file <вне репозитория>/merchant-keys.json \
  -webhook-url https://<merchant-mock>/hook
```

- По умолчанию 60 мерчантов (профиль 8.1) и 1 дополнительный (8.3): `-merchants`, `-extra`;
  допуск — `-tolerance-bps` (по умолчанию 100, максимум 500).
- xpub мерчанта номер `i` — `m/44'/60'/i'` тестовой мнемоники; адрес с индексом `n` — `m/44'/60'/i'/0/n`.
  Мнемоника читается из файла и в логи не попадает. Файл мнемоники и файл ключей должны лежать вне
  git-репозитория, иначе программа не стартует.
- API-ключи пишутся в `-keys-file` (режим 0600) сразу после регистрации: ключ показывается один раз.
- Повторный запуск идемпотентен: мерчанты из файла ключей не регистрируются заново, настройки,
  которые уже совпадают, не пишутся. Если мерчант из файла потерян на стороне сервиса (ключ не принят),
  запуск завершается ошибкой.
- Лимит регистрации 5 запросов/мин с IP: ответ `429` программа переждёт по `Retry-After`,
  поэтому 61 мерчант регистрируется около 13 минут.

Интеграционный тест (`make test-integration`) поднимает образ из `MERCHANT_SERVICE_IMAGE`
и без него пропускается: `merchant-service` в репозитории ещё нет.

## Тестовый USDT (FR-10)

Контракт из `testkit/erc20` разворачивается в Anvil и пополняет счета payer-sim:

```sh
go run ./testkit/stand-setup/cmd/stand-setup deploy-usdt \
  -rpc-url http://<anvil>:8545 \
  -deployer-key-file <вне репозитория>/deployer.key \
  -payer-keys-file <вне репозитория>/payer-keys.txt
```

- Адрес контракта печатается в stdout строкой `USDT_CONTRACT_ADDRESS=0x...`: его подставляют в конфигурацию.
- `-deployer-key-file` — hex-ключ счёта Anvil с ETH на комиссию (берётся первый). `-payer-keys-file` — тот же
  файл, что читает payer-sim (по ключу на строку): токены выпускаются на его адреса. Дополнительно можно
  передать `-recipients 0x..,0x..`. Файлы с ключами должны лежать вне git-репозитория.
- `-amount` — минимальных единиц на счёт (по умолчанию 10^12 = 1 000 000 USDT, `decimals` = 6).
- Каждый запуск разворачивает новый контракт; после выпуска балансы проверяются `balanceOf`.
- Байткод лежит в `internal/usdt/TestUSDT.bin` (вывод `forge build`). После правки контракта обновите его:
  `jq -r .bytecode.object testkit/erc20/out/TestUSDT.sol/TestUSDT.json | sed 's/^0x//' > testkit/stand-setup/internal/usdt/TestUSDT.bin`;
  тест `TestBytecodeMatchesForgeBuild` сверяет файл со сборкой, если `out/` есть.

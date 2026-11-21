# testkit/erc20

Тестовый USDT для SRS FR-10: минимальный ERC-20 на Solidity (Foundry). `decimals` = 6, суммы в минимальных
единицах (1 USDT = 10^6). Внешние зависимости — только `forge-std` (для тестов и скрипта).

Контракт `src/TestUSDT.sol`:

- стандартный интерфейс ERC-20 (`transfer`, `approve`, `transferFrom`, событие `Transfer`);
- `mint(address to, uint256 value)` открыт для любого адреса: контракт живёт только в тестовой сети Anvil
  и средств не хранит. Выпуск эмитирует `Transfer(0x0, to, value)`.

Образа у компонента нет: контракт собирается и разворачивается с рабочей машины
(в стенде этим занимается `testkit/stand-setup`).

## Сборка и тесты

Нужен [Foundry](https://book.getfoundry.sh/getting-started/installation). Из корня репозитория:

```sh
make erc20-test      # ставит forge-std v1.17.0 в lib/ (если нет) и запускает forge test
```

или вручную в `testkit/erc20`:

```sh
forge install --no-git foundry-rs/forge-std@v1.17.0
forge build
forge test
```

`lib/`, `out/`, `cache/` и `broadcast/` в git не попадают.

## Развёртывание в Anvil

```sh
anvil                                   # терминал 1, RPC http://127.0.0.1:8545

cd testkit/erc20                        # терминал 2
export DEPLOYER_KEY=<приватный ключ одного из счетов Anvil>
export MINT_TO=<адрес1>,<адрес2>        # необязательно: кому выпустить токены
export MINT_AMOUNT=1000000000000        # необязательно: минимальных единиц на адрес (по умолчанию 10^12 = 1 000 000 USDT)
forge script script/Deploy.s.sol --rpc-url http://127.0.0.1:8545 --private-key "$DEPLOYER_KEY" --broadcast
```

Адрес контракта печатается строкой `TestUSDT deployed at ...` и сохраняется в
`broadcast/Deploy.s.sol/31337/run-latest.json`. Проверка баланса:

```sh
cast call --rpc-url http://127.0.0.1:8545 <адрес контракта> "balanceOf(address)(uint256)" <адрес1>
```

Ключ не коммитится и в репозитории не хранится (см. AGENTS.md, «Ключи никогда не приватные»).

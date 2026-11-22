workspace "Weigate" "Шлюз криптоплатежей: счёт в USD, оплата в ETH, подписанный вебхук после N подтверждений. Только тестовая сеть." {

    !identifiers hierarchical

    model {
        merchant = person "Мерчант" "Выставляет счета через API и получает вебхуки"
        buyer = person "Покупатель" "Открывает страницу оплаты и переводит ETH на адрес счёта"

        weigate = softwareSystem "Weigate" "Шлюз криптоплатежей" {
            gateway = container "Istio Ingress Gateway" "L7-вход (Gateway API): проверка JWT по JWKS, rate limiting" "Istio, Envoy" {
                tags "Platform"
            }
            rls = container "Envoy Rate Limit Service" "Счётчики лимитов запросов для шлюза" "Envoy ratelimit" {
                tags "Platform"
            }
            merchantService = container "merchant-service" "Мерчанты, API-ключи, выпуск JWT, xpub, настройки вебхуков и допусков" "Go" {
                tags "Service"
            }
            invoiceService = container "invoice-service" "API счетов, страница оплаты, фиксация курса, адреса из xpub, автомат статусов счёта" "Go" {
                tags "Service"

                api = component "api" "HTTP API по OpenAPI-контракту: /api/v1/invoices, /pay/{id}; проверка JWT и запросов" "net/http, kin-openapi"
                jwks = component "jwks" "Загрузка и кэш ключей проверки подписи JWT из merchant-service" "Go"
                invoices = component "invoice" "Создание счёта: сумма в wei, адрес из xpub, идемпотентность, список и статус" "Go"
                rates = component "rates" "Клиент rate-service: таймаут, ограничение возраста курса" "Go"
                merchantProjection = component "projection" "Проекция мерчантов из merchant.events со счётчиком деривации" "Go, franz-go"
                chainConsumer = component "chain consumer" "Читает chain.blocks и chain.transfers, передаёт их в автомат статусов" "Go, franz-go"
                lifecycle = component "lifecycle" "Автомат статусов счёта по SRS 4.1.4; сроки по времени цепочки; состав на уровне L4" "Go"
                statusCache = component "cache" "Кэш статуса страницы оплаты, TTL 30 с" "Go, valkey-go"
                store = component "store" "Счета, проекция мерчантов, канонические блоки, переводы, outbox; миграции goose" "Go, pgx"
                outbox = component "outbox relay" "Публикует события из outbox в invoice.events" "pkg/outbox"

                # Уровень 4: код автомата статусов (internal/lifecycle).
                evaluate = component "Evaluate" "Чистая функция: счёт, переводы, голова цепочки и время цепочки дают новый статус, роли переводов и переходы" "evaluate.go" {
                    tags "Code"
                }
                engine = component "Engine" "Транзакция с блокировкой цепочки: NewHead, Reorg, Transfer; вызывает Evaluate, пишет события в outbox, сбрасывает кэш статуса" "engine.go" {
                    tags "Code"
                }
                barrier = component "Barrier" "Перед обработкой chain.new_head ждёт, пока группа chain.transfers применит ранее опубликованные переводы" "barrier.go" {
                    tags "Code"
                }
                lifecycleConfig = component "Config" "N подтверждений, ожидание после отката, окно отслеживания; значения из конфигурации" "evaluate.go" {
                    tags "Code"
                }
                lifecycleEvents = component "NewEvent" "Переход становится CloudEvent invoice.* со sequence" "events.go" {
                    tags "Code"
                }
            }
            chainWatcher = container "chain-watcher" "Чтение блоков, курсор, переводы ETH и ERC-20 на отслеживаемые адреса, реорганизации" "Go" {
                tags "Service"

                watcher = component "watcher" "Работа реплики-лидера: чтение блоков с курсора, проверка parentHash, откат и chain.reorg, публикация переводов и chain.new_head" "Go"
                transfers = component "transfers" "Поиск переводов ETH и ERC-20 (eth_getLogs по blockHash) на отслеживаемые адреса" "Go"
                nodeClient = component "node" "JSON-RPC клиент ноды через Circuit Breaker" "go-ethereum"
                chainState = component "chain" "Окно последних заголовков блоков и поиск общего предка" "Go"
                tracked = component "tracked" "Множество отслеживаемых адресов: книга и кэш в Valkey (SISMEMBER)" "Go, valkey-go"
                trackedProjection = component "projection" "Проекция отслеживаемых адресов из invoice.events" "Go, franz-go"
                chainStore = component "store" "Курсор и окно заголовков одной записью, условной по version" "mongo-go-driver"
                chainEvents = component "events" "CloudEvents chain.new_head, chain.reorg, chain.transfer с детерминированным event.id" "Go, franz-go"
                leader = component "leader" "Выбор реплики-лидера через Kubernetes Lease" "pkg/leader"
            }
            rateService = container "rate-service" "Котировки ETH/USD с двух источников, агрегация, контроль свежести" "Go" {
                tags "Service"
            }
            ledgerService = container "ledger-service" "Учёт по двойной записи: поступления и комиссия 0,5 %, API выписок" "Go" {
                tags "Service"
            }
            reconciliationService = container "reconciliation-service" "Периодическая сверка балансов адресов в блокчейне со счетами и проводками" "Go" {
                tags "Service"
            }
            webhookService = container "webhook-service" "Подписанные HMAC вебхуки, ретраи с backoff, DLQ, Circuit Breaker на endpoint" "Go" {
                tags "Service"
            }
            reportingService = container "reporting-service" "Отчёты мерчанту по оборотам и конверсии. Планируется: кода пока нет" "Go" {
                tags "Service" "Planned"
            }

            kafka = container "Kafka" "Топики событий, CloudEvents (JSON)" "Strimzi" {
                tags "Queue" "Platform"
            }
            postgres = container "PostgreSQL" "Данные merchant-, invoice- и ledger-service" "CloudNativePG" {
                tags "Database" "Platform"
            }
            mongo = container "MongoDB" "Данные chain-watcher и reconciliation-service; журнал доставок webhook-service" "Percona Operator" {
                tags "Database" "Platform"
            }
            valkey = container "Valkey" "Кэши, последний курс, лимиты" "Valkey" {
                tags "Database" "Platform"
            }
            clickhouse = container "ClickHouse" "Витрины для отчётов. Планируется: в weigate-gitops только заготовка оператора" "ClickHouse Operator" {
                tags "Database" "Platform" "Planned"
            }
            openbao = container "OpenBao" "Секреты сервисов и ключи подписи вебхуков; доставляются External Secrets" "OpenBao" {
                tags "Platform"
            }

            merchantMock = container "merchant-mock" "Принимает вебхуки, по команде отвечает ошибкой или с задержкой" "Go" {
                tags "Test"
            }
            rateMock = container "rate-mock" "Генератор курса вместо бирж" "Go" {
                tags "Test"
            }
            payerSim = container "payer-sim" "Получает счета и отправляет переводы в Anvil" "Go" {
                tags "Test"
            }
        }

        anvil = softwareSystem "Anvil" "Локальная EVM-нода тестовой сети" {
            tags "External"
        }
        exchanges = softwareSystem "Биржи (Coinbase, OKX)" "Источники курса ETH/USD" {
            tags "External"
        }
        merchantSystem = softwareSystem "Система мерчанта" "Принимает вебхуки" {
            tags "External"
        }

        # L1
        merchant -> weigate "Выставляет счета, получает статусы" "HTTPS, JWT"
        buyer -> weigate "Открывает страницу оплаты" "HTTPS"
        buyer -> anvil "Переводит ETH на адрес счёта"
        weigate -> anvil "Читает блоки и балансы" "JSON-RPC"
        weigate -> exchanges "Запрашивает котировки" "HTTPS"
        weigate -> merchantSystem "Отправляет вебхуки" "HTTPS, HMAC"

        # L2
        merchant -> weigate.gateway "API" "HTTPS"
        buyer -> weigate.gateway "Страница оплаты" "HTTPS"
        weigate.gateway -> weigate.rls "Проверка лимита" "gRPC"
        weigate.rls -> weigate.valkey "Счётчики"
        weigate.gateway -> weigate.merchantService "Регистрация, API-ключи, JWT" "HTTP"
        weigate.gateway -> weigate.invoiceService "Счета и страница оплаты" "HTTP"
        weigate.gateway -> weigate.ledgerService "Выписки" "HTTP"
        weigate.gateway -> weigate.reportingService "Отчёты" "HTTP"

        weigate.invoiceService -> weigate.rateService "Фиксирует курс" "HTTP, Circuit Breaker"
        weigate.invoiceService -> weigate.merchantService "Загружает JWKS" "HTTP"
        weigate.reconciliationService -> weigate.ledgerService "Читает выписки" "HTTP, Circuit Breaker"
        weigate.rateService -> exchanges "Котировки" "HTTPS, Circuit Breaker"
        weigate.rateService -> weigate.rateMock "Котировки в режиме mock" "HTTP"
        weigate.chainWatcher -> anvil "Блоки, логи" "JSON-RPC, Circuit Breaker"
        weigate.reconciliationService -> anvil "Балансы адресов" "JSON-RPC, Circuit Breaker"
        weigate.payerSim -> weigate.invoiceService "Получает счета" "HTTP"
        weigate.payerSim -> anvil "Отправляет переводы" "JSON-RPC"
        weigate.webhookService -> merchantSystem "Вебхуки" "HTTPS, HMAC, Circuit Breaker"
        weigate.webhookService -> weigate.merchantMock "Вебхуки на стенде" "HTTPS"

        weigate.merchantService -> weigate.kafka "merchant.events" "Kafka, outbox"
        weigate.invoiceService -> weigate.kafka "invoice.events" "Kafka, outbox"
        weigate.chainWatcher -> weigate.kafka "chain.blocks, chain.transfers" "Kafka"
        weigate.rateService -> weigate.kafka "rate.ticks, rate.latest" "Kafka"
        weigate.ledgerService -> weigate.kafka "ledger.events" "Kafka, outbox"
        weigate.reconciliationService -> weigate.kafka "reconciliation.events" "Kafka"
        weigate.webhookService -> weigate.kafka "webhook.retry, webhook.dlq" "Kafka"
        weigate.kafka -> weigate.invoiceService "merchant.events, chain.blocks, chain.transfers" "Kafka"
        weigate.kafka -> weigate.chainWatcher "invoice.events" "Kafka"
        weigate.kafka -> weigate.rateService "rate.latest" "Kafka"
        weigate.kafka -> weigate.ledgerService "invoice.events" "Kafka"
        weigate.kafka -> weigate.reconciliationService "invoice.events" "Kafka"
        weigate.kafka -> weigate.webhookService "invoice.events, merchant.events" "Kafka"
        weigate.kafka -> weigate.clickhouse "События для витрин" "Kafka"

        weigate.merchantService -> weigate.postgres "Мерчанты, outbox" "SQL"
        weigate.invoiceService -> weigate.postgres "Счета, цепочка, переводы, outbox" "SQL"
        weigate.ledgerService -> weigate.postgres "Проводки, outbox" "SQL"
        weigate.chainWatcher -> weigate.mongo "Курсор, окно блоков, адреса" "MongoDB"
        weigate.reconciliationService -> weigate.mongo "Отчёты, проекция счетов" "MongoDB"
        weigate.webhookService -> weigate.mongo "Журнал доставок" "MongoDB"
        weigate.invoiceService -> weigate.valkey "Кэш статусов" "RESP"
        weigate.chainWatcher -> weigate.valkey "Кэш адресов" "RESP"
        weigate.rateService -> weigate.valkey "Последний курс" "RESP"
        weigate.webhookService -> weigate.valkey "Дедупликация" "RESP"
        weigate.reportingService -> weigate.clickhouse "Читает витрины" "SQL"
        weigate.webhookService -> weigate.openbao "Секреты подписи" "HTTPS"

        # L3: invoice-service
        weigate.gateway -> weigate.invoiceService.api "Запросы" "HTTP"
        weigate.invoiceService.api -> weigate.invoiceService.jwks "Ключи проверки JWT"
        weigate.invoiceService.api -> weigate.invoiceService.invoices "Создать, прочитать счёт"
        weigate.invoiceService.api -> weigate.invoiceService.statusCache "Статус страницы оплаты"
        weigate.invoiceService.jwks -> weigate.merchantService "JWKS" "HTTP"
        weigate.invoiceService.invoices -> weigate.invoiceService.rates "Курс"
        weigate.invoiceService.invoices -> weigate.invoiceService.store "Счёт, индекс деривации, outbox"
        weigate.invoiceService.rates -> weigate.rateService "GET /rates/{pair}" "HTTP"
        weigate.invoiceService.merchantProjection -> weigate.invoiceService.store "Проекция мерчантов"
        weigate.kafka -> weigate.invoiceService.merchantProjection "merchant.events" "Kafka"
        weigate.kafka -> weigate.invoiceService.chainConsumer "chain.blocks, chain.transfers" "Kafka"
        weigate.invoiceService.chainConsumer -> weigate.invoiceService.lifecycle "NewHead, Reorg, Transfer"
        weigate.invoiceService.lifecycle -> weigate.invoiceService.store "Транзакция с блокировкой цепочки"
        weigate.invoiceService.lifecycle -> weigate.invoiceService.statusCache "Сбрасывает кэш"
        weigate.invoiceService.store -> weigate.postgres "SQL"
        weigate.invoiceService.statusCache -> weigate.valkey "RESP"
        weigate.invoiceService.store -> weigate.invoiceService.outbox "События в outbox"
        weigate.invoiceService.outbox -> weigate.kafka "invoice.events" "Kafka"

        # L4: автомат статусов
        weigate.invoiceService.chainConsumer -> weigate.invoiceService.barrier "Ждёт применения переводов"
        weigate.invoiceService.chainConsumer -> weigate.invoiceService.engine "NewHead, Reorg, Transfer"
        weigate.invoiceService.engine -> weigate.invoiceService.evaluate "Вычисляет следующее состояние"
        weigate.invoiceService.engine -> weigate.invoiceService.lifecycleEvents "Переходы в события"
        weigate.invoiceService.engine -> weigate.invoiceService.store "Записывает состояние и outbox"
        weigate.invoiceService.evaluate -> weigate.invoiceService.lifecycleConfig "N, ожидание после отката, окно"
        weigate.invoiceService.barrier -> weigate.kafka "Смещения группы chain.transfers" "Kafka"

        # L3: chain-watcher
        weigate.chainWatcher.leader -> weigate.chainWatcher.watcher "Запускает у лидера"
        weigate.chainWatcher.watcher -> weigate.chainWatcher.nodeClient "Блоки"
        weigate.chainWatcher.watcher -> weigate.chainWatcher.chainState "Проверка parentHash, откат"
        weigate.chainWatcher.watcher -> weigate.chainWatcher.transfers "Переводы блока"
        weigate.chainWatcher.watcher -> weigate.chainWatcher.chainStore "Курсор и окно"
        weigate.chainWatcher.watcher -> weigate.chainWatcher.chainEvents "События"
        weigate.chainWatcher.transfers -> weigate.chainWatcher.nodeClient "Логи"
        weigate.chainWatcher.transfers -> weigate.chainWatcher.tracked "Отслеживается ли адрес"
        weigate.chainWatcher.tracked -> weigate.valkey "SISMEMBER" "RESP"
        weigate.chainWatcher.trackedProjection -> weigate.chainWatcher.tracked "Новые адреса"
        weigate.kafka -> weigate.chainWatcher.trackedProjection "invoice.events" "Kafka"
        weigate.chainWatcher.trackedProjection -> weigate.mongo "tracked_addresses" "MongoDB"
        weigate.chainWatcher.chainStore -> weigate.mongo "chain_state" "MongoDB"
        weigate.chainWatcher.nodeClient -> anvil "JSON-RPC, Circuit Breaker"
        weigate.chainWatcher.chainEvents -> weigate.kafka "chain.blocks, chain.transfers" "Kafka"

        # Развёртывание
        stand = deploymentEnvironment "Стенд" {
            hyperv = deploymentNode "ПК стенда (Hyper-V)" "Виртуальные машины, созданные Terraform" "Windows, Hyper-V" {
                lbNode = deploymentNode "lb-1, lb-2" "Две виртуальные машины, плавающий VIP" "Debian" {
                    lb = infrastructureNode "Keepalived + HAProxy" "VIP и L4-балансировка на шлюз" "Ansible, роль lb"
                }
                k8s = deploymentNode "Kubernetes" "Кластер из talos-cp-1 и talos-w-1..3; ресурсы только через Git и ArgoCD" "Talos Linux" {
                    appsNs = deploymentNode "apps" "Неймспейс сервисов" "Namespace, Helm-чарты поверх weigate-lib" {
                        containerInstance weigate.invoiceService
                        containerInstance weigate.merchantService
                        containerInstance weigate.chainWatcher
                        containerInstance weigate.rateService
                        containerInstance weigate.ledgerService
                        containerInstance weigate.reconciliationService
                        containerInstance weigate.webhookService
                        containerInstance weigate.reportingService
                        containerInstance weigate.merchantMock
                        containerInstance weigate.rateMock
                        containerInstance weigate.payerSim
                    }
                    platformNs = deploymentNode "Платформа" "Компоненты из weigate-gitops/platform" "ArgoCD Applications" {
                        gatewayInst = containerInstance weigate.gateway
                        containerInstance weigate.rls
                        containerInstance weigate.kafka
                        containerInstance weigate.postgres
                        containerInstance weigate.mongo
                        containerInstance weigate.valkey
                        containerInstance weigate.clickhouse
                        containerInstance weigate.openbao
                        softwareSystemInstance anvil
                    }
                }
            }
            hyperv.lbNode.lb -> hyperv.k8s.platformNs.gatewayInst "Трафик на шлюз" "TCP"
        }
    }

    views {
        systemLandscape "L0-SystemLandscape" "System Landscape" {
            include *
            autoLayout lr
        }

        systemContext weigate "L1-SystemContext" "L1: контекст системы" {
            include *
            autoLayout lr
        }

        container weigate "L2-Container" "L2: контейнеры" {
            include *
            autoLayout lr
        }

        component weigate.invoiceService "L3-InvoiceService" "L3: компоненты invoice-service" {
            include *
            exclude "element.tag==Code"
            autoLayout lr
        }

        component weigate.chainWatcher "L3-ChainWatcher" "L3: компоненты chain-watcher" {
            include *
            autoLayout lr
        }

        component weigate.invoiceService "L4-InvoiceLifecycle" "L4: код автомата статусов счёта (internal/lifecycle)" {
            include weigate.invoiceService.chainConsumer weigate.invoiceService.store weigate.kafka
            include "element.tag==Code"
            autoLayout lr
        }

        dynamic weigate "Dynamic-Payment" "Оплата счёта: от создания до вебхука" {
            merchant -> weigate.gateway "1. POST /api/v1/invoices"
            weigate.gateway -> weigate.invoiceService "2. createInvoice"
            weigate.invoiceService -> weigate.rateService "3. GET /rates/ETH-USD"
            weigate.invoiceService -> weigate.postgres "4. Счёт и outbox"
            weigate.invoiceService -> weigate.kafka "5. invoice.created"
            weigate.kafka -> weigate.chainWatcher "6. Адрес на отслеживание"
            buyer -> anvil "7. Перевод ETH на адрес"
            weigate.chainWatcher -> anvil "8. Читает блок"
            weigate.chainWatcher -> weigate.kafka "9. chain.transfer, затем chain.new_head"
            weigate.kafka -> weigate.invoiceService "10. DETECTED, затем подтверждения"
            weigate.invoiceService -> weigate.kafka "11. invoice.paid после N подтверждений"
            weigate.kafka -> weigate.webhookService "12. invoice.paid"
            weigate.webhookService -> merchantSystem "13. Подписанный вебхук"
            autoLayout lr
        }

        deployment weigate stand "Deployment-Kubernetes" "Развёртывание на стенде" {
            include *
            autoLayout lr
        }

        styles {
            element "Person" {
                shape Person
                background #08427b
                color #ffffff
            }
            element "Software System" {
                background #1168bd
                color #ffffff
            }
            element "External" {
                background #999999
                color #ffffff
            }
            element "Container" {
                background #438dd5
                color #ffffff
            }
            element "Component" {
                background #85bbf0
                color #000000
            }
            element "Code" {
                background #c9e2f8
                color #000000
            }
            element "Database" {
                shape Cylinder
            }
            element "Queue" {
                shape Pipe
            }
            element "Platform" {
                background #6b8e9e
            }
            element "Test" {
                background #a3b8c4
                color #000000
            }
            element "Planned" {
                border Dashed
                opacity 60
            }
        }
    }
}

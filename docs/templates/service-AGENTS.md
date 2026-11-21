# AGENTS.md — <service>

<!-- Шаблон для services/<service>/AGENTS.md. Скопируйте, замените <…> и удалите комментарии.
Общие правила — в корневом AGENTS.md, здесь только то, что относится к сервису. -->

## Назначение

<Одна-две фразы: за что отвечает сервис.> Требования — SRS <разделы>, FR: <…>, NFR: <…>.

## API и топики

| Вид | Что | Контракт |
|---|---|---|
| HTTP | <`POST /api/v1/…`> | [`contracts/openapi/<service>.yaml`](../../contracts/openapi/<service>.yaml) |
| Публикует | <топик> | [`contracts/asyncapi/<service>.yaml`](../../contracts/asyncapi/<service>.yaml) |
| Потребляет | <топик> | [`contracts/asyncapi/<producer>.yaml`](../../contracts/asyncapi/<producer>.yaml) |

Синхронные вызовы других сервисов и внешних систем: <… или «нет»>.

## Хранилища

| Хранилище | Что хранит | Схема |
|---|---|---|
| <PostgreSQL / MongoDB / Valkey> | <…> | <`migrations/`, схема документов> |

## Конфигурация

| Переменная | Назначение | Значение по SRS |
|---|---|---|
| <`WEIGATE_…`> | <…> | <…> |

Секреты читаются из файлов: <пути монтирования>.

## Команды

```sh
go build ./services/<service>/...
go test -race ./services/<service>/...
go test -race -tags integration ./services/<service>/...   # Testcontainers: <какие контейнеры>
```

Общие проверки — цели `make` из корневого AGENTS.md.

## Особые правила

- <Инварианты сервиса, идемпотентность, реплика-лидер.>

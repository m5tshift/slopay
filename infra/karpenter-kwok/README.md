# infra/karpenter-kwok: Karpenter с провайдером KWOK

Сборка Karpenter с провайдером KWOK из исходников (SRS INF-1.2, ADR 0006). Решения о добавлении и удалении
узлов настоящие, сами узлы имитируются. Версия закреплена: `v1.15.1` (тег `kubernetes-sigs/karpenter`,
провайдер лежит в его каталоге `kwok/`).

Имитируемые узлы несут taint, toleration к нему есть только у демонстрационной нагрузки: поды Системы
на них не размещаются. NodePool с taint и установка в кластер (чарт `kwok/charts` upstream, неймспейс `karpenter`)
выходят за рамки этой сборки.

Образ собирает `ko`, а не Dockerfile: так собирает сам upstream (`make build`), он берёт `kwok/main.go`
из клона без своего Go-кода и вместе с нужным Go-тулчейном.

## Требования

`git`, `go` (версия из `go.mod` upstream, для v1.15.1 — 1.26), [`ko`](https://ko.build), право на push в реестр.
Команды запускает пользователь, не агент.

## Сборка

GHCR (по умолчанию `ghcr.io/m5tshift/weigate/karpenter-kwok`):

```sh
echo "$GH_TOKEN" | ko login ghcr.io -u <user> --password-stdin
infra/karpenter-kwok/build.sh
```

Zot (когда он доступен, SRS INF-1.4):

```sh
ko login <адрес-zot> -u <user> --password-stdin
REGISTRY=<адрес-zot>/karpenter-kwok infra/karpenter-kwok/build.sh
```

Образ: `<REGISTRY>:v1.15.1`, `linux/amd64`. Другую версию задаёт `KARPENTER_VERSION`; закреплённую меняют
в начале `build.sh` и здесь.

## Проверка

`make karpenter-kwok-check`: shellcheck и `build.sh` на подставных `git` и `ko`, без сети и реестра.

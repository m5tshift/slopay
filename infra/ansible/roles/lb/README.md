# Роль `lb`

HAProxy (L4) и Keepalived (VRRP, unicast) на VM `lb-1`, `lb-2` (SRS 4.1.8, INF-3.2, NFR-10).

- HAProxy слушает `<VIP>:443`, передаёт TCP без расшифровки на NodePort Istio Ingress Gateway
  рабочих узлов (`lb_https_nodeport`, по умолчанию `30443`) и добавляет PROXY protocol v2 (`send-proxy-v2`).
  Health check узла — TCP с PROXY-заголовком (`check-send-proxy`), `inter 2s fall 3 rise 2`.
- Keepalived: unicast, оба хоста `state BACKUP` + `nopreempt`, приоритеты 150 и 100 задаются в инвентаре
  (`lb_keepalived_priority`). `track_script` проверяет `pidof haproxy` раз в секунду, `fall 2`; при отказе
  приоритет падает на `lb_check_weight` (-60, больше разницы приоритетов; роль это проверяет).
  Расчёт: до 2 с на обнаружение + анонс с `advert_int 1`, итого около 3-4 с (≤ 5 с).
- Порт 80 не слушается: внешний трафик только HTTPS (SRS 4.1.10).

## Порядок с Istio Gateway

Тот же NodePort и приём PROXY protocol v2 должны быть зафиксированы в Istio Ingress Gateway
(`weigate-gitops`, отдельная задача), иначе соединения обрываются. Порядок: сначала Gateway, потом роль.

## Предусловия на VM

Debian 13, минимальная установка с SSH-сервером и `python3`; статический адрес вне DHCP-диапазона;
пользователь с `sudo`; в `apt` нет источника `cdrom`. Пакеты `haproxy`, `keepalived`, `procps` ставятся из
репозитория Debian.

## Запуск (из `infra/ansible`, запускает оператор)

1. `cp inventory/hosts.yml.example inventory/hosts.yml` и
   `cp inventory/group_vars/lb.yml.example inventory/group_vars/lb.yml`, подставить адреса стенда.
   Оба файла в `.gitignore`.
2. `export WEIGATE_LB_VRRP_PASSWORD=<не более 8 символов>`. Пароль не хранится в Git; длина ограничена
   `auth_type PASS`.
3. `ansible-playbook lb.yml --check --diff`, затем `ansible-playbook lb.yml`.
4. Проверка: `ip -4 addr show <интерфейс>` на хосте с VIP; `sudo systemctl stop haproxy` на нём —
   VIP появляется на втором хосте за ≤ 5 с (`ping <VIP>` с рабочей машины).

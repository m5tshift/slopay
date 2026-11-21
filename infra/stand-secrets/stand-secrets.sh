#!/usr/bin/env bash
# Secrets of the minimal stand (SRS 6.4, 4.1.10). Idempotent: an existing Secret is never overwritten.
# The values are generated here and never printed, never put into arguments of processes and never written
# outside a private temporary directory (umask 077, removed on exit). OpenBao + ESO (#63) replace the script.
# The script is run by the user with the admin kubeconfig (KUBECONFIG); the agent never runs it.
#   ./stand-secrets.sh                      create the missing Secrets
#   ./stand-secrets.sh --dry-run            only list what would be created
#   ./stand-secrets.sh --rotate GROUP       replace the Secrets of GROUP: jwt, anvil, postgres, mongodb or valkey
#   ./stand-secrets.sh --mnemonic-file F    take the Anvil mnemonic from file F instead of generating it
# Needs kubectl, openssl, jq and cast (Foundry).
set -euo pipefail
umask 077

PAYER_ACCOUNTS="${PAYER_ACCOUNTS:-10}"
APPS_NS="${APPS_NS:-apps}"
CHAIN_NS="${CHAIN_NS:-chain}"
DATA_NS="${DATA_NS:-data}"
VALKEY_USERS=(invoice-service chain-watcher rate-service)

dry_run=0
put_type=""
rotate=""
mnemonic_src=""

die() { echo "stand-secrets: $*" >&2; exit 1; }

while [ $# -gt 0 ]; do
  case "$1" in
    --dry-run) dry_run=1 ;;
    --rotate) shift; [ $# -gt 0 ] || die "--rotate needs a group"; rotate="$1" ;;
    --mnemonic-file) shift; [ $# -gt 0 ] || die "--mnemonic-file needs a path"; mnemonic_src="$1" ;;
    -h | --help) sed -n '2,11p' "${BASH_SOURCE[0]}"; exit 0 ;;
    *) die "unknown argument: $1" ;;
  esac
  shift
done
case "$rotate" in "" | jwt | anvil | postgres | mongodb | valkey) ;; *) die "unknown rotate group: $rotate" ;; esac
[ -z "$mnemonic_src" ] || [ -r "$mnemonic_src" ] || die "cannot read $mnemonic_src"

for tool in kubectl openssl jq cast; do
  command -v "$tool" >/dev/null || die "$tool is required"
done

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

exists() { kubectl get secret "$2" -n "$1" -o name >/dev/null 2>&1; }

# read_key NS NAME KEY FILE: value of an existing Secret key into FILE.
read_key() {
  kubectl get secret "$2" -n "$1" -o "jsonpath={.data.$3}" | base64 -d > "$4"
}

# put NS NAME GROUP KEY=FILE...: create the Secret; replace an existing one only when its group is rotated.
put() {
  local ns="$1" name="$2" group="$3"
  shift 3
  local args=() kv
  for kv in "$@"; do args+=("--from-file=$kv"); done
  [ -z "$put_type" ] || args+=("--type=$put_type")
  if exists "$ns" "$name"; then
    if [ "$group" != "$rotate" ]; then echo "exists   $ns/$name"; return; fi
    if [ "$dry_run" = 1 ]; then echo "would replace $ns/$name"; return; fi
    kubectl delete secret "$name" -n "$ns" >/dev/null
    kubectl create secret generic "$name" -n "$ns" "${args[@]}" >/dev/null
    echo "replaced $ns/$name"
  else
    if [ "$dry_run" = 1 ]; then echo "would create $ns/$name"; return; fi
    kubectl create secret generic "$name" -n "$ns" "${args[@]}" >/dev/null
    echo "created  $ns/$name"
  fi
}

# keep NS NAME GROUP: true when the Secret exists and its group is not rotated.
keep() { exists "$1" "$2" && [ "$3" != "$rotate" ]; }

# merchant-service JWT signing key: ECDSA P-256, PKCS#8 PEM.
jwt() {
  if keep "$APPS_NS" merchant-service-jwt-signing jwt || [ "$dry_run" = 1 ]; then
    put "$APPS_NS" merchant-service-jwt-signing jwt private.pem=/dev/null
    return
  fi
  openssl ecparam -name prime256v1 -genkey -noout | openssl pkcs8 -topk8 -nocrypt -out "$tmp/private.pem"
  put "$APPS_NS" merchant-service-jwt-signing jwt "private.pem=$tmp/private.pem"
}

# Anvil mnemonic and payer-sim keys: the keys are derived from the mnemonic (m/44'/60'/0'/0/i, as Anvil does).
anvil() {
  local have_m=0 have_k=0
  exists "$CHAIN_NS" anvil-mnemonic && have_m=1
  exists "$APPS_NS" payer-sim-payer-keys && have_k=1
  if [ "$rotate" != anvil ]; then
    if [ "$have_m" = 1 ] && [ "$have_k" = 1 ]; then
      put "$CHAIN_NS" anvil-mnemonic anvil mnemonic=/dev/null
      put "$APPS_NS" payer-sim-payer-keys anvil keys=/dev/null
      return
    fi
    [ "$have_m" = 1 ] || [ "$have_k" = 0 ] \
      || die "payer-sim-payer-keys exists without anvil-mnemonic: the keys must match the mnemonic, run with --rotate anvil"
  fi
  if [ "$dry_run" = 1 ]; then
    put "$CHAIN_NS" anvil-mnemonic anvil mnemonic=/dev/null
    put "$APPS_NS" payer-sim-payer-keys anvil keys=/dev/null
    return
  fi
  if [ "$rotate" != anvil ] && [ "$have_m" = 1 ]; then
    read_key "$CHAIN_NS" anvil-mnemonic mnemonic "$tmp/mnemonic"
  elif [ -n "$mnemonic_src" ]; then
    tr -s '[:space:]' ' ' < "$mnemonic_src" | sed 's/^ //; s/ $//' > "$tmp/mnemonic"
  else
    cast wallet new-mnemonic --json | jq -r '.data.mnemonic' > "$tmp/mnemonic"
  fi
  [ -s "$tmp/mnemonic" ] || die "empty mnemonic"
  local i key
  : > "$tmp/keys"
  for ((i = 0; i < PAYER_ACCOUNTS; i++)); do
    key="$(cast wallet private-key --mnemonic "$tmp/mnemonic" --mnemonic-index "$i")"
    [[ "$key" =~ ^0x[0-9a-fA-F]{64}$ ]] || die "cast returned an unexpected key for index $i"
    echo "$key" >> "$tmp/keys"
  done
  put "$CHAIN_NS" anvil-mnemonic anvil "mnemonic=$tmp/mnemonic"
  put "$APPS_NS" payer-sim-payer-keys anvil "keys=$tmp/keys"
}

# pair GROUP APPS_NAME DATA_NAME DATA_KEY [USERNAME]: one password in two places: the Secret APPS_NAME (key password,
# read by the service in apps) and the key DATA_KEY of DATA_NAME (read by the operator in data). With USERNAME the
# data Secret is kubernetes.io/basic-auth with keys username and password (CloudNativePG bootstrap.initdb.secret).
# An existing value wins (apps first), so the copies stay equal; a value is generated only when neither exists.
pair() {
  local group="$1" apps_name="$2" data_name="$3" data_key="$4" user="${5:-}"
  local pw="$tmp/pw-$data_name" dkv=() pending=0
  if [ -n "$user" ]; then dkv+=("username=/dev/null"); fi
  dkv+=("$data_key=/dev/null")
  exists "$APPS_NS" "$apps_name" || pending=1
  exists "$DATA_NS" "$data_name" || pending=1
  if [ "$dry_run" = 1 ] || { [ "$pending" = 0 ] && [ "$rotate" != "$group" ]; }; then
    put "$APPS_NS" "$apps_name" "$group" password=/dev/null
    [ -z "$user" ] || put_type=kubernetes.io/basic-auth
    put "$DATA_NS" "$data_name" "$group" "${dkv[@]}"
    put_type=""
    return
  fi
  if [ "$rotate" != "$group" ] && exists "$APPS_NS" "$apps_name"; then
    read_key "$APPS_NS" "$apps_name" password "$pw"
  elif [ "$rotate" != "$group" ] && exists "$DATA_NS" "$data_name"; then
    read_key "$DATA_NS" "$data_name" "$data_key" "$pw"
  else
    openssl rand -hex 24 | tr -d '\n' > "$pw"
  fi
  [ -s "$pw" ] || die "empty password for $data_name"
  dkv=()
  if [ -n "$user" ]; then printf '%s' "$user" > "$tmp/user-$data_name"; dkv+=("username=$tmp/user-$data_name"); fi
  dkv+=("$data_key=$pw")
  put "$APPS_NS" "$apps_name" "$group" "password=$pw"
  [ -z "$user" ] || put_type=kubernetes.io/basic-auth
  put "$DATA_NS" "$data_name" "$group" "${dkv[@]}"
  put_type=""
}

# PostgreSQL users of merchant-service and invoice-service (CloudNativePG clusters, #112).
postgres() {
  pair postgres merchant-service-postgres merchant-pg-app-credentials password merchant-service
  pair postgres invoice-service-postgres invoice-pg-app-credentials password invoice-service
}

# Password of the MongoDB user chain-watcher: apps copy and the key of data/mongodb-users (Percona, #113).
mongodb() {
  pair mongodb chain-watcher-mongodb mongodb-users chain-watcher
}

# Valkey: one password per service in <service>-valkey (apps) and the same values in valkey-users (data);
# the init container of Valkey turns the latter into the ACL. An existing value wins, so the copies stay equal.
valkey() {
  local u have_users=0 pending=0 kv=()
  exists "$DATA_NS" valkey-users && have_users=1
  for u in "${VALKEY_USERS[@]}"; do
    exists "$APPS_NS" "$u-valkey" || pending=1
    kv+=("$u=/dev/null")
  done
  [ "$have_users" = 1 ] || pending=1
  if [ "$dry_run" = 1 ] || { [ "$pending" = 0 ] && [ "$rotate" != valkey ]; }; then
    for u in "${VALKEY_USERS[@]}"; do put "$APPS_NS" "$u-valkey" valkey password=/dev/null; done
    put "$DATA_NS" valkey-users valkey "${kv[@]}"
    return
  fi
  for u in "${VALKEY_USERS[@]}"; do
    if [ "$rotate" != valkey ] && exists "$APPS_NS" "$u-valkey"; then
      read_key "$APPS_NS" "$u-valkey" password "$tmp/valkey-$u"
    elif [ "$rotate" != valkey ] && [ "$have_users" = 1 ]; then
      read_key "$DATA_NS" valkey-users "$u" "$tmp/valkey-$u"
    else
      openssl rand -hex 24 | tr -d '\n' > "$tmp/valkey-$u"
    fi
    [ -s "$tmp/valkey-$u" ] || die "empty Valkey password of $u"
  done
  kv=()
  for u in "${VALKEY_USERS[@]}"; do
    put "$APPS_NS" "$u-valkey" valkey "password=$tmp/valkey-$u"
    kv+=("$u=$tmp/valkey-$u")
  done
  put "$DATA_NS" valkey-users valkey "${kv[@]}"
}

jwt
anvil
postgres
mongodb
valkey

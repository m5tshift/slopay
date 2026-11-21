#!/usr/bin/env bash
# Offline checks for stand-secrets.sh with a fake kubectl (Secrets are directories) and a fake cast.
# Needs openssl and jq; no cluster, no Foundry.
set -euo pipefail

dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
script="$dir/stand-secrets.sh"
bash -n "$script"

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
mkdir "$tmp/bin"
export STORE="$tmp/store" ARGV_LOG="$tmp/argv.log"
mkdir "$STORE"
: > "$ARGV_LOG"

cat > "$tmp/bin/kubectl" <<'FAKE'
#!/usr/bin/env bash
echo "kubectl $*" >> "$ARGV_LOG"
verb="$1"; shift
if [ "$verb" = get ] || [ "$verb" = delete ]; then shift; fi   # "secret"
if [ "$verb" = create ]; then shift; shift; fi                  # "secret generic"
name="$1"; shift
ns=""; out=""; stype=""; files=()
while [ $# -gt 0 ]; do
  case "$1" in
    -n) ns="$2"; shift ;;
    -o) out="$2"; shift ;;
    --from-file=*) files+=("${1#--from-file=}") ;;
    --type=*) stype="${1#--type=}" ;;
  esac
  shift
done
secret="$STORE/$ns/$name"
case "$verb" in
  get)
    [ -d "$secret" ] || exit 1
    case "$out" in
      name) echo "secret/$name" ;;
      jsonpath=*) key="${out#jsonpath=\{.data.}"; base64 < "$secret/${key%\}}" ;;
    esac ;;
  delete) rm -rf "$secret" ;;
  create)
    case "$ns" in apps | chain | data) ;; *) echo "namespace $ns not found" >&2; exit 1 ;; esac
    [ ! -d "$secret" ] || { echo "AlreadyExists" >&2; exit 1; }
    mkdir -p "$secret"
    echo "${stype:-Opaque}" > "$secret/.type"
    for kv in "${files[@]}"; do cp "${kv#*=}" "$secret/${kv%%=*}"; done ;;
esac
FAKE

cat > "$tmp/bin/cast" <<'FAKE'
#!/usr/bin/env bash
echo "cast $*" >> "$ARGV_LOG"
if [ "$1 $2" = "wallet new-mnemonic" ]; then
  echo '{"data":{"mnemonic":"alpha bravo charlie delta echo foxtrot golf hotel india juliet kilo lima"}}'
elif [ "$1 $2" = "wallet private-key" ]; then
  [ -f "$4" ] || exit 1
  echo "0x$({ cat "$4"; echo "$6"; } | openssl dgst -sha256 | sed 's/.*= //')"
else
  exit 1
fi
FAKE
chmod +x "$tmp/bin/kubectl" "$tmp/bin/cast"
export PATH="$tmp/bin:$PATH"

fail() { echo "test: $*" >&2; exit 1; }
run() { "$script" "$@" > "$tmp/out.txt" 2> "$tmp/err.txt" || { cat "$tmp/err.txt" >&2; fail "script failed: $*"; }; }
snapshot() { (cd "$STORE" && find . -type f -exec cksum {} + | sort | cksum); }
val() { cat "$STORE/$1/$2/$3"; }

expected="apps/merchant-service-jwt-signing apps/payer-sim-payer-keys chain/anvil-mnemonic apps/chain-watcher-mongodb
apps/invoice-service-valkey apps/chain-watcher-valkey apps/rate-service-valkey data/valkey-users
apps/merchant-service-postgres apps/invoice-service-postgres data/merchant-pg-app-credentials data/invoice-pg-app-credentials
data/mongodb-users"

# dry run creates nothing and lists everything
run --dry-run
[ -z "$(ls "$STORE")" ] || fail "dry run created Secrets"
for s in $expected; do grep -q "would create $s\$" "$tmp/out.txt" || fail "dry run does not list $s"; done

# first run creates everything
run
for s in $expected; do grep -q "created  $s\$" "$tmp/out.txt" || fail "$s not created"; done

openssl pkey -in "$STORE/apps/merchant-service-jwt-signing/private.pem" -noout -text | grep -q 'prime256v1' \
  || fail "JWT key is not P-256"
[ "$(head -n 1 "$STORE/apps/merchant-service-jwt-signing/private.pem")" = "-----BEGIN PRIVATE"" KEY-----" ] || fail "JWT key is not PKCS#8"

[ "$(wc -l < "$STORE/apps/payer-sim-payer-keys/keys")" -eq 10 ] || fail "payer-keys does not hold 10 keys"
grep -qvE '^0x[0-9a-f]{64}$' "$STORE/apps/payer-sim-payer-keys/keys" && fail "payer-keys format"
[ "$(sort -u "$STORE/apps/payer-sim-payer-keys/keys" | wc -l)" -eq 10 ] || fail "payer keys repeat"
[ "$(wc -w < "$STORE/chain/anvil-mnemonic/mnemonic")" -eq 12 ] || fail "mnemonic is not 12 words"
want="0x$({ cat "$STORE/chain/anvil-mnemonic/mnemonic"; echo 3; } | openssl dgst -sha256 | sed 's/.*= //')"
[ "$(sed -n 4p "$STORE/apps/payer-sim-payer-keys/keys")" = "$want" ] || fail "payer keys are not derived from the mnemonic"

for u in invoice-service chain-watcher rate-service; do
  [ -s "$STORE/apps/$u-valkey/password" ] || fail "no password for $u"
  [ "$(val apps "$u-valkey" password)" = "$(val data valkey-users "$u")" ] || fail "valkey-users differs for $u"
done
[ "$(val apps invoice-service-valkey password)" != "$(val apps rate-service-valkey password)" ] \
  || fail "services share a Valkey password"

for c in merchant invoice; do
  [ -s "$STORE/apps/$c-service-postgres/password" ] || fail "no Postgres password for $c"
  [ "$(val apps "$c-service-postgres" password)" = "$(val data "$c-pg-app-credentials" password)" ] \
    || fail "Postgres copies differ for $c"
  [ "$(val data "$c-pg-app-credentials" username)" = "$c-service" ] || fail "Postgres username of $c"
  [ "$(cat "$STORE/data/$c-pg-app-credentials/.type")" = kubernetes.io/basic-auth ] || fail "type of $c credentials"
done
[ "$(val apps merchant-service-postgres password)" != "$(val apps invoice-service-postgres password)" ] \
  || fail "services share a Postgres password"
[ -s "$STORE/apps/chain-watcher-mongodb/password" ] || fail "no MongoDB password"
[ "$(val apps chain-watcher-mongodb password)" = "$(val data mongodb-users chain-watcher)" ] || fail "MongoDB copies differ"

# no value in stdout, stderr or process arguments
for f in "$tmp/out.txt" "$tmp/err.txt" "$ARGV_LOG"; do
  for v in "$STORE"/*/*/*; do
    [ "$(basename "$v")" != username ] || continue   # not a secret: the user name
    # shellcheck disable=SC2312
    ! grep -qF -f <(head -c 64 "$v" | head -n 1) "$f" || fail "a secret value leaked into $(basename "$f")"
  done
done

# second run changes nothing
before="$(snapshot)"
run
[ "$(snapshot)" = "$before" ] || fail "rerun changed Secrets"
grep -q created "$tmp/out.txt" && fail "rerun created Secrets"

# a missing Secret is created, the others stay
rm -rf "$STORE/apps/chain-watcher-valkey"
run
[ "$(val apps chain-watcher-valkey password)" = "$(val data valkey-users chain-watcher)" ] || fail "restored Valkey password differs"
[ "$(snapshot)" = "$before" ] || fail "restoring a Valkey Secret changed the others"

# a missing Postgres copy is restored from the existing one, in both directions
rm -rf "$STORE/data/invoice-pg-app-credentials"
run
[ "$(val apps invoice-service-postgres password)" = "$(val data invoice-pg-app-credentials password)" ] || fail "restored data Postgres copy differs"
[ "$(val data invoice-pg-app-credentials username)" = invoice-service ] || fail "restored username"
[ "$(snapshot)" = "$before" ] || fail "restoring a Postgres Secret changed the others"
rm -rf "$STORE/apps/merchant-service-postgres"
run
[ "$(val apps merchant-service-postgres password)" = "$(val data merchant-pg-app-credentials password)" ] || fail "restored apps Postgres copy differs"
rm -rf "$STORE/data/mongodb-users"
run
[ "$(val apps chain-watcher-mongodb password)" = "$(val data mongodb-users chain-watcher)" ] || fail "restored data MongoDB copy differs"
rm -rf "$STORE/apps/chain-watcher-mongodb"
cp -r "$STORE/data/mongodb-users" "$tmp/mu"
run
[ "$(val apps chain-watcher-mongodb password)" = "$(cat "$tmp/mu/chain-watcher")" ] || fail "restored apps MongoDB copy differs"
[ "$(snapshot)" = "$before" ] || fail "restoring copies changed the others"

# keys without the mnemonic: refuse
rm -rf "$STORE/chain/anvil-mnemonic"
"$script" > /dev/null 2>&1 && fail "keys without mnemonic accepted"
before="$(snapshot)"

# rotate replaces only its group
run --rotate anvil
grep -q 'replaced apps/payer-sim-payer-keys' "$tmp/out.txt" || fail "rotate did not replace the keys"
[ "$(wc -w < "$STORE/chain/anvil-mnemonic/mnemonic")" -eq 12 ] || fail "rotated mnemonic missing"
old_jwt="$(cksum < "$STORE/apps/merchant-service-jwt-signing/private.pem")"
run --rotate jwt
[ "$(cksum < "$STORE/apps/merchant-service-jwt-signing/private.pem")" != "$old_jwt" ] || fail "rotate jwt kept the key"
old_pw="$(val data valkey-users rate-service)"
run --rotate valkey
[ "$(val data valkey-users rate-service)" != "$old_pw" ] || fail "rotate valkey kept the password"
[ "$(val apps rate-service-valkey password)" = "$(val data valkey-users rate-service)" ] || fail "rotated copies differ"

old_pg="$(val apps invoice-service-postgres password)"; old_pm="$(val apps merchant-service-postgres password)"
run --rotate postgres
[ "$(val apps invoice-service-postgres password)" != "$old_pg" ] || fail "rotate postgres kept the invoice password"
[ "$(val apps merchant-service-postgres password)" != "$old_pm" ] || fail "rotate postgres kept the merchant password"
for c in merchant invoice; do
  [ "$(val apps "$c-service-postgres" password)" = "$(val data "$c-pg-app-credentials" password)" ] || fail "rotated Postgres copies differ"
  [ "$(val data "$c-pg-app-credentials" username)" = "$c-service" ] || fail "rotated username lost"
  [ "$(cat "$STORE/data/$c-pg-app-credentials/.type")" = kubernetes.io/basic-auth ] || fail "rotated type lost"
done
old_pw="$(val data mongodb-users chain-watcher)"
run --rotate mongodb
[ "$(val data mongodb-users chain-watcher)" != "$old_pw" ] || fail "rotate mongodb kept the password"
[ "$(val apps chain-watcher-mongodb password)" = "$(val data mongodb-users chain-watcher)" ] || fail "rotated MongoDB copies differ"
[ "$(val apps invoice-service-postgres password)" != "" ] || fail "unexpected"

# a mnemonic from a file is used as is
rm -rf "$STORE/chain" "$STORE/apps/payer-sim-payer-keys"
echo "one two three four five six seven eight nine ten eleven twelve" > "$tmp/m.txt"
run --mnemonic-file "$tmp/m.txt"
[ "$(val chain anvil-mnemonic mnemonic)" = "$(cat "$tmp/m.txt")" ] || fail "mnemonic file ignored"

# unknown rotate group
"$script" --rotate redis > /dev/null 2>&1 && fail "unknown rotate group accepted"

echo "stand-secrets-check: ok"

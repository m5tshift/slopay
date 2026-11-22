#!/usr/bin/env bash
# Renders the example consumer chart on top of weigate-lib and checks the result.
# Called by make helm-lint.
set -euo pipefail

dir=$(cd "$(dirname "$0")" && pwd)
fail() { echo "weigate-lib test: $*" >&2; exit 1; }

helm dependency update --skip-refresh "$dir" > /dev/null
helm lint --strict "$dir"
helm lint --strict "$dir" -f "$dir/values-all.yaml"

kinds() { grep -E '^kind: ' | sed 's/^kind: //' | LC_ALL=C sort | tr '\n' ' ' | sed 's/ $//'; }

# Defaults: optional resources (Istio, ExternalSecret, ServiceMonitor, HPA, Role) are off.
min=$(helm template example "$dir")
got=$(kinds <<< "$min")
want="Deployment PodDisruptionBudget Service ServiceAccount"
[ "$got" = "$want" ] || fail "default kinds: got '$got', want '$want'"
grep -q '^  replicas: 2$' <<< "$min" || fail "default: replicas: 2 expected"
grep -q 'automountServiceAccountToken: true' <<< "$min" && fail "default: service account token must not be mounted"

# Every option on.
all=$(helm template example "$dir" -f "$dir/values-all.yaml")
got=$(kinds <<< "$all")
want="Deployment DestinationRule ExternalSecret ExternalSecret HorizontalPodAutoscaler PodDisruptionBudget Role RoleBinding Service ServiceAccount ServiceMonitor VirtualService"
[ "$got" = "$want" ] || fail "all kinds: got '$got', want '$want'"
grep -q '^  replicas:' <<< "$all" && fail "with HPA the Deployment must not set replicas"
for s in 'path: /health/live' 'path: /health/ready' 'path: /metrics' \
	'value: "/var/run/secrets/weigate/postgres/password"' \
	'value: "/var/run/secrets/weigate/mongodb/password"' \
	'value: "/var/run/secrets/weigate/valkey/password"' \
	'name: OTEL_EXPORTER_OTLP_ENDPOINT' 'name: KAFKA_BROKERS' 'name: LEADER_LEASE_NAME' \
	'refreshInterval: 1m' 'consecutive5xxErrors: 5' 'timeout: 500ms' 'httpStatus: 503'; do
	grep -qF "$s" <<< "$all" || fail "all: '$s' not rendered"
done
# Passwords are files only (pkg/storage): no *_PASSWORD variables, no subPath mounts.
grep -qE 'name: [A-Z_]+_PASSWORD$' <<< "$all" && fail "password passed in an environment variable"
grep -q 'subPath' <<< "$all" && fail "secrets must be mounted as directories, not subPath"

# Token sidecar: native sidecar, token file, omitted secret and env key.
ob=$(helm template example "$dir" -f "$dir/values-all.yaml" --set openbao.enabled=true --set openbao.addr=http://bao.test:8200 \
	--set openbao.env.EXTRA_KEY=transit-key --set 'openbao.omitSecrets={valkey}' --set valkey.enabled=false --set env.DROP_ME=x --set 'openbao.omitEnv={DROP_ME}')
for s in 'restartPolicy: Always' 'name: openbao-agent' 'automountServiceAccountToken: true' 'name: OPENBAO_ADDR' \
	'value: "/var/run/secrets/weigate/openbao/token"' 'name: EXTRA_KEY' 'quay.io/openbao/openbao:2.7.3@sha256:'; do
	grep -qF "$s" <<< "$ob" || fail "openbao: '$s' not rendered"
done
grep -q 'name: DROP_ME' <<< "$ob" && fail "openbao: omitEnv key rendered"
grep -q 'name: secret-valkey' <<< "$ob" && fail "openbao: omitSecrets entry mounted"
grep -q 'openbao-agent' <<< "$all" && fail "openbao resources rendered by default"

# Invalid values must fail rendering with the expected message.
must_fail() {
	local want=$1 out; shift
	if out=$(helm template example "$dir" "$@" 2>&1); then fail "rendered without error, want '$want'"; fi
	grep -qF "$want" <<< "$out" || fail "want error '$want', got: $out"
}
must_fail "resources.requests.cpu is required" --set resources=null
must_fail "resources.limits.memory is required" --set resources.limits.memory=null
must_fail "image.tag is required" --set image.tag=
must_fail 'postgres.password.secret "missing" is not declared in secrets' \
	-f "$dir/values-all.yaml" --set postgres.password.secret=missing
must_fail "set delay.percentage or abort.percentage" \
	-f "$dir/values-all.yaml" --set istio.virtualService.fault.abort.percentage=0
must_fail "openbao.addr is required" --set openbao.enabled=true
must_fail "externalSecret.data is required" --set 'secrets[0].name=x' --set 'secrets[0].externalSecret.enabled=true'

echo "weigate-lib test: ok"

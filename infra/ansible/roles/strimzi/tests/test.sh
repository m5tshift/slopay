#!/usr/bin/env bash
# Generates the manifests of both profiles into a temporary directory and checks them. No cluster needed.
# Needs ansible-playbook and yq (mikefarah).
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
ansible_dir=$(cd "$here/../../.." && pwd)
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

fail() { echo "FAIL: $*" >&2; exit 1; }

check() { # check <file> <yq expression> <expected>
  local got
  got=$(yq ea "$2" "$1")
  [ "$got" = "$3" ] || fail "$1: $2 is '$got', expected '$3'"
}

generate() { # generate <dir> <profile>
  mkdir -p "$1"
  (cd "$ansible_dir" && ansible-playbook strimzi.yml -e "strimzi_gitops_dir=$1" -e "strimzi_profile=$2" >/dev/null)
}

topic_partitions() { # topic_partitions <topic>
  echo "[.] | map(select(.spec.topicName == \"$1\")) | .[0].spec.partitions"
}

for profile in minimal full; do
  d="$tmp/$profile"
  generate "$d" "$profile"
  generate "$tmp/$profile-again" "$profile"
  diff -r "$d" "$tmp/$profile-again" || fail "$profile: second run differs"
  out="$d/platform/strimzi"

  [ "$(find "$out" -type f | wc -l | tr -d ' ')" = 6 ] || fail "$profile: unexpected file set"
  for f in $(find "$out" -type f); do yq ea '.' "$f" >/dev/null || fail "$f: invalid YAML"; done

  kafka="$out/cluster/kafka.yaml"
  check "$kafka" '.kind' Kafka
  check "$kafka" '.spec.kafka.metricsConfig.type' jmxPrometheusExporter
  check "$kafka" '.spec.kafka.metricsConfig.valueFrom.configMapKeyRef.name' kafka-metrics
  check "$kafka" '.spec.kafkaExporter.topicRegex' '.*'
  check "$kafka" '.spec.kafkaExporter.groupRegex' '.*'
  check "$kafka" '.spec.kafka.config."auto.create.topics.enable"' false
  check "$out/cluster/nodepool.yaml" '.spec.roles | join(",")' controller,broker
  check "$out/cluster/nodepool.yaml" '.spec.jvmOptions."-Xmx"' 768m
  check "$out/cluster/nodepool.yaml" '.spec.jvmOptions."-Xms"' 768m
  check "$out/cluster/kafka-metrics.yaml" '.data."kafka-metrics-config.yml" | contains("quantile")' true
  check "$out/cluster/kustomization.yaml" '.resources | length' 4

  want_replicas=1 want_rf=1 want_isr=1 want_topics=6
  if [ "$profile" = full ]; then want_replicas=3 want_rf=3 want_isr=2 want_topics=12; fi
  check "$out/cluster/nodepool.yaml" '.spec.replicas' "$want_replicas"
  check "$kafka" '.spec.kafka.config."default.replication.factor"' "$want_rf"
  check "$kafka" '.spec.kafka.config."min.insync.replicas"' "$want_isr"
  check "$kafka" '.spec.kafka.config."offsets.topic.replication.factor"' "$want_rf"

  topics="$out/cluster/topics.yaml"
  check "$topics" '[.] | length' "$want_topics"
  check "$topics" "[.] | map(select(.spec.replicas != $want_rf)) | length" 0
  check "$topics" '[.] | map(select(.spec.config."cleanup.policy" == "compact") | .spec.topicName) | sort | join(",")' merchant.events,rate.latest
  check "$topics" '[.] | map(select(.spec.config."cleanup.policy" == "delete" and .spec.config."retention.ms" != 604800000)) | length' 0
  check "$topics" "$(topic_partitions chain.blocks)" 1
  check "$topics" "$(topic_partitions chain.transfers)" 6
  check "$topics" "$(topic_partitions invoice.events)" 6
  check "$topics" "$(topic_partitions merchant.events)" 3
  if [ "$profile" = full ]; then
    check "$topics" "$(topic_partitions ledger.events)" 3
    check "$topics" "$(topic_partitions webhook.dlq)" 1
    check "$topics" '[.] | map(select(.spec.topicName == "webhook.retry.30m")) | .[0].metadata.name' webhook-retry-30m
  fi
done

[ "$(cd "$tmp/full" && find . -type f -not -path './platform/strimzi/*' | wc -l | tr -d ' ')" = 0 ] || fail "files outside platform/strimzi"

echo "strimzi role: ok"

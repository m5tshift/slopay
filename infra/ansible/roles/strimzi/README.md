# Role strimzi

Generates the Strimzi operator values and the `Kafka`, `KafkaNodePool`, `KafkaTopic` manifests (SRS INF-2.3, 4.1.5)
into `platform/strimzi/` of a `weigate-gitops` clone. ArgoCD applies them; the role installs nothing in the cluster
(ADR 0004). The target directory is rewritten completely on every run.

## Run

```sh
cd infra/ansible
ansible-playbook strimzi.yml -e strimzi_gitops_dir=<path to the weigate-gitops clone> -e strimzi_profile=minimal
```

Then commit the result in `weigate-gitops` through a PR.

## Variables

See `defaults/main.yml`. `strimzi_profile` is `minimal` (default: one broker, RF 1, `min.insync.replicas` 1, SRS 6.4)
or `full` (three brokers, RF 3, `min.insync.replicas` 2, all topics of SRS 4.1.5).
Both profiles enable the JMX Exporter (ConfigMap `kafka-metrics`) and the Kafka Exporter, which feed the PodMonitor
`strimzi-kafka`, the Kafka dashboard and the alerts `KafkaConsumerLagHigh` and `WebhookDLQGrowing`.

## Test

`make ansible-test` generates both profiles into a temporary directory and checks them (CI job `ansible`).

{{/*
Default values of a Weigate component. The consumer's values are merged on top of them
(weigate-lib.values). Optional features are off: a minimal stand has no Istio, External
Secrets Operator or ServiceMonitor CRDs.
*/}}
{{- define "weigate-lib.defaults" -}}
nameOverride: ""
image:
  repository: ""
  tag: ""
  pullPolicy: IfNotPresent
imagePullSecrets: []
replicaCount: 2
command: []
args: []
service:
  type: ClusterIP
  port: 8080
# Required: requests and limits for cpu and memory (SRS 6.2).
resources: {}
probes:
  startup:
    periodSeconds: 2
    timeoutSeconds: 3
    failureThreshold: 60
  liveness:
    periodSeconds: 10
    timeoutSeconds: 3
    failureThreshold: 3
  readiness:
    periodSeconds: 5
    timeoutSeconds: 3
    failureThreshold: 3
terminationGracePeriodSeconds: 30
podAnnotations: {}
podLabels: {}
nodeSelector: {}
tolerations: []
affinity: {}
topologySpread:
  enabled: true
serviceAccount:
  create: true
  name: ""
hpa:
  enabled: false
  minReplicas: 2
  maxReplicas: 4
  targetCPUUtilizationPercentage: 70
  targetMemoryUtilizationPercentage: 0
pdb:
  enabled: true
  minAvailable: 1
serviceMonitor:
  enabled: false
  interval: 30s
  scrapeTimeout: 10s
  labels: {}
leader:
  enabled: false
telemetry:
  otlpEndpoint: ""
  insecure: true
  traceSampleRatio: ""
env: {}
extraEnv: []
secretStore:
  kind: ClusterSecretStore
  name: openbao
secrets: []
kafka:
  enabled: false
  prefix: KAFKA
  brokers: []
postgres:
  enabled: false
  prefix: POSTGRES
  url: ""
  password:
    secret: ""
    key: password
  maxConns: 0
mongodb:
  enabled: false
  prefix: MONGODB
  uri: ""
  username: ""
  password:
    secret: ""
    key: password
  authSource: ""
  database: ""
valkey:
  enabled: false
  prefix: VALKEY
  addrs: []
  username: ""
  password:
    secret: ""
    key: password
istio:
  destinationRule:
    enabled: false
    outlierDetection:
      consecutive5xxErrors: 5
      interval: 10s
      baseEjectionTime: 30s
      maxEjectionPercent: 50
    connectionPool: {}
  virtualService:
    enabled: false
    timeout: ""
    retries:
      attempts: 0
      perTryTimeout: ""
      retryOn: 5xx,reset,connect-failure,refused-stream
    fault:
      enabled: false
      delay:
        percentage: 0
        fixedDelay: 5s
      abort:
        percentage: 0
        httpStatus: 503
{{- end -}}

{{/* Defaults merged with the consumer's values, as YAML. Use: $v := include "weigate-lib.values" . | fromYaml */}}
{{- define "weigate-lib.values" -}}
{{- $defaults := include "weigate-lib.defaults" . | fromYaml -}}
{{- toYaml (mergeOverwrite $defaults (deepCopy .Values)) -}}
{{- end -}}

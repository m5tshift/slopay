{{/*
ExternalSecret for each entry of secrets with externalSecret.enabled (SRS INF-2.4). Without it
the Secret is expected to exist already; it is mounted either way.
*/}}
{{- define "weigate-lib.externalsecrets" -}}
{{- range .v.secrets }}
{{- $es := .externalSecret | default dict }}
{{- if $es.enabled }}
---
apiVersion: external-secrets.io/v1
kind: ExternalSecret
metadata:
  name: {{ include "weigate-lib.secretName" (dict "ctx" $ "s" .) }}
  labels:
    {{- include "weigate-lib.labels" $ | nindent 4 }}
spec:
  # Not more than 1 min, so a rotated password reaches the pod quickly (SRS INF-2.4).
  refreshInterval: {{ $es.refreshInterval | default "1m" }}
  secretStoreRef:
    kind: {{ ($es.secretStore | default dict).kind | default $.v.secretStore.kind }}
    name: {{ ($es.secretStore | default dict).name | default $.v.secretStore.name }}
  target:
    name: {{ include "weigate-lib.secretName" (dict "ctx" $ "s" .) }}
    creationPolicy: Owner
  data:
    {{- required (printf "secrets[%s].externalSecret.data is required" .name) $es.data | toYaml | nindent 4 }}
{{- end }}
{{- end }}
{{- end -}}

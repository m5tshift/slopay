{{/* Outlier detection for callers of this service (SRS 4.1.9, INF-3.1). */}}
{{- define "weigate-lib.destinationrule" -}}
{{- with .v.istio.destinationRule }}
{{- if .enabled }}
---
apiVersion: networking.istio.io/v1
kind: DestinationRule
metadata:
  name: {{ include "weigate-lib.fullname" $ }}
  labels:
    {{- include "weigate-lib.labels" $ | nindent 4 }}
spec:
  host: {{ include "weigate-lib.fullname" $ }}.{{ $.root.Release.Namespace }}.svc.cluster.local
  trafficPolicy:
    outlierDetection:
      {{- toYaml .outlierDetection | nindent 6 }}
    {{- with .connectionPool }}
    connectionPool:
      {{- toYaml . | nindent 6 }}
    {{- end }}
{{- end }}
{{- end }}
{{- end -}}

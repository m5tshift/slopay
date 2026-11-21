{{- define "weigate-lib.servicemonitor" -}}
{{- with .v.serviceMonitor }}
{{- if .enabled }}
---
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: {{ include "weigate-lib.fullname" $ }}
  labels:
    {{- include "weigate-lib.labels" $ | nindent 4 }}
    {{- with .labels }}
    {{- toYaml . | nindent 4 }}
    {{- end }}
spec:
  selector:
    matchLabels:
      {{- include "weigate-lib.selectorLabels" $ | nindent 6 }}
  endpoints:
    - port: http
      path: /metrics
      interval: {{ .interval }}
      scrapeTimeout: {{ .scrapeTimeout }}
{{- end }}
{{- end }}
{{- end -}}

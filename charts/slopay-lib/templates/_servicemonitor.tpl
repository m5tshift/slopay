{{- define "slopay-lib.servicemonitor" -}}
{{- with .v.serviceMonitor }}
{{- if .enabled }}
---
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: {{ include "slopay-lib.fullname" $ }}
  labels:
    {{- include "slopay-lib.labels" $ | nindent 4 }}
    {{- with .labels }}
    {{- toYaml . | nindent 4 }}
    {{- end }}
spec:
  selector:
    matchLabels:
      {{- include "slopay-lib.selectorLabels" $ | nindent 6 }}
  endpoints:
    - port: http
      path: /metrics
      interval: {{ .interval }}
      scrapeTimeout: {{ .scrapeTimeout }}
{{- end }}
{{- end }}
{{- end -}}

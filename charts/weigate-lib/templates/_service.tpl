{{- define "weigate-lib.service" -}}
---
apiVersion: v1
kind: Service
metadata:
  name: {{ include "weigate-lib.fullname" . }}
  labels:
    {{- include "weigate-lib.labels" . | nindent 4 }}
spec:
  type: {{ .v.service.type }}
  selector:
    {{- include "weigate-lib.selectorLabels" . | nindent 4 }}
  ports:
    # API, /health/* and /metrics share one port (pkg/telemetry Register).
    - name: http
      port: {{ .v.service.port }}
      targetPort: http
      protocol: TCP
      appProtocol: http
{{- end -}}

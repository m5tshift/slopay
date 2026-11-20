{{- define "slopay-lib.service" -}}
---
apiVersion: v1
kind: Service
metadata:
  name: {{ include "slopay-lib.fullname" . }}
  labels:
    {{- include "slopay-lib.labels" . | nindent 4 }}
spec:
  type: {{ .v.service.type }}
  selector:
    {{- include "slopay-lib.selectorLabels" . | nindent 4 }}
  ports:
    # API, /health/* and /metrics share one port (pkg/telemetry Register).
    - name: http
      port: {{ .v.service.port }}
      targetPort: http
      protocol: TCP
      appProtocol: http
{{- end -}}

{{- define "weigate-lib.hpa" -}}
{{- with .v.hpa }}
{{- if .enabled }}
---
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: {{ include "weigate-lib.fullname" $ }}
  labels:
    {{- include "weigate-lib.labels" $ | nindent 4 }}
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: {{ include "weigate-lib.fullname" $ }}
  minReplicas: {{ .minReplicas }}
  maxReplicas: {{ .maxReplicas }}
  metrics:
    - type: Resource
      resource:
        name: cpu
        target:
          type: Utilization
          averageUtilization: {{ .targetCPUUtilizationPercentage }}
    {{- if .targetMemoryUtilizationPercentage }}
    - type: Resource
      resource:
        name: memory
        target:
          type: Utilization
          averageUtilization: {{ .targetMemoryUtilizationPercentage }}
    {{- end }}
{{- end }}
{{- end }}
{{- end -}}

{{- define "slopay-lib.pdb" -}}
{{- if .v.pdb.enabled }}
---
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: {{ include "slopay-lib.fullname" . }}
  labels:
    {{- include "slopay-lib.labels" . | nindent 4 }}
spec:
  minAvailable: {{ .v.pdb.minAvailable }}
  selector:
    matchLabels:
      {{- include "slopay-lib.selectorLabels" . | nindent 6 }}
{{- end }}
{{- end -}}

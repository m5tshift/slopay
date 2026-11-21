{{- define "weigate-lib.pdb" -}}
{{- if .v.pdb.enabled }}
---
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: {{ include "weigate-lib.fullname" . }}
  labels:
    {{- include "weigate-lib.labels" . | nindent 4 }}
spec:
  minAvailable: {{ .v.pdb.minAvailable }}
  selector:
    matchLabels:
      {{- include "weigate-lib.selectorLabels" . | nindent 6 }}
{{- end }}
{{- end -}}

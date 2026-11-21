{{- define "weigate-lib.serviceaccount" -}}
{{- if .v.serviceAccount.create }}
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: {{ include "weigate-lib.serviceAccountName" . }}
  labels:
    {{- include "weigate-lib.labels" . | nindent 4 }}
automountServiceAccountToken: false
{{- end }}
{{- end -}}

{{- define "slopay-lib.serviceaccount" -}}
{{- if .v.serviceAccount.create }}
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: {{ include "slopay-lib.serviceAccountName" . }}
  labels:
    {{- include "slopay-lib.labels" . | nindent 4 }}
automountServiceAccountToken: false
{{- end }}
{{- end -}}

{{/* Agent config of the token sidecar (openbao.enabled): Kubernetes auth, token to a file sink. */}}
{{- define "weigate-lib.openbaoagent" -}}
{{- if .v.openbao.enabled }}
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ include "weigate-lib.fullname" . }}-openbao-agent
  labels:
    {{- include "weigate-lib.labels" . | nindent 4 }}
data:
  agent.hcl: |
    vault {
      address = {{ .v.openbao.addr | quote }}
    }
    auto_auth {
      method "kubernetes" {
        mount_path = {{ .v.openbao.authPath | quote }}
        config = {
          role = {{ default (include "weigate-lib.fullname" .) .v.openbao.role | quote }}
        }
      }
      sink "file" {
        config = {
          path = "/var/run/secrets/weigate/openbao/token"
          mode = 0440
        }
      }
    }
{{- end }}
{{- end -}}

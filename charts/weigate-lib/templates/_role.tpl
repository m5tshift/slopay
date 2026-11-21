{{/* Access to the Lease of pkg/leader (SRS 4.1.3); the Lease name is LEADER_LEASE_NAME. */}}
{{- define "weigate-lib.role" -}}
{{- if .v.leader.enabled }}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: {{ include "weigate-lib.fullname" . }}-leader
  labels:
    {{- include "weigate-lib.labels" . | nindent 4 }}
rules:
  - apiGroups: ["coordination.k8s.io"]
    resources: ["leases"]
    verbs: ["create"]
  - apiGroups: ["coordination.k8s.io"]
    resources: ["leases"]
    resourceNames: [{{ include "weigate-lib.fullname" . | quote }}]
    verbs: ["get", "update"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: {{ include "weigate-lib.fullname" . }}-leader
  labels:
    {{- include "weigate-lib.labels" . | nindent 4 }}
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: {{ include "weigate-lib.fullname" . }}-leader
subjects:
  - kind: ServiceAccount
    name: {{ include "weigate-lib.serviceAccountName" . }}
    namespace: {{ .root.Release.Namespace }}
{{- end }}
{{- end -}}

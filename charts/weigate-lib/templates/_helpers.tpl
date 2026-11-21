{{/*
Every template below takes a context dict: root is the consumer chart's root context,
v the merged values (weigate-lib.values).
*/}}

{{- define "weigate-lib.fullname" -}}
{{- default .root.Chart.Name .v.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "weigate-lib.selectorLabels" -}}
app.kubernetes.io/name: {{ include "weigate-lib.fullname" . }}
app.kubernetes.io/instance: {{ .root.Release.Name }}
{{- end -}}

{{- define "weigate-lib.labels" -}}
{{ include "weigate-lib.selectorLabels" . }}
app.kubernetes.io/version: {{ .v.image.tag | quote }}
app.kubernetes.io/part-of: weigate
app.kubernetes.io/managed-by: {{ .root.Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .root.Chart.Name .root.Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end -}}

{{- define "weigate-lib.serviceAccountName" -}}
{{- if .v.serviceAccount.create -}}
{{- default (include "weigate-lib.fullname" .) .v.serviceAccount.name -}}
{{- else -}}
{{- default "default" .v.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{/* Directory where the secret named by the logical name .name is mounted. */}}
{{- define "weigate-lib.secretDir" -}}
{{- printf "/var/run/secrets/weigate/%s" . -}}
{{- end -}}

{{/* Kubernetes Secret name of an entry of .v.secrets; args: dict "ctx" ctx "s" entry. */}}
{{- define "weigate-lib.secretName" -}}
{{- default (printf "%s-%s" (include "weigate-lib.fullname" .ctx) .s.name) .s.secretName -}}
{{- end -}}

{{/*
Path of a password file for a store; args: dict "ctx" ctx "store" name "password" {secret, key}.
Fails when the secret is not declared in .v.secrets.
*/}}
{{- define "weigate-lib.passwordFile" -}}
{{- $name := required (printf "%s.password.secret is required" .store) .password.secret -}}
{{- $found := false -}}
{{- range .ctx.v.secrets -}}
{{- if eq .name $name -}}{{- $found = true -}}{{- end -}}
{{- end -}}
{{- if not $found -}}
{{- fail (printf "%s.password.secret %q is not declared in secrets" .store $name) -}}
{{- end -}}
{{- printf "%s/%s" (include "weigate-lib.secretDir" $name) (required (printf "%s.password.key is required" .store) .password.key) -}}
{{- end -}}

{{/* Renders every resource of the component. Usage in a consumer chart: {{ include "weigate-lib.all" . }} */}}
{{- define "weigate-lib.all" -}}
{{- $ctx := dict "root" . "v" (include "weigate-lib.values" . | fromYaml) -}}
{{- include "weigate-lib.validate" $ctx -}}
{{- include "weigate-lib.serviceaccount" $ctx }}
{{ include "weigate-lib.deployment" $ctx }}
{{ include "weigate-lib.service" $ctx }}
{{- include "weigate-lib.hpa" $ctx }}
{{- include "weigate-lib.pdb" $ctx }}
{{- include "weigate-lib.servicemonitor" $ctx }}
{{- include "weigate-lib.role" $ctx }}
{{- include "weigate-lib.externalsecrets" $ctx }}
{{- include "weigate-lib.destinationrule" $ctx }}
{{- include "weigate-lib.virtualservice" $ctx }}
{{- end -}}

{{- define "weigate-lib.validate" -}}
{{- $_ := required "image.repository is required" .v.image.repository -}}
{{- $_ := required "image.tag is required" .v.image.tag -}}
{{- $r := .v.resources | default dict -}}
{{- $req := $r.requests | default dict -}}
{{- $lim := $r.limits | default dict -}}
{{- $_ := required "resources.requests.cpu is required (SRS 6.2)" $req.cpu -}}
{{- $_ := required "resources.requests.memory is required (SRS 6.2)" $req.memory -}}
{{- $_ := required "resources.limits.cpu is required (SRS 6.2)" $lim.cpu -}}
{{- $_ := required "resources.limits.memory is required (SRS 6.2)" $lim.memory -}}
{{- range .v.secrets -}}
{{- $_ := required "secrets[].name is required" .name -}}
{{- end -}}
{{- end -}}

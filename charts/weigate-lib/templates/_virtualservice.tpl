{{/*
Timeout and retries applied by callers of this service (SRS 4.1.9). Fault injection is off
unless istio.virtualService.fault.enabled, which acceptance test 8.2г (SRS 8) switches on.
*/}}
{{- define "weigate-lib.virtualservice" -}}
{{- with .v.istio.virtualService }}
{{- if .enabled }}
{{- $host := printf "%s.%s.svc.cluster.local" (include "weigate-lib.fullname" $) $.root.Release.Namespace }}
---
apiVersion: networking.istio.io/v1
kind: VirtualService
metadata:
  name: {{ include "weigate-lib.fullname" $ }}
  labels:
    {{- include "weigate-lib.labels" $ | nindent 4 }}
spec:
  hosts:
    - {{ $host }}
  http:
    - route:
        - destination:
            host: {{ $host }}
            port:
              number: {{ $.v.service.port }}
      {{- if .timeout }}
      timeout: {{ .timeout }}
      {{- end }}
      {{- if .retries.attempts }}
      retries:
        attempts: {{ .retries.attempts }}
        {{- if .retries.perTryTimeout }}
        perTryTimeout: {{ .retries.perTryTimeout }}
        {{- end }}
        retryOn: {{ .retries.retryOn | quote }}
      {{- end }}
      {{- if .fault.enabled }}
      {{- if not (or .fault.delay.percentage .fault.abort.percentage) }}
      {{- fail "istio.virtualService.fault: set delay.percentage or abort.percentage" }}
      {{- end }}
      fault:
        {{- if .fault.delay.percentage }}
        delay:
          percentage:
            value: {{ .fault.delay.percentage }}
          fixedDelay: {{ .fault.delay.fixedDelay }}
        {{- end }}
        {{- if .fault.abort.percentage }}
        abort:
          percentage:
            value: {{ .fault.abort.percentage }}
          httpStatus: {{ .fault.abort.httpStatus }}
        {{- end }}
      {{- end }}
{{- end }}
{{- end }}
{{- end -}}

{{- define "slopay-lib.env" -}}
{{- $v := .v -}}
- name: POD_NAME
  valueFrom:
    fieldRef:
      fieldPath: metadata.name
- name: POD_NAMESPACE
  valueFrom:
    fieldRef:
      fieldPath: metadata.namespace
{{- if $v.leader.enabled }}
- name: LEADER_LEASE_NAME
  value: {{ include "slopay-lib.fullname" . | quote }}
{{- end }}
- name: OTEL_SERVICE_NAME
  value: {{ include "slopay-lib.fullname" . | quote }}
{{- with $v.telemetry }}
{{- if .otlpEndpoint }}
- name: OTEL_EXPORTER_OTLP_ENDPOINT
  value: {{ .otlpEndpoint | quote }}
- name: OTEL_EXPORTER_OTLP_INSECURE
  value: {{ .insecure | toString | quote }}
{{- end }}
{{- if ne (toString .traceSampleRatio) "" }}
- name: OTEL_TRACES_SAMPLER_ARG
  value: {{ .traceSampleRatio | toString | quote }}
{{- end }}
{{- end }}
{{- with $v.kafka }}
{{- if .enabled }}
- name: {{ .prefix }}_BROKERS
  value: {{ required "kafka.brokers is required" .brokers | join "," | quote }}
{{- end }}
{{- end }}
{{- with $v.postgres }}
{{- if .enabled }}
- name: {{ .prefix }}_URL
  value: {{ required "postgres.url is required" .url | quote }}
- name: {{ .prefix }}_PASSWORD_FILE
  value: {{ include "slopay-lib.passwordFile" (dict "ctx" $ "store" "postgres" "password" .password) | quote }}
{{- if .maxConns }}
- name: {{ .prefix }}_MAX_CONNS
  value: {{ .maxConns | toString | quote }}
{{- end }}
{{- end }}
{{- end }}
{{- with $v.mongodb }}
{{- if .enabled }}
- name: {{ .prefix }}_URI
  value: {{ required "mongodb.uri is required" .uri | quote }}
- name: {{ .prefix }}_USERNAME
  value: {{ required "mongodb.username is required" .username | quote }}
- name: {{ .prefix }}_PASSWORD_FILE
  value: {{ include "slopay-lib.passwordFile" (dict "ctx" $ "store" "mongodb" "password" .password) | quote }}
- name: {{ .prefix }}_DATABASE
  value: {{ required "mongodb.database is required" .database | quote }}
{{- if .authSource }}
- name: {{ .prefix }}_AUTH_SOURCE
  value: {{ .authSource | quote }}
{{- end }}
{{- end }}
{{- end }}
{{- with $v.valkey }}
{{- if .enabled }}
- name: {{ .prefix }}_ADDRS
  value: {{ required "valkey.addrs is required" .addrs | join "," | quote }}
{{- if .username }}
- name: {{ .prefix }}_USERNAME
  value: {{ .username | quote }}
{{- end }}
{{- if .password.secret }}
- name: {{ .prefix }}_PASSWORD_FILE
  value: {{ include "slopay-lib.passwordFile" (dict "ctx" $ "store" "valkey" "password" .password) | quote }}
{{- end }}
{{- end }}
{{- end }}
{{- range $name := keys $v.env | sortAlpha }}
- name: {{ $name }}
  value: {{ index $v.env $name | toString | quote }}
{{- end }}
{{- with $v.extraEnv }}
{{ toYaml . }}
{{- end }}
{{- end -}}

{{- define "slopay-lib.deployment" -}}
{{- $v := .v -}}
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: {{ include "slopay-lib.fullname" . }}
  labels:
    {{- include "slopay-lib.labels" . | nindent 4 }}
spec:
  {{- if not $v.hpa.enabled }}
  replicas: {{ $v.replicaCount }}
  {{- end }}
  selector:
    matchLabels:
      {{- include "slopay-lib.selectorLabels" . | nindent 6 }}
  template:
    metadata:
      labels:
        {{- include "slopay-lib.labels" . | nindent 8 }}
        {{- with $v.podLabels }}
        {{- toYaml . | nindent 8 }}
        {{- end }}
      {{- with $v.podAnnotations }}
      annotations:
        {{- toYaml . | nindent 8 }}
      {{- end }}
    spec:
      serviceAccountName: {{ include "slopay-lib.serviceAccountName" . }}
      # The Kubernetes API is needed only for the leader Lease (pkg/leader).
      automountServiceAccountToken: {{ $v.leader.enabled }}
      terminationGracePeriodSeconds: {{ $v.terminationGracePeriodSeconds }}
      {{- with $v.imagePullSecrets }}
      imagePullSecrets:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      securityContext:
        runAsNonRoot: true
        runAsUser: 65532
        runAsGroup: 65532
        fsGroup: 65532
        seccompProfile:
          type: RuntimeDefault
      containers:
        - name: {{ include "slopay-lib.fullname" . }}
          image: "{{ $v.image.repository }}:{{ $v.image.tag }}"
          imagePullPolicy: {{ $v.image.pullPolicy }}
          {{- with $v.command }}
          command:
            {{- toYaml . | nindent 12 }}
          {{- end }}
          {{- with $v.args }}
          args:
            {{- toYaml . | nindent 12 }}
          {{- end }}
          ports:
            - name: http
              containerPort: {{ $v.service.port }}
              protocol: TCP
          env:
            {{- include "slopay-lib.env" . | nindent 12 }}
          startupProbe:
            httpGet:
              path: /health/live
              port: http
            {{- toYaml $v.probes.startup | nindent 12 }}
          livenessProbe:
            httpGet:
              path: /health/live
              port: http
            {{- toYaml $v.probes.liveness | nindent 12 }}
          readinessProbe:
            httpGet:
              path: /health/ready
              port: http
            {{- toYaml $v.probes.readiness | nindent 12 }}
          resources:
            {{- toYaml $v.resources | nindent 12 }}
          securityContext:
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: true
            capabilities:
              drop: ["ALL"]
          {{- /* Secrets are mounted as directories, not subPath: kubelet updates the files after rotation (SRS INF-2.4). */}}
          volumeMounts:
            - name: tmp
              mountPath: /tmp
            {{- range $v.secrets }}
            - name: secret-{{ .name }}
              mountPath: {{ include "slopay-lib.secretDir" .name }}
              readOnly: true
            {{- end }}
      volumes:
        - name: tmp
          emptyDir:
            sizeLimit: 64Mi
        {{- range $v.secrets }}
        - name: secret-{{ .name }}
          secret:
            secretName: {{ include "slopay-lib.secretName" (dict "ctx" $ "s" .) }}
            defaultMode: 0440
        {{- end }}
      {{- if $v.topologySpread.enabled }}
      topologySpreadConstraints:
        - maxSkew: 1
          topologyKey: kubernetes.io/hostname
          whenUnsatisfiable: ScheduleAnyway
          labelSelector:
            matchLabels:
              {{- include "slopay-lib.selectorLabels" . | nindent 14 }}
      {{- end }}
      {{- with $v.nodeSelector }}
      nodeSelector:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      {{- with $v.tolerations }}
      tolerations:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      {{- with $v.affinity }}
      affinity:
        {{- toYaml . | nindent 8 }}
      {{- end }}
{{- end -}}

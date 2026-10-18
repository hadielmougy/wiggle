{{- define "wiggle-coordinator.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "wiggle-coordinator.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "wiggle-coordinator.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "wiggle-coordinator.labels" -}}
helm.sh/chart: {{ include "wiggle-coordinator.chart" . }}
{{ include "wiggle-coordinator.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/component: coordinator
{{- end -}}

{{- define "wiggle-coordinator.selectorLabels" -}}
app.kubernetes.io/name: {{ include "wiggle-coordinator.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "wiggle-coordinator.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "wiggle-coordinator.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "wiggle-coordinator.image" -}}
{{- $reg := .Values.image.registry -}}
{{- $repo := .Values.image.repository -}}
{{- if .Values.image.digest -}}
{{- printf "%s/%s@%s" $reg $repo .Values.image.digest -}}
{{- else -}}
{{- printf "%s/%s:%s" $reg $repo (default .Chart.AppVersion .Values.image.tag) -}}
{{- end -}}
{{- end -}}

{{- define "wiggle-coordinator.jdbcSecretName" -}}
{{- if .Values.store.jdbc.existingSecret -}}
{{- .Values.store.jdbc.existingSecret -}}
{{- else -}}
{{- printf "%s-jdbc" (include "wiggle-coordinator.fullname" .) -}}
{{- end -}}
{{- end -}}

{{/* The container env, shared by the JDBC Deployment and the Ratis StatefulSet. */}}
{{- define "wiggle-coordinator.env" -}}
- name: WIGGLE_ROLE
  value: coordinator
- name: WIGGLE_NODE_NAME
  valueFrom:
    fieldRef:
      fieldPath: metadata.name
- name: WIGGLE_PORT
  value: {{ .Values.service.grpcPort | quote }}
{{- if eq .Values.store.backend "jdbc" }}
- name: WIGGLE_COORD_STORE
  valueFrom:
    secretKeyRef:
      name: {{ include "wiggle-coordinator.jdbcSecretName" . }}
      key: url
- name: WIGGLE_COORD_JDBC_USER
  valueFrom:
    secretKeyRef:
      name: {{ include "wiggle-coordinator.jdbcSecretName" . }}
      key: user
- name: WIGGLE_COORD_JDBC_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ include "wiggle-coordinator.jdbcSecretName" . }}
      key: password
- name: WIGGLE_COORD_JDBC_POOL
  value: {{ .Values.store.jdbc.poolSize | quote }}
{{- else if eq .Values.store.backend "ratis" }}
- name: WIGGLE_COORD_STORE
  value: "ratis://{{ .Values.store.ratis.dataDir }}"
{{- end }}
{{- range $k, $v := .Values.env }}
- name: {{ $k }}
  value: {{ $v | quote }}
{{- end }}
{{- end -}}

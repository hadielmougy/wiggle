{{/* Chart name (overridable by nameOverride). */}}
{{- define "wiggle.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* Fully-qualified app name (release-scoped). */}}
{{- define "wiggle.fullname" -}}
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

{{- define "wiggle.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "wiggle.labels" -}}
helm.sh/chart: {{ include "wiggle.chart" . }}
{{ include "wiggle.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "wiggle.selectorLabels" -}}
app.kubernetes.io/name: {{ include "wiggle.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "wiggle.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "wiggle.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{/* Full image reference: registry/repository pinned by digest if given, else by tag/appVersion. */}}
{{- define "wiggle.image" -}}
{{- $reg := .Values.image.registry -}}
{{- $repo := .Values.image.repository -}}
{{- if .Values.image.digest -}}
{{- printf "%s/%s@%s" $reg $repo .Values.image.digest -}}
{{- else -}}
{{- printf "%s/%s:%s" $reg $repo (default .Chart.AppVersion .Values.image.tag) -}}
{{- end -}}
{{- end -}}

{{/* Name of the Secret holding JDBC credentials (existing one, or the one we create). */}}
{{- define "wiggle.jdbcSecretName" -}}
{{- if .Values.storage.jdbc.existingSecret -}}
{{- .Values.storage.jdbc.existingSecret -}}
{{- else -}}
{{- printf "%s-jdbc" (include "wiggle.fullname" .) -}}
{{- end -}}
{{- end -}}

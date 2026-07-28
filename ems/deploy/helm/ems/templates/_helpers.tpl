{{- define "ems.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "ems.fullname" -}}
{{- printf "%s-%s" .Release.Name (include "ems.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "ems.labels" -}}
app.kubernetes.io/name: {{ include "ems.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end -}}

{{- define "ems.selectorLabels" -}}
app.kubernetes.io/name: {{ include "ems.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/*
Labels for resources this wrapper adds. `tier`/`release` match the official chart's own labels so
`kubectl get all -l release=airflow-v3` shows the whole stack.
*/}}
{{- define "airflow-v3.labels" -}}
tier: airflow
release: {{ .Release.Name }}
chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
heritage: {{ .Release.Service }}
{{- end }}

{{/*
The Airflow image, resolved exactly as the official chart's "airflow_image" helper resolves it, so
the wrapper Jobs always run the same image as the components (§5.4).
*/}}
{{- define "airflow-v3.image" -}}
{{- $a := .Values.airflow -}}
{{- $repository := $a.images.airflow.repository | default $a.defaultAirflowRepository -}}
{{- if $a.images.airflow.digest -}}
{{- printf "%s@%s" $repository $a.images.airflow.digest -}}
{{- else -}}
{{- printf "%s:%s" $repository ($a.images.airflow.tag | default $a.defaultAirflowTag) -}}
{{- end -}}
{{- end }}

{{/*
Pod-level settings shared by the wrapper Jobs: same nodepool, pull secrets and UID/GID as the
Airflow components (§6.1, §6.6).
*/}}
{{- define "airflow-v3.podSpec" -}}
{{- $a := .Values.airflow -}}
automountServiceAccountToken: false
securityContext:
  runAsUser: {{ $a.uid }}
  runAsGroup: {{ $a.gid }}
  fsGroup: {{ $a.gid }}
{{- with $a.nodeSelector }}
nodeSelector:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- with $a.tolerations }}
tolerations:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- with $a.imagePullSecrets }}
imagePullSecrets:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- end }}

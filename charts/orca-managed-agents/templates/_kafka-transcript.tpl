{{/* Shared only by registry, harness and observability-exporter ConfigMaps. */}}
{{- define "orca-managed-agents.kafkaTranscriptEncoding" -}}
{{- $kafka := .Values.transcriptStore.kafka -}}
{{- $sr := default (dict) $kafka.schemaRegistry -}}
KAFKA_TRANSCRIPT_ENCODING: {{ default "raw" $kafka.encoding | quote }}
{{- if $sr.url }}
KAFKA_SCHEMA_REGISTRY_URL: {{ $sr.url | quote }}
KAFKA_SCHEMA_REGISTRY_SUBJECT: {{ $sr.subject | quote }}
KAFKA_SCHEMA_REGISTRY_AUTO_REGISTER: {{ $sr.autoRegister | quote }}
KAFKA_SCHEMA_REGISTRY_AUTH_MODE: {{ $sr.authMode | quote }}
KAFKA_SCHEMA_REGISTRY_REQUEST_TIMEOUT_MS: {{ $sr.requestTimeoutMs | quote }}
{{- with $sr.caFile }}
KAFKA_SCHEMA_REGISTRY_CA_FILE: {{ . | quote }}
{{- end }}
{{- with $sr.certFile }}
KAFKA_SCHEMA_REGISTRY_CERT_FILE: {{ . | quote }}
{{- end }}
{{- with $sr.keyFile }}
KAFKA_SCHEMA_REGISTRY_KEY_FILE: {{ . | quote }}
{{- end }}
{{- end }}
{{- end }}

{{- define "orca-managed-agents.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "orca-managed-agents.fullname" -}}
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

{{- define "orca-managed-agents.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" -}}
{{- end -}}

{{- define "orca-managed-agents.labels" -}}
helm.sh/chart: {{ include "orca-managed-agents.chart" . }}
app.kubernetes.io/name: {{ include "orca-managed-agents.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: orca-managed-agents
{{- end -}}

{{- define "orca-managed-agents.selectorLabels" -}}
app.kubernetes.io/name: {{ include "orca-managed-agents.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "orca-managed-agents.registryServiceAccountName" -}}
{{- if .Values.registry.serviceAccount.create -}}
{{- default (printf "%s-registry" (include "orca-managed-agents.fullname" .)) .Values.registry.serviceAccount.name -}}
{{- else -}}
{{- .Values.registry.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "orca-managed-agents.harnessServiceAccountName" -}}
{{- if .Values.harness.serviceAccount.create -}}
{{- default (printf "%s-harness" (include "orca-managed-agents.fullname" .)) .Values.harness.serviceAccount.name -}}
{{- else -}}
{{- .Values.harness.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "orca-managed-agents.aiGatewayServiceAccountName" -}}
{{- if .Values.aiGateway.serviceAccount.create -}}
{{- default (printf "%s-ai-gateway" (include "orca-managed-agents.fullname" .)) .Values.aiGateway.serviceAccount.name -}}
{{- else -}}
{{- .Values.aiGateway.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "orca-managed-agents.observabilityExporterServiceAccountName" -}}
{{- $observabilityExporter := default (dict) .Values.observabilityExporter -}}
{{- $serviceAccount := default (dict) (dig "serviceAccount" (dict) $observabilityExporter) -}}
{{- if (dig "create" true $serviceAccount) -}}
{{- default (printf "%s-observability-exporter" (include "orca-managed-agents.fullname" .)) (dig "name" "" $serviceAccount) -}}
{{- else -}}
{{- dig "name" "" $serviceAccount -}}
{{- end -}}
{{- end -}}

{{- define "orca-managed-agents.toolsetServiceAccountName" -}}
{{- $toolset := default (dict) .Values.toolset -}}
{{- $serviceAccount := dig "serviceAccount" (dict) $toolset -}}
{{- if (dig "create" false $serviceAccount) -}}
{{- default (include "orca-managed-agents.toolsetName" .) (get $serviceAccount "name") -}}
{{- else -}}
{{- get $serviceAccount "name" -}}
{{- end -}}
{{- end -}}

{{/*
Name of the Secret holding chart-managed credentials. Empty when the chart
neither creates one nor points at an existing one — every template that reads it
MUST treat an empty result as "no Secret" so pods never reference an object that
was not created. Credentials then come from `<component>.secretKeyRefs` or
`<component>.extraEnvFrom` (bootstrap-delivered Secrets) instead.
*/}}
{{- define "orca-managed-agents.secretName" -}}
{{- if .Values.secrets.existingSecret -}}
{{- .Values.secrets.existingSecret -}}
{{- else if .Values.secrets.create -}}
{{- printf "%s-secrets" (include "orca-managed-agents.fullname" .) -}}
{{- end -}}
{{- end -}}

{{- define "orca-managed-agents.databaseSecretName" -}}
{{- default (include "orca-managed-agents.secretName" .) .Values.external.databases.existingSecret -}}
{{- end -}}

{{- define "orca-managed-agents.secretStoreName" -}}
{{- default (printf "%s-secret-store" (include "orca-managed-agents.fullname" .)) .Values.secretStore.kubernetes.secretName -}}
{{- end -}}

{{/*
Workload names. Each component takes a `nameOverride` so in-cluster DNS names
can be pinned to what existing callers already dial (an inherited `registry`
Service, an istio VirtualService host, a hard-coded agent-provider URL). The
internal and admin Registry Services derive from the same base name.
*/}}
{{- define "orca-managed-agents.registryServiceName" -}}
{{- default (printf "%s-registry" (include "orca-managed-agents.fullname" .)) .Values.registry.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "orca-managed-agents.registryInternalServiceName" -}}
{{- printf "%s-internal" (include "orca-managed-agents.registryServiceName" . | trunc 54 | trimSuffix "-") -}}
{{- end -}}

{{- define "orca-managed-agents.registryAdminServiceName" -}}
{{- printf "%s-admin" (include "orca-managed-agents.registryServiceName" . | trunc 57 | trimSuffix "-") -}}
{{- end -}}

{{- define "orca-managed-agents.harnessServiceName" -}}
{{- default (printf "%s-harness" (include "orca-managed-agents.fullname" .)) .Values.harness.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "orca-managed-agents.aiGatewayServiceName" -}}
{{- default (printf "%s-ai-gateway" (include "orca-managed-agents.fullname" .)) .Values.aiGateway.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "orca-managed-agents.toolsetName" -}}
{{- $toolset := default (dict) .Values.toolset -}}
{{- $baseName := default (include "orca-managed-agents.fullname" .) (get $toolset "nameOverride") -}}
{{- printf "%s-toolset" ($baseName | trunc 55 | trimSuffix "-") -}}
{{- end -}}

{{- define "orca-managed-agents.registryInternalUrl" -}}
{{- printf "http://%s:%v" (include "orca-managed-agents.registryInternalServiceName" .) .Values.registry.service.internalPort -}}
{{- end -}}

{{- define "orca-managed-agents.registryPublicUrl" -}}
{{- printf "http://%s:%v" (include "orca-managed-agents.registryServiceName" .) .Values.registry.service.port -}}
{{- end -}}

{{- define "orca-managed-agents.toolsetRegistryUrl" -}}
{{- $toolset := default (dict) .Values.toolset -}}
{{- default (include "orca-managed-agents.registryPublicUrl" .) (get $toolset "registryUrl") -}}
{{- end -}}

{{- define "orca-managed-agents.toolsetImage" -}}
{{- $images := default (dict) .Values.images -}}
{{- $toolsetImage := default (dict) (get $images "toolset") -}}
{{- if (get $toolsetImage "digest") -}}
{{- printf "%s@%s" (get $toolsetImage "repository") (get $toolsetImage "digest") -}}
{{- else -}}
{{- printf "%s:%s" (get $toolsetImage "repository") (required "images.toolset.tag is required when images.toolset.digest is empty" (get $toolsetImage "tag")) -}}
{{- end -}}
{{- end -}}

{{/*
Cluster-qualified Registry URL. Sandboxes run outside the release namespace, so
URLs handed to them (the git credential helper callback) cannot rely on the
short Service name resolving in the caller's namespace.
*/}}
{{- define "orca-managed-agents.registryPublicFqdn" -}}
{{- printf "http://%s.%s.svc.cluster.local:%v" (include "orca-managed-agents.registryServiceName" .) .Release.Namespace .Values.registry.service.port -}}
{{- end -}}

{{- define "orca-managed-agents.gitCredsPublicUrl" -}}
{{- default (printf "%s/v1/git-creds" (include "orca-managed-agents.registryPublicFqdn" .)) .Values.harness.gitCredsPublicUrl -}}
{{- end -}}

{{- define "orca-managed-agents.aiGatewayMcpUrl" -}}
{{- printf "http://%s:%v" (include "orca-managed-agents.aiGatewayServiceName" .) .Values.aiGateway.service.dataPort -}}
{{- end -}}

{{/*
Postgres CA bundle mounted for outbound TLS to a central/managed instance.
Emitted only when external.databases.tls.caSecretName is set, so in-cluster
(non-TLS) installs render no extra volume. Reused by the registry deployment
(container + migrate init-container), the migrate Job, and the harness
deployment so the mount path stays identical everywhere the DSN's sslrootcert
points.
*/}}
{{- define "orca-managed-agents.pgCaVolume" -}}
{{- if .Values.external.databases.tls.caSecretName }}
- name: pg-ca
  secret:
    secretName: {{ .Values.external.databases.tls.caSecretName | quote }}
    items:
      - key: {{ .Values.external.databases.tls.caKey | quote }}
        path: {{ .Values.external.databases.tls.caKey | quote }}
{{- end }}
{{- end -}}

{{- define "orca-managed-agents.pgCaVolumeMount" -}}
{{- if .Values.external.databases.tls.caSecretName }}
- name: pg-ca
  mountPath: {{ .Values.external.databases.tls.mountPath | quote }}
  readOnly: true
{{- end }}
{{- end -}}

{{/*
Secret holding the session-JWT public key the ai-gateway trusts, as a JSON
{name, key} pair. Defaults to the chart Secret's SESSION_JWT_PUBLIC_KEY_PEM so a
self-contained install keeps working; an external keypair Secret (per-DP JWT
trust anchor delivered by a bootstrap pipeline) wins when configured. Empty when
neither exists — the ai-gateway Deployment's `required` call rejects that
combination.
*/}}
{{- define "orca-managed-agents.sessionJwtPublicKeySecret" -}}
{{- $external := .Values.sessionJwt.publicKeySecret | default (dict) -}}
{{- if $external.name -}}
{{- dict "name" $external.name "key" (default "SESSION_JWT_PUBLIC_KEY_PEM" $external.key) | toJson -}}
{{- else -}}
{{- $chartSecret := include "orca-managed-agents.secretName" . -}}
{{- if $chartSecret -}}
{{- dict "name" $chartSecret "key" "SESSION_JWT_PUBLIC_KEY_PEM" | toJson -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Render one `env` entry sourced from a Secret, honoring per-variable overrides.

Resolution order:
  1. `<component>.secretKeyRefs.<ENV_NAME>` — an externally managed Secret that
     may hold the value under its own key name (bootstrap-delivered credentials,
     external-secrets-operator output, ...).
  2. the caller-provided fallback Secret, when set (for example the dedicated
     `external.databases.existingSecret`).
  3. the chart / `secrets.existingSecret` Secret under the canonical key name.

Nothing is emitted when neither source exists, so `<component>.extraEnvFrom` can
be the only credential source without leaving a dangling secretKeyRef behind.

Args: dict "ctx" $ "key" <ENV_NAME> "refs" <secretKeyRefs map>
           "optional" <bool> "fallbackSecretName" <optional Secret name>
*/}}
{{- define "orca-managed-agents.secretEnv" -}}
{{- $key := .key -}}
{{- $refs := default (dict) .refs -}}
{{- $hasOverride := hasKey $refs $key -}}
{{- $override := index $refs $key -}}
{{- $optional := .optional | default false -}}
{{- $fallbackSecretName := dig "fallbackSecretName" "" . -}}
{{- if $hasOverride -}}
{{- if not (kindIs "map" $override) -}}
{{- fail (printf "secretKeyRefs.%s must be a map with a non-empty name" $key) -}}
{{- end -}}
- name: {{ $key }}
  valueFrom:
    secretKeyRef:
      name: {{ required (printf "secretKeyRefs.%s requires a Secret name" $key) (dig "name" "" $override) }}
      key: {{ default $key (dig "key" "" $override) }}
      optional: {{ dig "optional" $optional $override }}
{{- else -}}
{{- $secretName := default (include "orca-managed-agents.secretName" .ctx) $fallbackSecretName -}}
{{- if $secretName -}}
- name: {{ $key }}
  valueFrom:
    secretKeyRef:
      name: {{ $secretName }}
      key: {{ $key }}
      optional: {{ $optional }}
{{- end -}}
{{- end -}}
{{- end -}}

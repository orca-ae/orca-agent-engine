#!/usr/bin/env bash
# Copyright The Orca Authors
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
CHART_DIR="$ROOT_DIR/charts/orca-managed-agents"
CLUSTER_NAME="orca-secret-store-${RANDOM}-$$"
TMP_DIR="$(mktemp -d)"

cleanup() {
  kind delete cluster --name "$CLUSTER_NAME" >/dev/null 2>&1 || true
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

for command in kind docker helm kubectl python3; do
  command -v "$command" >/dev/null 2>&1 || {
    echo "missing required command: $command" >&2
    exit 1
  }
done

kind create cluster --name "$CLUSTER_NAME" --wait 90s >/dev/null

# Simulate the previous chart release: same resources, LocalSecretStore default,
# and no persistent-store validation/resources.
cp -R "$CHART_DIR" "$TMP_DIR/old-chart"
python3 - \
  "$TMP_DIR/old-chart/values.yaml" \
  "$TMP_DIR/old-chart/templates/serviceaccount.yaml" <<'PY'
from pathlib import Path
import sys

values_path = Path(sys.argv[1])
values = values_path.read_text()
values = values.replace("  mode: kubernetes\n", "  mode: local\n", 1)
values = values.replace(
    "  toolset:\n    repository: ghcr.io/orca-ae/orca-cli\n",
    "  orcaCli:\n    repository: ghcr.io/orca-ae/orca-cli\n",
    1,
)
values = values.replace("\ntoolset:\n  enabled: true\n", "\norcaCli:\n  enabled: false\n", 1)
if "\ntoolset:\n" in values or "\n  toolset:\n" in values:
    raise SystemExit("failed to remove the new toolset values schema from old-chart")
values_path.write_text(values)

service_account_path = Path(sys.argv[2])
service_accounts = service_account_path.read_text()
marker = "{{- $toolset := default (dict) .Values.toolset }}"
if marker not in service_accounts:
    raise SystemExit("failed to find toolset ServiceAccount block in old-chart")
service_account_path.write_text(service_accounts.split(marker, 1)[0])
PY
cat > "$TMP_DIR/old-chart/templates/validation.yaml" <<'EOF'
{{- if and .Values.migrations.registry.initContainer.enabled .Values.migrations.registry.job.enabled -}}
{{- fail "migrations.registry: only one of initContainer or job may be enabled at a time" -}}
{{- end -}}
EOF

# Simulate the pre-rename toolset Deployment. Its explicit name override was
# the complete resource name, and its selector used the immutable orca-cli
# component label. The new chart must create a differently named Deployment
# instead of trying to patch this selector in place.
rm "$TMP_DIR/old-chart/templates/deployment-toolset.yaml"
cat > "$TMP_DIR/old-chart/templates/deployment-orca-cli.yaml" <<'EOF'
{{- if .Values.orcaCli.enabled -}}
apiVersion: apps/v1
kind: Deployment
metadata:
  name: {{ .Values.orcaCli.nameOverride }}
  labels:
    {{- include "orca-managed-agents.labels" . | nindent 4 }}
    app.kubernetes.io/component: orca-cli
spec:
  replicas: 0
  selector:
    matchLabels:
      {{- include "orca-managed-agents.selectorLabels" . | nindent 6 }}
      app.kubernetes.io/component: orca-cli
  template:
    metadata:
      labels:
        {{- include "orca-managed-agents.selectorLabels" . | nindent 8 }}
        app.kubernetes.io/component: orca-cli
    spec:
      containers:
        - name: orca-cli
          image: busybox:1.36
          command: [/bin/sh, -c, "sleep infinity"]
{{- end -}}
EOF

cat > "$TMP_DIR/old-values.yaml" <<'EOF'
external:
  databases:
    registryUrl: postgres://orca:secret@postgres.example:5432/registry
    filestoreUrl: postgres://orca:secret@postgres.example:5432/filestore
    memorystoreUrl: postgres://orca:secret@postgres.example:5432/memorystore
secrets:
  values:
    sessionJwtPrivateKeyPem: test-private-key
    sessionJwtPublicKeyPem: test-public-key
# Required by chart validation. This test only exercises the SecretStore path so
# the role never has to resolve, but without it the upgrade below fails on the
# object-store guard and the LocalSecretStore assertion would read as a pass.
objectStorage:
  stsRoleArn: arn:aws:iam::123456789012:role/orca-session-s3
orcaCli:
  enabled: true
  nameOverride: pinned-toolset
  accessToken:
    secretKeyRef:
      name: orca-toolset-access
      key: access-token
EOF

cat > "$TMP_DIR/migrated-values.yaml" <<'EOF'
external:
  databases:
    registryUrl: postgres://orca:secret@postgres.example:5432/registry
    filestoreUrl: postgres://orca:secret@postgres.example:5432/filestore
    memorystoreUrl: postgres://orca:secret@postgres.example:5432/memorystore
secrets:
  values:
    sessionJwtPrivateKeyPem: test-private-key
    sessionJwtPublicKeyPem: test-public-key
objectStorage:
  stsRoleArn: arn:aws:iam::123456789012:role/orca-session-s3
registry:
  oidcAllowedIssuers: https://issuer.example.com
toolset:
  enabled: true
  replicaCount: 0
  nameOverride: pinned-toolset
  accessToken:
    secretKeyRef:
      name: orca-toolset-access
      key: access-token
EOF

helm install orca "$TMP_DIR/old-chart" \
  --namespace orca-test \
  --create-namespace \
  -f "$TMP_DIR/old-values.yaml" >/dev/null

set +e
reuse_output="$(helm upgrade orca "$CHART_DIR" \
  --namespace orca-test \
  --reuse-values 2>&1)"
reuse_rc=$?
set -e
if [[ $reuse_rc -eq 0 || "$reuse_output" != *"orcaCli values were renamed to toolset"* ]]; then
  printf '%s\n' "$reuse_output" >&2
  echo "legacy values were not rejected with reset-values migration guidance" >&2
  exit 1
fi

set +e
upgrade_output="$(helm upgrade orca "$CHART_DIR" \
  --namespace orca-test \
  --reset-values \
  -f "$TMP_DIR/migrated-values.yaml" 2>&1)"
upgrade_rc=$?
set -e
if [[ $upgrade_rc -eq 0 || "$upgrade_output" != *"existing release uses LocalSecretStore"* ]]; then
  printf '%s\n' "$upgrade_output" >&2
  echo "unacknowledged LocalSecretStore cutover was not rejected" >&2
  exit 1
fi

helm upgrade orca "$CHART_DIR" \
  --namespace orca-test \
  --reset-values \
  -f "$TMP_DIR/migrated-values.yaml" \
  --set secretStore.kubernetes.confirmLocalCutover=true >/dev/null

if kubectl -n orca-test get deployment pinned-toolset >/dev/null 2>&1; then
  echo "legacy Orca CLI Deployment survived the toolset rename" >&2
  exit 1
fi
kubectl -n orca-test get deployment pinned-toolset-toolset >/dev/null
component="$(kubectl -n orca-test get deployment pinned-toolset-toolset \
  -o jsonpath='{.spec.selector.matchLabels.app\.kubernetes\.io/component}')"
[[ "$component" == "toolset" ]] || {
  echo "renamed toolset Deployment has unexpected selector: $component" >&2
  exit 1
}

secret_name="$(kubectl -n orca-test get secret \
  -l app.kubernetes.io/component=registry-secret-store \
  -o jsonpath='{.items[0].metadata.name}')"
[[ -n "$secret_name" ]] || {
  echo "persistent SecretStore Secret was not created" >&2
  exit 1
}

kubectl -n orca-test patch secret "$secret_name" --type merge \
  -p '{"data":{"upgrade-probe":"cGVyc2lzdGVk"}}' >/dev/null

helm upgrade orca "$CHART_DIR" \
  --namespace orca-test \
  --reset-values \
  -f "$TMP_DIR/migrated-values.yaml" >/dev/null

probe="$(kubectl -n orca-test get secret "$secret_name" \
  -o jsonpath='{.data.upgrade-probe}')"
[[ "$probe" == "cGVyc2lzdGVk" ]] || {
  echo "Helm upgrade removed SecretStore data" >&2
  exit 1
}

echo "SecretStore cutover, toolset rename, and Helm upgrade persistence test passed"

#!/usr/bin/env bash
# Copyright The Orca Authors
# SPDX-License-Identifier: Apache-2.0

# Bound and retry kind image imports. A wedged Docker export/containerd import
# must fail with diagnostics instead of consuming the whole GitHub Actions job
# timeout and canceling the remaining real-agent matrix.

set -u

# Opt in for large images on Docker-backed, overlayfs kind nodes. Streaming
# avoids kind load docker-image's complete temporary archive on the host.
load_image() {
  if [[ "${KIND_IMAGE_LOAD_STREAM:-0}" == 1 ]]; then
    "${timeout_bin}" --signal=TERM --kill-after=30s "${load_timeout}" \
      bash -euo pipefail -c '
        nodes=$("$1" get nodes --name "$4")
        if [[ -z "$nodes" ]]; then
          echo "No kind nodes found for $4" >&2
          exit 1
        fi
        while IFS= read -r node; do
          "$2" image save "$3" | "$2" exec --privileged -i "$node" \
            ctr --namespace=k8s.io images import --all-platforms --digests --snapshotter=overlayfs -
        done <<< "$nodes"
      ' -- "${kind_bin}" "${docker_bin}" "${image}" "${cluster_name}"
  else
    "${timeout_bin}" --signal=TERM --kill-after=30s "${load_timeout}" \
      "${kind_bin}" load docker-image "${image}" --name "${cluster_name}"
  fi
}

if [[ "$#" -ne 2 ]]; then
  echo "usage: $0 <image> <kind-cluster-name>" >&2
  exit 2
fi

image="$1"
cluster_name="$2"
timeout_bin="${TIMEOUT_BIN:-timeout}"
kind_bin="${KIND_BIN:-kind}"
docker_bin="${DOCKER_BIN:-docker}"
load_timeout="${KIND_IMAGE_LOAD_TIMEOUT:-8m}"
max_attempts="${KIND_IMAGE_LOAD_MAX_ATTEMPTS:-2}"
retry_delay_seconds="${KIND_IMAGE_LOAD_RETRY_DELAY_SECONDS:-5}"

if ! [[ "${max_attempts}" =~ ^[1-9][0-9]*$ ]]; then
  echo "KIND_IMAGE_LOAD_MAX_ATTEMPTS must be a positive integer" >&2
  exit 2
fi
if ! [[ "${retry_delay_seconds}" =~ ^[0-9]+$ ]]; then
  echo "KIND_IMAGE_LOAD_RETRY_DELAY_SECONDS must be a non-negative integer" >&2
  exit 2
fi

for ((attempt = 1; attempt <= max_attempts; attempt += 1)); do
  echo "Loading ${image} into kind cluster ${cluster_name} (attempt ${attempt}/${max_attempts})"
  if load_image; then
    exit 0
  else
    exit_code="$?"
  fi

  echo "::warning::kind image load failed for ${image} with status ${exit_code} on attempt ${attempt}/${max_attempts}"
  df -h / || true
  "${docker_bin}" system df || true
  "${docker_bin}" exec "${cluster_name}-control-plane" crictl images || true

  if [[ "${attempt}" -lt "${max_attempts}" ]]; then
    sleep "${retry_delay_seconds}"
  fi
done

exit "${exit_code}"

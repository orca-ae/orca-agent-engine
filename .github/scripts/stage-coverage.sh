#!/usr/bin/env bash
# Copyright The Orca Authors
# SPDX-License-Identifier: Apache-2.0

#
# Flatten per-package Vitest coverage reports into one directory for upload.
#
# `actions/upload-artifact` derives the archive root from the least-common
# ancestor of the files it matches, so uploading `**/coverage/unit/...`
# directly would change the resulting layout depending on how many packages
# happened to match — one match roots the archive at that package, several root
# it at the repo. Staging to a flat directory with deterministic names removes
# that variable. The merge script attributes each file to a package from the
# paths inside the report, so these filenames only need to be unique.
set -euo pipefail

kind="${1:?usage: stage-coverage.sh <unit|integration>}"
out="coverage-artifacts"
mkdir -p "$out"

count=0
while IFS= read -r f; do
  slug="${f%"/coverage/$kind/coverage-final.json"}"
  slug="${slug//\//__}"
  cp "$f" "$out/${slug}.${kind}.json"
  count=$((count + 1))
done < <(find packages services -path "*/coverage/$kind/coverage-final.json" 2>/dev/null || true)

echo "Staged $count $kind coverage report(s) into $out/"

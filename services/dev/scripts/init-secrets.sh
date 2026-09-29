#!/usr/bin/env bash
# Copyright The Orca Authors
# SPDX-License-Identifier: Apache-2.0

# init-secrets.sh — generate the local-stack RSA keypair used by registry and
# ai-gateway.
#
# Idempotent: if `services/dev/secrets/session-jwt.pem` already exists, the
# script exits 0 without touching anything. Re-running is safe.
#
# Outputs:
#   services/dev/secrets/session-jwt.pem      — PKCS8 RSA-2048 private key (PEM)
#   services/dev/secrets/session-jwt-pub.pem  — SPKI public key (PEM)
#
# The ai-gateway config is a checked-in template at
# services/dev/ai-gateway-config.yaml; this script does not generate gateway
# config anymore.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEV_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
SECRETS_DIR="${DEV_DIR}/secrets"
PRIV_KEY="${SECRETS_DIR}/session-jwt.pem"
PUB_KEY="${SECRETS_DIR}/session-jwt-pub.pem"

# Create the secrets dir atomically with mode 700. Doing `mkdir -p` then a
# follow-up `chmod 700` leaves a brief window where the dir is world-readable
# (whatever the caller's umask permits). `(umask 077 && mkdir -p)` in a
# subshell scopes the umask change, and applies it to any directory mkdir
# creates — without affecting later commands in this script.
(umask 077 && mkdir -p "${SECRETS_DIR}")

if [[ -s "${PRIV_KEY}" && -s "${PUB_KEY}" ]]; then
  echo "init-secrets: ${PRIV_KEY##*/}, ${PUB_KEY##*/} already exist; nothing to do."
  exit 0
fi

if ! command -v openssl >/dev/null 2>&1; then
  echo "init-secrets: openssl not found on PATH" >&2
  exit 1
fi

if [[ ! -s "${PRIV_KEY}" ]]; then
  echo "init-secrets: generating RSA-2048 PKCS8 private key at ${PRIV_KEY}"
  # `genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048` is portable across
  # OpenSSL 1.1.x and 3.x on macOS + Linux. Output is PKCS8 PEM by default,
  # which jose's importPKCS8 expects (registry-service-ts SessionJwtMinter).
  openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out "${PRIV_KEY}"
  chmod 600 "${PRIV_KEY}"
fi

if [[ ! -s "${PUB_KEY}" ]]; then
  echo "init-secrets: deriving SPKI public key at ${PUB_KEY}"
  # `pkey -pubout` writes SPKI PEM, which ai-gateway's static JWT validator
  # loads at startup.
  openssl pkey -in "${PRIV_KEY}" -pubout -out "${PUB_KEY}"
  chmod 644 "${PUB_KEY}"
fi

echo "init-secrets: done."

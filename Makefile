# Copyright The Orca Authors
# SPDX-License-Identifier: Apache-2.0

# Orca Agent Engine — local-stack Makefile.
#
# Two layers of targets:
#   `dev-up` / `dev-down`   — infra only (Postgres + RustFS + selected
#                              transcript broker via compose).
#                              Backwards-compatible with services/dev/README.md.
#   `stack-up` / `stack-down` — infra + app services. registry-service-ts and
#                                harness-server run NATIVELY on the host so the
#                                harness can use `srt` without container-in-
#                                container headaches; ai-gateway runs as the
#                                configured external compose image.

SHELL := /usr/bin/env bash

COMPOSE      := docker compose -f services/dev/docker-compose.yml
COMPOSE_ALL  := $(COMPOSE) --profile kafka --profile pulsar
DEV_DIR      := services/dev
SCRIPTS_DIR  := $(DEV_DIR)/scripts
RUN_DIR      := $(DEV_DIR)/run

.PHONY: help dev-up dev-down stack-up stack-down stack-status secrets services-up services-down \
	self-hosted-up self-hosted-down

help:
	@echo "Orca Agent Engine — local-stack targets:"
	@echo "  make dev-up         start infra only (postgres + rustfs + transcript backend)"
	@echo "  make dev-down       stop infra only"
	@echo "  make stack-up       start infra + app services (full stack)"
	@echo "  make stack-down     stop app services + infra"
	@echo "  make stack-status   print port table + service pids"
	@echo "  make secrets        regenerate the local JWT keypair (idempotent)"
	@echo ""
	@echo "  make self-hosted-up    start the MINIMAL colocated stack and attach this"
	@echo "                         machine as a self_hosted environment"
	@echo "  make self-hosted-down  stop it (add FRESH=1 to self-hosted-up for a clean DB)"

# ------------------------------------------------------------------------- infra

dev-up:
	services="$$($(SCRIPTS_DIR)/dev-infra-services.sh up)" || exit $$?; \
	$(COMPOSE) up -d $$services
	# Wait only on the long-running services. `rustfs-bootstrap` is a one-shot
	# `rc mb` job that exits 0 after creating the bucket — `--wait` would
	# treat that exit as a failure.
	services="$$($(SCRIPTS_DIR)/dev-infra-services.sh wait)" || exit $$?; \
	$(COMPOSE) up -d --wait $$services

dev-down:
	$(COMPOSE_ALL) down --remove-orphans

# ------------------------------------------------------------------------- stack

# `stack-up` is the one-command entrypoint:
#   1. compose up infra (idempotent — `up -d` is fine if already running).
#   2. init-secrets (idempotent — no-op when secrets already exist).
#   3. start-services (probes prereqs, builds, applies migrations, spawns
#      registry/harness and starts ai-gateway, waits for /healthz).
stack-up: dev-up secrets services-up

stack-down: services-down dev-down

stack-status:
	@backend="$$($(SCRIPTS_DIR)/dev-infra-services.sh backend)" || exit $$?; \
	echo "transcript backend: $$backend"
	@echo "----- containers ($(COMPOSE)) -----"
	@$(COMPOSE) ps || true
	@echo
	@echo "----- ports -----"
	@printf "  %-14s %s\n" "registry"     "http://localhost:$${REGISTRY_HTTP_PORT:-8080}/healthz"
	@printf "  %-14s %s\n" "harness"      "http://localhost:$${HARNESS_HTTP_PORT:-9094}/healthz"
	@printf "  %-14s %s\n" "ai-gateway"  "http://localhost:$${AI_GATEWAY_ADMIN_PORT:-9099}/healthz"
	@printf "  %-14s %s\n" "postgres"     "localhost:5432 (orca/orca)"
	@backend="$$($(SCRIPTS_DIR)/dev-infra-services.sh backend)" || exit $$?; \
	if [[ "$$backend" == "kafka" ]]; then \
		printf "  %-14s %s\n" "kafka" "localhost:9092"; \
	fi
	@backend="$$($(SCRIPTS_DIR)/dev-infra-services.sh backend)" || exit $$?; \
	if [[ "$$backend" == "pulsar" ]]; then \
		printf "  %-14s %s\n" "pulsar" "localhost:6650 (admin http://localhost:18081)"; \
	fi
	@printf "  %-14s %s\n" "rustfs"       "http://localhost:9000 (console http://localhost:9001/rustfs/console/)"
	@echo
	@echo "----- pids ($(RUN_DIR)/) -----"
	@for name in registry harness; do \
		f="$(RUN_DIR)/$$name.pid"; \
		if [[ -f "$$f" ]]; then \
			pid=$$(cat "$$f" 2>/dev/null); \
			if [[ -n "$$pid" ]] && kill -0 "$$pid" 2>/dev/null; then \
				printf "  %-12s pid=%s (running)\n" "$$name" "$$pid"; \
			else \
				printf "  %-12s pid=%s (stale)\n" "$$name" "$$pid"; \
			fi; \
		else \
			printf "  %-12s (not running)\n" "$$name"; \
		fi; \
	done

# ------------------------------------------------------------------------- helpers

secrets:
	$(SCRIPTS_DIR)/init-secrets.sh

services-up:
	$(SCRIPTS_DIR)/start-services.sh

services-down:
	$(SCRIPTS_DIR)/stop-services.sh

# ------------------------------------------------------------- self-hosted (colocated)

# A SEPARATE stack from `stack-up`, not a variant of it. `stack-up` runs the
# `separate` path: registry + harness-server + ai-gateway, on whichever transcript
# broker `TRANSCRIPT_STORE_BACKEND` names. This one runs the `colocated` path, where
# the registry itself coordinates the session over the worker tunnel — so it needs
# neither harness-server nor a broker, and its compose file is Postgres + RustFS only.
#
# It also does something `stack-up` does not: it creates a `target=self_hosted`
# Environment over the public API and attaches THIS MACHINE to it, via `oeadm worker`.
# From there a session for a `colocated` agent runs its loop and its tools here.
#
# The script leaves the registry and the worker running behind pid files in
# services/dev/run, so a second invocation restarts them against a freshly created
# environment (an Env Key is echoed exactly once and never stored, so reusing one is
# not possible — a new environment is minted instead).
#
# FRESH=1 drops the Postgres volume first. A database left over from an earlier
# checkout is the likeliest way this fails for someone else.
self-hosted-up:
	@FRESH="$(FRESH)" $(SCRIPTS_DIR)/start-self-hosted.sh

self-hosted-down:
	@$(SCRIPTS_DIR)/stop-self-hosted.sh

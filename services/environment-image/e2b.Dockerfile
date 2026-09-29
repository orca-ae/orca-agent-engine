# Copyright The Orca Authors
# SPDX-License-Identifier: Apache-2.0

# E2B template wrapper for the Orca Environment image.
#
# E2B's `e2b template build` builds a Dockerfile with THIS file's directory as
# the build context — it cannot see the monorepo at the repo root the main
# `Dockerfile` needs. So instead of rebuilding the monorepo here, this template
# is `FROM` the already-built-and-pushed Orca Environment image (see README.md:
# `docker build -f services/environment-image/Dockerfile` → push to a registry
# → set the ref below).
#
# Build the template (produces a template id for E2B_ENVIRONMENT_TEMPLATE_ID):
#   e2b template build -c "sleep infinity"        # from this directory
#
# Override the base image ref per environment as needed:
#   docker build supplies it via --build-arg; e2b reads it from e2b.toml.
ARG ORCA_ENVIRONMENT_IMAGE=ghcr.io/orca-ae/orca-environment:latest
FROM ${ORCA_ENVIRONMENT_IMAGE}

# The base image already carries environment-worker + session-runner at
# /opt/orca/* and the /home/user/orca-environment workspace. Nothing else is
# needed for the E2B template — E2B keeps the sandbox alive and the registry
# launcher execs the worker in. Add provider CLIs (codex/cursor/pi) or FUSE
# tooling here if the deployment needs them (see README.md "Extending").

# Copyright The Orca Authors
# SPDX-License-Identifier: Apache-2.0

# build context: repo root.  Used as: docker build -f services/harness-server/Dockerfile .
FROM node:22-slim AS build
WORKDIR /app
# Build the patched Pulsar binding against the pinned static C++ client.
RUN apt-get update && apt-get install -y --no-install-recommends python3 make g++ binutils xz-utils \
    && rm -rf /var/lib/apt/lists/*
# Pin pnpm 9 to match lockfileVersion 9.0 and avoid pnpm 10/11 behavior drift
# (strict build-script approval, `pnpm deploy` workspace-injection requirement).
RUN npm install -g pnpm@9.15.9
# Copy workspace manifests for dependency installation layer caching
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml tsconfig.base.json .npmrc ./
COPY patches ./patches
COPY packages/codex-harness/package.json ./packages/codex-harness/
COPY packages/sdk-harness/package.json ./packages/sdk-harness/
COPY packages/pi-harness/package.json ./packages/pi-harness/
COPY services/harness-server/package.json ./services/harness-server/
COPY services/observability-exporter/package.json ./services/observability-exporter/
COPY services/registry-service-ts/package.json ./services/registry-service-ts/
COPY services/sandbox-harness/package.json ./services/sandbox-harness/
COPY packages/agent-event-contract/package.json ./packages/agent-event-contract/
COPY packages/transcript-store/package.json ./packages/transcript-store/
COPY packages/transcript-store-types/package.json ./packages/transcript-store-types/
COPY packages/file-store/package.json ./packages/file-store/
COPY packages/memory-store/package.json ./packages/memory-store/
COPY packages/skill-store/package.json ./packages/skill-store/
COPY packages/harness-catalog/package.json ./packages/harness-catalog/
COPY packages/sandbox-runtime/package.json ./packages/sandbox-runtime/
COPY packages/cloud-sandbox/package.json ./packages/cloud-sandbox/
COPY packages/harness-tunnel/package.json ./packages/harness-tunnel/
COPY packages/guardrails/package.json ./packages/guardrails/
COPY packages/e2e-tests/package.json ./packages/e2e-tests/
COPY services/proto ./services/proto
RUN pnpm install --frozen-lockfile --filter @orca/harness-server...
# Copy workspace library sources first, then the service source
COPY packages/agent-event-contract/tsconfig.json ./packages/agent-event-contract/
COPY packages/agent-event-contract/src ./packages/agent-event-contract/src
COPY packages/transcript-store-types/tsconfig.json ./packages/transcript-store-types/
COPY packages/transcript-store-types/src ./packages/transcript-store-types/src
COPY packages/transcript-store/tsconfig.json ./packages/transcript-store/
COPY packages/transcript-store/src ./packages/transcript-store/src
COPY packages/file-store/tsconfig.json ./packages/file-store/
COPY packages/file-store/src ./packages/file-store/src
COPY packages/memory-store/tsconfig.json ./packages/memory-store/
COPY packages/memory-store/src ./packages/memory-store/src
COPY packages/skill-store/tsconfig.json ./packages/skill-store/
COPY packages/skill-store/src ./packages/skill-store/src
COPY packages/harness-catalog/tsconfig.json ./packages/harness-catalog/
COPY packages/harness-catalog/src ./packages/harness-catalog/src
COPY packages/codex-harness/tsconfig.json ./packages/codex-harness/
COPY packages/codex-harness/src ./packages/codex-harness/src
COPY packages/sdk-harness/tsconfig.json ./packages/sdk-harness/
COPY packages/sdk-harness/src ./packages/sdk-harness/src
COPY packages/pi-harness/tsconfig.json ./packages/pi-harness/
COPY packages/pi-harness/src ./packages/pi-harness/src
COPY packages/guardrails/tsconfig.json ./packages/guardrails/
COPY packages/guardrails/src ./packages/guardrails/src
COPY packages/sandbox-runtime/tsconfig.json ./packages/sandbox-runtime/
COPY packages/sandbox-runtime/src ./packages/sandbox-runtime/src
COPY services/harness-server/tsconfig.json ./services/harness-server/
COPY services/harness-server/src ./services/harness-server/src
# Build workspace dependencies first, then the service
RUN pnpm -F @orca/sdk-harness build && \
    pnpm -F @orca/harness-catalog build && \
    pnpm -F @orca/pi-harness build && \
    pnpm -F @orca/codex-harness build && \
    pnpm -F @orca/agent-event-contract build && \
    pnpm -F @orca/transcript-store-types build && \
    pnpm -F @orca/transcript-store build && \
    pnpm -F @orca/file-store build && \
    pnpm -F @orca/memory-store build && \
    pnpm -F @orca/skill-store build && \
    pnpm -F @orca/guardrails build && \
    pnpm -F @orca/sandbox-runtime build && \
    pnpm -F @orca/harness-server build
# Produce a self-contained deploy directory with all node_modules resolved
# (no broken workspace symlinks in the runtime image)
RUN pnpm deploy --filter @orca/harness-server --prod /deploy

FROM node:22-slim AS runtime
WORKDIR /app
ENV NODE_ENV=production
# GitWorker uses simple-git, which shells out to `git clone` for github_repository resources.
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates git && \
    rm -rf /var/lib/apt/lists/*
# /deploy carries package.json ("type": "module" — required for ESM dist),
# node_modules, and the built dist; copy dist explicitly as a guard against
# pack-time file filtering.
COPY --from=build /deploy /app
COPY --from=build /app/services/harness-server/dist /app/dist
# The Claude Agent SDK probes musl packages BEFORE glibc packages and spawns
# the first platform package that resolves. pnpm installs both variants, and
# the musl `claude` binary cannot exec on this glibc base (ENOENT on the musl
# loader, surfaced as "native binary not found"). Drop the musl variants so
# resolution falls through to the glibc package for the image architecture.
RUN rm -rf /app/node_modules/.pnpm/@anthropic-ai+claude-agent-sdk-linux-x64-musl@* \
           /app/node_modules/@anthropic-ai/claude-agent-sdk-linux-x64-musl \
           /app/node_modules/.pnpm/@anthropic-ai+claude-agent-sdk-linux-arm64-musl@* \
           /app/node_modules/@anthropic-ai/claude-agent-sdk-linux-arm64-musl
USER 1000
EXPOSE 9094 50055
CMD ["node", "dist/main.js"]

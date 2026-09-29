# Copyright The Orca Authors
# SPDX-License-Identifier: Apache-2.0

# build context: repo root.  Used as: docker build -f services/registry-service-ts/Dockerfile .
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
COPY services/registry-service-ts/package.json ./services/registry-service-ts/
COPY services/harness-server/package.json ./services/harness-server/
COPY packages/agent-event-contract/package.json ./packages/agent-event-contract/
COPY packages/file-store/package.json ./packages/file-store/
COPY packages/memory-store/package.json ./packages/memory-store/
COPY packages/skill-store/package.json ./packages/skill-store/
COPY packages/transcript-store/package.json ./packages/transcript-store/
COPY packages/transcript-store-types/package.json ./packages/transcript-store-types/
COPY packages/harness-catalog/package.json ./packages/harness-catalog/
COPY packages/sandbox-runtime/package.json ./packages/sandbox-runtime/
COPY packages/cloud-sandbox/package.json ./packages/cloud-sandbox/
COPY packages/harness-tunnel/package.json ./packages/harness-tunnel/
COPY packages/guardrails/package.json ./packages/guardrails/
COPY packages/e2e-tests/package.json ./packages/e2e-tests/
COPY services/proto ./services/proto
RUN pnpm install --frozen-lockfile --filter @orca/registry-service-ts...
# Copy and build workspace library sources first
COPY packages/agent-event-contract/tsconfig.json ./packages/agent-event-contract/
COPY packages/agent-event-contract/src ./packages/agent-event-contract/src
COPY packages/file-store/tsconfig.json ./packages/file-store/
COPY packages/file-store/src ./packages/file-store/src
COPY packages/memory-store/tsconfig.json ./packages/memory-store/
COPY packages/memory-store/src ./packages/memory-store/src
COPY packages/skill-store/tsconfig.json ./packages/skill-store/
COPY packages/skill-store/src ./packages/skill-store/src
COPY packages/transcript-store-types/tsconfig.json ./packages/transcript-store-types/
COPY packages/transcript-store-types/src ./packages/transcript-store-types/src
COPY packages/transcript-store/tsconfig.json ./packages/transcript-store/
COPY packages/transcript-store/src ./packages/transcript-store/src
COPY packages/harness-catalog/tsconfig.json ./packages/harness-catalog/
COPY packages/harness-catalog/src ./packages/harness-catalog/src
COPY packages/sandbox-runtime/tsconfig.json ./packages/sandbox-runtime/
COPY packages/sandbox-runtime/src ./packages/sandbox-runtime/src
COPY packages/cloud-sandbox/tsconfig.json ./packages/cloud-sandbox/
COPY packages/cloud-sandbox/src ./packages/cloud-sandbox/src
COPY packages/harness-tunnel/tsconfig.json ./packages/harness-tunnel/
COPY packages/harness-tunnel/src ./packages/harness-tunnel/src
COPY packages/guardrails/tsconfig.json ./packages/guardrails/
COPY packages/guardrails/src ./packages/guardrails/src
RUN pnpm -F @orca/agent-event-contract build && \
    pnpm -F @orca/file-store build && \
    pnpm -F @orca/memory-store build && \
    pnpm -F @orca/skill-store build && \
    pnpm -F @orca/transcript-store-types build && \
    pnpm -F @orca/transcript-store build && \
    pnpm -F @orca/sandbox-runtime build && \
    pnpm -F @orca/cloud-sandbox build && \
    pnpm -F @orca/harness-tunnel build && \
    pnpm -F @orca/guardrails build && \
    pnpm -F @orca/harness-catalog build
# Copy and build the service itself
COPY services/registry-service-ts/tsconfig.json ./services/registry-service-ts/
COPY services/registry-service-ts/buf.gen.yaml ./services/registry-service-ts/
COPY services/registry-service-ts/src ./services/registry-service-ts/src
RUN pnpm -F @orca/registry-service-ts build
# Produce a self-contained deploy directory with all node_modules resolved
# (no broken workspace symlinks in the runtime image)
RUN pnpm deploy --filter @orca/registry-service-ts --prod /deploy

FROM node:22-slim AS runtime
# Registry prepares attached Git snapshots; no upstream credential leaves this process.
RUN apt-get update && apt-get install -y --no-install-recommends git ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
ENV NODE_ENV=production
# /deploy carries package.json ("type": "module" — required for ESM dist),
# node_modules, and the built dist; copy dist explicitly as a guard against
# pack-time file filtering.
COPY --from=build /deploy /app
COPY --from=build /app/services/registry-service-ts/dist /app/dist
COPY --from=build /app/services/registry-service-ts/src/persistence/postgres/migrations /app/src/persistence/postgres/migrations
USER 1000
EXPOSE 8080 50054
CMD ["node", "dist/main.js"]

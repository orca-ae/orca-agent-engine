# Copyright The Orca Authors
# SPDX-License-Identifier: Apache-2.0

FROM node:22-slim AS build
WORKDIR /app
# Compile the patched Pulsar binding; also support better-sqlite3 source builds
# without putting compilers in the runtime image.
RUN apt-get update && apt-get install -y --no-install-recommends python3 make g++ binutils xz-utils \
    && rm -rf /var/lib/apt/lists/*
RUN corepack enable && corepack prepare pnpm@9 --activate
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml tsconfig.base.json .npmrc ./
COPY patches ./patches
COPY services/observability-exporter/package.json ./services/observability-exporter/
COPY packages/agent-event-contract/package.json ./packages/agent-event-contract/
COPY packages/transcript-store-types/package.json ./packages/transcript-store-types/
COPY packages/transcript-store/package.json ./packages/transcript-store/
RUN pnpm install --frozen-lockfile --filter @orca/observability-exporter...
COPY packages/agent-event-contract ./packages/agent-event-contract
COPY packages/transcript-store-types ./packages/transcript-store-types
COPY packages/transcript-store ./packages/transcript-store
COPY services/observability-exporter ./services/observability-exporter
RUN pnpm --filter @orca/observability-exporter... build
RUN pnpm deploy --filter @orca/observability-exporter --prod /deploy

FROM node:22-slim AS runtime
WORKDIR /app
ENV NODE_ENV=production
ENV OBSERVABILITY_KAFKA_STATE_DIRECTORY=/var/run/orca/exporter-state
RUN mkdir -p /var/run/orca/exporter-state && chown 1000:1000 /var/run/orca/exporter-state
COPY --from=build /deploy /app
COPY --from=build /app/services/observability-exporter/dist /app/dist
USER 1000
EXPOSE 8080
CMD ["node", "dist/main.js"]

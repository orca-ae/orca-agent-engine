# Copyright The Orca Authors
# SPDX-License-Identifier: Apache-2.0

# Orca Environment image — the cloud sandbox image for `target=cloud`
# `colocated` sessions.
#
# Bundles `environment-worker` + `session-runner` (Node 22) so a caller that
# provisions an E2B / OpenSandbox box can exec the worker inside it —
# `node /opt/orca/environment-worker/dist/main.js` — and the worker in turn
# spawns `node /opt/orca/session-runner/dist/main.js` per session. This is a
# TOOLBOX image, not a listening service: it boots and idles; nothing inside it
# starts the worker.
#
# Build context = repo root:
#   docker build -f services/environment-image/Dockerfile -t orca-environment:dev .
#
# The worker/runner command settings on the calling side must point at the baked
# paths below — see README.md.

ARG BUBBLEWRAP_VERSION=0.12.0
ARG BUBBLEWRAP_SHA256=9760d007363e3abba7c747489910f9f82d9fca53ba3bd3282e396fa3c97a3314

FROM node:22-trixie-slim@sha256:517aa41d78545cb1b8c67b13655b4c13ede1ee9df1da8aab54cd7434aefbcaf8 AS build
WORKDIR /app
# Pin pnpm 9 to match lockfileVersion 9.0 (matches the other service Dockerfiles;
# avoids pnpm 10/11 `pnpm deploy` workspace-injection behavior drift).
RUN npm install -g pnpm@9.15.9

# Workspace manifests first, for dependency-install layer caching.
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml tsconfig.base.json .npmrc ./
COPY patches ./patches
COPY packages/codex-harness/package.json ./packages/codex-harness/
COPY packages/sdk-harness/package.json ./packages/sdk-harness/
COPY packages/pi-harness/package.json ./packages/pi-harness/
# pnpm deploy resolves the runner workspace references, including test-only catalog imports.
COPY packages/harness-catalog/package.json ./packages/harness-catalog/
COPY services/environment-worker/package.json ./services/environment-worker/
COPY services/session-runner/package.json ./services/session-runner/
COPY packages/guardrails/package.json ./packages/guardrails/
COPY packages/harness-tunnel/package.json ./packages/harness-tunnel/
COPY packages/sandbox-runtime/package.json ./packages/sandbox-runtime/
COPY packages/transcript-store-types/package.json ./packages/transcript-store-types/
RUN pnpm install --frozen-lockfile \
      --filter @orca/environment-worker... \
      --filter @orca/session-runner...

# Build the workspace library deps (harness-tunnel: both; sandbox-runtime +
# transcript-store-types: session-runner) before the two binaries.
COPY packages/codex-harness/tsconfig.json ./packages/codex-harness/
COPY packages/codex-harness/src ./packages/codex-harness/src
COPY packages/sdk-harness/tsconfig.json ./packages/sdk-harness/
COPY packages/sdk-harness/src ./packages/sdk-harness/src
COPY packages/pi-harness/tsconfig.json ./packages/pi-harness/
COPY packages/pi-harness/src ./packages/pi-harness/src
COPY packages/harness-catalog/tsconfig.json ./packages/harness-catalog/
COPY packages/harness-catalog/src ./packages/harness-catalog/src
COPY packages/guardrails/tsconfig.json ./packages/guardrails/
COPY packages/guardrails/src ./packages/guardrails/src
COPY packages/harness-tunnel/tsconfig.json ./packages/harness-tunnel/
COPY packages/harness-tunnel/src ./packages/harness-tunnel/src
COPY packages/sandbox-runtime/tsconfig.json ./packages/sandbox-runtime/
COPY packages/sandbox-runtime/src ./packages/sandbox-runtime/src
COPY packages/transcript-store-types/tsconfig.json ./packages/transcript-store-types/
COPY packages/transcript-store-types/src ./packages/transcript-store-types/src
RUN pnpm -F @orca/sdk-harness build && \
    pnpm -F @orca/harness-catalog build && \
    pnpm -F @orca/pi-harness build && \
    pnpm -F @orca/codex-harness build && \
    pnpm -F @orca/guardrails build && \
    pnpm -F @orca/harness-tunnel build && \
    pnpm -F @orca/sandbox-runtime build && \
    pnpm -F @orca/transcript-store-types build

# Build the two binaries.
COPY services/environment-worker/tsconfig.json ./services/environment-worker/
COPY services/environment-worker/src ./services/environment-worker/src
COPY services/session-runner/tsconfig.json ./services/session-runner/
COPY services/session-runner/src ./services/session-runner/src
RUN pnpm -F @orca/environment-worker build && \
    pnpm -F @orca/session-runner build

# Self-contained deploy dirs: resolved production node_modules, no broken
# workspace symlinks in the runtime image (same approach as the service images).
RUN pnpm deploy --filter @orca/environment-worker --prod /deploy/environment-worker && \
    pnpm deploy --filter @orca/session-runner --prod /deploy/session-runner

# Keep the pinned build identical to the sandbox-harness image. Both run
# nested filesystem, PID and network namespaces under OpenSandbox gVisor.
FROM node:22-trixie-slim@sha256:517aa41d78545cb1b8c67b13655b4c13ede1ee9df1da8aab54cd7434aefbcaf8 AS bubblewrap-build

# Debian's bubblewrap package is built for its native kernel floor and can
# compile out the safe openat(2) fallback. gVisor does not implement openat2,
# which bubblewrap 0.12 uses for secure source-path resolution. Build the
# security-fixed release with the fallback retained instead of depending on
# the mutable apt package build configuration.
ARG BUBBLEWRAP_VERSION
ARG BUBBLEWRAP_SHA256
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        curl \
        libcap-dev \
        meson \
        ninja-build \
        pkg-config \
        xz-utils \
    && curl -fsSLo /tmp/bubblewrap.tar.xz \
        "https://github.com/containers/bubblewrap/releases/download/v${BUBBLEWRAP_VERSION}/bubblewrap-${BUBBLEWRAP_VERSION}.tar.xz" \
    && echo "${BUBBLEWRAP_SHA256}  /tmp/bubblewrap.tar.xz" | sha256sum -c - \
    && mkdir /tmp/bubblewrap-src \
    && tar -xJf /tmp/bubblewrap.tar.xz -C /tmp/bubblewrap-src --strip-components=1 \
    && meson setup /tmp/bubblewrap-build /tmp/bubblewrap-src \
        --prefix=/usr \
        --buildtype=release \
        --strip \
        -Dassume_kernel='' \
        -Dbash_completion=disabled \
        -Dman=disabled \
        -Dselinux=disabled \
        -Dtests=false \
        -Dzsh_completion=disabled \
    && meson compile -C /tmp/bubblewrap-build \
    && DESTDIR=/tmp/bubblewrap-install meson install -C /tmp/bubblewrap-build \
    && /tmp/bubblewrap-install/usr/bin/bwrap --version \
        | grep -Fx "bubblewrap ${BUBBLEWRAP_VERSION}" \
    && package_root=/tmp/bubblewrap-package \
    && architecture="$(dpkg --print-architecture)" \
    && install -Dm0755 /tmp/bubblewrap-install/usr/bin/bwrap \
        "${package_root}/usr/lib/orca/bwrap.real" \
    && install -Dm0644 /tmp/bubblewrap-src/COPYING \
        "${package_root}/usr/share/doc/bubblewrap/copyright" \
    && install -d "${package_root}/usr/bin" "${package_root}/DEBIAN" \
    && ln -s /usr/local/bin/orca-gvisor-bwrap "${package_root}/usr/bin/bwrap" \
    && printf '%s\n' \
        'Package: bubblewrap' \
        "Version: ${BUBBLEWRAP_VERSION}-orca1" \
        'Section: admin' \
        'Priority: optional' \
        "Architecture: ${architecture}" \
        'Maintainer: Orca Agent Engine <maintainers@runorca.ai>' \
        'Depends: libc6, libcap2' \
        'Description: Bubblewrap sandbox with gVisor-compatible path fallback' \
        > "${package_root}/DEBIAN/control" \
    && dpkg-deb --build --root-owner-group "${package_root}" /tmp/bubblewrap.deb \
    && dpkg-deb --info /tmp/bubblewrap.deb \
        | grep -F "Version: ${BUBBLEWRAP_VERSION}-orca1"

FROM node:22-trixie-slim@sha256:517aa41d78545cb1b8c67b13655b4c13ede1ee9df1da8aab54cd7434aefbcaf8 AS runtime
ARG BUBBLEWRAP_VERSION
# git: `environment-worker`'s worktree offload and `session-runner`'s repo
# mounts shell out to real git. ca-certificates: TLS to the LLM gateway.
RUN apt-get update && \
    apt-get install -y --no-install-recommends git ca-certificates curl libcap2 socat util-linux ripgrep && \
    rm -rf /var/lib/apt/lists/*
COPY --from=bubblewrap-build /tmp/bubblewrap.deb /tmp/bubblewrap.deb
COPY services/sandbox-harness/orca-gvisor-bwrap /usr/local/bin/orca-gvisor-bwrap
RUN dpkg -i /tmp/bubblewrap.deb \
    && rm /tmp/bubblewrap.deb \
    && chmod 0755 /usr/local/bin/orca-gvisor-bwrap \
    && bash -n /usr/local/bin/orca-gvisor-bwrap \
    && bwrap --version \
    && test ! -u /usr/lib/orca/bwrap.real \
    && test -z "$(dpkg -V bubblewrap)"
ENV NODE_ENV=production

# Both binaries at stable paths the caller's worker/runner command settings point
# at. The explicit dist copy guards against
# pack-time file filtering dropping dist from `pnpm deploy` (same guard the
# registry/harness-server images use); it also carries the worker's sibling
# `dist/worktree-thread.js` and the runner's `dist/harness/*/bridge-entry.js`
# + `dist/harness/pi/orca-extension.mjs`, which both resolve relative to their
# own `dist/main.js` at runtime.
COPY --from=build /deploy/environment-worker /opt/orca/environment-worker
COPY --from=build /app/services/environment-worker/dist /opt/orca/environment-worker/dist
COPY --from=build /deploy/session-runner /opt/orca/session-runner
COPY --from=build /app/services/session-runner/dist /opt/orca/session-runner/dist

# The SDK runs in a private worker handle; model Bash uses this SRT executable.
# Resolve through the deployed workspace package so the lockfile selects SRT.
RUN node --input-type=module -e "import { createRequire } from 'node:module'; import { symlinkSync } from 'node:fs'; const runner = createRequire('/opt/orca/session-runner/package.json'); const sandbox = createRequire(runner.resolve('@orca/sandbox-runtime')); symlinkSync(sandbox.resolve('@anthropic-ai/sandbox-runtime/dist/cli.js'), '/usr/local/bin/srt');" \
    && srt --version

# Workspace dir the worker creates per-session runner workspaces under. It is the
# default a caller uses unless it overrides the workspace setting; see README.md.
RUN mkdir -p /home/user/orca-environment && \
    chown -R 1000:1000 /home/user /opt/orca
WORKDIR /home/user/orca-environment
USER 1000

# Toolbox image: idle so the provider keeps the box alive and the registry
# launcher can exec the worker in. E2B/OpenSandbox may override the start
# command with their own keep-alive; this default supports plain `docker run`
# and OpenSandbox.
CMD ["sleep", "infinity"]

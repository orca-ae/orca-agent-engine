# Copyright The Orca Authors
# SPDX-License-Identifier: Apache-2.0

# build context: repo root. Used as:
#   docker build -f services/sandbox-harness/Dockerfile .
# Trixie supplies s3fs >=1.93; Bookworm's 1.90 rejects required compat_dir.
ARG BUBBLEWRAP_VERSION=0.12.0
ARG BUBBLEWRAP_SHA256=9760d007363e3abba7c747489910f9f82d9fca53ba3bd3282e396fa3c97a3314

FROM node:22-trixie-slim@sha256:517aa41d78545cb1b8c67b13655b4c13ede1ee9df1da8aab54cd7434aefbcaf8 AS build
WORKDIR /app
RUN corepack enable && corepack prepare pnpm@9.15.9 --activate

COPY package.json pnpm-lock.yaml pnpm-workspace.yaml tsconfig.base.json ./
COPY patches ./patches
COPY packages/harness-catalog/package.json ./packages/harness-catalog/
COPY packages/codex-harness/package.json ./packages/codex-harness/
COPY packages/sdk-harness/package.json ./packages/sdk-harness/
COPY packages/pi-harness/package.json ./packages/pi-harness/
COPY services/sandbox-harness/package.json ./services/sandbox-harness/
RUN pnpm install --frozen-lockfile --filter @orca/sandbox-harness...

COPY packages/codex-harness/tsconfig.json ./packages/codex-harness/
COPY packages/codex-harness/src ./packages/codex-harness/src
COPY packages/sdk-harness/tsconfig.json ./packages/sdk-harness/
COPY packages/sdk-harness/src ./packages/sdk-harness/src
COPY packages/pi-harness/tsconfig.json ./packages/pi-harness/
COPY packages/pi-harness/src ./packages/pi-harness/src
COPY packages/harness-catalog/tsconfig.json ./packages/harness-catalog/
COPY packages/harness-catalog/src ./packages/harness-catalog/src
COPY services/sandbox-harness/tsconfig.json ./services/sandbox-harness/
COPY services/sandbox-harness/src ./services/sandbox-harness/src
RUN pnpm -F @orca/sdk-harness build && \
    pnpm -F @orca/harness-catalog build && \
    pnpm -F @orca/pi-harness build && \
    pnpm -F @orca/codex-harness build \
    && pnpm -F @orca/sandbox-harness build

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
WORKDIR /app/services/sandbox-harness

# Claude Agent SDK uses bubblewrap for its Linux filesystem sandbox. The write
# policy is fail-closed, so this is a runtime dependency rather than optional
# hardening. The entrypoint creates the outer read-only-root namespace while
# OpenSandbox's gVisor-scoped SYS_ADMIN capability is available, then disables
# privilege gains and drops to UID/GID 1000 with an empty capability bounding
# set before Node or agent starts. Harness-server may mount S3-FUSE before
# releasing the ready marker; agent namespace contains no sudo path, no
# capabilities, and a fresh /dev without gVisor's parent /dev/fuse device.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        fuse3 \
        git \
        jq \
        libcap2 \
        s3fs \
        socat \
        util-linux \
    && rm -rf /var/lib/apt/lists/* \
    && command -v setpriv \
    && command -v unshare \
    && command -v mount \
    && command -v realpath \
    && command -v umount \
    && test "$(id -u node):$(id -g node)" = 1000:1000 \
    && s3fs --version \
    && s3fs --help 2>&1 | grep -F 'compat_dir' \
    && fusermount3 --version \
    && socat -V >/dev/null

COPY --from=bubblewrap-build /tmp/bubblewrap.deb /tmp/bubblewrap.deb
RUN dpkg -i /tmp/bubblewrap.deb \
    && rm /tmp/bubblewrap.deb \
    && dpkg-query -W -f='${Package} ${Version}\n' bubblewrap \
        | grep -Fx "bubblewrap ${BUBBLEWRAP_VERSION}-orca1" \
    && test "$(readlink /usr/bin/bwrap)" = /usr/local/bin/orca-gvisor-bwrap \
    && test ! -u /usr/lib/orca/bwrap.real \
    && test -s /usr/share/doc/bubblewrap/copyright \
    && test -z "$(dpkg -V bubblewrap)"

RUN sed -i 's/^#user_allow_other$/user_allow_other/' /etc/fuse.conf \
    && grep -q '^user_allow_other$' /etc/fuse.conf

ARG DEFAULT_AGENT=claude
ENV NODE_ENV=production \
    PORT=4096 \
    SANDBOX_HARNESS_DEFAULT_AGENT=${DEFAULT_AGENT} \
    SANDBOX_HARNESS_DEFAULT_PERMISSION_MODE=acceptEdits \
    SANDBOX_HARNESS_DEFAULT_CWD=/mnt/session/outputs

COPY --from=build /app/node_modules /app/node_modules
COPY --from=build /app/packages/sdk-harness/package.json /app/packages/sdk-harness/package.json
COPY --from=build /app/packages/sdk-harness/dist /app/packages/sdk-harness/dist
COPY --from=build /app/packages/sdk-harness/node_modules /app/packages/sdk-harness/node_modules
COPY --from=build /app/packages/pi-harness/package.json /app/packages/pi-harness/package.json
COPY --from=build /app/packages/pi-harness/dist /app/packages/pi-harness/dist
COPY --from=build /app/packages/pi-harness/node_modules /app/packages/pi-harness/node_modules
COPY --from=build /app/packages/codex-harness/package.json /app/packages/codex-harness/package.json
COPY --from=build /app/packages/codex-harness/dist /app/packages/codex-harness/dist
COPY --from=build /app/packages/codex-harness/node_modules /app/packages/codex-harness/node_modules
COPY --from=build /app/packages/harness-catalog/package.json /app/packages/harness-catalog/package.json
COPY --from=build /app/packages/harness-catalog/dist /app/packages/harness-catalog/dist
COPY --from=build /app/services/sandbox-harness/node_modules ./node_modules
COPY --from=build /app/services/sandbox-harness/package.json ./package.json
COPY --from=build /app/services/sandbox-harness/dist ./dist
COPY services/sandbox-harness/docker-entrypoint.sh /usr/local/bin/orca-sandbox-harness
COPY services/sandbox-harness/orca-gvisor-bwrap /usr/local/bin/orca-gvisor-bwrap
COPY services/harness-server/sandbox-templates/orca-default/orca-git-creds /usr/local/bin/orca-git-creds
COPY services/harness-server/sandbox-templates/orca-default/orca-s3fs-mount /usr/local/bin/orca-s3fs-mount

# Prepare the shared session-resource roots before dropping privileges. Git's
# helper is configured system-wide; it reads only the short-lived session JWT
# injected at sandbox acquire time, never a repository PAT.
RUN chmod 0755 /usr/local/bin/orca-sandbox-harness /usr/local/bin/orca-gvisor-bwrap /usr/local/bin/orca-git-creds /usr/local/bin/orca-s3fs-mount \
    && bash -n /usr/local/bin/orca-gvisor-bwrap \
    && bwrap --version \
    && git config --system credential.helper /usr/local/bin/orca-git-creds \
    && git config --system credential.useHttpPath true \
    && git config --system safe.directory '*' \
    && mkdir -p /mnt/memory /mnt/session/outputs /workspace \
    && chown -R 1000:1000 /mnt/memory /mnt/session /workspace

EXPOSE 4096
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
  CMD node -e "fetch('http://127.0.0.1:' + (process.env.PORT || 4096) + '/healthz').then((r) => process.exit(r.ok ? 0 : 1)).catch(() => process.exit(1))"
CMD ["/usr/local/bin/orca-sandbox-harness"]

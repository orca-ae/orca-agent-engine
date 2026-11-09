# Copyright The Orca Authors
# SPDX-License-Identifier: Apache-2.0

# OpenSandbox tool-execution image for `mode=separate` sessions.
#
# The upstream image supplies execd and the language/tooling environment. Orca
# adds bubblewrap for the write-policy namespace plus the FUSE userspace tools
# used by output capture and memory_store mounts. gVisor supplies /dev/fuse
# inside sandbox; OpenSandbox adds only SETFCAP + SYS_ADMIN without host
# devices or privileged container. This image also exposes upstream Node
# binary on narrow PATH used by bounded reads.
FROM node:22-trixie-slim@sha256:517aa41d78545cb1b8c67b13655b4c13ede1ee9df1da8aab54cd7434aefbcaf8 AS package-probe-build
RUN apt-get update && apt-get install -y --no-install-recommends gcc libc6-dev
COPY services/harness-server/sandbox-templates/orca-default/orca-environment-probe.c /tmp/orca-environment-probe.c
RUN cc -Os -static -Wall -Werror /tmp/orca-environment-probe.c -o /tmp/orca-environment-probe

# Git 2.43 lacks proactive Basic auth: it waits for 401 while the scoped resolver denies missing markers.
FROM ubuntu:24.04@sha256:33ceb71981b602c1a7443a53469e4dba065f7503eab3078a2d7a57a2ab987517 AS git-client-build
ARG SANDBOX_GIT_VERSION=2.47.3
ARG SANDBOX_GIT_SHA256=9c2eb1250781b3e5bfef098572d07fdf132d67e6c065e4307332ade9819a1501
ARG SANDBOX_CURL_VERSION=8.5.0-2ubuntu10.16
ARG SANDBOX_GH_VERSION=2.45.0-1ubuntu0.3
RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        build-essential ca-certificates "curl=$SANDBOX_CURL_VERSION" "gh=$SANDBOX_GH_VERSION" \
        libcurl4-openssl-dev libexpat1-dev libssl-dev zlib1g-dev xz-utils \
    && curl -fsSLo /tmp/git.tar.xz "https://www.kernel.org/pub/software/scm/git/git-$SANDBOX_GIT_VERSION.tar.xz" \
    && echo "$SANDBOX_GIT_SHA256  /tmp/git.tar.xz" | sha256sum -c - \
    && mkdir /tmp/git \
    && tar -xJf /tmp/git.tar.xz -C /tmp/git --strip-components=1 \
    && make -C /tmp/git -j2 prefix=/usr/local NO_GETTEXT=YesPlease NO_TCLTK=YesPlease install \
    && test "$(git --version)" = "git version $SANDBOX_GIT_VERSION" \
    && rm -rf /tmp/git /tmp/git.tar.xz /var/lib/apt/lists/*

FROM docker.io/opensandbox/code-interpreter:v1.1.0@sha256:133a3c1720dd52291a019740c2987e7164ea6de79e23d8198798e58950ae2e6e
COPY --from=package-probe-build /tmp/orca-environment-probe /usr/local/libexec/orca-environment-probe
COPY --from=git-client-build /usr/local/ /usr/local/

COPY packages/sandbox-runtime/bootstrap/network.mjs /usr/local/libexec/orca-sandbox-network.mjs
USER root
ARG SANDBOX_CURL_VERSION=8.5.0-2ubuntu10.16
ARG SANDBOX_GH_VERSION=2.45.0-1ubuntu0.3
RUN node_path="$(command -v node)" \
    && test "${node_path#/}" != "${node_path}" \
    && test -x "${node_path}" \
    && if [ "${node_path}" != /usr/local/bin/node ]; then \
      test ! -e /usr/local/bin/node; \
      ln -s "${node_path}" /usr/local/bin/node; \
    fi \
    && /usr/bin/env -i PATH=/usr/local/bin:/usr/bin node --version \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
        bubblewrap \
        "curl=$SANDBOX_CURL_VERSION" \
        "gh=$SANDBOX_GH_VERSION" \
        libexpat1 \
        fuse3 \
        s3fs \
    && rm -rf /var/lib/apt/lists/* \
    && bwrap --version \
    && command -v setpriv \
    && command -v mount \
    && command -v realpath \
    && command -v umount \
    && test "$(id -u ubuntu):$(id -g ubuntu)" = 1000:1000 \
    && s3fs --version \
    && s3fs --help 2>&1 | grep -F 'compat_dir' \
    && fusermount3 --version

COPY services/harness-server/sandbox-templates/orca-default/orca-s3fs-mount /usr/local/bin/orca-s3fs-mount
RUN chmod 0755 /usr/local/bin/orca-s3fs-mount

# Memory/output mounts use allow_other so the later agent namespace can read
# files served by the root-owned s3fs daemon.
RUN sed -i 's/^#user_allow_other$/user_allow_other/' /etc/fuse.conf \
    && grep -q '^user_allow_other$' /etc/fuse.conf \
    && install -d -m 0777 /mnt/inputs /mnt/memory /mnt/session/outputs

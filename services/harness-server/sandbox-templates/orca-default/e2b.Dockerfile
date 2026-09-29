# Copyright The Orca Authors
# SPDX-License-Identifier: Apache-2.0

# Orca custom E2B sandbox template.
#
# Built on top of E2B's upstream code-interpreter image (Debian-based, ships
# Python 3 + Node), and adds s3fs-fuse + fuse3 + a constrained sudo mount
# helper. `orca-start.sh` mounts the session output prefix read-write at
# /mnt/session/outputs; MemoryFuseStrategy mounts memory stores under
# /mnt/memory/. File resources are delivered host-side (tarball prefetch),
# never via an in-sandbox blob mount.
#
# Operator workflow lives in README.md alongside this file. Do NOT change the
# pinned base digest without coordinating with operator who rebuilds + publishes
# template. Update digest deliberately after repeating spike checklist.
FROM e2bdev/code-interpreter:latest@sha256:442ec598ec8ca4ed01b5bb24ad6e4f2e6ac80fd88f1564ff9a01e82da18e1e3b

USER root

# The bounded read/edit helper runs Node with a cleared environment and the
# root-owned PATH below. Normalize the upstream Node binary into that PATH and
# fail the template build if the contract cannot be satisfied.
RUN node_path="$(command -v node)" \
    && test "${node_path#/}" != "${node_path}" \
    && test -x "${node_path}" \
    && if [ "${node_path}" != /usr/local/bin/node ]; then \
      test ! -e /usr/local/bin/node; \
      ln -s "${node_path}" /usr/local/bin/node; \
    fi \
    && /usr/bin/env -i PATH=/usr/local/bin:/usr/bin node --version

# s3fs-fuse + fuse3 (FUSE 3 userspace tools), plus the bits the start script
# needs (curl is handy for diag, sudo for the NOPASSWD mount entry, ca-certs
# so s3fs can talk to HTTPS object stores).
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        s3fs \
        fuse3 \
        ca-certificates \
        curl \
        jq \
        git \
        bubblewrap \
        util-linux \
        sudo \
    && rm -rf /var/lib/apt/lists/* \
    && command -v realpath \
    && command -v setpriv \
    && test "$(id -u user):$(id -g user)" = 1000:1000 \
    && s3fs --help 2>&1 | grep -F 'compat_dir'

COPY orca-s3fs-mount /usr/local/bin/orca-s3fs-mount
RUN chmod 0755 /usr/local/bin/orca-s3fs-mount

# E2B sandboxes default to running as user `user`. Give that user passwordless
# sudo for the constrained mount helper and mount teardown.
# Remove the upstream image's blanket `NOPASSWD: ALL` rule first; package
# installation is complete above and runtime setup needs no general root shell.
# NOTE: sudoers matches literal invocation paths (it does not follow symlinks),
# so allow both Debian umount paths used by cleanup/deactivation.
RUN sed -E -i '/^[[:space:]]*user[[:space:]]+ALL=\(ALL\)[[:space:]]+NOPASSWD:[[:space:]]+ALL[[:space:]]*$/d' /etc/sudoers \
    && ! grep -Eq '^[[:space:]]*user[[:space:]]+ALL=\(ALL\)[[:space:]]+NOPASSWD:[[:space:]]+ALL[[:space:]]*$' /etc/sudoers \
    && printf '%s\n' \
      'Defaults!/usr/local/bin/orca-s3fs-mount env_keep += "ORCA_S3_ACCESS_KEY_ID ORCA_S3_SECRET_ACCESS_KEY ORCA_S3_SESSION_TOKEN"' \
      'user ALL=(root) NOPASSWD: /usr/local/bin/orca-s3fs-mount' \
      'user ALL=(root) NOPASSWD: /usr/bin/umount, /bin/umount' \
      > /etc/sudoers.d/orca-fuse \
    && chmod 0440 /etc/sudoers.d/orca-fuse \
    && visudo -cf /etc/sudoers

# Pre-create every mount point the harness's strategies write into. /mnt is
# root-owned at runtime (E2B's base image), so non-privileged `mkdir -p` from
# the strategies fails with EACCES unless each leaf already exists with
# user:user ownership. Strategies that need a per-resource subdir (e.g.
# memory_fuse → /mnt/memory/{store}/, tarball_prefetch → /mnt/inputs/{file})
# can mkdir under these owned roots without sudo.
RUN mkdir -p /mnt/inputs /mnt/memory /mnt/session/outputs \
    && chown -R user:user /mnt/inputs /mnt/memory /mnt/session

# FUSE's `allow_other` option (used by every s3fs mount in this image so the
# `user` user can read files served by the root-mounted daemon) requires
# `user_allow_other` in /etc/fuse.conf. The Debian default ships this option
# commented out, so without this RUN line reads under the output/memory FUSE
# mounts surface as "Permission denied" even though the mount looks
# successful.
RUN sed -i 's/^#user_allow_other$/user_allow_other/' /etc/fuse.conf \
    && grep -q '^user_allow_other$' /etc/fuse.conf

# Start script runs at sandbox boot via `start_cmd` in e2b.toml. It mounts
# (or no-ops) and exits — it does NOT exec the agent. The harness drives the
# agent via `commands.run` after the sandbox is up.
COPY start.sh /usr/local/bin/orca-start.sh
RUN chmod +x /usr/local/bin/orca-start.sh

# Bake and register the git credential helper system-wide at image
# build time. Without per-session ORCA_GIT_CREDS_URL + ORCA_GIT_CREDS_TOKEN,
# the helper returns no credentials and Git falls back to anonymous access.
COPY orca-git-creds /usr/local/bin/orca-git-creds
RUN chmod 0755 /usr/local/bin/orca-git-creds \
    && git config --system credential.helper /usr/local/bin/orca-git-creds \
    && git config --system credential.useHttpPath true \
    && git config --system safe.directory '*'

USER user

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
FROM docker.io/opensandbox/code-interpreter:v1.1.0@sha256:133a3c1720dd52291a019740c2987e7164ea6de79e23d8198798e58950ae2e6e

USER root
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

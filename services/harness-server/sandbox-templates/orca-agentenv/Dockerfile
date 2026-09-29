# Copyright The Orca Authors
# SPDX-License-Identifier: Apache-2.0

# AgentENV tool-execution image for `mode=separate` sessions.
#
# AgentENV supplies envd from its tools drive; this image supplies the userland
# that Orca's tool adapter and Bubblewrap write-policy boundary execute inside
# the Firecracker guest. The adapter currently uses envd file transfer rather
# than guest FUSE, so no FUSE device or s3fs package is part of this contract.
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
        git \
        util-linux \
    && rm -rf /var/lib/apt/lists/* \
    && bwrap --version \
    && command -v bash \
    && command -v realpath \
    && command -v setpriv \
    && command -v timeout \
    && test "$(id -u ubuntu):$(id -g ubuntu)" = 1000:1000

COPY services/harness-server/sandbox-templates/orca-default/orca-git-creds /usr/local/bin/orca-git-creds
RUN chmod 0755 /usr/local/bin/orca-git-creds \
    && git config --system credential.helper /usr/local/bin/orca-git-creds \
    && git config --system credential.useHttpPath true \
    && git config --system safe.directory '*' \
    && install -d -m 0777 /mnt/inputs /mnt/memory /mnt/session/outputs

# envd executes tools as the image's configured user. The trusted adapter
# needs root for setup and then runs every agent command inside Bubblewrap as
# uid/gid 1000 with all capabilities dropped.
USER root
CMD ["tail", "-f", "/dev/null"]

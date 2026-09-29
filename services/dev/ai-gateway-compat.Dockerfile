# Copyright The Orca Authors
# SPDX-License-Identifier: Apache-2.0

# Compatibility wrapper for the published orca-ai-gateway image.
#
# Some release candidates ship a binary built on a newer glibc than the
# distroless Debian 12 runtime in the image. This keeps the external gateway
# binary but repackages it on distroless Debian 13 so local-stack / CI can keep
# using the published artifact while gateway release packaging is corrected
# upstream.

ARG AI_GATEWAY_SOURCE_IMAGE=ghcr.io/orca-ae/orca-ai-gateway:v0.4.3-rc.3
FROM ${AI_GATEWAY_SOURCE_IMAGE} AS source

FROM gcr.io/distroless/cc-debian13:nonroot

COPY --from=source /usr/local/bin/orca-gateway /usr/local/bin/orca-gateway

EXPOSE 8090 9099
ENTRYPOINT ["/usr/local/bin/orca-gateway"]
CMD ["run", "--config", "/etc/orca-gateway/config.yaml"]

#!/usr/bin/env bash
# Copyright The Orca Authors
# SPDX-License-Identifier: Apache-2.0

# Orca sandbox startup hook.
#
# Runs as user `user` at sandbox boot (see start_cmd in e2b.toml). Mounts the
# session output prefix via s3fs-fuse when ORCA_FUSE_OUTPUTS_ENABLE=1. When
# the flag is unset (or 0) this script is a no-op so the same sandbox
# template also serves sessions without output capture. File resources are
# never mounted here — the harness delivers them host-side via the
# tarball_prefetch strategy (sandbox creds carry no file-blob access).
#
# CONTRACT: this script mounts and exits 0. It does NOT exec the agent. The
# harness invokes the agent process afterwards via `commands.run`. Anything
# the script leaves running (e.g. s3fs daemons) must be self-contained.
#
# Idempotent: re-invoking on an already-mounted target is a no-op (guarded by
# `mountpoint -q`). Failure to mount when the enable flag is set exits 1 with
# a clear diagnostic.
set -euo pipefail

# Track mount points we own in this script invocation so an ERR trap can
# unwind partial mounts (e.g. outputs mounted, a later step failed) before
# exit. Without this, set -e leaves the mount active with stale creds and
# the next invocation's mountpoint -q guard skips it.
declare -a _orca_owned_mounts=()

# shellcheck disable=SC2329 # invoked indirectly by ERR trap
cleanup_partial_mounts() {
    local rc=$?
    if [ "${#_orca_owned_mounts[@]}" -gt 0 ]; then
        log "cleanup: unmounting partial mounts after failure: ${_orca_owned_mounts[*]}"
        local mp
        for mp in "${_orca_owned_mounts[@]}"; do
            # `|| true` swallows umount errors so this trap never re-fires.
            sudo umount "${mp}" 2>/dev/null || true
        done
    fi
    exit "${rc}"
}
trap cleanup_partial_mounts ERR

log() {
    printf 'ORCA: %s\n' "$*"
}

fail() {
    printf 'ORCA: %s\n' "$*" >&2
    exit 1
}

require_env() {
    local var
    for var in "$@"; do
        if [ -z "${!var:-}" ]; then
            fail "${var} is required when ORCA_FUSE_OUTPUTS_ENABLE=1"
        fi
    done
}

mount_outputs() {
    if mountpoint -q /mnt/session/outputs; then
        log "outputs mount already active at /mnt/session/outputs"
        return 0
    fi
    require_env S3_BUCKET S3_ENDPOINT SESSION_OUTPUT_PREFIX AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY

    # use_cache="" disables local on-disk cache so writes flush to S3 (output
    # capture lists the prefix). ensure_diskfree=0 keeps s3fs
    # from refusing to upload when the sandbox has minimal scratch space.
    local path_style_opt=""
    local path_style_value
    path_style_value="$(printf '%s' "${S3_FORCE_PATH_STYLE:-true}" | tr '[:upper:]' '[:lower:]')"
    case "${path_style_value}" in
        true|1|yes) path_style_opt="use_path_request_style," ;;
        false|0|no) ;;
        *) fail "S3_FORCE_PATH_STYLE must be true or false" ;;
    esac
    local opts="allow_other,${path_style_opt}use_cache=,ensure_diskfree=0,compat_dir,uid=1000,gid=1000,umask=0022,url=${S3_ENDPOINT}"
    if ! ORCA_S3_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID}" \
        ORCA_S3_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY}" \
        ORCA_S3_SESSION_TOKEN="${AWS_SESSION_TOKEN:-}" \
        sudo --preserve-env=ORCA_S3_ACCESS_KEY_ID,ORCA_S3_SECRET_ACCESS_KEY,ORCA_S3_SESSION_TOKEN \
        /usr/local/bin/orca-s3fs-mount \
        "${S3_BUCKET}:/${SESSION_OUTPUT_PREFIX}" \
        /mnt/session/outputs \
        "${opts}"; then
        fail "s3fs outputs mount failed - check FUSE capability and /etc/sudoers.d/orca-fuse"
    fi
    _orca_owned_mounts+=("/mnt/session/outputs")
    log "mounted ${S3_BUCKET}:/${SESSION_OUTPUT_PREFIX} -> /mnt/session/outputs (rw)"
}

outputs_enable="${ORCA_FUSE_OUTPUTS_ENABLE:-0}"

if [ "${outputs_enable}" = "1" ]; then
    mount_outputs
else
    log "FUSE mounts disabled (prefetch path active)"
fi

log "startup complete"
exit 0

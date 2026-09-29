// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

const SANDBOX_WORK_DIR_MARKERS =
  /orca-harness|sbx_local_|orca-sandbox-|sbx_inmem_|\/mnt\/session\/outputs/i;

export function mentionsSandboxWorkDir(text: string): boolean {
  return SANDBOX_WORK_DIR_MARKERS.test(text);
}

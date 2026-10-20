// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Markers that identify a path as a per-session sandbox work-dir root, across every
// runtime and both execution engines.
//
// The point of the assertion these back is CONTAINMENT: a tool call must run in the
// session's own root, not in the cwd of whatever process is driving it. The markers
// therefore have to cover each runtime's naming, because a regex that only knows the
// separate path would pass vacuously on a colocated run that had no containment at all.
//
//   orca-harness          harness-server's work dir (`HARNESS_WORK_DIR`)
//   sbx_local_            the srt-wrapped Local runtime's per-session id
//   sbx_inmem_            the InMemory runtime's per-session id
//   orca-sandbox-         the InMemory runtime's `mkdtemp` root, under the OS temp dir.
//                         This is what a COLOCATED session gets by default, since the
//                         session-runner falls back to InMemory unless `SANDBOX_RUNTIME=local`
//                         resolves — see `services/session-runner/src/sandbox/seam.ts`.
//   runner_token_         the colocated root under `SANDBOX_RUNTIME=local`: the worker
//                         creates `join(WORKSPACE_DIR, runnerId)` per runner and the runtime
//                         puts `sessions/<id>` beneath it, so the runner id is the only
//                         stable component — `WORKSPACE_DIR` is operator-configurable.
//   /mnt/session/outputs  the policy-enforced output path, identical in both engines.
const SANDBOX_WORK_DIR_MARKERS =
  /orca-harness|sbx_local_|orca-sandbox-|sbx_inmem_|runner_token_|\/mnt\/session\/outputs/i;

export function mentionsSandboxWorkDir(text: string): boolean {
  return SANDBOX_WORK_DIR_MARKERS.test(text);
}

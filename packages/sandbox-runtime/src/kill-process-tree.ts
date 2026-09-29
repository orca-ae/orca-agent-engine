// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { ChildProcess } from 'node:child_process';

/**
 * Signal the child's whole process group, falling back to the child alone.
 *
 * Spawned shells fork the real command (`bash -c 'cmd'` forks `cmd`; the local
 * runtime's wrapper chain is `sh -> sandbox-exec -> bash -c 'cmd'` on macOS).
 * Signalling only the outer process reaps the shell and orphans the running
 * command — which keeps executing (and holding the stdio pipes open, so
 * `close` never fires) after the caller was told the command was killed.
 * Callers spawn with `detached: true` so the child leads its own group and the
 * negative-pid kill reaches the full tree.
 */
export function killSandboxProcessTree(
  proc: ChildProcess,
  signal: NodeJS.Signals = 'SIGKILL',
): void {
  // A settled child keeps its pid populated, but the kernel may already have
  // recycled the process GROUP id — a group SIGKILL after exit (the routine
  // `finally { handle.kill(); }` after awaiting `exited`) would then hit an
  // unrelated pgid. Node's own proc.kill() no-ops after exit; mirror that.
  if (proc.exitCode !== null || proc.signalCode !== null) {
    return;
  }
  if (proc.pid !== undefined) {
    try {
      process.kill(-proc.pid, signal);
      return;
    } catch {
      // Group already gone (or the child never became a leader): fall through.
    }
  }
  try {
    proc.kill(signal);
  } catch {
    // Already dead.
  }
}

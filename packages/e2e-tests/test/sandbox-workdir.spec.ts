// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { mentionsSandboxWorkDir } from '../src/sandbox-workdir.js';

describe('mentionsSandboxWorkDir', () => {
  it('accepts local, in-memory, and policy-enforced sandbox work-dir markers', () => {
    expect(mentionsSandboxWorkDir('/var/tmp/orca-harness/sessions/sbx_local_1_abc')).toBe(true);
    expect(mentionsSandboxWorkDir('/private/var/folders/x/T/orca-sandbox-kvVKQr')).toBe(true);
    expect(mentionsSandboxWorkDir('/tmp/anything/sbx_inmem_1_abc')).toBe(true);
    expect(mentionsSandboxWorkDir('/mnt/session/outputs')).toBe(true);
  });

  it('accepts the colocated per-runner work-dir root', () => {
    // `SANDBOX_RUNTIME=local` on the colocated path: the environment-worker creates
    // `join(WORKSPACE_DIR, runnerId)` for each runner it spawns, and the runtime puts
    // `sessions/<id>` beneath that. WORKSPACE_DIR is operator-configurable, so the
    // runner id is the only component that is always present.
    expect(
      mentionsSandboxWorkDir(
        '/var/tmp/orca-self-hosted-workspaces/runner_token_557a8f0c/sessions/sbx_local_1_abc',
      ),
    ).toBe(true);
    // A relocated WORKSPACE_DIR must still be recognized.
    expect(mentionsSandboxWorkDir('/srv/agents/runner_token_deadbeef/sessions/x')).toBe(true);
  });

  it('rejects generic paths', () => {
    expect(mentionsSandboxWorkDir('/tmp/not-the-session-root')).toBe(false);
  });
});

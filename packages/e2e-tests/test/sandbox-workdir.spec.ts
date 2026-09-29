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

  it('rejects generic paths', () => {
    expect(mentionsSandboxWorkDir('/tmp/not-the-session-root')).toBe(false);
  });
});

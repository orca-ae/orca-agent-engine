// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { stableSessionThreadId } from '../../src/index.js';

describe('stableSessionThreadId', () => {
  it('preserves deterministic primary and subagent thread IDs', () => {
    expect(stableSessionThreadId('ws_1', 'ses_1', '')).toBe(
      'sth_d2a92fa2-25e6-5f6a-b6a0-ed8a6893ac60',
    );
    expect(stableSessionThreadId('ws_1', 'ses_1', 'subagents/child')).toBe(
      'sth_21a43d28-e7cd-56ed-af7f-06f496c1c1a4',
    );
  });

  it('scopes thread IDs by workspace, session, and subpath', () => {
    const threadId = stableSessionThreadId('ws_1', 'ses_1', 'subagents/child');

    expect(stableSessionThreadId('ws_2', 'ses_1', 'subagents/child')).not.toBe(threadId);
    expect(stableSessionThreadId('ws_1', 'ses_2', 'subagents/child')).not.toBe(threadId);
    expect(stableSessionThreadId('ws_1', 'ses_1', 'subagents/other')).not.toBe(threadId);
  });
});

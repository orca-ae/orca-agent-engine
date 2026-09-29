// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { parseWorkspaceId } from '../../src/auth/workspace-id.js';

describe('parseWorkspaceId', () => {
  it.each(['ws_alpha', 'workspace-123', '01234567-89ab-cdef'])('accepts %s', (value) => {
    expect(parseWorkspaceId(value)).toBe(value);
  });

  it.each([123, {}, '', '../other', 'ws/other', 'ws\\other', `ws_${'x'.repeat(126)}`])(
    'rejects unsafe OIDC workspace claim %j',
    (value) => {
      expect(parseWorkspaceId(value)).toBeNull();
    },
  );
});

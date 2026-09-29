// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { VaultUpdate } from '../../src/contracts/vaults.contract.js';

describe('Vault update contract', () => {
  it('accepts Anthropic-compatible nullable display_name updates', () => {
    expect(VaultUpdate.safeParse({}).success).toBe(true);
    expect(VaultUpdate.safeParse({ display_name: 'Production credentials' }).success).toBe(true);
    expect(VaultUpdate.safeParse({ display_name: null }).success).toBe(true);
  });
});

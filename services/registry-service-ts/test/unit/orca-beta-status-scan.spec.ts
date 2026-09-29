// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { dialectOf } from '../../scripts/orca-beta-status-scan.mjs';

describe('orca-beta status condition classification', () => {
  it('recognizes a direct dialect term after another conjunct', () => {
    expect(dialectOf("create.auth.type === 'provider' && !isOrcaBetaRequest(req.headers)")).toBe(
      'default',
    );
    expect(dialectOf('ready && isOrcaBetaRequest(req.headers)')).toBe('orca-beta');
  });

  it('recognizes provider visibility conditions without guessing at arbitrary helpers', () => {
    expect(dialectOf('!row || !isCredentialVisible(row, isOrcaBetaRequest(req.headers))')).toBe(
      'default',
    );
    expect(dialectOf('isCredentialVisible(row, orcaBeta)')).toBe('orca-beta');
    expect(dialectOf('someOtherHelper(row, isOrcaBetaRequest(req.headers))')).toBeNull();
  });
});

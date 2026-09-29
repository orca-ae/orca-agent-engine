// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { internalContract } from '../../src/contracts/internal.contract.js';

describe('internal MCP destination contract', () => {
  it('accepts the strict request and secret-free response shapes', () => {
    const route = internalContract.resolveMcpDestination;

    expect(route.body.parse({ backend: 'github' })).toEqual({ backend: 'github' });
    expect(() => route.body.parse({ backend: 'github', extra: true })).toThrow();
    expect(
      route.responses[200].parse({
        url: 'https://mcp.example.test/rpc',
        credential_id: null,
        revision: 7,
      }),
    ).toEqual({
      url: 'https://mcp.example.test/rpc',
      credential_id: null,
      revision: 7,
    });
    for (const url of [
      'ftp://mcp.example.test/rpc',
      'https://user:secret@mcp.example.test/rpc',
      'https://mcp.example.test/rpc#credential',
    ]) {
      expect(() =>
        route.responses[200].parse({
          url,
          credential_id: null,
          revision: 7,
        }),
      ).toThrow();
    }
    for (const unexpected of [
      { extra: true },
      { secret_value: 'must-not-pass' },
      { access_secret_ref: 'must-not-pass' },
    ]) {
      expect(() =>
        route.responses[200].parse({
          url: 'https://mcp.example.test/rpc',
          credential_id: null,
          revision: 7,
          ...unexpected,
        }),
      ).toThrow();
    }
    for (const revision of [0, Number.MAX_SAFE_INTEGER + 1]) {
      expect(() =>
        route.responses[200].parse({
          url: 'https://mcp.example.test/rpc',
          credential_id: null,
          revision,
        }),
      ).toThrow();
    }
  });
});

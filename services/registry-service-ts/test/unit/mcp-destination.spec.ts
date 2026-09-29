// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import {
  canonicalMcpServerUrl,
  compareCredentialCandidates,
  mcpDestinationRevision,
} from '../../src/domain/mcp-destination.js';

describe('MCP destination credential ordering', () => {
  const vaultOrder = new Map([
    ['vlt_first', 0],
    ['vlt_second', 1],
  ]);

  it('orders by session vault precedence before timestamps', () => {
    const candidates = [
      candidate('vcrd_older_second_vault', 'vlt_second', 1),
      candidate('vcrd_newer_first_vault', 'vlt_first', 2),
    ];

    candidates.sort((left, right) => compareCredentialCandidates(left, right, vaultOrder));
    expect(candidates.map(({ id }) => id)).toEqual([
      'vcrd_newer_first_vault',
      'vcrd_older_second_vault',
    ]);
  });

  it('orders same-vault candidates by created_at then id', () => {
    const candidates = [
      candidate('vcrd_b', 'vlt_first', 2),
      candidate('vcrd_c', 'vlt_first', 1),
      candidate('vcrd_a', 'vlt_first', 1),
    ];

    candidates.sort((left, right) => compareCredentialCandidates(left, right, vaultOrder));
    expect(candidates.map(({ id }) => id)).toEqual(['vcrd_a', 'vcrd_c', 'vcrd_b']);
  });
});

describe('MCP destination binding identity', () => {
  it('normalizes scheme, host, default ports, and trailing slashes', () => {
    expect(canonicalMcpServerUrl('HTTPS://Example.COM:443/mcp///')).toBe('https://example.com/mcp');
    expect(canonicalMcpServerUrl('http://Example.COM:80/')).toBe('http://example.com');
  });

  it('preserves non-default ports, paths, and query strings', () => {
    expect(canonicalMcpServerUrl('https://Example.COM:8443/mcp/?tenant=one')).toBe(
      'https://example.com:8443/mcp?tenant=one',
    );
  });

  it('rejects unsupported or credential-bearing URLs', () => {
    expect(canonicalMcpServerUrl('file:///etc/passwd')).toBeNull();
    expect(canonicalMcpServerUrl('https://user:secret@example.com/mcp')).toBeNull();
    expect(canonicalMcpServerUrl('https://example.com/mcp#secret')).toBeNull();
  });

  it('uses a stable positive safe revision that changes with binding tuple', () => {
    const first = mcpDestinationRevision('https://example.com/mcp', 'vcrd_one');
    expect(first).toBe(mcpDestinationRevision('https://example.com/mcp', 'vcrd_one'));
    expect(first).toBeGreaterThan(0);
    expect(Number.isSafeInteger(first)).toBe(true);
    expect(first).not.toBe(mcpDestinationRevision('https://example.com/mcp/', 'vcrd_one'));
    expect(first).not.toBe(mcpDestinationRevision('https://example.com/mcp', 'vcrd_two'));
    expect(first).not.toBe(mcpDestinationRevision('https://example.com/mcp', null));
  });
});

function candidate(id: string, vaultId: string, timestamp: number) {
  return { id, vaultId, createdAt: new Date(timestamp) };
}

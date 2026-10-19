// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { expect, it, vi } from 'vitest';
import { archivedSubpaths, assertNotArchived } from '../../src/archive.js';
import { installManagedFetch } from '../../src/transport.js';

it('blocks model preflight for archived ancestors before dispatch, but not siblings', async () => {
  const parent = 'subagents/11111111-1111-1111-1111-111111111111';
  const child = 'subagents/22222222-2222-2222-2222-222222222222';
  const sibling = 'subagents/33333333-3333-3333-3333-333333333333';
  const parents = new Map([
    [parent, ''],
    [child, parent],
    [sibling, ''],
  ]);
  const archived = archivedSubpaths([parent]);
  const nativeFetch = vi.fn();
  const original = globalThis.fetch;
  globalThis.fetch = nativeFetch;
  const release = installManagedFetch(
    () => ({ origin: 'http://127.0.0.1', token: 'token', sessionId: 'session' }),
    () => {},
    (id) => assertNotArchived('subagents/' + id, archived, (path) => parents.get(path)),
  );
  try {
    await expect(
      fetch('http://127.0.0.1/v1/proxy/deepseek/v1/messages', {
        method: 'POST',
        body: '{}',
        headers: { 'x-deepseek-harness-session-id': child.slice(10) },
      }),
    ).rejects.toThrow('archived');
    expect(nativeFetch).not.toHaveBeenCalled();
    expect(() => assertNotArchived(sibling, archived, (path) => parents.get(path))).not.toThrow();
    archived.add('');
    expect(() => assertNotArchived(sibling, archived, (path) => parents.get(path))).toThrow(
      'archived',
    );
    expect(() => archivedSubpaths(['../other'])).toThrow('snapshot');
  } finally {
    release();
    globalThis.fetch = original;
  }
});

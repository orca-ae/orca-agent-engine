// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { Pool } from 'pg';
import { describe, expect, it, vi } from 'vitest';
import { PostgresMemoryMetadataStore } from '../../src/metadata/postgres.js';

describe('Postgres store write lock cleanup', () => {
  function setup(unlockError?: Error, acquireError?: Error) {
    const client = {
      query: vi.fn(async (sql: string) => {
        if (sql.includes('pg_advisory_unlock') && unlockError) throw unlockError;
        if (sql.includes('pg_advisory_lock(') && acquireError) throw acquireError;
        return { rows: [] };
      }),
      release: vi.fn(),
    };
    const metadata = new PostgresMemoryMetadataStore({
      connect: vi.fn(async () => client),
    } as unknown as Pool);
    return { client, metadata };
  }

  it('unlocks and returns successful work without discarding the connection', async () => {
    const { client, metadata } = setup();
    await expect(metadata.withStoreWriteLock('ws_lock', 'mems_lock', async () => 42)).resolves.toBe(
      42,
    );
    expect(client.query).toHaveBeenCalledTimes(2);
    expect(client.release.mock.calls).toEqual([[]]);
  });

  it('discards the connection and preserves the work failure when unlock also fails', async () => {
    const workError = new Error('work failed');
    const unlockError = new Error('unlock failed');
    const { client, metadata } = setup(unlockError);
    await expect(
      metadata.withStoreWriteLock('ws_lock', 'mems_lock', async () => {
        throw workError;
      }),
    ).rejects.toBe(workError);
    expect(client.query).toHaveBeenCalledTimes(2);
    expect(client.release.mock.calls).toEqual([[unlockError]]);
  });

  it('reports unlock failure after successful work and discards the connection', async () => {
    const unlockError = new Error('unlock failed');
    const { client, metadata } = setup(unlockError);
    await expect(metadata.withStoreWriteLock('ws_lock', 'mems_lock', async () => 42)).rejects.toBe(
      unlockError,
    );
    expect(client.release.mock.calls).toEqual([[unlockError]]);
  });

  it('releases the connection without running work or unlocking when lock acquisition fails', async () => {
    const acquireError = new Error('lock failed');
    const { client, metadata } = setup(undefined, acquireError);
    const work = vi.fn(async () => 42);
    await expect(metadata.withStoreWriteLock('ws_lock', 'mems_lock', work)).rejects.toBe(
      acquireError,
    );
    expect(work).not.toHaveBeenCalled();
    expect(client.query).toHaveBeenCalledTimes(1);
    expect(client.release.mock.calls).toEqual([[]]);
  });
});

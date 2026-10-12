// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { setTimeout as delay } from 'node:timers/promises';
import { afterEach, describe, expect, it } from 'vitest';
import { Client, Pool } from 'pg';
import { loadConfig } from '../../src/config.js';

const connectionString =
  process.env['DATABASE_URL'] ?? 'postgres://orca:orca@localhost:5432/registry';

describe.each(['file', 'memory'] as const)('%s store pool retention (real pg driver)', (store) => {
  const pools: Pool[] = [];

  function openPool(min: number): Pool {
    const prefix = store === 'file' ? 'FILESTORE' : 'MEMORYSTORE';
    const config = loadConfig({
      [`${prefix}_POOL_MAX`]: '2',
      [`${prefix}_POOL_MIN`]: String(min),
      [`${prefix}_POOL_IDLE_TIMEOUT_MS`]: '40',
    });
    const pool = new Pool({
      connectionString,
      max: store === 'file' ? config.fileStorePoolMax : config.memorystorePoolMax,
      min: store === 'file' ? config.fileStorePoolMin : config.memorystorePoolMin,
      idleTimeoutMillis:
        store === 'file' ? config.fileStorePoolIdleTimeoutMs : config.memorystorePoolIdleTimeoutMs,
    });
    pools.push(pool);
    return pool;
  }

  afterEach(async () => {
    await Promise.all(pools.splice(0).map((pool) => pool.end()));
  });

  it('does not pre-create clients and reuses an established connection beyond the idle timeout', async () => {
    const pool = openPool(1);
    expect(pool.totalCount).toBe(0);
    const first = await pool.query<{ pid: number }>('select pg_backend_pid() as pid');
    await delay(120);
    expect(pool.totalCount).toBe(1);
    expect(pool.idleCount).toBe(1);
    const second = await pool.query<{ pid: number }>('select pg_backend_pid() as pid');
    expect(second.rows[0]!.pid).toBe(first.rows[0]!.pid);
  });

  it('restores idle eviction when the minimum is zero', async () => {
    const pool = openPool(0);
    const first = await pool.query<{ pid: number }>('select pg_backend_pid() as pid');
    await expect.poll(() => pool.totalCount, { timeout: 2000 }).toBe(0);
    const second = await pool.query<{ pid: number }>('select pg_backend_pid() as pid');
    expect(second.rows[0]!.pid).not.toBe(first.rows[0]!.pid);
  });

  it('removes a failed idle client and reconnects on the next request', async () => {
    const pool = openPool(1);
    const errors: Error[] = [];
    pool.on('error', (error) => errors.push(error));
    const first = await pool.query<{ pid: number }>('select pg_backend_pid() as pid');
    const control = new Client({ connectionString });
    try {
      await control.connect();
      await control.query('select pg_terminate_backend($1)', [first.rows[0]!.pid]);
      await expect.poll(() => errors.length, { timeout: 2000 }).toBe(1);
      expect(pool.totalCount).toBe(0);
      const second = await pool.query<{ pid: number }>('select pg_backend_pid() as pid');
      expect(second.rows[0]!.pid).not.toBe(first.rows[0]!.pid);
    } finally {
      await control.end();
    }
  });
});

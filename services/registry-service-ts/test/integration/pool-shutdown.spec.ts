// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { randomBytes } from 'node:crypto';
import { Pool, type Client } from 'pg';
import { describe, expect, it } from 'vitest';
import { trackPoolShutdown } from '../helpers/pool-shutdown.js';

const connectionString =
  process.env['DATABASE_URL'] ?? 'postgres://orca:orca@localhost:5432/registry';

describe('fresh PostgreSQL pool shutdown', () => {
  it.each(['idle', 'retired', 'checked out'] as const)(
    'waits for an %s client socket before dropping its database',
    async (state) => {
      const databaseName = `registry_pool_shutdown_${process.pid}_${randomBytes(4).toString('hex')}`;
      const adminUrl = new URL(connectionString);
      adminUrl.pathname = '/postgres';
      const databaseUrl = new URL(connectionString);
      databaseUrl.pathname = `/${databaseName}`;
      const admin = new Pool({ connectionString: adminUrl.toString(), max: 1 });
      const pool = new Pool({
        connectionString: databaseUrl.toString(),
        max: 1,
        maxUses: state === 'retired' ? 1 : Infinity,
      });
      const closePool = trackPoolShutdown(pool);
      let closing: Promise<void> | undefined;
      let release: (() => void) | undefined;
      let stream: Client['connection']['stream'] | undefined;
      try {
        await admin.query(`CREATE DATABASE "${databaseName}"`);
        const client = await pool.connect();
        release = () => client.release();
        const result = await client.query<{ pid: number }>('SELECT pg_backend_pid() AS pid');
        // Hold the protocol Terminate packet to expose the socket-close race deterministically.
        stream = (client as unknown as Client).connection.stream;
        stream.cork();
        if (state !== 'checked out') {
          release();
          release = undefined;
        }
        let closed = false;
        closing = closePool().then(() => {
          closed = true;
        });
        await expect.poll(() => (state === 'checked out' ? pool.ending : pool.ended)).toBe(true);
        expect(closed).toBe(false);
        const remaining = await admin.query<{ count: number }>(
          'SELECT count(*)::int AS count FROM pg_stat_activity WHERE pid = $1',
          [result.rows[0]!.pid],
        );
        expect(remaining.rows[0]!.count).toBe(1);
        release?.();
        release = undefined;
        stream.uncork();
        await closing;
        expect(closed).toBe(true);
        await admin.query(`DROP DATABASE "${databaseName}"`);
      } finally {
        release?.();
        stream?.uncork();
        try {
          await (closing ?? closePool());
          await admin.query(`DROP DATABASE IF EXISTS "${databaseName}"`);
        } finally {
          await admin.end();
        }
      }
    },
  );
});

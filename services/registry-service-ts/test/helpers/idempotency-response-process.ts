// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import assert from 'node:assert/strict';
import { setImmediate } from 'node:timers/promises';
import Fastify from 'fastify';
import { registerVaultsRoutes } from '../../src/api/vaults.routes.js';
import { registerClaudePublicEdge } from '../../src/middleware/claude-edge.js';
import {
  buildIdempotencyPreHandler,
  buildIdempotencyResponseHook,
  type IdempotencyStore,
} from '../../src/middleware/idempotency.js';
import type { DbClient } from '../../src/persistence/postgres/client.js';
import { vaultCredentials, vaults } from '../../src/persistence/postgres/schema.js';

// Run in a child: a duplicate writeHead on a real socket is an uncaught error,
// not merely a rejected app.inject() call. Keep that failure isolated from Vitest.
const mode = process.argv[2];
const operation = process.argv[3] ?? 'create';
let row: typeof vaults.$inferInsert = {
  id: 'vlt_test',
  workspaceId: 'ws_test',
  displayName: 'test vault',
  createdAt: new Date(),
  updatedAt: new Date(),
};
let inserts = 0;
let transactions = 0;
let updates = 0;
const selectVaultRows = async () => (row.deletedAt ? [] : [row]);
const db = {
  insert: () => ({
    values: async (value: typeof vaults.$inferInsert) => {
      row = value;
      inserts++;
    },
  }),
  select: () => ({
    from: (table: unknown) => ({
      where: () => {
        if (table === vaultCredentials) return Promise.resolve([]);
        assert.equal(table, vaults);
        return { limit: selectVaultRows, for: () => ({ limit: selectVaultRows }) };
      },
    }),
  }),
  update: (table: unknown) => {
    assert.equal(table, vaults);
    return {
      set: (value: Partial<typeof row>) => ({
        where: async () => {
          Object.assign(row, value);
          updates++;
        },
      }),
    };
  },
  transaction: async (fn: (tx: DbClient) => Promise<unknown>, options: unknown) => {
    assert.deepEqual(options, { isolationLevel: 'serializable' });
    transactions++;
    return fn(db);
  },
} as unknown as DbClient;
let cached: Awaited<ReturnType<IdempotencyStore['get']>> = null;
let cacheWrites = 0;
const store: IdempotencyStore = {
  get: async () => cached,
  put: async (_workspace, _scope, _key, value) => {
    cacheWrites++;
    if (mode !== 'immediate') await setImmediate();
    if (mode === 'failure') throw new Error('cache unavailable');
    cached = value;
  },
};
const app = Fastify();
registerClaudePublicEdge(app);
app.addHook('preHandler', async (req) => {
  req.auth = { workspaceId: 'ws_test', principal: 'test', scopes: [], authMethod: 'api-key' };
});
app.addHook('preHandler', buildIdempotencyPreHandler(store));
app.addHook('onSend', buildIdempotencyResponseHook(store));
registerVaultsRoutes(app, db);
app.get('/healthz', async () => ({ status: 'ok' }));
const address = await app.listen({ host: '127.0.0.1', port: 0 });
try {
  const bodies: string[] = [];
  for (let i = 0; i < 2; i++) {
    const route = operation === 'delete' ? '/v1/vaults/vlt_test' : '/v1/vaults';
    const response = await fetch(`${address}${route}`, {
      method: operation === 'delete' ? 'DELETE' : 'POST',
      headers: {
        ...(operation === 'create' ? { 'content-type': 'application/json' } : {}),
        ...(mode === 'no-key' ? {} : { 'idempotency-key': 'same' }),
      },
      ...(operation === 'create' ? { body: JSON.stringify({ display_name: 'test vault' }) } : {}),
    });
    const body = await response.text();
    assert.equal(response.status, 200);
    if (operation === 'delete') {
      assert.deepEqual(JSON.parse(body), { id: 'vlt_test', type: 'vault_deleted' });
    } else {
      assert.equal(JSON.parse(body).type, 'vault');
      assert.equal(JSON.parse(body).display_name, 'test vault');
    }
    bodies.push(body);
  }
  if (operation === 'delete') {
    assert.equal(inserts, 0);
    assert.equal(transactions, 1, 'replay must not rerun the deletion transaction');
    assert.equal(updates, 1);
    assert.ok(row.deletedAt instanceof Date);
    assert.equal(bodies[0], bodies[1], 'replay preserves the deletion response after soft-delete');
  } else if (mode === 'failure' || mode === 'no-key') {
    assert.equal(inserts, 2);
  } else {
    assert.equal(inserts, 1, 'replay must not execute the handler');
    assert.equal(bodies[0], bodies[1], 'replay preserves response bytes');
  }
  assert.equal(cacheWrites, mode === 'no-key' ? 0 : 2);
  // Let late hook completions run before confirming the process is still serving.
  await setImmediate();
  const health = await fetch(`${address}/healthz`);
  assert.equal(health.status, 200);
  assert.deepEqual(await health.json(), { status: 'ok' });
  console.log('response lifecycle ok');
} finally {
  await app.close();
}

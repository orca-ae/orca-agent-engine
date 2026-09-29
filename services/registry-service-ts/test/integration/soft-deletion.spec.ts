// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { afterAll, beforeAll, expect, it } from 'vitest';
import type { FastifyInstance } from 'fastify';
import type { Pool } from 'pg';
import { buildCombinedTestApp } from '../../src/server.js';
import {
  buildStubFileStore,
  buildStubStore,
  buildTestJwtMinter,
  closeTestDb,
  getTestDb,
  STUB_SSE_CONFIG,
} from './setup.js';
import { createTestApiKey, uniqueWorkspace } from './fixtures.js';

let app: FastifyInstance;
let pool: Pool;
let apiKey: string;
let foreignKey: string;
beforeAll(async () => {
  const fixture = await getTestDb();
  pool = fixture.pool;
  apiKey = await createTestApiKey(fixture.db, uniqueWorkspace('soft_delete'));
  foreignKey = await createTestApiKey(fixture.db, uniqueWorkspace('soft_delete_foreign'));
  app = buildCombinedTestApp({
    db: fixture.db,
    oidc: { allowedIssuers: [], audience: 'orca-managed-agents' },
    store: buildStubStore(),
    sse: STUB_SSE_CONFIG,
    jwtMinter: buildTestJwtMinter(),
    fileStore: buildStubFileStore(),
  });
  await app.ready();
});
afterAll(async () => {
  await app?.close();
  await closeTestDb();
});

it.each([
  {
    table: 'agents',
    payload: { name: 'retained-agent', model: { provider: 'anthropic', id: 'claude-sonnet-4-5' } },
  },
  { table: 'environments', payload: { name: 'retained-environment' } },
  { table: 'vaults', payload: { display_name: 'retained-vault' } },
])('retains $table without exposing or reviving deleted objects', async ({ table, payload }) => {
  const headers = { 'x-api-key': apiKey };
  const created = await app.inject({ method: 'POST', url: `/v1/${table}`, headers, payload });
  expect(created.statusCode, created.body).toBe(200);
  const id = created.json().id;
  const url = `/v1/${table}/${id}`;
  expect(
    (await app.inject({ method: 'DELETE', url, headers: { 'x-api-key': foreignKey } })).statusCode,
  ).toBe(404);
  expect((await app.inject({ method: 'GET', url, headers })).statusCode).toBe(200);
  expect((await app.inject({ method: 'DELETE', url, headers })).statusCode).toBe(200);
  const read = () => pool.query(`SELECT deleted_at FROM ${table} WHERE id = $1`, [id]);
  const { rows } = await read();
  expect(rows).toEqual([{ deleted_at: expect.any(Date) }]);
  expect((await app.inject({ method: 'GET', url, headers })).statusCode).toBe(404);
  expect((await app.inject({ method: 'DELETE', url, headers })).statusCode).toBe(404);
  expect(
    (await app.inject({ method: 'POST', url: `${url}/archive`, headers, payload: {} })).statusCode,
  ).toBe(404);
  const listed = await app.inject({
    method: 'GET',
    url: `/v1/${table}?include_archived=true`,
    headers,
  });
  expect(listed.statusCode, listed.body).toBe(200);
  expect(listed.json().data.some((row: { id: string }) => row.id === id)).toBe(false);
  expect((await read()).rows).toEqual(rows);
  const replacement = await app.inject({ method: 'POST', url: `/v1/${table}`, headers, payload });
  expect(replacement.statusCode, replacement.body).toBe(200);
  expect(replacement.json().id).not.toBe(id);
});

it('revokes a Session observability pin exactly once for direct deleted_at writes', async () => {
  const headers = { 'x-api-key': apiKey };
  const agent = await app.inject({
    method: 'POST',
    url: '/v1/agents',
    headers,
    payload: { name: 'pin-agent', model: { provider: 'anthropic', id: 'claude-sonnet-4-5' } },
  });
  const environment = await app.inject({
    method: 'POST',
    url: '/v1/environments',
    headers,
    payload: { name: 'pin-environment' },
  });
  const session = await app.inject({
    method: 'POST',
    url: '/v1/sessions',
    headers,
    payload: { agent_id: agent.json().id, environment_id: environment.json().id },
  });
  expect(session.statusCode, session.body).toBe(200);
  const id = session.json().id;
  const readPin = () =>
    pool.query(
      'SELECT status, deleted_at, session_revocation_epoch FROM session_observability_bindings WHERE session_id = $1',
      [id],
    );
  const before = (await readPin()).rows[0];
  const fence = new Date('2035-01-01T00:00:00.000Z');
  await pool.query('UPDATE sessions SET deleted_at = $1 WHERE id = $2', [fence, id]);
  const after = (await readPin()).rows[0];
  expect(after).toMatchObject({ status: 'deleted', deleted_at: fence });
  expect(BigInt(after.session_revocation_epoch)).toBe(BigInt(before.session_revocation_epoch) + 1n);
  await pool.query('UPDATE sessions SET deleted_at = $1 WHERE id = $2', [fence, id]);
  expect((await readPin()).rows[0]).toEqual(after);
  expect((await app.inject({ method: 'GET', url: `/v1/sessions/${id}`, headers })).statusCode).toBe(
    404,
  );
});

it('serializes Agent deletion with concurrent version creation and retains every version', async () => {
  const headers = { 'x-api-key': apiKey };
  const created = await app.inject({
    method: 'POST',
    url: '/v1/agents',
    headers,
    payload: {
      name: 'concurrent-agent',
      model: { provider: 'anthropic', id: 'claude-sonnet-4-5' },
    },
  });
  expect(created.statusCode).toBe(200);
  const id = created.json().id;
  const [update, deleted] = await Promise.all([
    app.inject({
      method: 'POST',
      url: `/v1/agents/${id}`,
      headers,
      payload: { system: 'Updated concurrently' },
    }),
    app.inject({ method: 'DELETE', url: `/v1/agents/${id}`, headers }),
  ]);
  expect([200, 404, 409]).toContain(update.statusCode);
  expect(deleted.statusCode, deleted.body).toBe(200);
  const { rows } = await pool.query('SELECT deleted_at FROM agent_versions WHERE agent_id = $1', [
    id,
  ]);
  expect(rows.length).toBeGreaterThan(0);
  expect(rows.every((row) => row.deleted_at !== null)).toBe(true);
});

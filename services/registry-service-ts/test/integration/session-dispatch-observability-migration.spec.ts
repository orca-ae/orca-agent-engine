// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { randomBytes } from 'node:crypto';
import { Client, Pool, type QueryResult } from 'pg';
import { describe, expect, it } from 'vitest';
import { runMigrations } from '../../src/migrate.js';

const REGISTRY_DATABASE_URL =
  process.env['DATABASE_URL'] ?? 'postgres://orca:orca@localhost:5432/registry';
const ADMIN_DATABASE_URL = (() => {
  const url = new URL(REGISTRY_DATABASE_URL);
  url.pathname = '/postgres';
  return url.toString();
})();
const INDEX_NAME = 'session_events_index_unprocessed_client_user_idx';

describe('session dispatch observability migration', () => {
  it('builds the index after the journal checkpoint and heals interrupted runs', async () => {
    const databaseName = `registry_dispatch_index_${process.pid}_${randomBytes(4).toString('hex')}`;
    const adminPool = new Pool({ connectionString: ADMIN_DATABASE_URL, max: 1 });
    const databaseUrl = new URL(REGISTRY_DATABASE_URL);
    databaseUrl.pathname = `/${databaseName}`;
    let client: Client | undefined;

    try {
      await adminPool.query(`CREATE DATABASE "${databaseName}"`);
      await runMigrations(databaseUrl.toString());

      client = new Client({ connectionString: databaseUrl.toString() });
      await client.connect();
      await expectUnprocessedClientUserIndex(client);

      // This recreates the checkpointed state after a process dies before its
      // concurrent index command starts.
      await client.query(`DROP INDEX CONCURRENTLY "${INDEX_NAME}"`);
      await expect(indexDefinition(client)).resolves.toBeUndefined();

      await runMigrations(databaseUrl.toString());
      await expectUnprocessedClientUserIndex(client);

      await client.query(`DROP INDEX CONCURRENTLY "${INDEX_NAME}"`);
      await createInvalidIndex(client);
      expect((await indexDefinition(client))?.isValid).toBe(false);

      await runMigrations(databaseUrl.toString());
      await expectUnprocessedClientUserIndex(client);
    } finally {
      await client?.end().catch(() => {});
      try {
        await adminPool.query(`DROP DATABASE IF EXISTS "${databaseName}"`);
      } finally {
        await adminPool.end().catch(() => {});
      }
    }
  }, 60_000);
});

interface QueryClient {
  query<T extends Record<string, unknown> = Record<string, unknown>>(
    text: string,
    values?: unknown[],
  ): Promise<QueryResult<T>>;
}

async function expectUnprocessedClientUserIndex(client: QueryClient): Promise<void> {
  const definition = await indexDefinition(client);

  expect(definition).toBeDefined();
  expect(definition?.isValid).toBe(true);
  expect(definition?.sql).toContain('(workspace_id, produced_at)');
  expect(definition?.sql).toContain("visibility = 'public'::text");
  expect(definition?.sql).toContain("produced_by = 'client'::text");
  expect(definition?.sql).toContain("kind ~~ 'user.%'::text");
  expect(definition?.sql).toContain('processed_at IS NULL');
}

async function indexDefinition(
  client: QueryClient,
): Promise<{ isValid: boolean; sql: string } | undefined> {
  const result = await client.query<{ is_valid: boolean; sql: string }>(
    `
      SELECT i.indisvalid AS is_valid, pg_get_indexdef(i.indexrelid) AS sql
      FROM pg_index AS i
      JOIN pg_class AS index_relation ON index_relation.oid = i.indexrelid
      WHERE index_relation.relname = $1
    `,
    [INDEX_NAME],
  );
  const row = result.rows[0];
  return row === undefined ? undefined : { isValid: row.is_valid, sql: row.sql };
}

async function createInvalidIndex(client: QueryClient): Promise<void> {
  await client.query(`
    INSERT INTO organizations (id, name) VALUES ('org_dispatch_index', 'Dispatch index');
    INSERT INTO workspaces (id, organization_id, name, created_by)
    VALUES ('ws_dispatch_index', 'org_dispatch_index', 'Dispatch index', 'migration-test');
    INSERT INTO session_events_index (
      workspace_id, session_id, seq, event_id, produced_at, produced_by, kind, visibility
    ) VALUES
      (
        'ws_dispatch_index', 'ses_dispatch_index_a', 1, 'evt_dispatch_index_a',
        '2026-08-27T00:00:00.000Z', 'client', 'user.message', 'public'
      ),
      (
        'ws_dispatch_index', 'ses_dispatch_index_b', 1, 'evt_dispatch_index_b',
        '2026-08-27T00:00:01.000Z', 'client', 'user.message', 'public'
      );
  `);
  await expect(
    client.query(
      `CREATE UNIQUE INDEX CONCURRENTLY "${INDEX_NAME}" ON "session_events_index" ("workspace_id")`,
    ),
  ).rejects.toMatchObject({ code: '23505' });
}

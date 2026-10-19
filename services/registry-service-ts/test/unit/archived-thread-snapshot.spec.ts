// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { expect, it, vi } from 'vitest';
import { PgDialect } from 'drizzle-orm/pg-core';
import type { SQL } from 'drizzle-orm';
import type { DbClient } from '../../src/persistence/postgres/client.js';
import { sessionThreads } from '../../src/persistence/postgres/schema.js';
import { loadArchivedThreadSubpaths } from '../../src/domain/prepare-execution.js';

it('loads archive fences only from durable workspace/session-scoped rows and propagates query failure', async () => {
  const subpath = 'subagents/11111111-1111-4111-8111-111111111111';
  const where = vi.fn(async (_condition: SQL) => [{ subpath }]);
  const from = vi.fn(() => ({ where }));
  const db = { select: () => ({ from }) } as unknown as DbClient;
  expect(await loadArchivedThreadSubpaths(db, 'ws_test', 'ses_test')).toEqual([subpath]);
  expect(from).toHaveBeenCalledWith(sessionThreads);
  const query = new PgDialect().sqlToQuery(where.mock.calls[0]![0]);
  expect(query.params).toEqual(['ws_test', 'ses_test']);
  expect(query.sql).toContain('archived_at');
  expect(query.sql).toContain('is not null');
  where.mockRejectedValueOnce(new Error('database unavailable'));
  await expect(loadArchivedThreadSubpaths(db, 'ws_test', 'ses_test')).rejects.toThrow(
    'database unavailable',
  );
});

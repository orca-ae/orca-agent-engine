// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { afterAll, beforeAll, describe } from 'vitest';
import { Pool } from 'pg';
import { applyMigrations, PostgresMemoryMetadataStore } from '../../src/metadata/postgres.js';
import { deletionContract } from '../support/deletion-contract.js';

describe('Postgres deletion checkpoint contract', () => {
  const pool = new Pool({
    connectionString:
      process.env['MEMORYSTORE_DATABASE_URL'] ?? 'postgres://orca:orca@localhost:5432/memorystore',
  });
  let metadata: PostgresMemoryMetadataStore;
  beforeAll(async () => {
    await applyMigrations(pool);
    metadata = new PostgresMemoryMetadataStore(pool);
  });
  afterAll(async () => {
    await pool.end();
  });
  deletionContract(() => metadata, () => new PostgresMemoryMetadataStore(pool));
});

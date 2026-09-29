// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { readFile } from 'node:fs/promises';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { sql } from 'drizzle-orm';
import type { DbClient } from '../../src/persistence/postgres/client.js';
import { closeTestDb, getTestDb } from './setup.js';

const MIGRATION_URL = new URL(
  '../../src/persistence/postgres/migrations/0036_adorable_dreaming_celestial.sql',
  import.meta.url,
);

describe('Skill migration reset (integration)', () => {
  let db: DbClient;

  beforeAll(async () => {
    ({ db } = await getTestDb());
  });

  afterAll(async () => {
    await closeTestDb();
  });

  it('removes every legacy Skill reference before deleting legacy rows', async () => {
    const migration = await readFile(MIGRATION_URL, 'utf8');
    const resetStatements = migration
      .split('CREATE TABLE "session_skill_bindings"')[0]!
      .split('--> statement-breakpoint')
      .map((statement) => statement.trim())
      .filter(Boolean);

    await db.transaction(async (tx) => {
      await tx.execute(
        sql.raw('CREATE TEMP TABLE "agents" ("skills" jsonb NOT NULL) ON COMMIT DROP'),
      );
      await tx.execute(
        sql.raw('CREATE TEMP TABLE "agent_versions" ("snapshot" jsonb NOT NULL) ON COMMIT DROP'),
      );
      await tx.execute(
        sql.raw('CREATE TEMP TABLE "sessions" ("agent_overrides" jsonb NULL) ON COMMIT DROP'),
      );
      await tx.execute(
        sql.raw('CREATE TEMP TABLE "skills" ("latest_version_id" text NULL) ON COMMIT DROP'),
      );
      await tx.execute(
        sql.raw('CREATE TEMP TABLE "skill_versions" ("id" text NOT NULL) ON COMMIT DROP'),
      );

      await tx.execute(sql.raw(`INSERT INTO "agents" VALUES ('[{"skill_id":"skill_old"}]')`));
      await tx.execute(
        sql.raw(
          `INSERT INTO "agent_versions" VALUES ('{"name":"legacy","skills":[{"skill_id":"skill_old"}]}')`,
        ),
      );
      await tx.execute(
        sql.raw(
          `INSERT INTO "sessions" VALUES ('{"system":"override","skills":[{"skill_id":"skill_old"}]}'), (NULL)`,
        ),
      );
      await tx.execute(sql.raw(`INSERT INTO "skills" VALUES ('sklv_old')`));
      await tx.execute(sql.raw(`INSERT INTO "skill_versions" VALUES ('sklv_old')`));

      for (const statement of resetStatements) {
        await tx.execute(sql.raw(statement));
      }

      const agentRows = await tx.execute(sql.raw('SELECT "skills" FROM "agents"'));
      const versionRows = await tx.execute(sql.raw('SELECT "snapshot" FROM "agent_versions"'));
      const sessionRows = await tx.execute(
        sql.raw('SELECT "agent_overrides" FROM "sessions" ORDER BY "agent_overrides" NULLS LAST'),
      );
      const skillRows = await tx.execute(sql.raw('SELECT count(*)::int AS count FROM "skills"'));
      const skillVersionRows = await tx.execute(
        sql.raw('SELECT count(*)::int AS count FROM "skill_versions"'),
      );

      expect(agentRows.rows).toEqual([{ skills: [] }]);
      expect(versionRows.rows).toEqual([{ snapshot: { name: 'legacy', skills: [] } }]);
      expect(sessionRows.rows).toEqual([
        { agent_overrides: { system: 'override' } },
        { agent_overrides: null },
      ]);
      expect(skillRows.rows[0]?.['count']).toBe(0);
      expect(skillVersionRows.rows[0]?.['count']).toBe(0);
    });
  });
});

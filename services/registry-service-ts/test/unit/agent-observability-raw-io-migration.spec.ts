// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { readFileSync } from 'node:fs';
import { describe, expect, it } from 'vitest';
import { agentObservabilityPlatformPolicy } from '../../src/persistence/postgres/schema.js';

const migrationRoot = new URL('../../src/persistence/postgres/migrations/', import.meta.url);
const read = (path: string) => readFileSync(new URL(path, migrationRoot), 'utf8');

describe('raw IO additive migration', () => {
  it('preserves the default platform maximum and all persisted values', () => {
    expect(agentObservabilityPlatformPolicy.maxCaptureMode.default).toBe('metadata_only');
    const migration = read('0060_agent_observability_raw_io.sql');
    expect(migration.match(/ADD CONSTRAINT/g)).toHaveLength(5);
    expect(migration.match(/DROP CONSTRAINT/g)).toHaveLength(5);
    expect(migration.split('CREATE OR REPLACE FUNCTION')[0]).not.toMatch(
      /\b(UPDATE|INSERT|DELETE)\b/,
    );
  });

  it('changes only five capture CHECKs in the generated snapshot', () => {
    const previous = JSON.parse(read('meta/0059_snapshot.json'));
    const current = JSON.parse(read('meta/0060_snapshot.json'));
    expect(current.prevId).toBe(previous.id);
    const expected = JSON.parse(
      JSON.stringify(previous).replaceAll(
        "('metadata_only', 'redacted_io')",
        "('metadata_only', 'redacted_io', 'raw_io')",
      ),
    );
    expect(current).toEqual({ ...expected, id: current.id, prevId: previous.id });
    const journal = JSON.parse(read('meta/_journal.json'));
    // Later additive migrations must not invalidate the raw-I/O history check.
    expect(journal.entries.find((entry: { idx: number }) => entry.idx === 60)).toMatchObject({
      idx: 60,
      tag: '0060_agent_observability_raw_io',
      breakpoints: true,
    });
  });

  it('updates the archive guard without altering revocation/CAS behavior', () => {
    const extract = (sql: string) =>
      sql.slice(sql.indexOf('CREATE OR REPLACE FUNCTION')).split('$$;')[0] + '$$;';
    const previous = extract(read('0056_workspace_archive_observability_revocation.sql'));
    const current = extract(read('0060_agent_observability_raw_io.sql'));
    expect(current).toBe(
      previous.replace(
        "      AND setting_capture_ceiling IS DISTINCT FROM 'redacted_io'",
        "      AND setting_capture_ceiling IS DISTINCT FROM 'redacted_io'\n      AND setting_capture_ceiling IS DISTINCT FROM 'raw_io'",
      ),
    );
  });

  it('does not scan Session pins while the migration holds its exclusive lock', () => {
    const migration = read('0060_agent_observability_raw_io.sql');
    const sessionCheck = migration.match(
      /ALTER TABLE "session_observability_bindings" ADD CONSTRAINT [^;]+;/,
    )?.[0];
    expect(sessionCheck).toMatch(/CHECK .* NOT VALID;/);
    expect(migration).not.toContain('VALIDATE CONSTRAINT');
  });
});

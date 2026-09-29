// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { validateSkillRefs } from '../../src/api/agents.routes.js';
import type { DbClient } from '../../src/persistence/postgres/client.js';

interface FakeVersionRow {
  id: string;
  skillId?: string;
  latestVersionId?: string | null;
  versionIdentifier?: string;
  name: string;
  packageSha256: string;
}

// Minimal fluent stub mirroring the single batched
// db.select(...).from(...).innerJoin(...).where(...) query used to resolve custom refs.
function fakeDb(found: boolean | FakeVersionRow[], queryCounter?: { selects: number }): DbClient {
  const rows = (
    typeof found === 'boolean'
      ? found
        ? [{ id: 'skillver_abc', name: 'test-skill', packageSha256: 'a'.repeat(64) }]
        : []
      : found
  ).map((row) => ({
    skillId: 'skill_abc',
    latestVersionId: row.id,
    versionIdentifier: '1759178010641129',
    ...row,
  }));
  const chain = {
    select: () => {
      if (queryCounter) queryCounter.selects += 1;
      return chain;
    },
    from: () => chain,
    innerJoin: () => chain,
    where: async () => rows,
  };
  return chain as unknown as DbClient;
}

describe('validateSkillRefs (typed agent skill references)', () => {
  it('accepts an anthropic ref without touching the database', async () => {
    const out = await validateSkillRefs(fakeDb(false), 'ws_1', [
      { type: 'anthropic', skill_id: 'pdf' },
    ]);
    expect(out).toEqual([{ type: 'anthropic', skill_id: 'pdf', version: 'latest' }]);
  });

  it('preserves an explicit anthropic version without requiring a local catalog', async () => {
    const out = await validateSkillRefs(fakeDb(false), 'ws_1', [
      { type: 'anthropic', skill_id: 'pdf', version: '2026-04-01' },
    ]);
    expect(out).toEqual([{ type: 'anthropic', skill_id: 'pdf', version: '2026-04-01' }]);
  });

  it('rejects a numeric custom version ordinal', async () => {
    const out = await validateSkillRefs(fakeDb(true), 'ws_1', [
      { type: 'custom', skill_id: 'skill_abc', version: 2 },
    ]);
    expect(out).toEqual({
      error:
        'custom skill reference version must be "latest" or a decimal timestamp version identifier',
    });
  });

  it('rejects a custom ref whose public version identifier is missing', async () => {
    const out = await validateSkillRefs(fakeDb(false), 'ws_1', [
      { type: 'custom', skill_id: 'skill_abc', version: '9999999999999999' },
    ]);
    expect('error' in out).toBe(true);
  });

  it('preserves a retained custom ref that no longer resolves when explicitly allowed', async () => {
    const out = await validateSkillRefs(
      fakeDb(false),
      'ws_1',
      [{ type: 'custom', skill_id: 'skill_abc', version: 'latest' }],
      { allowUnresolvedCustomRefs: true },
    );
    expect(out).toEqual([{ type: 'custom', skill_id: 'skill_abc', version: 'latest' }]);
  });

  it('rejects raw skillver_ string entries', async () => {
    const out = await validateSkillRefs(fakeDb(true), 'ws_1', ['skillver_legacy']);
    expect('error' in out).toBe(true);
  });

  it('rejects a non-array input', async () => {
    const out = await validateSkillRefs(fakeDb(true), 'ws_1', { bad: true });
    expect('error' in out).toBe(true);
  });

  it('rejects an unknown ref type', async () => {
    const out = await validateSkillRefs(fakeDb(true), 'ws_1', [
      { type: 'mystery', skill_id: 'skill_abc' },
    ]);
    expect('error' in out).toBe(true);
  });

  it('validates the latest custom version when version is omitted', async () => {
    const out = await validateSkillRefs(fakeDb(true), 'ws_1', [
      { type: 'custom', skill_id: 'skill_abc' },
    ]);
    expect(out).toEqual([{ type: 'custom', skill_id: 'skill_abc', version: 'latest' }]);
  });

  it('validates and preserves an explicit latest custom version', async () => {
    const out = await validateSkillRefs(fakeDb(true), 'ws_1', [
      { type: 'custom', skill_id: 'skill_abc', version: 'latest' },
    ]);
    expect(out).toEqual([{ type: 'custom', skill_id: 'skill_abc', version: 'latest' }]);
  });

  it('validates and preserves a version identifier', async () => {
    const out = await validateSkillRefs(fakeDb(true), 'ws_1', [
      { type: 'custom', skill_id: 'skill_abc', version: '1759178010641129' },
    ]);
    expect(out).toEqual([{ type: 'custom', skill_id: 'skill_abc', version: '1759178010641129' }]);
  });

  it('rejects refs that resolve to the same custom SkillVersion', async () => {
    const row = {
      id: 'skillver_duplicate',
      name: 'duplicate-skill',
      packageSha256: 'a'.repeat(64),
    };
    const out = await validateSkillRefs(fakeDb([row, row]), 'ws_1', [
      { type: 'custom', skill_id: 'skill_abc', version: 'latest' },
      { type: 'custom', skill_id: 'skill_abc', version: '1759178010641129' },
    ]);
    expect(out).toEqual({
      error: 'custom skill skill_abc resolves to the same version more than once',
    });
  });

  it('rejects custom refs with the same effective name and different package digests', async () => {
    const out = await validateSkillRefs(
      fakeDb([
        {
          id: 'skillver_one',
          skillId: 'skill_one',
          name: 'shared-name',
          packageSha256: 'a'.repeat(64),
        },
        {
          id: 'skillver_two',
          skillId: 'skill_two',
          name: 'shared-name',
          packageSha256: 'b'.repeat(64),
        },
      ]),
      'ws_1',
      [
        { type: 'custom', skill_id: 'skill_one', version: 'latest' },
        { type: 'custom', skill_id: 'skill_two', version: 'latest' },
      ],
    );
    expect(out).toEqual({
      error:
        'skill name shared-name resolves to different packages for skill IDs skill_one, skill_two',
    });
  });

  it('allows custom refs with the same effective name and package digest', async () => {
    const packageSha256 = 'a'.repeat(64);
    const queryCounter = { selects: 0 };
    const out = await validateSkillRefs(
      fakeDb(
        [
          { id: 'skillver_one', skillId: 'skill_one', name: 'shared-name', packageSha256 },
          { id: 'skillver_two', skillId: 'skill_two', name: 'shared-name', packageSha256 },
        ],
        queryCounter,
      ),
      'ws_1',
      [
        { type: 'custom', skill_id: 'skill_one', version: 'latest' },
        { type: 'custom', skill_id: 'skill_two', version: 'latest' },
      ],
    );
    expect(out).toEqual([
      { type: 'custom', skill_id: 'skill_one', version: 'latest' },
      { type: 'custom', skill_id: 'skill_two', version: 'latest' },
    ]);
    expect(queryCounter.selects).toBe(1);
  });

  it('rejects an identical anthropic ref without requiring a local catalog', async () => {
    const out = await validateSkillRefs(fakeDb(false), 'ws_1', [
      { type: 'anthropic', skill_id: 'pdf' },
      { type: 'anthropic', skill_id: 'pdf', version: 'latest' },
    ]);
    expect(out).toEqual({
      error: 'agent skills repeat anthropic skill pdf version latest',
    });
  });

  it('rejects more than 500 refs before querying the database', async () => {
    const queryCounter = { selects: 0 };
    const out = await validateSkillRefs(
      fakeDb(false, queryCounter),
      'ws_1',
      Array.from({ length: 501 }, (_, index) => ({
        type: 'anthropic',
        skill_id: `catalog-${index}`,
      })),
    );
    expect(out).toEqual({ error: 'skills must contain at most 500 entries' });
    expect(queryCounter.selects).toBe(0);
  });

  it('rejects an internal skill-version row id as a public version selector', async () => {
    const out = await validateSkillRefs(fakeDb(true), 'ws_1', [
      { type: 'custom', skill_id: 'skill_abc', version: 'skillver_abc' },
    ]);
    expect(out).toEqual({
      error:
        'custom skill reference version must be "latest" or a decimal timestamp version identifier',
    });
  });

  it('normalizes a null custom version selector to latest', async () => {
    const out = await validateSkillRefs(fakeDb(true), 'ws_1', [
      { type: 'custom', skill_id: 'skill_abc', version: null },
    ]);
    expect(out).toEqual([{ type: 'custom', skill_id: 'skill_abc', version: 'latest' }]);
  });

  it('rejects an anthropic ref missing skill_id', async () => {
    const out = await validateSkillRefs(fakeDb(true), 'ws_1', [{ type: 'anthropic' }]);
    expect('error' in out).toBe(true);
  });
});

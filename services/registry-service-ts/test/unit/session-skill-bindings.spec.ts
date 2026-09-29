// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import type { DbClient } from '../../src/persistence/postgres/client.js';
import {
  resolveSessionSkillBindings,
  SessionSkillBindingError,
} from '../../src/domain/session-skill-bindings.js';

interface VersionRow {
  id: string;
  skillId: string;
  source: 'anthropic' | 'custom';
  latestVersionId: string | null;
  version: number;
  versionIdentifier: string;
  name: string;
  description: string;
  entrypoint: string;
  packageSha256: string;
  packageSizeBytes: number;
}

function snapshot(input: {
  id: string;
  version?: number;
  skills?: unknown[];
  subagents?: Array<{ id: string; version: number }>;
}) {
  return {
    id: input.id,
    name: input.id,
    version: input.version ?? 1,
    model: { provider: 'anthropic', id: 'claude' },
    system: null,
    tools: [],
    mcp_servers: [],
    skills: input.skills ?? [],
    metadata: {},
    multiagent:
      input.subagents === undefined
        ? null
        : {
            type: 'coordinator',
            agents: input.subagents.map((ref) => ({ type: 'agent', ...ref })),
          },
  };
}

function fakeDb(agentSnapshots: unknown[], versionRows: VersionRow[]): DbClient {
  const pendingSnapshots = [...agentSnapshots];
  return {
    select(selection: Record<string, unknown>) {
      if ('snapshot' in selection) {
        const builder = {
          from: () => builder,
          innerJoin: () => builder,
          where: () => builder,
          for: () => builder,
          limit: async () => {
            const next = pendingSnapshots.shift();
            return next === undefined ? [] : [{ snapshot: next }];
          },
        };
        return builder;
      }
      const builder = {
        from: () => builder,
        innerJoin: () => builder,
        where: () => builder,
        for: async () => versionRows,
      };
      return builder;
    },
  } as unknown as DbClient;
}

function versionRow(input: Partial<VersionRow> & Pick<VersionRow, 'id' | 'skillId' | 'name'>) {
  return {
    source: 'custom' as const,
    latestVersionId: input.id,
    version: 1,
    versionIdentifier: '1759178010641129',
    description: 'Test skill',
    entrypoint: 'SKILL.md',
    packageSha256: 'a'.repeat(64),
    packageSizeBytes: 1,
    ...input,
  };
}

describe('session skill binding resolution', () => {
  it('pins the primary and direct coordinator roster in declared order', async () => {
    const db = fakeDb(
      [
        snapshot({
          id: 'agt_primary',
          skills: [{ type: 'custom', skill_id: 'skill_primary', version: 'latest' }],
          subagents: [{ id: 'agt_child', version: 1 }],
        }),
        snapshot({
          id: 'agt_child',
          skills: [{ type: 'custom', skill_id: 'skill_child', version: '1759178010641130' }],
        }),
      ],
      [
        versionRow({
          id: 'skillver_primary',
          skillId: 'skill_primary',
          name: 'primary-skill',
        }),
        versionRow({
          id: 'skillver_child',
          skillId: 'skill_child',
          name: 'child-skill',
          latestVersionId: 'skillver_newer',
          version: 2,
          versionIdentifier: '1759178010641130',
          packageSha256: 'b'.repeat(64),
        }),
      ],
    );

    await expect(
      resolveSessionSkillBindings(db, {
        workspaceId: 'ws_test',
        sessionId: 'ses_test',
        primaryAgentId: 'agt_primary',
        primaryAgentVersion: 1,
      }),
    ).resolves.toEqual([
      expect.objectContaining({
        agentId: 'agt_primary',
        ordinal: 0,
        skillVersionId: 'skillver_primary',
      }),
      expect.objectContaining({
        agentId: 'agt_child',
        ordinal: 0,
        skillVersionId: 'skillver_child',
      }),
    ]);
  });

  it('rejects a nested coordinator snapshot that the Harness cannot execute', async () => {
    const db = fakeDb(
      [
        snapshot({
          id: 'agt_primary',
          subagents: [{ id: 'agt_child', version: 1 }],
        }),
        snapshot({
          id: 'agt_child',
          subagents: [{ id: 'agt_grandchild', version: 1 }],
        }),
      ],
      [],
    );

    await expect(
      resolveSessionSkillBindings(db, {
        workspaceId: 'ws_test',
        sessionId: 'ses_test',
        primaryAgentId: 'agt_primary',
        primaryAgentVersion: 1,
      }),
    ).rejects.toThrow(/nested coordinator/);
  });

  it('uses the primary session skill override, including an empty replacement', async () => {
    const db = fakeDb(
      [
        snapshot({
          id: 'agt_primary',
          skills: [{ type: 'custom', skill_id: 'skill_original', version: 'latest' }],
        }),
      ],
      [],
    );

    await expect(
      resolveSessionSkillBindings(db, {
        workspaceId: 'ws_test',
        sessionId: 'ses_test',
        primaryAgentId: 'agt_primary',
        primaryAgentVersion: 1,
        primarySkillRefsOverride: [],
      }),
    ).resolves.toEqual([]);
  });

  it('normalizes a null version in a primary session skill override to latest', async () => {
    const db = fakeDb(
      [snapshot({ id: 'agt_primary' })],
      [versionRow({ id: 'skillver_primary', skillId: 'skill_primary', name: 'primary-skill' })],
    );

    await expect(
      resolveSessionSkillBindings(db, {
        workspaceId: 'ws_test',
        sessionId: 'ses_test',
        primaryAgentId: 'agt_primary',
        primaryAgentVersion: 1,
        primarySkillRefsOverride: [{ type: 'custom', skill_id: 'skill_primary', version: null }],
      }),
    ).resolves.toEqual([
      expect.objectContaining({
        agentId: 'agt_primary',
        ordinal: 0,
        skillVersionId: 'skillver_primary',
      }),
    ]);
  });

  it('rejects a duplicate effective SkillVersion within one agent', async () => {
    const db = fakeDb(
      [
        snapshot({
          id: 'agt_primary',
          skills: [
            { type: 'custom', skill_id: 'skill_duplicate', version: 'latest' },
            { type: 'custom', skill_id: 'skill_duplicate', version: '1759178010641129' },
          ],
        }),
      ],
      [
        versionRow({
          id: 'skillver_duplicate',
          skillId: 'skill_duplicate',
          name: 'duplicate-skill',
        }),
      ],
    );

    await expect(
      resolveSessionSkillBindings(db, {
        workspaceId: 'ws_test',
        sessionId: 'ses_test',
        primaryAgentId: 'agt_primary',
        primaryAgentVersion: 1,
      }),
    ).rejects.toThrow(/more than once/);
  });

  it('rejects the same effective name with different package digests across agents', async () => {
    const db = fakeDb(
      [
        snapshot({
          id: 'agt_primary',
          skills: [{ type: 'custom', skill_id: 'skill_one', version: 'latest' }],
          subagents: [{ id: 'agt_child', version: 1 }],
        }),
        snapshot({
          id: 'agt_child',
          skills: [{ type: 'custom', skill_id: 'skill_two', version: 'latest' }],
        }),
      ],
      [
        versionRow({ id: 'skillver_one', skillId: 'skill_one', name: 'shared-name' }),
        versionRow({
          id: 'skillver_two',
          skillId: 'skill_two',
          name: 'shared-name',
          packageSha256: 'b'.repeat(64),
        }),
      ],
    );

    await expect(
      resolveSessionSkillBindings(db, {
        workspaceId: 'ws_test',
        sessionId: 'ses_test',
        primaryAgentId: 'agt_primary',
        primaryAgentVersion: 1,
      }),
    ).rejects.toThrow(/different packages/);
  });

  it('enforces the 500-binding limit across the primary and coordinator roster', async () => {
    const refs = Array.from({ length: 501 }, (_, index) => ({
      type: 'custom',
      skill_id: `skill_${index}`,
      version: 'latest',
    }));
    const db = fakeDb([snapshot({ id: 'agt_primary', skills: refs })], []);

    await expect(
      resolveSessionSkillBindings(db, {
        workspaceId: 'ws_test',
        sessionId: 'ses_test',
        primaryAgentId: 'agt_primary',
        primaryAgentVersion: 1,
      }),
    ).rejects.toBeInstanceOf(SessionSkillBindingError);
  });

  it.each([2, 'skillver_legacy'])(
    'rejects a non-public custom version selector %j in a stored snapshot',
    async (version) => {
      const db = fakeDb(
        [
          snapshot({
            id: 'agt_primary',
            skills: [{ type: 'custom', skill_id: 'skill_one', version }],
          }),
        ],
        [],
      );

      await expect(
        resolveSessionSkillBindings(db, {
          workspaceId: 'ws_test',
          sessionId: 'ses_test',
          primaryAgentId: 'agt_primary',
          primaryAgentVersion: 1,
        }),
      ).rejects.toThrow(/invalid version reference/);
    },
  );
});

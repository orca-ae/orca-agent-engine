// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { validateAgentConfiguration } from '../../src/api/agents.routes.js';
import { AgentCreate, AgentUpdate } from '../../src/contracts/agents.contract.js';
import { modelToApi, normalizeModelForStorage } from '../../src/contracts/model-wire.js';

const baseAgent = {
  name: 'Test agent',
  model: { provider: 'anthropic', id: 'claude-3-5-sonnet-20240620' },
  system: 'You are helpful.',
  mcp_servers: [],
  skills: [],
  metadata: {},
};

describe('agent contract tool configs', () => {
  it('accepts Anthropic-compatible name and description lengths', () => {
    expect(
      AgentCreate.safeParse({
        ...baseAgent,
        name: 'n'.repeat(256),
        description: 'd'.repeat(2048),
      }).success,
    ).toBe(true);
    expect(AgentCreate.safeParse({ ...baseAgent, name: 'n'.repeat(257) }).success).toBe(false);
    expect(AgentCreate.safeParse({ ...baseAgent, description: 'd'.repeat(2049) }).success).toBe(
      false,
    );
  });

  it('accepts Claude nullable create/update fields and an update without a CAS version', () => {
    expect(
      AgentCreate.safeParse({
        name: 'Nullable agent',
        model: { id: 'claude-sonnet-4-6', effort: 'high' },
        system: null,
      }).success,
    ).toBe(true);
    expect(
      AgentUpdate.safeParse({
        description: null,
        mcp_servers: null,
        metadata: { remove_me: null },
        skills: null,
        system: null,
        tools: null,
      }).success,
    ).toBe(true);
  });

  it('accepts only public string selectors for custom Skill versions', () => {
    for (const version of [undefined, null, 'latest', '1759178010641129']) {
      expect(
        AgentCreate.safeParse({
          ...baseAgent,
          skills: [{ type: 'custom', skill_id: 'skill_test', version }],
        }).success,
      ).toBe(true);
    }

    for (const version of [1, '0', '00', 'skillver_internal', 'not-a-version']) {
      expect(
        AgentCreate.safeParse({
          ...baseAgent,
          skills: [{ type: 'custom', skill_id: 'skill_test', version }],
        }).success,
      ).toBe(false);
    }
  });

  it('caps Agent create and update skill refs at 500 entries', () => {
    const skills = Array.from({ length: 501 }, (_, index) => ({
      type: 'anthropic',
      skill_id: `catalog-${index}`,
    }));

    expect(AgentCreate.safeParse({ ...baseAgent, skills: skills.slice(0, 500) }).success).toBe(
      true,
    );
    expect(AgentCreate.safeParse({ ...baseAgent, skills }).success).toBe(false);
    expect(AgentUpdate.safeParse({ skills }).success).toBe(false);
  });

  it('normalizes and serializes model effort without dropping it', () => {
    const normalized = normalizeModelForStorage({
      id: 'claude-opus-5',
      speed: 'fast',
      effort: { type: 'xhigh' },
    });
    expect(normalized).toEqual({
      provider: 'anthropic',
      id: 'claude-opus-5',
      speed: 'fast',
      effort: 'xhigh',
    });
    if ('error' in normalized) return;
    expect(modelToApi(normalized, false)).toEqual({
      id: 'claude-opus-5',
      speed: 'fast',
      effort: { type: 'xhigh' },
    });
  });

  it('validates record-form tool permission policies while preserving opaque config fields', () => {
    const parsed = AgentCreate.safeParse({
      ...baseAgent,
      tools: [
        {
          type: 'mcp_toolset',
          mcp_server_name: 'github',
          configs: {
            create_issue: {
              enabled: true,
              permission_policy: { type: 'always_ask' },
              max_uses: 3,
            },
          },
        },
      ],
    });

    expect(parsed.success).toBe(true);
    if (!parsed.success) return;
    expect(parsed.data.tools[0]).toMatchObject({
      configs: {
        create_issue: {
          enabled: true,
          permission_policy: { type: 'always_ask' },
          max_uses: 3,
        },
      },
    });
  });

  it.each(['always_deny', 'invalid_policy'])(
    'rejects the unsupported permission policy %s',
    (permissionPolicy) => {
      const parsed = AgentCreate.safeParse({
        ...baseAgent,
        tools: [
          {
            type: 'mcp_toolset',
            mcp_server_name: 'github',
            configs: {
              create_issue: { permission_policy: { type: permissionPolicy } },
            },
          },
        ],
      });

      expect(parsed.success).toBe(false);
      expect(
        validateAgentConfiguration(
          [
            {
              type: 'mcp_toolset',
              mcp_server_name: 'github',
              configs: [
                {
                  name: 'create_issue',
                  permission_policy: { type: permissionPolicy },
                },
              ],
            },
          ],
          [{ type: 'url', name: 'github', url: 'https://example.com/mcp' }],
        ),
      ).toMatch(/must be always_allow or always_ask/);
    },
  );

  it('rejects legacy string permission policies in record-form tool configs', () => {
    const parsed = AgentCreate.safeParse({
      ...baseAgent,
      tools: [
        {
          type: 'mcp_toolset',
          mcp_server_name: 'github',
          configs: {
            create_issue: { permission_policy: 'always_deny' },
          },
        },
      ],
    });

    expect(parsed.success).toBe(false);
  });

  it('requires the complete Claude custom-tool request shape', () => {
    expect(
      AgentCreate.safeParse({
        ...baseAgent,
        tools: [{ type: 'custom', name: 'lookup' }],
      }).success,
    ).toBe(false);
    expect(
      AgentCreate.safeParse({
        ...baseAgent,
        tools: [
          {
            type: 'custom',
            name: 'lookup',
            description: 'Look up a record',
            input_schema: { type: 'object', required: 'query' },
          },
        ],
      }).success,
    ).toBe(false);
  });

  it('preserves arbitrary JSON Schema extensions on custom-tool input_schema', () => {
    const parsed = AgentCreate.safeParse({
      ...baseAgent,
      tools: [
        {
          type: 'custom',
          name: 'lookup',
          description: 'Look up a record',
          input_schema: {
            type: 'object',
            properties: { query: { type: 'string' } },
            required: ['query'],
            additionalProperties: false,
            unevaluatedProperties: false,
            'x-orca-extension': { mode: 'strict' },
          },
        },
      ],
    });

    expect(parsed.success).toBe(true);
    if (!parsed.success) return;
    expect(parsed.data.tools[0]).toMatchObject({
      input_schema: {
        additionalProperties: false,
        unevaluatedProperties: false,
        'x-orca-extension': { mode: 'strict' },
      },
    });
  });

  it.each(['web_fetch', 'web_search'])('rejects reserved custom tool name %s', (name) => {
    expect(
      AgentCreate.safeParse({
        ...baseAgent,
        tools: [
          {
            type: 'custom',
            name,
            description: 'Conflicts with a built-in tool',
            input_schema: { type: 'object' },
          },
        ],
      }).success,
    ).toBe(false);
  });

  it('rejects duplicate custom names and unreferenced MCP servers', () => {
    expect(
      validateAgentConfiguration(
        [
          {
            type: 'custom',
            name: 'lookup',
            description: 'First lookup',
            input_schema: { type: 'object' },
          },
          {
            type: 'custom',
            name: 'lookup',
            description: 'Second lookup',
            input_schema: { type: 'object' },
          },
        ],
        [],
      ),
    ).toMatch(/unique/);

    expect(
      validateAgentConfiguration(
        [],
        [{ type: 'url', name: 'github', url: 'https://example.com/mcp' }],
      ),
    ).toMatch(/must be referenced/);
  });
});

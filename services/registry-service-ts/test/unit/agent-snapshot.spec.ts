// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { parseSnapshotBody } from '../../../session-runner/src/snapshot.js';
import { buildSessionStartInput } from '../../../session-runner/src/harness/provider.js';
// Unit spec for the credential-free runtime agent snapshot builder.
//
// At session start the registry composes a SNAPSHOT of everything a runner needs
// to run a turn for an (agent, environment, session) triple, and delivers it over
// the tunnel. The snapshot is PURE-built from already-resolved records:
//   - model provider + id (the LLM the agent runs);
//   - PROVIDER selection (which agent harness/provider the multi-provider runner
//     must spin up — the launch frame omits this by design, so it MUST ride the
//     snapshot);
//   - system prompt COMPOSED with the agent's skills' system prompts;
//   - the tool allowlist, INTERSECTED across the agent's tools and each skill's
//     tool allowlist, then expanded to the concrete Orca tool names;
//   - the enabled MCP server names (the mcp_toolset servers in the intersected set);
//   - the egress config (gateway or sidecar) — credential-free.
//
// The snapshot is CREDENTIAL-FREE: no upstream secret ever appears. Gateway mode
// carries an opaque scoped JWT + vault-id references; sidecar carries vault
// references only.

import { describe, it, expect } from 'vitest';
import { buildAgentSnapshot, type AgentSnapshotInput } from '../../src/domain/agent-snapshot.js';
import { buildGatewayEgress, buildSidecarEgress } from '../../src/domain/credential-egress.js';
import { assertSnapshotCredentialFree } from '../../src/domain/egress-credential-free.js';

function baseInput(overrides: Partial<AgentSnapshotInput> = {}): AgentSnapshotInput {
  return {
    agent: {
      model: { provider: 'anthropic', id: 'claude-opus-4' },
      system: 'You are a helpful agent.',
      tools: [{ type: 'agent_toolset' }, { type: 'mcp_toolset', mcp_server_name: 'github' }],
    },
    skills: [],
    provider: 'claude',
    egress: buildGatewayEgress({
      sessionId: 'ses_snap_1',
      gatewayMcpUrl: 'https://ai-gateway.internal/mcp',
      sessionJwt: 'eyJ.gateway.jwt',
      mcpServers: [{ name: 'github', url: 'https://github.example/mcp' }],
      vaultByUrl: new Map(),
    }),
    ...overrides,
  };
}

describe('buildAgentSnapshot', () => {
  it('carries the model provider + id from the agent', () => {
    const snap = buildAgentSnapshot(baseInput());
    expect(snap.model.provider).toBe('anthropic');
    expect(snap.model.id).toBe('claude-opus-4');
  });

  it('carries the harness/provider selection (the launch frame omits it)', () => {
    const snap = buildAgentSnapshot(baseInput({ provider: 'codex' }));
    expect(snap.provider).toBe('codex');
  });

  it('uses the agent system prompt verbatim when there are no skills', () => {
    const snap = buildAgentSnapshot(baseInput());
    expect(snap.system).toBe('You are a helpful agent.');
  });

  it('composes the agent system prompt with each skill system prompt, in order', () => {
    const snap = buildAgentSnapshot(
      baseInput({
        skills: [
          { systemPrompt: 'Skill A guidance.', toolAllowlist: null },
          { systemPrompt: 'Skill B guidance.', toolAllowlist: null },
        ],
      }),
    );
    expect(snap.system).toBe('You are a helpful agent.\n\nSkill A guidance.\n\nSkill B guidance.');
  });

  it('expands agent_toolset to the concrete Orca tool names', () => {
    const snap = buildAgentSnapshot(baseInput());
    // agent_toolset expands to the file/shell logical tools.
    expect(snap.allowed_tool_names.sort()).toEqual(
      ['bash', 'delete', 'edit', 'glob', 'grep', 'list', 'read', 'write'].sort(),
    );
  });

  it('keeps the agent_toolset expansion when a skill allowlist re-grants agent_toolset', () => {
    // The tool-allowlist vocabulary is tool TYPES: a skill narrows by naming
    // `agent_toolset` (keep the whole file/shell toolset) or omitting it. A skill
    // that grants only agent_toolset keeps the 8 logical tools and drops the
    // separate mcp_toolset capability.
    const snap = buildAgentSnapshot(
      baseInput({
        skills: [{ systemPrompt: '', toolAllowlist: ['agent_toolset'] }],
      }),
    );
    expect(snap.allowed_tool_names.sort()).toEqual(
      ['bash', 'delete', 'edit', 'glob', 'grep', 'list', 'read', 'write'].sort(),
    );
  });

  it('intersection is cumulative across multiple skills (each narrows further)', () => {
    // Agent grants agent_toolset + a custom tool + mcp_toolset; skill 1 keeps
    // agent_toolset + the custom tool; skill 2 ∩ keeps the custom tool only.
    const snap = buildAgentSnapshot(
      baseInput({
        agent: {
          model: { provider: 'anthropic', id: 'claude-opus-4' },
          system: 'You are a helpful agent.',
          tools: [
            { type: 'agent_toolset' },
            {
              type: 'custom',
              name: 'lookup_invoice',
              description: 'Look up an invoice',
              input_schema: { type: 'object' },
            },
            { type: 'mcp_toolset', mcp_server_name: 'github' },
          ],
        },
        skills: [
          { systemPrompt: '', toolAllowlist: ['agent_toolset', 'lookup_invoice'] },
          { systemPrompt: '', toolAllowlist: ['lookup_invoice', 'mcp_toolset'] },
        ],
      }),
    );
    // Only the custom tool survives both skills; agent_toolset is intersected away
    // so none of the file/shell logical tools remain, and mcp_toolset is gone too.
    expect(snap.allowed_tool_names).toEqual(['lookup_invoice']);
    expect(snap.allowed_mcp_server_names).toEqual([]);
  });

  it('surfaces a custom tool by its name alongside the agent_toolset expansion', () => {
    const snap = buildAgentSnapshot(
      baseInput({
        agent: {
          model: { provider: 'anthropic', id: 'claude-opus-4' },
          system: '',
          tools: [
            { type: 'agent_toolset' },
            {
              type: 'custom',
              name: 'lookup_invoice',
              description: 'Look up an invoice',
              input_schema: { type: 'object' },
            },
          ],
        },
      }),
    );
    expect(snap.allowed_tool_names.sort()).toEqual(
      ['bash', 'delete', 'edit', 'glob', 'grep', 'list', 'lookup_invoice', 'read', 'write'].sort(),
    );
  });

  it('a skill with a null allowlist does not narrow the tools', () => {
    const snap = buildAgentSnapshot(
      baseInput({
        skills: [
          { systemPrompt: '', toolAllowlist: ['agent_toolset'] },
          { systemPrompt: '', toolAllowlist: null },
        ],
      }),
    );
    expect(snap.allowed_tool_names.sort()).toEqual(
      ['bash', 'delete', 'edit', 'glob', 'grep', 'list', 'read', 'write'].sort(),
    );
  });

  it('surfaces the enabled mcp_toolset server names that survive the intersection', () => {
    const snap = buildAgentSnapshot(baseInput());
    expect(snap.allowed_mcp_server_names).toEqual(['github']);
  });

  it('drops mcp_toolset servers when mcp_toolset is intersected away by a skill', () => {
    // A skill that allows only agent_toolset removes mcp_toolset → no mcp servers.
    const snap = buildAgentSnapshot(
      baseInput({
        skills: [{ systemPrompt: '', toolAllowlist: ['agent_toolset'] }],
      }),
    );
    expect(snap.allowed_mcp_server_names).toEqual([]);
  });

  it('embeds the gateway egress config in the snapshot', () => {
    const snap = buildAgentSnapshot(baseInput());
    expect(snap.egress.mode).toBe('gateway');
  });

  it('embeds the sidecar egress config in the snapshot', () => {
    const snap = buildAgentSnapshot(
      baseInput({
        egress: buildSidecarEgress({
          bindings: [{ host: 'github.com', scheme: 'bearer', vaultId: 'vlt_gh' }],
        }),
      }),
    );
    expect(snap.egress.mode).toBe('sidecar');
    if (snap.egress.mode !== 'sidecar') throw new Error('unreachable');
    expect(snap.egress.sidecar.entries[0]!.host).toBe('github.com');
  });

  it('is credential-free: STRUCTURALLY conforms to the secret-free snapshot shape', () => {
    // Structural guarantee (not a string heuristic): the snapshot envelope carries
    // only known non-credential fields and the egress block passes the structural
    // credential-free check, so a NOVEL secret-shaped field anywhere fails loud
    // regardless of its name/value. The opaque JWT + vault references that ARE
    // allowed pass; a raw upstream secret could not.
    const snap = buildAgentSnapshot(baseInput());
    expect(() => assertSnapshotCredentialFree(snap)).not.toThrow();
  });

  it('handles an agent with no tools (empty allowlist + no mcp servers)', () => {
    const snap = buildAgentSnapshot(
      baseInput({
        agent: {
          model: { provider: 'anthropic', id: 'claude-opus-4' },
          system: '',
          tools: [],
        },
      }),
    );
    expect(snap.allowed_tool_names).toEqual([]);
    expect(snap.allowed_mcp_server_names).toEqual([]);
    expect(snap.system).toBe('');
  });

  it('omits the multiagent block for a single-agent snapshot (purely additive)', () => {
    const snap = buildAgentSnapshot(baseInput());
    // A single agent carries no roster — the runner's maybeWrapCoordinator never fires.
    expect(snap.multiagent).toBeUndefined();
    expect('multiagent' in snap).toBe(false);
  });

  it('omits custom_spec for a provider that did not declare one (purely additive)', () => {
    const snap = buildAgentSnapshot(baseInput({ provider: 'claude' }));
    // Only the `custom` native-CLI provider carries a spec; every other snapshot omits
    // the field entirely so its wire shape is byte-for-byte unchanged.
    expect(snap.custom_spec).toBeUndefined();
    expect('custom_spec' in snap).toBe(false);
  });

  it('carries the custom_spec verbatim for a custom native-CLI agent', () => {
    // The generic `custom` provider's declarative CLI spec rides the snapshot opaque —
    // the pure builder never inspects it; the runner's custom harness parses it.
    const spec = {
      command: 'my-cli',
      argv: ['--session', '{sessionId}'],
      stdout: { mode: 'text' },
    };
    const snap = buildAgentSnapshot(baseInput({ provider: 'custom', customSpec: spec }));
    expect(snap.provider).toBe('custom');
    expect(snap.custom_spec).toEqual(spec);
    // Serialized on the wire as `custom_spec` — the runner reads exactly this key.
    expect(JSON.parse(JSON.stringify(snap)).custom_spec).toEqual(spec);
  });

  it('carries the resolved runtime multiagent block for a coordinator', () => {
    // The RESOLVED runtime shape: each roster member carries its OWN sub-snapshot (so
    // the runner constructs a subagent harness without another round-trip), plus a self
    // member that carries only a name (the runner derives its snapshot).
    const memberSnap = buildAgentSnapshot(
      baseInput({
        agent: {
          model: { provider: 'anthropic', id: 'claude-haiku-4' },
          system: 'research',
          tools: [],
        },
      }),
    );
    const snap = buildAgentSnapshot(
      baseInput({
        multiagent: {
          type: 'coordinator',
          primary_thread_id: '',
          agents: [
            { agent_name: 'researcher', snapshot: memberSnap },
            { type: 'self', agent_name: 'coordinator' },
          ],
        },
      }),
    );
    expect(snap.multiagent).toBeDefined();
    expect(snap.multiagent!.type).toBe('coordinator');
    expect(snap.multiagent!.agents).toHaveLength(2);
    const first = snap.multiagent!.agents[0]!;
    if ('snapshot' in first) {
      expect(first.agent_name).toBe('researcher');
      expect(first.snapshot.system).toBe('research');
      expect(first.snapshot.model.id).toBe('claude-haiku-4');
    } else {
      throw new Error('expected an agent member with a resolved snapshot');
    }
    expect(snap.multiagent!.agents[1]).toEqual({ type: 'self', agent_name: 'coordinator' });
  });
});

describe('custom callback snapshot definitions', () => {
  const custom = {
    type: 'custom',
    name: 'lookup_ticket',
    description: 'Find a ticket',
    input_schema: {
      type: 'object',
      properties: { id: { type: 'string' } },
      required: ['id'],
      additionalProperties: false,
    },
  };
  it('carries only declared tools surviving every Skill allowlist, preserving schema', () => {
    const input = baseInput();
    input.agent.tools = [custom, { ...custom, name: 'excluded' }];
    input.skills = [{ systemPrompt: '', toolAllowlist: ['lookup_ticket'] }];
    const built = buildAgentSnapshot(input);
    const parsed = parseSnapshotBody(Buffer.from(JSON.stringify(built) + '\n'));
    expect(
      buildSessionStartInput(parsed, { workspaceId: 'ws', sessionId: 'ses' }).agentSnapshot
        .custom_tools,
    ).toEqual([
      { name: custom.name, description: custom.description, input_schema: custom.input_schema },
    ]);
    input.skills.push({ systemPrompt: '', toolAllowlist: [] });
    expect(buildAgentSnapshot(input).custom_tools).toBeUndefined();
  });
  it.each(
    [
      [custom, custom],
      [{ ...custom, name: 'bash' }],
      [{ ...custom, name: 'mcp__server__tool' }],
      [{ ...custom, name: 'sys_terminal_launch' }],
      [{ ...custom, description: '' }],
      [{ type: 'custom', name: 'missing_schema' }],
      [{ ...custom, input_schema: { type: 'string' } }],
    ].map((tools) => ({ tools })),
  )('rejects malformed, duplicate, and reserved callbacks: %j', ({ tools }) => {
    const input = baseInput();
    input.agent.tools = tools;
    expect(() => buildAgentSnapshot(input)).toThrow();
  });
});

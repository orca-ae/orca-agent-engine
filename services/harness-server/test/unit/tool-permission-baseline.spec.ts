// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * BEHAVIORAL BASELINE — tool permission decisions.
 *
 * This file characterizes tool-permission behavior exactly as it exists today,
 * at the three places a permission decision is made:
 *
 *   1. `buildToolPermissionPolicies` (src/runner/dispatcher.ts) — folds an
 *      agent's declared toolsets into the policy map carried on the runtime
 *      snapshot.
 *   2. `canUseTool` (src/harness/claude/index.ts) — resolves a policy for a
 *      concrete tool name at call time and allows, denies, or parks the turn on
 *      a human confirmation.
 *   3. `buildSdkToolOptions` (src/harness/in-sandbox/index.ts) — decides which
 *      tools are handed to a sandboxed session at all, since that session has
 *      no way to ask.
 *
 * The permission decision is scheduled to move into the guardrail evaluation
 * engine. This suite is what makes that move provable rather than hopeful: it
 * MUST keep passing UNMODIFIED across the refactor. If a test here has to be
 * edited to stay green, observable behavior changed — that is a finding to
 * raise, not a test to adjust.
 *
 * Every test is named for the behavior it pins, and documents what the code
 * does TODAY, not what it arguably ought to do. Where current behavior looks
 * surprising, the test says so in a comment and still asserts reality.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { ReadOptions, TailOptions, TranscriptStore } from '@orca/transcript-store';

const { queryMock } = vi.hoisted(() => ({ queryMock: vi.fn() }));

vi.mock('@anthropic-ai/claude-agent-sdk', () => ({
  query: queryMock,
  createSdkMcpServer: vi.fn((cfg: unknown) => ({ type: 'sdk', instance: cfg })),
  tool: vi.fn((name: string, description: string, inputSchema: unknown, handler: unknown) => ({
    name,
    description,
    inputSchema,
    handler,
  })),
}));

import { buildToolPermissionPolicies } from '../../src/runner/dispatcher.js';
import { ClaudeAgentSdkHarness } from '../../src/harness/claude/index.js';
import { ClaudeAgentSdkAdapter } from '../../src/harness/claude/session-adapter.js';
import { InSandboxHarness } from '../../src/harness/in-sandbox/index.js';
import type {
  HarnessChannel,
  HarnessTransport,
  OpenSessionOptions,
  RawSandboxEvent,
} from '../../src/harness/in-sandbox/transport.js';
import type { SessionStartInput } from '../../src/harness/agent-harness.js';
import type { AgentToolEntry } from '../../src/clients/registry.js';

// ---------------------------------------------------------------------------
// Shared fixtures
// ---------------------------------------------------------------------------

type PolicyName = 'always_allow' | 'always_ask' | 'always_deny';

/** Build the snapshot policy map the way the dispatcher builds it for a session. */
function policiesFor(
  tools: AgentToolEntry[],
  mcpServers: Array<{ name: string; url: string }> = [],
  enabledServerNames?: Iterable<string>,
): Record<string, PolicyName> {
  return buildToolPermissionPolicies({ tools, mcp_servers: mcpServers }, enabledServerNames);
}

// ---------------------------------------------------------------------------
// 1. Snapshot policy construction (dispatcher)
// ---------------------------------------------------------------------------

describe('baseline: how declared toolsets become snapshot permission policies', () => {
  it('gives an enabled remote MCP toolset a wildcard policy of always_ask when none is declared', () => {
    expect(
      policiesFor(
        [{ type: 'mcp_toolset', mcp_server_name: 'tickets' }],
        [{ name: 'tickets', url: 'https://mcp.example.com' }],
      ),
    ).toEqual({ 'mcp__tickets__*': 'always_ask' });
  });

  it('never invents a wildcard policy for the built-in toolset when none is declared', () => {
    // Unlike a remote MCP toolset, the built-in toolset gets no `mcp__orca__*`
    // fallback key. The absence is what makes the runtime default (always_allow)
    // apply to built-ins.
    expect(policiesFor([{ type: 'agent_toolset' }])).toEqual({});
  });

  it('resolves a toolset with enabled false to a wildcard always_deny', () => {
    expect(
      policiesFor(
        [{ type: 'mcp_toolset', mcp_server_name: 'tickets', default_config: { enabled: false } }],
        [{ name: 'tickets', url: 'https://mcp.example.com' }],
        ['tickets'],
      ),
    ).toEqual({ 'mcp__tickets__*': 'always_deny' });
  });

  it('lets enabled false outrank a permission policy declared on the same default config', () => {
    expect(
      policiesFor(
        [
          {
            type: 'mcp_toolset',
            mcp_server_name: 'tickets',
            default_config: { enabled: false, permission_policy: 'always_allow' },
          },
        ],
        [{ name: 'tickets', url: 'https://mcp.example.com' }],
        ['tickets'],
      ),
    ).toEqual({ 'mcp__tickets__*': 'always_deny' });
  });

  it('resolves enabled false on the built-in toolset to a built-in wildcard always_deny', () => {
    expect(policiesFor([{ type: 'agent_toolset', default_config: { enabled: false } }])).toEqual({
      'mcp__orca__*': 'always_deny',
    });
  });

  it('lets an explicit per-tool policy beat the toolset default policy', () => {
    expect(
      policiesFor(
        [
          {
            type: 'mcp_toolset',
            mcp_server_name: 'tickets',
            default_config: { permission_policy: 'always_ask' },
            configs: [{ name: 'search', permission_policy: 'always_allow' }],
          },
        ],
        [{ name: 'tickets', url: 'https://mcp.example.com' }],
      ),
    ).toEqual({
      'mcp__tickets__*': 'always_ask',
      mcp__tickets__search: 'always_allow',
    });
  });

  it('reads per-tool configs from the object-map shape the same as the array shape', () => {
    expect(
      policiesFor(
        [
          {
            type: 'mcp_toolset',
            mcp_server_name: 'tickets',
            default_config: { permission_policy: 'always_ask' },
            configs: { search: { permission_policy: 'always_allow' } },
          },
        ],
        [{ name: 'tickets', url: 'https://mcp.example.com' }],
      ),
    ).toEqual({
      'mcp__tickets__*': 'always_ask',
      mcp__tickets__search: 'always_allow',
    });
  });

  it('lets a per-tool enabled false deny one tool while the rest of the toolset stays askable', () => {
    expect(
      policiesFor(
        [
          {
            type: 'mcp_toolset',
            mcp_server_name: 'tickets',
            configs: [{ name: 'search', enabled: false }],
          },
        ],
        [{ name: 'tickets', url: 'https://mcp.example.com' }],
      ),
    ).toEqual({
      'mcp__tickets__*': 'always_ask',
      mcp__tickets__search: 'always_deny',
    });
  });

  it('emits no per-tool key when a config only re-enables a tool that the default already enabled', () => {
    // `enabled: true` under an already-enabled default is a no-op: the tool
    // inherits the wildcard, and no exact key is written.
    expect(
      policiesFor(
        [
          {
            type: 'mcp_toolset',
            mcp_server_name: 'tickets',
            configs: [{ name: 'search', enabled: true }],
          },
        ],
        [{ name: 'tickets', url: 'https://mcp.example.com' }],
      ),
    ).toEqual({ 'mcp__tickets__*': 'always_ask' });
  });

  it('gives a tool re-enabled under a disabled default the default toolset permission policy', () => {
    expect(
      policiesFor(
        [
          {
            type: 'mcp_toolset',
            mcp_server_name: 'tickets',
            default_config: { enabled: false, permission_policy: 'always_ask' },
            configs: [{ name: 'search', enabled: true }],
          },
        ],
        [{ name: 'tickets', url: 'https://mcp.example.com' }],
        ['tickets'],
      ),
    ).toEqual({
      'mcp__tickets__*': 'always_deny',
      mcp__tickets__search: 'always_ask',
    });
  });

  it('falls back to always_allow for a tool re-enabled under a disabled default with no declared policy', () => {
    expect(
      policiesFor(
        [
          {
            type: 'mcp_toolset',
            mcp_server_name: 'tickets',
            default_config: { enabled: false },
            configs: [{ name: 'search', enabled: true }],
          },
        ],
        [{ name: 'tickets', url: 'https://mcp.example.com' }],
        ['tickets'],
      ),
    ).toEqual({
      'mcp__tickets__*': 'always_deny',
      mcp__tickets__search: 'always_allow',
    });
  });

  it('keeps always_deny as a storable policy name even though callers only declare allow or ask', () => {
    // always_deny is not part of the externally declarable policy vocabulary,
    // but it survives end to end as an internal policy value and must keep
    // doing so — it is the only representation of a hard block.
    expect(
      policiesFor([
        { type: 'agent_toolset', configs: [{ name: 'bash', permission_policy: 'always_deny' }] },
      ]),
    ).toEqual({ mcp__orca__bash: 'always_deny' });
  });

  it('accepts both the bare-string and the object form of a permission policy', () => {
    expect(
      policiesFor([{ type: 'agent_toolset', default_config: { permission_policy: 'always_ask' } }]),
    ).toEqual({ 'mcp__orca__*': 'always_ask' });
    expect(
      policiesFor([
        {
          type: 'agent_toolset_20260401',
          default_config: { permission_policy: { type: 'always_ask' } },
        },
      ]),
    ).toEqual({ 'mcp__orca__*': 'always_ask' });
  });

  it('ignores a permission policy whose value is neither a known name nor a typed object', () => {
    expect(
      policiesFor([
        { type: 'agent_toolset', default_config: { permission_policy: { type: 'sometimes' } } },
      ]),
    ).toEqual({});
    expect(
      policiesFor([{ type: 'agent_toolset', default_config: { permission_policy: 7 } }]),
    ).toEqual({});
  });

  it('ignores a non-boolean enabled value and keeps the inherited enabled state', () => {
    // Only a real boolean counts; `enabled: null` leaves the default in force,
    // so no per-tool deny is written.
    expect(
      policiesFor(
        [
          {
            type: 'mcp_toolset',
            mcp_server_name: 'tickets',
            configs: [{ name: 'search', enabled: null }],
          },
        ],
        [{ name: 'tickets', url: 'https://mcp.example.com' }],
      ),
    ).toEqual({ 'mcp__tickets__*': 'always_ask' });
  });

  it('drops a per-tool config that names a tool belonging to a different server', () => {
    // Silent drop: the config is discarded rather than rejected or remapped.
    expect(
      policiesFor(
        [
          {
            type: 'mcp_toolset',
            mcp_server_name: 'tickets',
            configs: [{ name: 'mcp__docs__search', permission_policy: 'always_allow' }],
          },
        ],
        [{ name: 'tickets', url: 'https://mcp.example.com' }],
      ),
    ).toEqual({ 'mcp__tickets__*': 'always_ask' });
  });

  it('keeps a per-tool config that names its own server explicitly', () => {
    expect(
      policiesFor(
        [
          {
            type: 'mcp_toolset',
            mcp_server_name: 'tickets',
            configs: [{ name: 'mcp__tickets__search', permission_policy: 'always_allow' }],
          },
        ],
        [{ name: 'tickets', url: 'https://mcp.example.com' }],
      ),
    ).toEqual({
      'mcp__tickets__*': 'always_ask',
      mcp__tickets__search: 'always_allow',
    });
  });

  it('lets the first declared toolset win the wildcard key but the last declared config win a tool key', () => {
    // Asymmetric precedence between duplicate declarations. Easy to break
    // silently when the fold is rewritten.
    expect(
      policiesFor([
        {
          type: 'agent_toolset',
          default_config: { permission_policy: 'always_ask' },
          configs: [{ name: 'bash', permission_policy: 'always_allow' }],
        },
        {
          type: 'agent_toolset',
          default_config: { permission_policy: 'always_allow' },
          configs: [{ name: 'bash', permission_policy: 'always_deny' }],
        },
      ]),
    ).toEqual({
      'mcp__orca__*': 'always_ask',
      mcp__orca__bash: 'always_deny',
    });
  });

  it('drops a per-tool config whose name is empty', () => {
    expect(
      policiesFor(
        [
          {
            type: 'mcp_toolset',
            mcp_server_name: 'tickets',
            configs: [{ name: '', permission_policy: 'always_allow' }],
          },
        ],
        [{ name: 'tickets', url: 'https://mcp.example.com' }],
      ),
    ).toEqual({ 'mcp__tickets__*': 'always_ask' });
  });

  it('emits no policy for a server that is enabled but not declared in the agent MCP server list', () => {
    expect(
      policiesFor([{ type: 'mcp_toolset', mcp_server_name: 'tickets' }], [], ['tickets']),
    ).toEqual({});
  });

  it('emits no policy for a server whose name is not an acceptable server name', () => {
    expect(
      policiesFor(
        [{ type: 'mcp_toolset', mcp_server_name: '_tickets' }],
        [{ name: '_tickets', url: 'https://mcp.example.com' }],
        ['_tickets'],
      ),
    ).toEqual({});
  });

  it('never treats the built-in server name as a remote MCP server', () => {
    expect(
      policiesFor(
        [
          {
            type: 'mcp_toolset',
            mcp_server_name: 'orca',
            default_config: { permission_policy: 'always_allow' },
          },
        ],
        [{ name: 'orca', url: 'https://mcp.example.com' }],
        ['orca'],
      ),
    ).toEqual({});
  });
});

// ---------------------------------------------------------------------------
// 2. Runtime permission resolution (Claude harness `canUseTool`)
// ---------------------------------------------------------------------------

function emptyAsyncIterable<T>(): AsyncIterable<T> {
  return {
    [Symbol.asyncIterator](): AsyncIterator<T> {
      return {
        async next(): Promise<IteratorResult<T>> {
          return { done: true, value: undefined as T };
        },
      };
    },
  };
}

function buildStubStore(): TranscriptStore {
  return {
    append: async () => [],
    read: (_workspaceId: string, _sessionId: string, _opts: ReadOptions) =>
      emptyAsyncIterable<never>(),
    tail: (_workspaceId: string, _sessionId: string, _opts: TailOptions) =>
      emptyAsyncIterable<never>(),
    archive: async () => {},
    close: async () => {},
  } satisfies TranscriptStore;
}

interface PermissionOutcome {
  behavior: 'allow' | 'deny';
  message?: string;
  updatedInput?: Record<string, unknown>;
  toolUseID?: string;
}

type CanUseTool = (
  toolName: string,
  input: Record<string, unknown>,
  options: { signal: AbortSignal; toolUseID: string; agentID?: string },
) => Promise<PermissionOutcome>;

interface SubagentHook {
  hooks: Array<(input: unknown, id: string, options: unknown) => Promise<unknown>>;
}

interface StartedTurn {
  harness: ClaudeAgentSdkHarness;
  canUseTool: CanUseTool;
  startSubagent: (runtimeAgentId: string, agentType: string) => Promise<void>;
  stopSubagent: (runtimeAgentId: string) => Promise<void>;
}

/**
 * Start a session and run one empty turn so the SDK options — and with them the
 * live `canUseTool` gate — are observable.
 */
async function startTurn(input: SessionStartInput): Promise<StartedTurn> {
  queryMock.mockReturnValueOnce(emptyAsyncIterable());
  const harness = new ClaudeAgentSdkHarness({
    apiKey: 'unused',
    modelDefault: 'fake',
    adapter: new ClaudeAgentSdkAdapter(buildStubStore(), input.workspaceId),
    workspaceId: input.workspaceId,
    sessionId: input.sessionId,
  });
  await harness.start(input);
  await harness.submit({
    kind: 'user.message',
    payload: { content: [{ type: 'text', text: 'go' }] },
  });
  const options = queryMock.mock.calls.at(-1)?.[0]?.options as {
    canUseTool: CanUseTool;
    hooks?: { SubagentStart?: SubagentHook[]; SubagentStop?: SubagentHook[] };
  };
  const fire = async (
    phase: 'SubagentStart' | 'SubagentStop',
    payload: Record<string, unknown>,
  ) => {
    const hook = options.hooks?.[phase]?.[0]?.hooks[0];
    if (!hook) throw new Error(`baseline fixture: no ${phase} hook was registered`);
    await hook({ hook_event_name: phase, ...payload }, `hook_${phase}`, {
      signal: new AbortController().signal,
    });
  };
  return {
    harness,
    canUseTool: options.canUseTool,
    startSubagent: (agent_id, agent_type) => fire('SubagentStart', { agent_id, agent_type }),
    stopSubagent: (agent_id) =>
      fire('SubagentStop', { agent_id, stop_hook_active: false, agent_transcript_path: '/tmp/x' }),
  };
}

const ASK = 'ask' as const;

function afterMacrotask(): Promise<typeof ASK> {
  return new Promise((resolve) => setImmediate(() => resolve(ASK)));
}

let toolUseCounter = 0;

/**
 * Classify what the permission gate did with one tool call. `allow` and `deny`
 * settle immediately; `ask` parks the call on a human confirmation that never
 * arrives in these tests, which is exactly the observable difference.
 */
async function decide(
  canUseTool: CanUseTool,
  toolName: string,
  options: { agentID?: string; input?: Record<string, unknown>; signal?: AbortSignal } = {},
): Promise<'allow' | 'deny' | typeof ASK> {
  toolUseCounter += 1;
  const outcome = await Promise.race([
    canUseTool(toolName, options.input ?? { probe: true }, {
      signal: options.signal ?? new AbortController().signal,
      toolUseID: `toolu_${toolUseCounter}`,
      ...(options.agentID === undefined ? {} : { agentID: options.agentID }),
    }),
    afterMacrotask(),
  ]);
  return outcome === ASK ? ASK : outcome.behavior;
}

/** Drain framing events and return the first tool-use event the harness surfaces. */
async function nextToolUseEvent(
  harness: ClaudeAgentSdkHarness,
): Promise<{ kind: string; payload: unknown }> {
  const iterator = harness.events()[Symbol.asyncIterator]();
  for (;;) {
    const next = await iterator.next();
    expect(next.done).toBe(false);
    if (next.value.kind === 'agent.tool_use' || next.value.kind === 'agent.mcp_tool_use') {
      return next.value;
    }
  }
}

describe('baseline: what always_ask does to a turn', () => {
  beforeEach(() => {
    queryMock.mockReset();
  });

  it('surfaces a built-in tool for confirmation and parks the session on a required action', async () => {
    const { harness, canUseTool } = await startTurn({
      workspaceId: 'ws_baseline',
      sessionId: 'ses_baseline',
      agentSnapshot: { tool_permission_policies: { mcp__orca__bash: 'always_ask' } },
    });

    void canUseTool(
      'mcp__orca__bash',
      { cmd: 'pwd' },
      { signal: new AbortController().signal, toolUseID: 'toolu_ask_builtin' },
    );

    await expect(nextToolUseEvent(harness)).resolves.toMatchObject({
      kind: 'agent.tool_use',
      payload: { name: 'mcp__orca__bash', input: { cmd: 'pwd' } },
    });
    expect(harness.hasPendingRequiredAction()).toBe(true);
  });

  it('surfaces a remote MCP tool for confirmation under its own server and unqualified name', async () => {
    const { harness, canUseTool } = await startTurn({
      workspaceId: 'ws_baseline',
      sessionId: 'ses_baseline',
      agentSnapshot: {},
      remoteMcpToolsets: [{ serverName: 'tickets' }],
    });

    void canUseTool(
      'mcp__tickets__search',
      { query: 'open' },
      { signal: new AbortController().signal, toolUseID: 'toolu_ask_remote' },
    );

    await expect(nextToolUseEvent(harness)).resolves.toMatchObject({
      kind: 'agent.mcp_tool_use',
      payload: { name: 'search', mcp_server_name: 'tickets', input: { query: 'open' } },
    });
    expect(harness.hasPendingRequiredAction()).toBe(true);
  });

  it('leaves the session with no required action when the policy resolves to allow', async () => {
    const { harness, canUseTool } = await startTurn({
      workspaceId: 'ws_baseline',
      sessionId: 'ses_baseline',
      agentSnapshot: { tool_permission_policies: { mcp__orca__bash: 'always_allow' } },
    });

    await expect(decide(canUseTool, 'mcp__orca__bash')).resolves.toBe('allow');
    expect(harness.hasPendingRequiredAction()).toBe(false);
  });
});

describe('baseline: how the runtime permission gate resolves a tool name', () => {
  beforeEach(() => {
    queryMock.mockReset();
  });

  it('prefers an exact tool-name policy over the server wildcard policy', async () => {
    const { canUseTool } = await startTurn({
      workspaceId: 'ws_baseline',
      sessionId: 'ses_baseline',
      agentSnapshot: {
        tool_permission_policies: {
          'mcp__tickets__*': 'always_allow',
          mcp__tickets__search: 'always_deny',
        },
      },
      remoteMcpToolsets: [{ serverName: 'tickets' }],
    });

    expect(await decide(canUseTool, 'mcp__tickets__search')).toBe('deny');
    expect(await decide(canUseTool, 'mcp__tickets__list')).toBe('allow');
  });

  it('prefers an exact built-in tool policy over the built-in wildcard policy', async () => {
    const { canUseTool } = await startTurn({
      workspaceId: 'ws_baseline',
      sessionId: 'ses_baseline',
      agentSnapshot: {
        tool_permission_policies: {
          'mcp__orca__*': 'always_allow',
          mcp__orca__bash: 'always_deny',
        },
      },
    });

    expect(await decide(canUseTool, 'mcp__orca__bash')).toBe('deny');
    expect(await decide(canUseTool, 'mcp__orca__read')).toBe('allow');
  });

  it('falls back to the server wildcard policy when no exact key exists', async () => {
    const { canUseTool } = await startTurn({
      workspaceId: 'ws_baseline',
      sessionId: 'ses_baseline',
      agentSnapshot: {
        tool_permission_policies: { 'mcp__orca__*': 'always_ask' },
      },
    });

    expect(await decide(canUseTool, 'mcp__orca__bash')).toBe(ASK);
  });

  it('falls back to always_ask for a declared remote toolset with no policy of its own', async () => {
    const { canUseTool } = await startTurn({
      workspaceId: 'ws_baseline',
      sessionId: 'ses_baseline',
      agentSnapshot: {},
      remoteMcpToolsets: [{ serverName: 'tickets' }],
    });

    expect(await decide(canUseTool, 'mcp__tickets__search')).toBe(ASK);
  });

  it('uses the declared toolset permission policy instead of the always_ask fallback', async () => {
    const { canUseTool } = await startTurn({
      workspaceId: 'ws_baseline',
      sessionId: 'ses_baseline',
      agentSnapshot: {},
      remoteMcpToolsets: [{ serverName: 'tickets', permissionPolicy: 'always_allow' }],
    });

    expect(await decide(canUseTool, 'mcp__tickets__search')).toBe('allow');
  });

  it('denies a remote MCP tool whose server is neither policied nor a declared toolset', async () => {
    const { canUseTool } = await startTurn({
      workspaceId: 'ws_baseline',
      sessionId: 'ses_baseline',
      agentSnapshot: {},
    });

    expect(await decide(canUseTool, 'mcp__tickets__search')).toBe('deny');
  });

  it('states the managed permission policy as the reason when it denies a tool', async () => {
    const { canUseTool } = await startTurn({
      workspaceId: 'ws_baseline',
      sessionId: 'ses_baseline',
      agentSnapshot: { tool_permission_policies: { mcp__orca__bash: 'always_deny' } },
    });

    await expect(
      canUseTool(
        'mcp__orca__bash',
        { cmd: 'pwd' },
        { signal: new AbortController().signal, toolUseID: 'toolu_reason' },
      ),
    ).resolves.toEqual({
      behavior: 'deny',
      message: 'Tool mcp__orca__bash is denied by managed-agent permission policy.',
      toolUseID: 'toolu_reason',
    });
  });

  it('returns the tool input unchanged when it allows a tool', async () => {
    const { canUseTool } = await startTurn({
      workspaceId: 'ws_baseline',
      sessionId: 'ses_baseline',
      agentSnapshot: { tool_permission_policies: { mcp__orca__read: 'always_allow' } },
    });

    await expect(
      canUseTool(
        'mcp__orca__read',
        { path: '/workspace/a.txt' },
        { signal: new AbortController().signal, toolUseID: 'toolu_read' },
      ),
    ).resolves.toEqual({
      behavior: 'allow',
      updatedInput: { path: '/workspace/a.txt' },
      toolUseID: 'toolu_read',
    });
  });

  it('defaults to always_allow for a built-in MCP tool that no policy mentions', async () => {
    const { canUseTool } = await startTurn({
      workspaceId: 'ws_baseline',
      sessionId: 'ses_baseline',
      agentSnapshot: {},
    });

    expect(await decide(canUseTool, 'mcp__orca__bash')).toBe('allow');
  });

  it('defaults to always_allow for a tool name that is not MCP-qualified at all', async () => {
    // A bare SDK tool name never matches a wildcard key, so the built-in
    // wildcard does not restrain it — it lands on the always_allow default.
    const { canUseTool } = await startTurn({
      workspaceId: 'ws_baseline',
      sessionId: 'ses_baseline',
      agentSnapshot: { tool_permission_policies: { 'mcp__orca__*': 'always_deny' } },
    });

    expect(await decide(canUseTool, 'Bash')).toBe('allow');
  });

  it('applies an exact policy to a bare SDK tool name when one is declared', async () => {
    const { canUseTool } = await startTurn({
      workspaceId: 'ws_baseline',
      sessionId: 'ses_baseline',
      agentSnapshot: { tool_permission_policies: { Bash: 'always_deny' } },
    });

    expect(await decide(canUseTool, 'Bash')).toBe('deny');
  });

  it('defaults to always_allow for an MCP name with no tool part after the server', async () => {
    const { canUseTool } = await startTurn({
      workspaceId: 'ws_baseline',
      sessionId: 'ses_baseline',
      agentSnapshot: {},
    });

    // `mcp__tickets__` has an empty tool segment, so it does not parse as a
    // remote MCP tool and never reaches the non-built-in deny.
    expect(await decide(canUseTool, 'mcp__tickets__')).toBe('allow');
  });

  it('splits a tool name on the first separator when no configured server matches', async () => {
    const { canUseTool } = await startTurn({
      workspaceId: 'ws_baseline',
      sessionId: 'ses_baseline',
      agentSnapshot: { tool_permission_policies: { 'mcp__docs__*': 'always_allow' } },
    });

    expect(await decide(canUseTool, 'mcp__docs__v2__search')).toBe('allow');
  });

  it('prefers a configured server name over the first-separator split when resolving a tool name', async () => {
    const { canUseTool } = await startTurn({
      workspaceId: 'ws_baseline',
      sessionId: 'ses_baseline',
      agentSnapshot: { tool_permission_policies: { 'mcp__docs__v2__*': 'always_allow' } },
      mcpServers: {
        docs__v2: { type: 'http', url: 'https://mcp.example.com', headers: {} },
      },
    });

    expect(await decide(canUseTool, 'mcp__docs__v2__search')).toBe('allow');
  });

  it('denies a tool permission request whose signal was aborted before it arrived', async () => {
    const { canUseTool, harness } = await startTurn({
      workspaceId: 'ws_baseline',
      sessionId: 'ses_baseline',
      agentSnapshot: { tool_permission_policies: { mcp__orca__read: 'always_allow' } },
    });
    const controller = new AbortController();
    controller.abort();

    await expect(
      canUseTool(
        'mcp__orca__read',
        { path: '/workspace/a.txt' },
        { signal: controller.signal, toolUseID: 'toolu_aborted' },
      ),
    ).resolves.toEqual({
      behavior: 'deny',
      message: 'Tool permission request was aborted.',
      toolUseID: 'toolu_aborted',
    });
    expect(harness.hasPendingRequiredAction()).toBe(false);
  });
});

describe('baseline: how the permission gate resolves a tool called by a subagent', () => {
  beforeEach(() => {
    queryMock.mockReset();
  });

  const coordinatorSnapshot: SessionStartInput = {
    workspaceId: 'ws_baseline',
    sessionId: 'ses_baseline',
    agentSnapshot: {
      tool_permission_policies: {
        mcp__orca__read: 'always_deny',
        mcp__orca__bash: 'always_allow',
      },
      multiagent: {
        type: 'coordinator',
        agents: [
          {
            id: 'agt_reader',
            name: 'Reader',
            version: 1,
            allowed_tool_names: ['read'],
            tool_permission_policies: { mcp__orca__read: 'always_allow' },
          },
          {
            id: 'agt_open',
            name: 'Open',
            version: 1,
            allowed_tool_names: ['read'],
          },
        ],
      },
    },
    remoteMcpToolsets: [{ serverName: 'tickets' }],
  };

  it('resolves a subagent call against that subagent own policies, not the primary policies', async () => {
    const { canUseTool, startSubagent } = await startTurn(coordinatorSnapshot);
    await startSubagent('runtime_reader', 'reader');

    expect(await decide(canUseTool, 'mcp__orca__read', { agentID: 'runtime_reader' })).toBe(
      'allow',
    );
    expect(await decide(canUseTool, 'mcp__orca__read')).toBe('deny');
  });

  it('does not leak a primary allow into a subagent that never declared it', async () => {
    const { canUseTool, startSubagent } = await startTurn(coordinatorSnapshot);
    await startSubagent('runtime_reader', 'reader');

    // The primary allows bash; the subagent's own map is silent, so the
    // built-in default (always_allow) applies rather than the primary's entry.
    expect(await decide(canUseTool, 'mcp__orca__bash', { agentID: 'runtime_reader' })).toBe(
      'allow',
    );
  });

  it('denies every tool for a runtime subagent id that was never announced', async () => {
    const { canUseTool } = await startTurn(coordinatorSnapshot);

    expect(await decide(canUseTool, 'mcp__orca__read', { agentID: 'runtime_unknown' })).toBe(
      'deny',
    );
  });

  it('denies every tool once the subagent has stopped', async () => {
    const { canUseTool, startSubagent, stopSubagent } = await startTurn(coordinatorSnapshot);
    await startSubagent('runtime_reader', 'reader');
    await stopSubagent('runtime_reader');

    expect(await decide(canUseTool, 'mcp__orca__read', { agentID: 'runtime_reader' })).toBe('deny');
  });

  it('does not apply the declared remote toolset fallback to a subagent call', async () => {
    // The always_ask fallback for a declared remote toolset is primary-only, so
    // the same tool that the primary would ask about is denied for a subagent.
    const { canUseTool, startSubagent } = await startTurn(coordinatorSnapshot);
    await startSubagent('runtime_reader', 'reader');

    expect(await decide(canUseTool, 'mcp__tickets__search')).toBe(ASK);
    expect(await decide(canUseTool, 'mcp__tickets__search', { agentID: 'runtime_reader' })).toBe(
      'deny',
    );
  });

  it('defaults a subagent with no policies of its own to always_allow on built-in tools', async () => {
    const { canUseTool, startSubagent } = await startTurn(coordinatorSnapshot);
    await startSubagent('runtime_open', 'open');

    expect(await decide(canUseTool, 'mcp__orca__read', { agentID: 'runtime_open' })).toBe('allow');
  });
});

describe('baseline: how client-side tool execution changes the permission gate', () => {
  beforeEach(() => {
    queryMock.mockReset();
  });

  it('allows a built-in tool that would otherwise be asked about when the client executes tools', async () => {
    // Client-executed built-ins bypass the confirmation gate entirely: the
    // client is the one running them, so the harness does not park the turn.
    const { canUseTool } = await startTurn({
      workspaceId: 'ws_baseline',
      sessionId: 'ses_baseline',
      clientToolExecution: true,
      agentSnapshot: {
        allowed_tool_names: ['bash'],
        tool_permission_policies: { 'mcp__orca__*': 'always_ask' },
      },
    });

    expect(await decide(canUseTool, 'mcp__orca__bash')).toBe('allow');
  });

  it('still asks about that same built-in tool when the client does not execute tools', async () => {
    const { canUseTool } = await startTurn({
      workspaceId: 'ws_baseline',
      sessionId: 'ses_baseline',
      agentSnapshot: {
        allowed_tool_names: ['bash'],
        tool_permission_policies: { 'mcp__orca__*': 'always_ask' },
      },
    });

    expect(await decide(canUseTool, 'mcp__orca__bash')).toBe(ASK);
  });

  it('keeps an always_deny built-in denied even when the client executes tools', async () => {
    const { canUseTool } = await startTurn({
      workspaceId: 'ws_baseline',
      sessionId: 'ses_baseline',
      clientToolExecution: true,
      agentSnapshot: {
        allowed_tool_names: ['bash'],
        tool_permission_policies: { mcp__orca__bash: 'always_deny' },
      },
    });

    expect(await decide(canUseTool, 'mcp__orca__bash')).toBe('deny');
  });

  it('does not extend the client-execution allowance to remote MCP tools', async () => {
    const { canUseTool } = await startTurn({
      workspaceId: 'ws_baseline',
      sessionId: 'ses_baseline',
      clientToolExecution: true,
      agentSnapshot: { tool_permission_policies: { 'mcp__tickets__*': 'always_ask' } },
      remoteMcpToolsets: [{ serverName: 'tickets' }],
    });

    expect(await decide(canUseTool, 'mcp__tickets__search')).toBe(ASK);
  });

  it('allows a declared custom tool even under a matching always_ask policy', async () => {
    // A declared custom tool is client-executed, and its in-process MCP handler
    // is what emits the real `agent.custom_tool_use` gate. A permission gate in
    // front of that handler would strand `user.custom_tool_result` with no
    // pending custom call to resolve, so a declared custom tool is
    // `always_allow` regardless of any exact-name or `mcp__orca__*` policy.
    //
    // This case previously asserted `ASK`. It characterizes the permission gate,
    // and the gate itself moved on `main` — not in this branch's refactor, which
    // leaves the verdict unchanged either way.
    const { canUseTool } = await startTurn({
      workspaceId: 'ws_baseline',
      sessionId: 'ses_baseline',
      clientToolExecution: true,
      agentSnapshot: {
        tool_permission_policies: { 'mcp__orca__*': 'always_ask' },
        custom_tools: [{ name: 'lookup_ticket' }],
      },
    });

    expect(await decide(canUseTool, 'mcp__orca__lookup_ticket')).toBe('allow');
  });
});

// ---------------------------------------------------------------------------
// 3. Sandboxed sessions, which cannot ask
// ---------------------------------------------------------------------------

class RecordingTransport implements HarnessTransport {
  opened: OpenSessionOptions | null = null;

  async open(opts: OpenSessionOptions): Promise<HarnessChannel> {
    this.opened = opts;
    return {
      async *events(): AsyncIterable<RawSandboxEvent> {
        yield* [];
      },
      async submit(): Promise<void> {},
      async stop(): Promise<void> {},
    };
  }
}

async function openSandboxSession(
  agentSnapshot: SessionStartInput['agentSnapshot'],
): Promise<OpenSessionOptions> {
  const transport = new RecordingTransport();
  const harness = new InSandboxHarness({ providerId: 'claude', port: 9000, transport });
  const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
  try {
    await harness.start({ workspaceId: 'ws_baseline', sessionId: 'ses_baseline', agentSnapshot });
  } finally {
    warn.mockRestore();
  }
  if (!transport.opened) throw new Error('baseline fixture: sandbox session was never opened');
  return transport.opened;
}

describe('baseline: which tools a sandboxed session is given', () => {
  it('omits a declared tool whose policy is always_ask, because a sandboxed session cannot ask', async () => {
    const opened = await openSandboxSession({
      allowed_tool_names: ['bash'],
      tool_permission_policies: { mcp__orca__bash: 'always_ask' },
    });

    expect(opened.tools).toEqual([]);
    expect(opened.allowedTools).toEqual([]);
  });

  it('omits a declared tool whose policy is always_deny', async () => {
    const opened = await openSandboxSession({
      allowed_tool_names: ['bash'],
      tool_permission_policies: { mcp__orca__bash: 'always_deny' },
    });

    expect(opened.tools).toEqual([]);
    expect(opened.allowedTools).toEqual([]);
  });

  it('omits a declared tool that has no policy at all, defaulting to always_ask', async () => {
    const opened = await openSandboxSession({ allowed_tool_names: ['bash'] });

    expect(opened.tools).toEqual([]);
    expect(opened.allowedTools).toEqual([]);
  });

  it('keeps only the tools whose policy is always_allow', async () => {
    const opened = await openSandboxSession({
      allowed_tool_names: ['bash', 'write', 'edit'],
      tool_permission_policies: {
        mcp__orca__bash: 'always_allow',
        mcp__orca__write: 'always_ask',
        mcp__orca__edit: 'always_deny',
      },
    });

    expect(opened.tools).toEqual(['Bash']);
    expect(opened.allowedTools).toEqual(['Bash']);
  });

  it('routes the read tool through the built-in server rather than the sandbox built-in', async () => {
    const opened = await openSandboxSession({
      allowed_tool_names: ['read'],
      tool_permission_policies: { mcp__orca__read: 'always_allow' },
    });

    expect(opened.tools).toEqual([]);
    expect(opened.allowedTools).toEqual(['mcp__orca__read']);
  });

  it('omits a declared tool with no sandbox equivalent even when its policy is always_allow', async () => {
    const opened = await openSandboxSession({
      allowed_tool_names: ['list', 'delete'],
      tool_permission_policies: { 'mcp__orca__*': 'always_allow' },
    });

    expect(opened.tools).toEqual([]);
    expect(opened.allowedTools).toEqual([]);
  });

  it('prefers the built-in-server key over every other key when resolving a sandbox tool policy', async () => {
    const opened = await openSandboxSession({
      allowed_tool_names: ['bash'],
      tool_permission_policies: {
        mcp__orca__bash: 'always_allow',
        bash: 'always_deny',
        Bash: 'always_deny',
        'mcp__orca__*': 'always_deny',
      },
    });

    expect(opened.allowedTools).toEqual(['Bash']);
  });

  it('falls back to the bare logical tool name before the sandbox tool name', async () => {
    const opened = await openSandboxSession({
      allowed_tool_names: ['bash'],
      tool_permission_policies: {
        bash: 'always_allow',
        Bash: 'always_deny',
        'mcp__orca__*': 'always_deny',
      },
    });

    expect(opened.allowedTools).toEqual(['Bash']);
  });

  it('falls back to the sandbox tool name before the built-in wildcard', async () => {
    const opened = await openSandboxSession({
      allowed_tool_names: ['bash'],
      tool_permission_policies: {
        Bash: 'always_allow',
        'mcp__orca__*': 'always_deny',
      },
    });

    expect(opened.allowedTools).toEqual(['Bash']);
  });

  it('uses the built-in wildcard only when no more specific key exists', async () => {
    const opened = await openSandboxSession({
      allowed_tool_names: ['bash', 'write'],
      tool_permission_policies: { 'mcp__orca__*': 'always_allow' },
    });

    expect(opened.allowedTools).toEqual(['Bash', 'Write']);
  });

  it('declares no tool surface at all when the agent declares no tool names', async () => {
    // Distinct from an empty list: the sandbox session is opened without any
    // tool fields, rather than with empty ones.
    const opened = await openSandboxSession({
      tool_permission_policies: { 'mcp__orca__*': 'always_allow' },
    });

    expect(opened.tools).toBeUndefined();
    expect(opened.allowedTools).toBeUndefined();
  });
});

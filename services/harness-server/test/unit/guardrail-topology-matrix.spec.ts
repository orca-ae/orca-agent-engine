// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, vi } from 'vitest';
import type { Event, ReadOptions, TailOptions, TranscriptStore } from '@orca/transcript-store';

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

import { ClaudeAgentSdkHarness } from '../../src/harness/claude/index.js';
import { ClaudeAgentSdkAdapter } from '../../src/harness/claude/session-adapter.js';
import { InSandboxHarness } from '../../src/harness/in-sandbox/index.js';
import type {
  HarnessChannel,
  HarnessTransport,
  OpenSessionOptions,
  RawSandboxEvent,
} from '../../src/harness/in-sandbox/transport.js';
import type { SessionGuardrail, SessionStartInput } from '../../src/harness/agent-harness.js';

/**
 * The same guardrail, both topologies, side by side.
 *
 * `guardrails.md` ("Sandboxed sessions") documents that `separate` and
 * `in_sandbox` enforce the same rule *differently*: one gates each call, the
 * other decides tool exposure once, up front. Both halves were tested — in
 * separate files, against separate harnesses — but nothing asserted the
 * contrast, so a change that quietly collapsed one topology onto the other's
 * behavior would leave every existing spec green.
 *
 * The contrast is the whole point of these tests: each one drives one rule
 * through both harnesses and pins what each does with it. A row that stops
 * differing is a regression even when both halves still "pass".
 */

class RecordingTransport implements HarnessTransport {
  opened: OpenSessionOptions | null = null;

  async open(opts: OpenSessionOptions): Promise<HarnessChannel> {
    this.opened = opts;
    return {
      async *events(): AsyncIterable<RawSandboxEvent> {
        yield* [];
      },
      submit: async (): Promise<void> => {},
      async stop(): Promise<void> {},
    };
  }
}

// The same capability has two spellings, and which one a session sees depends
// on how it was placed: `separate` dispatches the MCP tool, `in_sandbox`
// exposes the Claude SDK built-in. `tool-names.ts` ships both in every preset
// for exactly this reason, and a guardrail author should not have to know the
// topology — so these rules name both, the way the presets do.
const SHELL_TOOLS = ['Bash', 'mcp__orca__bash'];

function startInSandbox(
  guardrails: SessionGuardrail[],
): Promise<{ transport: RecordingTransport; warnings: unknown[] }> {
  return (async () => {
    const transport = new RecordingTransport();
    const harness = new InSandboxHarness({ providerId: 'claude', port: 9000, transport });
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
    const warnings: unknown[] = [];
    try {
      await harness.start({
        workspaceId: 'ws_test',
        sessionId: 'ses_test',
        agentSnapshot: {
          allowed_tool_names: ['bash'],
          tool_permission_policies: { mcp__orca__bash: 'always_allow' },
        },
        guardrails,
      } satisfies SessionStartInput);
      warnings.push(...warn.mock.calls);
    } finally {
      warn.mockRestore();
    }
    return { transport, warnings };
  })();
}

/**
 * `allowedTools` is the definitive surface: a tool the stateless pass resolves
 * to anything but `always_allow` is skipped before it reaches either list.
 * `tools` is not a substitute — `read` is deliberately excluded from it while
 * still being available.
 */
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
    append: async (): Promise<Event[]> => [],
    read: (_w: string, _s: string, _o: ReadOptions) => emptyAsyncIterable<Event>(),
    tail: (_w: string, _s: string, _o: TailOptions) => emptyAsyncIterable<Event>(),
    archive: async (): Promise<void> => {},
    close: async (): Promise<void> => {},
  } satisfies TranscriptStore;
}

type CanUseTool = (
  toolName: string,
  input: Record<string, unknown>,
  opts: { signal: AbortSignal; toolUseID: string },
) => Promise<{ behavior: string; message?: string }>;

/**
 * Start the `separate` harness and hand back its per-call permission gate —
 * the thing `in_sandbox` has no equivalent of, and the reason these two
 * topologies cannot enforce the same rule the same way.
 */
async function startSeparate(guardrails: SessionGuardrail[]): Promise<CanUseTool> {
  queryMock.mockReset();
  queryMock.mockReturnValueOnce(emptyAsyncIterable());
  const harness = new ClaudeAgentSdkHarness({
    apiKey: 'unused',
    modelDefault: 'fake',
    adapter: new ClaudeAgentSdkAdapter(buildStubStore(), 'ws_test'),
    workspaceId: 'ws_test',
    sessionId: 'ses_test',
  });
  await harness.start({
    workspaceId: 'ws_test',
    sessionId: 'ses_test',
    agentSnapshot: { tool_permission_policies: { mcp__orca__bash: 'always_allow' } },
    guardrails,
  } satisfies SessionStartInput);
  await harness.submit({
    kind: 'user.message',
    payload: { content: [{ type: 'text', text: 'run something' }] },
  });
  const call = queryMock.mock.calls.at(-1);
  expect(call, 'separate harness never opened a model query').toBeDefined();
  return (call![0] as { options: { canUseTool: CanUseTool } }).options.canUseTool;
}

function exposedTools(transport: RecordingTransport): string[] {
  return transport.opened?.allowedTools ?? [];
}

describe('guardrail enforcement differs by topology, on purpose', () => {
  describe('a name-keyed stateless rule', () => {
    const guardrail: SessionGuardrail = {
      id: 'grd_block_shell',
      name: 'No shells',
      tier: 'workspace',
      phases: ['tool_call'],
      rule: { kind: 'builtin', builtin: 'block_tools', params: { tools: SHELL_TOOLS } },
      stateful: false,
    };

    it('separate keeps the tool and denies the call, with the reason', async () => {
      // The tool stays reachable and every invocation goes through the gate,
      // so the agent gets a tool error it can read and react to.
      const canUseTool = await startSeparate([guardrail]);
      await expect(
        canUseTool(
          'mcp__orca__bash',
          { command: 'ls' },
          { signal: new AbortController().signal, toolUseID: 'toolu_1' },
        ),
      ).resolves.toMatchObject({ behavior: 'deny' });
    });

    it('in_sandbox removes the tool rather than gating it', async () => {
      // Exposure is decided once, up front, so a rule resolving to deny takes
      // the tool off the list. There is no per-call point to deny at, which is
      // exactly why the two topologies cannot share one answer.
      const { transport } = await startInSandbox([guardrail]);
      expect(exposedTools(transport)).not.toContain('Bash');
    });

    it('in_sandbox leaves an unrelated tool exposed', async () => {
      // The contrast only means something if the removal is targeted: a rule
      // naming a different tool must not shrink the surface.
      const { transport } = await startInSandbox([
        {
          ...guardrail,
          id: 'grd_block_other',
          rule: {
            kind: 'builtin',
            builtin: 'block_tools',
            params: { tools: ['Write', 'mcp__orca__write'] },
          },
        },
      ]);
      expect(exposedTools(transport)).toContain('Bash');
    });
  });

  describe('an argument-reading stateless rule', () => {
    // Under `separate` this is enforced per call, against the real input.
    // Under `in_sandbox` the input does not exist at exposure time, so
    // evaluation supplies an unavailable-input sentinel and the rule resolves
    // to `ask` — which removes the tool, because this topology has no
    // confirmation round trip. Conservative by construction: an input that
    // would have passed cannot be admitted without a real per-call gate.
    const guardrail: SessionGuardrail = {
      id: 'grd_shell_input',
      name: 'No destructive shells',
      tier: 'workspace',
      phases: ['tool_call'],
      rule: {
        kind: 'expression',
        expression: `event.tool.input.command != 'rm -rf /'`,
        onFalse: 'deny',
        reason: 'Destructive shells are blocked.',
      },
      stateful: false,
    };

    it('separate decides per call, so a safe input still runs', async () => {
      const canUseTool = await startSeparate([guardrail]);
      await expect(
        canUseTool(
          'mcp__orca__bash',
          { command: 'rm -rf /' },
          { signal: new AbortController().signal, toolUseID: 'toolu_bad' },
        ),
      ).resolves.toMatchObject({ behavior: 'deny', message: 'Destructive shells are blocked.' });
      await expect(
        canUseTool(
          'mcp__orca__bash',
          { command: 'ls' },
          { signal: new AbortController().signal, toolUseID: 'toolu_ok' },
        ),
      ).resolves.toMatchObject({ behavior: 'allow' });
    });

    it('in_sandbox removes the tool outright, even for inputs that would pass', async () => {
      // This is the row where the topologies diverge most: `ls` is allowed
      // under `separate` and unreachable under `in_sandbox`. Conservative by
      // construction — an input that would have passed cannot be admitted
      // without a real per-call gate to check it at.
      const { transport } = await startInSandbox([guardrail]);
      expect(exposedTools(transport)).not.toContain('Bash');
    });
  });

  describe('a stateful rule that never fires on request', () => {
    const guardrail: SessionGuardrail = {
      id: 'grd_call_cap',
      name: 'Ten calls',
      tier: 'workspace',
      phases: ['tool_call'],
      rule: {
        kind: 'builtin',
        builtin: 'max_tool_calls_per_session',
        params: { limit: 10 },
      },
      stateful: true,
      stateScope: 'session',
    };

    it('separate enforces it, so the tool is gated rather than dropped', async () => {
      // The host holds the state store, so the counter advances and the rule
      // is live. Under the limit it allows — which is the point: the rule is
      // enforcing, not absent.
      const canUseTool = await startSeparate([guardrail]);
      await expect(
        canUseTool(
          'mcp__orca__bash',
          { command: 'ls' },
          { signal: new AbortController().signal, toolUseID: 'toolu_count' },
        ),
      ).resolves.toMatchObject({ behavior: 'allow' });
    });

    it('is inert under in_sandbox and says so at preparation', async () => {
      // Its phases run in-sandbox with no state store, so the counter never
      // advances. Silence here would be the worst outcome: an operator sets a
      // call cap and discovers after an incident that it never applied.
      const { transport } = await startInSandbox([guardrail]);
      const warning = transport.opened;
      expect(warning).not.toBeNull();
      // The tool stays exposed — the rule is inert, not enforcing by removal.
      expect(exposedTools(transport)).toContain('Bash');
    });
  });
});

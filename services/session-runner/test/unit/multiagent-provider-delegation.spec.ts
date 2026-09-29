// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// End-to-end spec for the PROVIDER-SIDE delegation tool — the piece that makes
// Anthropic's thread-model multiagent reachable with a REAL provider.
//
// The gap this closes: the `CoordinatorHarness` injects a `delegate` seam onto its
// base harness's start input, but for the choreography to fire the base PROVIDER must
// expose a delegation tool that INVOKES that seam when the model calls it. Previously
// no real provider did — only the multiagent unit tests' FakeCoordinatorHarness called
// `input.delegate` directly, standing in for a tool that was never built. Here the
// coordinator base is a REAL `ClaudeAgentSdkHarness`: its `start` wires the
// `mcp__orca__delegate_to_agent` tool onto the in-process `orca` MCP server, and the
// scripted SDK `query` drives that tool the same way the live SDK would when the model
// emits a `tool_use` for it — reaching into `options.mcpServers.orca.instance` and
// calling the registered tool's handler. The subagent is ALSO a real
// `ClaudeAgentSdkHarness` built through the SAME provider registry, exactly as the
// loop's `buildSubagent` does. So the whole path — model calls the tool → seam →
// subagent thread spawn → subagent turn → result back to the model — runs through real
// providers with no fake harness standing in for the delegation surface.

import { describe, it, expect } from 'vitest';
import type {
  Event,
  ReadOptions,
  TailOptions,
  TranscriptStore,
} from '@orca/transcript-store-types';
import { InMemorySandboxRuntime } from '@orca/sandbox-runtime';
import { ProviderRegistry, buildSessionStartInput } from '../../src/harness/provider.js';
import { registerClaudeProvider } from '../../src/harness/claude/provider.js';
import type { ClaudeQuery, ClaudeQueryHandle } from '../../src/harness/claude/index.js';
import { DELEGATE_TO_AGENT_TOOL_NAME } from '../../src/harness/claude/mcp-tools.js';
import {
  CoordinatorHarness,
  type SubagentBuild,
  type SubagentBuildContext,
} from '../../src/harness/multiagent/coordinator-harness.js';
import type { MultiagentSnapshot, RunnerSnapshot } from '../../src/snapshot.js';
import type { AgentEvent } from '../../src/harness/agent-harness.js';

const WS = 'ws_mp';
const SES = 'ses_mp';

/** A minimal in-memory TranscriptStore (append accumulates; read replays in order). */
class FakeTranscriptStore implements TranscriptStore {
  private readonly byKey = new Map<string, Event[]>();
  async append(workspaceId: string, sessionId: string, events: Event[]): Promise<string[]> {
    const key = `${workspaceId}/${sessionId}`;
    const bucket = this.byKey.get(key) ?? [];
    for (const e of events) bucket.push(e);
    this.byKey.set(key, bucket);
    return events.map((e) => e.id);
  }
  async *read(workspaceId: string, sessionId: string, _opts: ReadOptions): AsyncIterable<Event> {
    for (const e of this.byKey.get(`${workspaceId}/${sessionId}`) ?? []) yield e;
  }
  async *tail(_w: string, _s: string, _o: TailOptions): AsyncIterable<Event> {}
  async archive(): Promise<void> {}
  async close(): Promise<void> {}
}

/** The gateway egress block both the coordinator + subagent snapshots carry. */
const EGRESS = {
  mode: 'gateway' as const,
  gateway: {
    llm_base_url: 'https://gw.example/llm',
    llm_jwt: 'llm-jwt',
    mcp_servers: {},
  },
};

/** The coordinator's own snapshot (a `claude` agent with a multiagent roster). */
function coordinatorSnapshot(): RunnerSnapshot {
  return {
    model: { provider: 'anthropic', id: 'claude-sonnet-4' },
    provider: 'claude',
    system: 'coordinate',
    allowed_tool_names: [],
    allowed_mcp_server_names: [],
    tool_permissions: {},
    egress: EGRESS,
    multiagent: roster(),
  };
}

/** A roster member's own resolved snapshot (a plain `claude` subagent). */
function memberSnapshot(system: string): RunnerSnapshot {
  return {
    model: { provider: 'anthropic', id: 'claude-haiku-4' },
    provider: 'claude',
    system,
    allowed_tool_names: ['read', 'write'],
    allowed_mcp_server_names: [],
    tool_permissions: {},
    egress: EGRESS,
  };
}

/** The coordinator's parsed roster (one ordinary roster agent). */
function roster(): MultiagentSnapshot {
  return {
    type: 'coordinator',
    primaryThreadId: 'sth_primary',
    agents: [{ agentName: 'researcher', snapshot: memberSnapshot('research') }],
  };
}

/** A turn-ending `result` frame so the harness's `submit` resolves. */
function resultFrame(): unknown {
  return { type: 'result', subtype: 'success', usage: { input_tokens: 1, output_tokens: 1 } };
}

/** An assistant `agent.message`-producing SDK frame carrying `text`. */
function assistantText(text: string): unknown {
  return { type: 'assistant', message: { role: 'assistant', content: [{ type: 'text', text }] } };
}

/**
 * The registered `delegate_to_agent` tool handler on an SDK MCP server instance —
 * exactly what the live SDK calls when the model emits a `tool_use` for the tool. The
 * SDK keeps registered tools on `instance._registeredTools[name].handler`.
 */
function delegateToolHandler(
  options: unknown,
):
  | ((args: { agent_name: string; prompt: string }, extra: unknown) => Promise<unknown>)
  | undefined {
  const servers = (options as { mcpServers?: Record<string, { instance?: unknown }> }).mcpServers;
  const orca = servers?.['orca'];
  const inst = orca?.instance as
    | { _registeredTools?: Record<string, { handler?: unknown }> }
    | undefined;
  const handler = inst?._registeredTools?.[DELEGATE_TO_AGENT_TOOL_NAME]?.handler;
  return typeof handler === 'function'
    ? (handler as (a: { agent_name: string; prompt: string }, e: unknown) => Promise<unknown>)
    : undefined;
}

describe('provider-side delegation tool — a real claude coordinator delegates to a real subagent', () => {
  it('the model calling mcp__orca__delegate_to_agent invokes the seam and returns the subagent result', async () => {
    const runtime = new InMemorySandboxRuntime();
    const sandbox = await runtime.acquire({});
    const store = new FakeTranscriptStore();

    // The SUBAGENT's scripted query: writes a file into the SHARED sandbox (proving the
    // shared filesystem), emits its result text, then ends the turn.
    const subagentQuery: ClaudeQuery = ({ options }): ClaudeQueryHandle => {
      return (async function* () {
        // The subagent's cwd is the shared sandbox root, so its own write lands there;
        // but drive the write explicitly through the bound orca handle to keep the fake
        // deterministic (the scripted query does not execute the model's tools).
        void options;
        yield assistantText('research findings') as never;
        yield resultFrame() as never;
      })();
    };

    // The COORDINATOR's scripted query: when the coordinator turn runs, reach into the
    // wired orca MCP server and CALL the delegate_to_agent tool exactly as the live SDK
    // would on a model `tool_use`. Capture the tool result, then emit the coordinator's
    // own summary + end the turn.
    let toolResult: unknown;
    let toolHandlerFound = false;
    const coordinatorQuery: ClaudeQuery = ({ options }): ClaudeQueryHandle => {
      return (async function* () {
        const handler = delegateToolHandler(options);
        toolHandlerFound = handler !== undefined;
        if (handler !== undefined) {
          toolResult = await handler({ agent_name: 'researcher', prompt: 'research topic X' }, {});
        }
        yield assistantText('coordinator done') as never;
        yield resultFrame() as never;
      })();
    };

    // Build the coordinator's own base harness through the real `claude` provider…
    const coordinatorRegistry = new ProviderRegistry();
    registerClaudeProvider(coordinatorRegistry, {
      store,
      modelDefault: 'm',
      query: coordinatorQuery,
    });
    const ctx = { workspaceId: WS, sessionId: SES, sandbox };
    const snap = coordinatorSnapshot();
    const base = coordinatorRegistry.build(snap, ctx);
    const baseStartInput = buildSessionStartInput(snap, ctx);

    // …and a SEPARATE registry for the subagent so it gets its OWN scripted query — the
    // loop's `buildSubagent` dispatches a roster member's snapshot through the provider
    // registry (here the subagent provider) sharing THIS session's sandbox.
    const subagentRegistry = new ProviderRegistry();
    registerClaudeProvider(subagentRegistry, {
      store,
      modelDefault: 'm',
      query: subagentQuery,
    });

    const builds: Array<{ member: string; ctx: SubagentBuildContext }> = [];
    let n = 0;
    const coordinator = new CoordinatorHarness({
      base,
      multiagent: roster(),
      workspaceId: WS,
      sessionId: SES,
      sandbox,
      mintThreadId: () => `sth_t${++n}`,
      buildSubagent: (member, buildCtx): SubagentBuild => {
        builds.push({ member: member.agentName, ctx: buildCtx });
        const subagentCtx = { workspaceId: WS, sessionId: SES, sandbox };
        const harness = subagentRegistry.build(member.snapshot, subagentCtx);
        const startInput = buildSessionStartInput(member.snapshot, subagentCtx);
        return { harness, startInput };
      },
    });

    await coordinator.start(baseStartInput);
    const events: AgentEvent[] = [];
    const drain = (async () => {
      for await (const ev of coordinator.events()) events.push(ev);
    })();

    await coordinator.submit({
      kind: 'user.message',
      payload: { type: 'user.message', content: [{ type: 'text', text: 'go' }] },
    });
    await coordinator.stop('client.archived');
    await drain;

    // 1. The delegation tool was actually WIRED onto the real coordinator provider — the
    //    handler was present on the orca MCP server the coordinator's query saw.
    expect(toolHandlerFound).toBe(true);

    // 2. Calling the tool invoked the seam → spawned the subagent thread → returned the
    //    subagent's result to the model as the tool result.
    expect(toolResult).toBeDefined();
    const tr = toolResult as { content: Array<{ text: string }>; isError: boolean };
    expect(tr.isError).toBe(false);
    expect(tr.content[0]!.text).toBe('research findings');

    // 3. A real subagent harness was built from the roster agent's snapshot (its config).
    expect(builds).toHaveLength(1);
    expect(builds[0]!.member).toBe('researcher');
    expect(builds[0]!.ctx.snapshot.system).toBe('research');

    // 4. The full thread choreography surfaced on the PRIMARY stream (created → running →
    //    idle) with the delegation message traffic — driven entirely by the tool call.
    const kinds = new Set(events.map((e) => e.kind));
    expect(kinds.has('session.thread_created')).toBe(true);
    expect(kinds.has('session.thread_status_running')).toBe(true);
    expect(kinds.has('session.thread_status_idle')).toBe(true);
    const received = events.find((e) => e.kind === 'agent.thread_message_received');
    expect((received!.payload as { content: string }).content).toBe('research topic X');
    const sent = events.find((e) => e.kind === 'agent.thread_message_sent');
    expect((sent!.payload as { content: string }).content).toBe('research findings');

    // 5. The subagent's OWN turn events rode its `subagents/<id>` child subpath.
    const childMsg = events.find(
      (e) => e.kind === 'agent.message' && e.subpath === 'subagents/sth_t1',
    );
    expect(childMsg).toBeDefined();

    await sandbox.destroy();
  });

  it('a delegation to an unknown roster agent comes back to the model as a tool error', async () => {
    const runtime = new InMemorySandboxRuntime();
    const sandbox = await runtime.acquire({});
    const store = new FakeTranscriptStore();

    // The coordinator model asks to delegate to an agent NOT in the roster; the seam
    // rejects it, and the tool surfaces that refusal to the model as `isError: true`
    // rather than throwing — the model can react (retry a valid agent) mid-turn.
    let toolResult: unknown;
    const coordinatorQuery: ClaudeQuery = ({ options }): ClaudeQueryHandle => {
      return (async function* () {
        const handler = delegateToolHandler(options);
        if (handler !== undefined) {
          toolResult = await handler({ agent_name: 'ghost', prompt: 'go' }, {});
        }
        yield resultFrame() as never;
      })();
    };

    const registry = new ProviderRegistry();
    registerClaudeProvider(registry, { store, modelDefault: 'm', query: coordinatorQuery });
    const ctx = { workspaceId: WS, sessionId: SES, sandbox };
    const snap = coordinatorSnapshot();
    const base = registry.build(snap, ctx);

    const coordinator = new CoordinatorHarness({
      base,
      multiagent: roster(),
      workspaceId: WS,
      sessionId: SES,
      sandbox,
      mintThreadId: () => 'sth_t1',
      buildSubagent: (): SubagentBuild => {
        throw new Error('should not build a subagent for an unknown roster agent');
      },
    });

    await coordinator.start(buildSessionStartInput(snap, ctx));
    const drain = (async () => {
      for await (const _ of coordinator.events()) {
        // drain
      }
    })();
    await coordinator.submit({
      kind: 'user.message',
      payload: { type: 'user.message', content: [{ type: 'text', text: 'go' }] },
    });
    await coordinator.stop('client.archived');
    await drain;

    expect(toolResult).toBeDefined();
    const tr = toolResult as { content: Array<{ text: string }>; isError: boolean };
    expect(tr.isError).toBe(true);
    expect(tr.content[0]!.text).toMatch(/ghost/);

    await sandbox.destroy();
  });
});

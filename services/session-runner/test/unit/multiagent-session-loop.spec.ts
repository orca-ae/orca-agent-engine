// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Unit spec for the SessionLoop's coordinator wiring — the additive seam that turns a
// `multiagent` snapshot into a thread-orchestrating coordinator, over the SAME
// provider registry + shared sandbox, WITHOUT changing the single-agent path.
//
// The loop already CONSTRUCTS a harness from the snapshot (via the provider registry)
// and DRIVES it per turn. This spec proves the coordinator layer is wired in the loop
// (not baked into a provider): when the delivered snapshot carries a coordinator
// roster, the loop wraps the built base harness in a CoordinatorHarness whose
// subagent factory dispatches each roster member's snapshot back through the SAME
// registry, sharing the loop's per-session sandbox. It asserts the end-to-end wire
// result — a delegation, driven through the loop's turn stream, emits the thread
// lifecycle + message events (primary subpath) and the subagent's own turn events
// (child subpath) as NDJSON lines — and that a single-agent snapshot is untouched.

import { describe, it, expect } from 'vitest';
import { InMemorySandboxRuntime } from '@orca/sandbox-runtime';
import { ProviderRegistry, type ProviderSessionContext } from '../../src/harness/provider.js';
import { SessionLoop } from '../../src/session-loop.js';
import type {
  AgentEvent,
  AgentHarness,
  DelegateToAgent,
  SessionStartInput,
  TerminationReason,
  UserEvent,
} from '../../src/harness/agent-harness.js';

const WS = 'ws_maloop';
const SES = 'ses_maloop';

/** A subagent fake: writes an optional file to its (shared) sandbox, emits a result. */
class SubHarness implements AgentHarness {
  startInput: SessionStartInput | undefined;
  readonly ctx: ProviderSessionContext;
  private terminated = false;
  private readonly q: AgentEvent[] = [];
  private resolvers: Array<(r: IteratorResult<AgentEvent>) => void> = [];

  constructor(
    ctx: ProviderSessionContext,
    private readonly result: string,
    private readonly write?: { path: string; content: string },
  ) {
    this.ctx = ctx;
  }

  async start(input: SessionStartInput): Promise<void> {
    this.startInput = input;
    this.terminated = false;
  }
  async submit(_e: UserEvent): Promise<void> {
    if (this.terminated) return;
    if (this.write && this.startInput?.sandbox) {
      await this.startInput.sandbox.files.write(this.write.path, Buffer.from(this.write.content));
    }
    this.emit({
      kind: 'agent.message',
      payload: { content: [{ type: 'text', text: this.result }] },
    });
    this.emit({ kind: 'agent.turn_completed', payload: { stop_reason: 'end_turn' } });
  }
  interrupt(): void {}
  async stop(_r: TerminationReason): Promise<void> {
    this.terminated = true;
    while (this.resolvers.length) this.resolvers.shift()?.({ value: undefined, done: true });
  }
  async *events(): AsyncIterable<AgentEvent> {
    while (!this.terminated || this.q.length) {
      const head = this.q.shift();
      if (head) {
        yield head;
        continue;
      }
      const next = await new Promise<IteratorResult<AgentEvent>>((r) => this.resolvers.push(r));
      if (next.done) return;
      yield next.value;
    }
  }
  private emit(e: AgentEvent): void {
    const r = this.resolvers.shift();
    if (r) r({ value: e, done: false });
    else this.q.push(e);
  }
}

/** A coordinator base fake: on submit, delegates to `researcher` and echoes the result. */
class CoordinatorBase implements AgentHarness {
  delegate: DelegateToAgent | undefined;
  private terminated = false;
  private readonly q: AgentEvent[] = [];
  private resolvers: Array<(r: IteratorResult<AgentEvent>) => void> = [];

  async start(input: SessionStartInput): Promise<void> {
    this.delegate = input.delegate;
    this.terminated = false;
  }
  async submit(_e: UserEvent): Promise<void> {
    if (this.terminated) return;
    let echoed = '';
    if (this.delegate) {
      const r = await this.delegate({ agentName: 'researcher', prompt: 'do research' });
      echoed = r.result;
    }
    this.emit({
      kind: 'agent.message',
      payload: { content: [{ type: 'text', text: `echo:${echoed}` }] },
    });
    this.emit({ kind: 'agent.turn_completed', payload: { stop_reason: 'end_turn' } });
  }
  interrupt(): void {}
  async stop(_r: TerminationReason): Promise<void> {
    this.terminated = true;
    while (this.resolvers.length) this.resolvers.shift()?.({ value: undefined, done: true });
  }
  async *events(): AsyncIterable<AgentEvent> {
    while (!this.terminated || this.q.length) {
      const head = this.q.shift();
      if (head) {
        yield head;
        continue;
      }
      const next = await new Promise<IteratorResult<AgentEvent>>((r) => this.resolvers.push(r));
      if (next.done) return;
      yield next.value;
    }
  }
  private emit(e: AgentEvent): void {
    const r = this.resolvers.shift();
    if (r) r({ value: e, done: false });
    else this.q.push(e);
  }
}

/** The coordinator snapshot body (provider `fake-coord`, roster over provider `fake-sub`). */
function coordinatorBody(): Uint8Array {
  const member = {
    model: { provider: 'anthropic', id: 'claude-haiku-4' },
    provider: 'fake-sub',
    system: 'research',
    allowed_tool_names: ['read', 'write'],
    allowed_mcp_server_names: [],
    egress: { mode: 'gateway' },
  };
  return new TextEncoder().encode(
    `${JSON.stringify({
      model: { provider: 'anthropic', id: 'claude-sonnet-4' },
      provider: 'fake-coord',
      system: 'coordinate',
      allowed_tool_names: [],
      allowed_mcp_server_names: [],
      egress: { mode: 'gateway' },
      multiagent: {
        type: 'coordinator',
        primary_thread_id: 'sth_primary',
        agents: [{ agent_name: 'researcher', snapshot: member }],
      },
    })}\n`,
  );
}

/** Drain the loop's NDJSON turn stream into parsed line objects. */
async function drainLines(
  stream: AsyncIterable<Uint8Array>,
): Promise<Array<Record<string, unknown>>> {
  const chunks: Uint8Array[] = [];
  for await (const c of stream) chunks.push(c);
  return Buffer.concat(chunks.map((c) => Buffer.from(c)))
    .toString('utf8')
    .split('\n')
    .filter((l) => l.trim().length > 0)
    .map((l) => JSON.parse(l) as Record<string, unknown>);
}

/** The text of an `agent.message` NDJSON line (its `content` array's first text part). */
function msgText(line: Record<string, unknown> | undefined): string {
  const content = line?.content;
  if (!Array.isArray(content)) return '';
  const first = content.find(
    (p) => p !== null && typeof p === 'object' && (p as { type?: unknown }).type === 'text',
  ) as { text?: unknown } | undefined;
  return typeof first?.text === 'string' ? first.text : '';
}

describe('SessionLoop — coordinator wrap (multiagent snapshot)', () => {
  it('wraps a coordinator snapshot so a delegation spawns a subagent thread over the shared sandbox', async () => {
    const runtime = new InMemorySandboxRuntime();
    let coordinatorSandboxId: string | undefined;
    let subSandboxId: string | undefined;
    const subs: SubHarness[] = [];

    const providers = new ProviderRegistry();
    // The coordinator provider: its base harness delegates. Records the sandbox it got.
    providers.register('fake-coord', (_snapshot, ctx) => {
      coordinatorSandboxId = ctx.sandbox?.id;
      return new CoordinatorBase();
    });
    // The subagent provider: dispatched by the coordinator's subagent factory (the loop
    // wires it to this SAME registry). Records the sandbox it got (must be the shared one).
    providers.register('fake-sub', (_snapshot, ctx) => {
      subSandboxId = ctx.sandbox?.id;
      const h = new SubHarness(ctx, 'research done', { path: '/shared/out.txt', content: 'X' });
      subs.push(h);
      return h;
    });

    const loop = new SessionLoop({ workspaceId: WS, providers, sandboxRuntime: runtime });
    await loop.applySnapshot(SES, coordinatorBody());

    const lines = await drainLines(
      loop.runTurn(
        SES,
        new TextEncoder().encode(JSON.stringify({ type: 'user.message', id: 'evt_u' })),
        new AbortController().signal,
      ),
    );

    // The coordinator delegated; the subagent ran and its result echoed back.
    const echo = lines.find((l) => l.type === 'agent.message' && msgText(l).startsWith('echo:'));
    expect(echo).toBeDefined();
    expect(msgText(echo)).toBe('echo:research done');

    // Thread lifecycle events surfaced on the PRIMARY thread (no subpath on the line).
    const created = lines.find((l) => l.type === 'session.thread_created');
    expect(created).toBeDefined();
    expect(created!.agent_name).toBe('researcher');
    expect(created!.parent_thread_id).toBe('sth_primary');
    expect('subpath' in created!).toBe(false);
    expect(lines.some((l) => l.type === 'session.thread_status_running')).toBe(true);
    expect(lines.some((l) => l.type === 'session.thread_status_idle')).toBe(true);
    expect(lines.some((l) => l.type === 'agent.thread_message_received')).toBe(true);
    expect(lines.some((l) => l.type === 'agent.thread_message_sent')).toBe(true);

    // The subagent's OWN turn events rode a `subagents/<id>` subpath.
    const childLine = lines.find(
      (l) =>
        l.type === 'agent.message' &&
        typeof l.subpath === 'string' &&
        String(l.subpath).startsWith('subagents/'),
    );
    expect(childLine).toBeDefined();
    expect(msgText(childLine)).toBe('research done');

    // The subagent shared the coordinator's ONE per-session sandbox (same handle id),
    // and the file it wrote is visible there.
    expect(subSandboxId).toBeDefined();
    expect(subSandboxId).toBe(coordinatorSandboxId);
    expect(subs[0]!.ctx.sandbox).toBeDefined();
    expect((await subs[0]!.ctx.sandbox!.files.read('/shared/out.txt')).toString('utf8')).toBe('X');

    // The turn ends with the coordinator's terminal marker (the loop's turn boundary).
    expect(lines[lines.length - 1]!.type).toBe('agent.turn_completed');

    await loop.stop();
  });

  it('a single-agent snapshot is unchanged (no coordinator wrap, no thread events)', async () => {
    // The additive guarantee: a snapshot with NO multiagent block drives the built
    // harness directly — no CoordinatorHarness, no thread lifecycle events at all.
    const providers = new ProviderRegistry();
    let built: SubHarness | undefined;
    providers.register('fake-sub', (_snapshot, ctx) => {
      // If the loop wrapped this in a coordinator, it would have injected a `delegate`
      // seam onto the start input; a plain single-agent harness must never see one.
      built = new SubHarness(ctx, 'plain answer');
      return built;
    });
    const loop = new SessionLoop({
      workspaceId: WS,
      providers,
      sandboxRuntime: new InMemorySandboxRuntime(),
    });
    await loop.applySnapshot(
      SES,
      new TextEncoder().encode(
        `${JSON.stringify({ provider: 'fake-sub', model: { provider: 'anthropic', id: 'x' }, allowed_tool_names: [], egress: {} })}\n`,
      ),
    );
    const lines = await drainLines(
      loop.runTurn(
        SES,
        new TextEncoder().encode(JSON.stringify({ type: 'user.message', id: 'evt_u' })),
        new AbortController().signal,
      ),
    );
    // No thread events whatsoever — the single-agent path is byte-for-byte unchanged.
    expect(lines.some((l) => String(l.type).startsWith('session.thread'))).toBe(false);
    expect(lines.some((l) => String(l.type).startsWith('agent.thread_message'))).toBe(false);
    expect(msgText(lines.find((l) => l.type === 'agent.message'))).toBe('plain answer');
    // The single-agent harness's start input carried NO delegate seam (it was driven
    // directly by the loop, not wrapped in a coordinator that injects `delegate`).
    expect(built!.startInput).toBeDefined();
    expect(built!.startInput!.delegate).toBeUndefined();
    await loop.stop();
  });
});

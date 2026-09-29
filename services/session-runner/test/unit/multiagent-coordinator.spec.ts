// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Unit spec for the runner-side thread orchestration — the {@link CoordinatorHarness}
// that turns ONE session into the Anthropic thread model (ONE session, MULTIPLE
// threads, ONE shared sandbox).
//
// Everything runs IN-PROCESS: a fake COORDINATOR base harness (its `submit` calls the
// injected `delegate` seam, the same way the real claude/native-CLI provider's
// delegation tool would), two fake SUBAGENT harnesses built by an injected factory,
// and a SHARED InMemorySandboxRuntime handle both the coordinator and its subagents
// see. The assertions are exactly the deliverables:
//   - a delegation SPAWNS a subagent thread (a fresh AgentHarness from the roster
//     agent's config), runs its turn, and returns ITS result to the coordinator;
//   - the thread lifecycle + message events are emitted (created/running/idle +
//     message_received/sent) on the PRIMARY thread, and the subagent's own turn events
//     ride its `subagents/<id>` subpath;
//   - the SHARED sandbox is visible to both (a file the subagent writes is readable by
//     the coordinator through the same handle);
//   - the concurrency cap (<=25 threads) and one-level delegation (a subagent gets no
//     `delegate` seam, so it can never itself delegate) hold.

import { describe, it, expect } from 'vitest';
import { InMemorySandboxRuntime } from '@orca/sandbox-runtime';
import type { SandboxHandle } from '../../src/sandbox/seam.js';
import {
  CoordinatorHarness,
  DelegationError,
  MAX_CONCURRENT_THREADS,
  type SubagentBuildContext,
} from '../../src/harness/multiagent/coordinator-harness.js';
import type { MultiagentSnapshot, RunnerSnapshot } from '../../src/snapshot.js';
import type {
  AgentEvent,
  AgentHarness,
  DelegateToAgent,
  SessionStartInput,
  TerminationReason,
  UserEvent,
} from '../../src/harness/agent-harness.js';

const WS = 'ws_co';
const SES = 'ses_co';

/** A roster member sub-snapshot (a subagent's own config). */
function memberSnapshot(system: string): RunnerSnapshot {
  return {
    model: { provider: 'anthropic', id: 'claude-haiku-4' },
    provider: 'claude',
    system,
    allowed_tool_names: ['read', 'write'],
    allowed_mcp_server_names: [],
    tool_permissions: {},
    egress: { mode: 'gateway' },
  };
}

/** The coordinator's parsed multiagent roster (two ordinary roster agents). */
function roster(): MultiagentSnapshot {
  return {
    type: 'coordinator',
    primaryThreadId: 'sth_primary',
    agents: [
      { agentName: 'researcher', snapshot: memberSnapshot('research') },
      { agentName: 'writer', snapshot: memberSnapshot('write') },
    ],
  };
}

/**
 * A fake subagent harness. On `submit` it (optionally) writes a file into the SHARED
 * sandbox it was handed, then emits one `agent.message` carrying its scripted result +
 * a terminal `agent.turn_completed`. Records the start input it was built with (so a
 * test can assert it got the shared sandbox and NO `delegate` seam — one-level).
 */
class FakeSubagentHarness implements AgentHarness {
  startInput: SessionStartInput | undefined;
  readonly submitted: UserEvent[] = [];
  stopReason: TerminationReason | undefined;
  private terminated = false;
  private readonly outQueue: AgentEvent[] = [];
  private outResolvers: Array<(r: IteratorResult<AgentEvent>) => void> = [];

  constructor(
    private readonly result: string,
    private readonly writeFile?: { path: string; content: string },
  ) {}

  async start(input: SessionStartInput): Promise<void> {
    this.startInput = input;
    this.terminated = false;
  }

  async submit(event: UserEvent): Promise<void> {
    this.submitted.push(event);
    if (this.terminated) return;
    if (this.writeFile !== undefined && this.startInput?.sandbox !== undefined) {
      await this.startInput.sandbox.files.write(
        this.writeFile.path,
        Buffer.from(this.writeFile.content, 'utf8'),
      );
    }
    this.emit({
      kind: 'agent.message',
      payload: { content: [{ type: 'text', text: this.result }] },
    });
    this.emit({ kind: 'agent.turn_completed', payload: { stop_reason: 'end_turn' } });
  }

  interrupt(): void {
    /* no in-flight async turn to abort in this double */
  }

  async stop(reason: TerminationReason): Promise<void> {
    this.stopReason = reason;
    this.terminated = true;
    while (this.outResolvers.length > 0) {
      this.outResolvers.shift()?.({ value: undefined, done: true });
    }
  }

  async *events(): AsyncIterable<AgentEvent> {
    while (!this.terminated || this.outQueue.length > 0) {
      const head = this.outQueue.shift();
      if (head !== undefined) {
        yield head;
        continue;
      }
      const next = await new Promise<IteratorResult<AgentEvent>>((resolve) => {
        this.outResolvers.push(resolve);
      });
      if (next.done) return;
      yield next.value;
    }
  }

  private emit(event: AgentEvent): void {
    const resolver = this.outResolvers.shift();
    if (resolver !== undefined) {
      resolver({ value: event, done: false });
      return;
    }
    this.outQueue.push(event);
  }
}

/**
 * A fake COORDINATOR base harness. It captures the `delegate` seam from its start
 * input, then on each `submit` runs a scripted sequence of delegations (awaiting each
 * result) and finally emits one `agent.message` summarizing the results it got back +
 * a terminal marker — the shape a real coordinator model would produce after calling
 * the delegation tool.
 */
class FakeCoordinatorHarness implements AgentHarness {
  startInput: SessionStartInput | undefined;
  delegate: DelegateToAgent | undefined;
  /** Results the coordinator got back from its delegations (for assertions). */
  readonly delegationResults: string[] = [];
  /** A hook a test sets to script what the coordinator delegates on `submit`. */
  onSubmit: ((delegate: DelegateToAgent) => Promise<void>) | undefined;
  private terminated = false;
  private readonly outQueue: AgentEvent[] = [];
  private outResolvers: Array<(r: IteratorResult<AgentEvent>) => void> = [];

  async start(input: SessionStartInput): Promise<void> {
    this.startInput = input;
    this.delegate = input.delegate;
    this.terminated = false;
  }

  async submit(_event: UserEvent): Promise<void> {
    if (this.terminated) return;
    if (this.onSubmit !== undefined && this.delegate !== undefined) {
      await this.onSubmit(this.delegate);
    }
    this.emit({
      kind: 'agent.message',
      payload: {
        content: [
          { type: 'text', text: `coordinator done: ${this.delegationResults.join(' | ')}` },
        ],
      },
    });
    this.emit({ kind: 'agent.turn_completed', payload: { stop_reason: 'end_turn' } });
  }

  interrupt(): void {
    /* no-op double */
  }

  async stop(_reason: TerminationReason): Promise<void> {
    this.terminated = true;
    while (this.outResolvers.length > 0) {
      this.outResolvers.shift()?.({ value: undefined, done: true });
    }
  }

  async *events(): AsyncIterable<AgentEvent> {
    while (!this.terminated || this.outQueue.length > 0) {
      const head = this.outQueue.shift();
      if (head !== undefined) {
        yield head;
        continue;
      }
      const next = await new Promise<IteratorResult<AgentEvent>>((resolve) => {
        this.outResolvers.push(resolve);
      });
      if (next.done) return;
      yield next.value;
    }
  }

  private emit(event: AgentEvent): void {
    const resolver = this.outResolvers.shift();
    if (resolver !== undefined) {
      resolver({ value: event, done: false });
      return;
    }
    this.outQueue.push(event);
  }
}

/** Drain a harness's `events()` into an array until the stream ends (harness stopped). */
async function drainEvents(harness: AgentHarness): Promise<AgentEvent[]> {
  const out: AgentEvent[] = [];
  for await (const ev of harness.events()) {
    out.push(ev);
  }
  return out;
}

/** A monotonic thread-id minter so tests can predict the ids (sth_t1, sth_t2, …). */
function seqThreadIds(): () => string {
  let n = 0;
  return () => `sth_t${++n}`;
}

/**
 * Build a coordinator harness over a scripted base + a subagent factory that hands
 * back the queued fakes (one per delegation), recording each build context. Returns
 * the harness plus handles for assertions.
 */
function buildCoordinator(opts: {
  base: FakeCoordinatorHarness;
  subagents: FakeSubagentHarness[];
  sandbox?: SandboxHandle;
}): {
  coordinator: CoordinatorHarness;
  builds: Array<{ member: string; ctx: SubagentBuildContext }>;
} {
  const queue = [...opts.subagents];
  const builds: Array<{ member: string; ctx: SubagentBuildContext }> = [];
  const coordinator = new CoordinatorHarness({
    base: opts.base,
    multiagent: roster(),
    workspaceId: WS,
    sessionId: SES,
    mintThreadId: seqThreadIds(),
    ...(opts.sandbox !== undefined ? { sandbox: opts.sandbox } : {}),
    buildSubagent: (member, ctx) => {
      builds.push({ member: member.agentName, ctx });
      const next = queue.shift();
      if (next === undefined) throw new Error('test wired too few subagent fakes');
      // Project the subagent's start input from its own snapshot, sharing the
      // coordinator's sandbox + gate — the shape the loop's real factory produces.
      const startInput: SessionStartInput = {
        workspaceId: WS,
        sessionId: SES,
        agentSnapshot: { system: ctx.snapshot.system },
        ...(ctx.sandbox !== undefined ? { sandbox: ctx.sandbox } : {}),
        ...(ctx.confirmTool !== undefined ? { confirmTool: ctx.confirmTool } : {}),
      };
      return { harness: next, startInput };
    },
  });
  return { coordinator, builds };
}

describe('CoordinatorHarness — delegation spawns a subagent thread and returns its result', () => {
  it('runs a delegated turn, streams thread events, and hands the result back to the coordinator', async () => {
    const runtime = new InMemorySandboxRuntime();
    const sandbox = await runtime.acquire({});
    const base = new FakeCoordinatorHarness();
    const researcher = new FakeSubagentHarness('research findings', {
      path: '/shared/research.txt',
      content: 'DATA',
    });
    const { coordinator, builds } = buildCoordinator({ base, subagents: [researcher], sandbox });

    // Script the coordinator to delegate ONE task to `researcher` and remember the result.
    base.onSubmit = async (delegate) => {
      const r = await delegate({ agentName: 'researcher', prompt: 'research topic X' });
      base.delegationResults.push(r.result);
    };

    await coordinator.start({ workspaceId: WS, sessionId: SES, agentSnapshot: {} });
    const events: AgentEvent[] = [];
    const drain = (async () => {
      for await (const ev of coordinator.events()) events.push(ev);
    })();

    await coordinator.submit({ kind: 'user.message', payload: { type: 'user.message' } });
    await coordinator.stop('client.archived');
    await drain;

    // 1. The subagent RAN and its result flowed back to the coordinator.
    expect(base.delegationResults).toEqual(['research findings']);
    expect(researcher.submitted).toHaveLength(1);

    // 2. A subagent thread was SPAWNED from the roster agent's config (its own snapshot).
    expect(builds).toHaveLength(1);
    expect(builds[0]!.member).toBe('researcher');
    expect(builds[0]!.ctx.snapshot.system).toBe('research');
    expect(builds[0]!.ctx.sessionThreadId).toBe('sth_t1');

    // 3. Thread LIFECYCLE + MESSAGE events were emitted on the PRIMARY thread (empty subpath).
    const byKind = (kind: string): AgentEvent[] => events.filter((e) => e.kind === kind);
    const created = byKind('session.thread_created');
    expect(created).toHaveLength(1);
    expect(created[0]!.payload).toEqual({
      session_thread_id: 'sth_t1',
      agent_name: 'researcher',
      parent_thread_id: 'sth_primary',
    });
    expect(created[0]!.subpath).toBe('');
    expect(byKind('session.thread_status_running')).toHaveLength(1);
    const idle = byKind('session.thread_status_idle');
    expect(idle).toHaveLength(1);
    expect((idle[0]!.payload as Record<string, unknown>).stop_reason).toBe('end_turn');
    // The delegation prompt (received) + the result (sent) surfaced on the primary thread.
    const received = byKind('agent.thread_message_received');
    expect(received[0]!.payload).toMatchObject({
      session_thread_id: 'sth_t1',
      from_session_thread_id: 'sth_primary',
      content: 'research topic X',
    });
    const sent = byKind('agent.thread_message_sent');
    expect(sent[0]!.payload).toMatchObject({
      session_thread_id: 'sth_t1',
      to_session_thread_id: 'sth_primary',
      content: 'research findings',
    });

    // 4. The subagent's OWN turn events rode its `subagents/<id>` subpath.
    const childMessages = events.filter(
      (e) => e.kind === 'agent.message' && e.subpath === 'subagents/sth_t1',
    );
    expect(childMessages).toHaveLength(1);
    expect(childMessages[0]!.payload).toEqual({
      content: [{ type: 'text', text: 'research findings' }],
    });

    // 5. The coordinator's own message rode the PRIMARY thread (no child subpath).
    const coordinatorMessage = events.find(
      (e) => e.kind === 'agent.message' && (e.subpath ?? '') === '',
    );
    expect(
      (coordinatorMessage!.payload as { content: Array<{ text: string }> }).content[0]!.text,
    ).toContain('research findings');

    // 6. The SHARED sandbox is visible to both: the file the subagent wrote is readable
    //    through the SAME handle the coordinator holds.
    expect(builds[0]!.ctx.sandbox).toBe(sandbox);
    const shared = await sandbox.files.read('/shared/research.txt');
    expect(shared.toString('utf8')).toBe('DATA');

    await sandbox.destroy();
  });

  it('the shared sandbox lets two subagents collaborate through one filesystem', async () => {
    const runtime = new InMemorySandboxRuntime();
    const sandbox = await runtime.acquire({});
    const base = new FakeCoordinatorHarness();
    // The researcher writes a file; the writer reads it back and reports its content —
    // proving BOTH subagents share the coordinator's ONE sandbox filesystem.
    const researcher = new FakeSubagentHarness('wrote data', {
      path: '/work/shared.txt',
      content: 'hello-from-researcher',
    });
    const writer = new FakeSubagentHarness('read it');
    const { coordinator } = buildCoordinator({ base, subagents: [researcher, writer], sandbox });

    base.onSubmit = async (delegate) => {
      const r1 = await delegate({ agentName: 'researcher', prompt: 'write the file' });
      base.delegationResults.push(r1.result);
      const r2 = await delegate({ agentName: 'writer', prompt: 'read the file' });
      base.delegationResults.push(r2.result);
    };

    await coordinator.start({ workspaceId: WS, sessionId: SES, agentSnapshot: {} });
    const drain = drainEvents(coordinator);
    await coordinator.submit({ kind: 'user.message', payload: { type: 'user.message' } });
    await coordinator.stop('client.archived');
    await drain;

    // Both delegations ran serially, each in its own thread over the same sandbox.
    expect(base.delegationResults).toEqual(['wrote data', 'read it']);
    // The writer subagent saw the file the researcher wrote (one shared filesystem).
    expect((await sandbox.files.read('/work/shared.txt')).toString('utf8')).toBe(
      'hello-from-researcher',
    );
    // The two threads got distinct ids.
    expect(writer.startInput?.sandbox).toBe(sandbox);
    expect(researcher.startInput?.sandbox).toBe(sandbox);
    await sandbox.destroy();
  });
});

describe('CoordinatorHarness — one-level delegation (a subagent can never delegate)', () => {
  it('builds a subagent WITHOUT a delegate seam, so it exposes no delegation tool', async () => {
    const base = new FakeCoordinatorHarness();
    const researcher = new FakeSubagentHarness('done');
    const { coordinator, builds } = buildCoordinator({ base, subagents: [researcher] });

    base.onSubmit = async (delegate) => {
      await delegate({ agentName: 'researcher', prompt: 'go' });
    };
    await coordinator.start({ workspaceId: WS, sessionId: SES, agentSnapshot: {} });
    const drain = drainEvents(coordinator);
    await coordinator.submit({ kind: 'user.message', payload: {} });
    await coordinator.stop('client.archived');
    await drain;

    // The subagent's start input carried NO `delegate` — one-level delegation is
    // enforced structurally: the coordinator strips any delegate off the subagent's
    // start input, so a subagent provider never sees a delegation seam, never wires a
    // delegation tool, and so can never spawn its own subagents.
    expect(researcher.startInput).toBeDefined();
    expect(researcher.startInput!.delegate).toBeUndefined();
    // The subagent build context's snapshot is not a coordinator (no roster to recurse).
    expect(builds[0]!.ctx.snapshot.multiagent).toBeUndefined();
  });

  it('strips a delegate the factory left on the subagent start input (defense-in-depth)', async () => {
    // Even if a factory carelessly projected a `delegate` onto the subagent's start
    // input, the coordinator MUST strip it — the one-level guarantee cannot depend on a
    // factory being careful. Here the factory deliberately sets a delegate; assert the
    // subagent was still started without one.
    const base = new FakeCoordinatorHarness();
    const researcher = new FakeSubagentHarness('done');
    const coordinator = new CoordinatorHarness({
      base,
      multiagent: roster(),
      workspaceId: WS,
      sessionId: SES,
      mintThreadId: seqThreadIds(),
      buildSubagent: (_member, ctx) => ({
        harness: researcher,
        startInput: {
          workspaceId: WS,
          sessionId: SES,
          agentSnapshot: {},
          // A rogue delegate the coordinator must strip (one-level guard).
          delegate: async () => ({ sessionThreadId: ctx.sessionThreadId, result: '' }),
        },
      }),
    });
    base.onSubmit = async (delegate) => {
      await delegate({ agentName: 'researcher', prompt: 'go' });
    };
    await coordinator.start({ workspaceId: WS, sessionId: SES, agentSnapshot: {} });
    const drain = drainEvents(coordinator);
    await coordinator.submit({ kind: 'user.message', payload: {} });
    await coordinator.stop('client.archived');
    await drain;

    expect(researcher.startInput!.delegate).toBeUndefined();
  });
});

describe('CoordinatorHarness — refusals surface as delegation errors', () => {
  it('rejects a delegation to an agent NOT in the roster (fail-fast, no thread spawned)', async () => {
    const base = new FakeCoordinatorHarness();
    const { coordinator, builds } = buildCoordinator({ base, subagents: [] });

    let error: Error | undefined;
    base.onSubmit = async (delegate) => {
      try {
        await delegate({ agentName: 'ghost', prompt: 'go' });
      } catch (e) {
        error = e as Error;
      }
    };
    await coordinator.start({ workspaceId: WS, sessionId: SES, agentSnapshot: {} });
    const drain = drainEvents(coordinator);
    await coordinator.submit({ kind: 'user.message', payload: {} });
    await coordinator.stop('client.archived');
    await drain;

    // An unknown roster agent is a clean rejection — no thread was ever spawned.
    expect(error).toBeDefined();
    expect(error!.message).toMatch(/ghost/);
    expect(builds).toHaveLength(0);
  });

  it('terminates the announced thread + fails cleanly when the subagent build throws', async () => {
    // A build failure (e.g. the roster agent's provider is unregistered on this runner)
    // happens AFTER the thread was announced (created/running). The coordinator must not
    // strand a `running` row: it emits a `session.thread_status_terminated` and surfaces
    // a clean delegation error to the model.
    const base = new FakeCoordinatorHarness();
    const coordinator = new CoordinatorHarness({
      base,
      multiagent: roster(),
      workspaceId: WS,
      sessionId: SES,
      mintThreadId: seqThreadIds(),
      buildSubagent: () => {
        throw new Error('no provider registered for this roster agent');
      },
    });

    let error: Error | undefined;
    base.onSubmit = async (delegate) => {
      try {
        await delegate({ agentName: 'researcher', prompt: 'go' });
      } catch (e) {
        error = e as Error;
      }
    };
    await coordinator.start({ workspaceId: WS, sessionId: SES, agentSnapshot: {} });
    const events: AgentEvent[] = [];
    const drain = (async () => {
      for await (const ev of coordinator.events()) events.push(ev);
    })();
    await coordinator.submit({ kind: 'user.message', payload: {} });
    await coordinator.stop('client.archived');
    await drain;

    // The delegation failed cleanly, and the announced thread was terminated (not left
    // running) — created + running + terminated, no idle.
    expect(error).toBeDefined();
    expect(error!.message).toMatch(/no provider registered/);
    expect(events.some((e) => e.kind === 'session.thread_created')).toBe(true);
    const terminated = events.filter((e) => e.kind === 'session.thread_status_terminated');
    expect(terminated).toHaveLength(1);
    expect(terminated[0]!.payload).toEqual({ session_thread_id: 'sth_t1' });
    expect(events.some((e) => e.kind === 'session.thread_status_idle')).toBe(false);
  });

  it('fails the delegation when the subagent turn CRASHES (never an empty success)', async () => {
    // `drainTurn` must CONTAIN the subagent's `submit` rejection — an escaping one would
    // break the merged stream mid-flush — and containing it used to erase it completely:
    // `collectedText()` returned '', so the delegation resolved with an empty string and
    // the coordinator model was handed `{ text: '', isError: false }`. That reads as "the
    // specialist had nothing useful to say", and the model reasons on from a fabricated
    // empty answer rather than learning the turn crashed. The crash must reach the model as
    // an ERROR, and the thread must end TERMINATED (its turn never finished), not idle.
    const base = new FakeCoordinatorHarness();
    const crashing = new CrashingSubagentHarness('subagent model transport died');
    const coordinator = new CoordinatorHarness({
      base,
      multiagent: roster(),
      workspaceId: WS,
      sessionId: SES,
      mintThreadId: seqThreadIds(),
      buildSubagent: () => ({
        harness: crashing,
        startInput: { workspaceId: WS, sessionId: SES, agentSnapshot: {} },
      }),
    });

    let error: Error | undefined;
    let resolved: string | undefined;
    base.onSubmit = async (delegate) => {
      try {
        resolved = (await delegate({ agentName: 'researcher', prompt: 'go' })).result;
      } catch (e) {
        error = e as Error;
      }
    };
    await coordinator.start({ workspaceId: WS, sessionId: SES, agentSnapshot: {} });
    const events: AgentEvent[] = [];
    const drain = (async () => {
      for await (const ev of coordinator.events()) events.push(ev);
    })();
    await coordinator.submit({ kind: 'user.message', payload: {} });
    await coordinator.stop('client.archived');
    await drain;

    // The delegation REJECTED (the tool surfaces `isError: true`) — it did NOT resolve
    // with the accumulator's empty string.
    expect(resolved).toBeUndefined();
    expect(error).toBeInstanceOf(DelegationError);
    expect(error!.message).toMatch(/subagent model transport died/);
    // The announced thread ended TERMINATED, exactly once, and never idled…
    const terminated = events.filter((e) => e.kind === 'session.thread_status_terminated');
    expect(terminated).toHaveLength(1);
    expect(terminated[0]!.payload).toEqual({ session_thread_id: 'sth_t1' });
    expect(events.some((e) => e.kind === 'session.thread_status_idle')).toBe(false);
    // …and no empty answer was announced back to the primary thread.
    expect(events.some((e) => e.kind === 'agent.thread_message_sent')).toBe(false);
  });

  it('rejects a delegation once the concurrency cap is reached (<=25 threads)', async () => {
    // The cap is enforced on CONCURRENT (in-flight) threads. We hold subagents open with
    // a gate, launch the cap-many concurrently, then assert the next delegation is refused.
    const base = new FakeCoordinatorHarness();
    const held: HeldSubagentHarness[] = [];
    const queue: HeldSubagentHarness[] = [];
    for (let i = 0; i < MAX_CONCURRENT_THREADS; i++) {
      const h = new HeldSubagentHarness(`r${i}`);
      held.push(h);
      queue.push(h);
    }
    // A roster with a single agent name, reused for every concurrent delegation.
    const coordinator = new CoordinatorHarness({
      base,
      multiagent: {
        type: 'coordinator',
        primaryThreadId: 'sth_primary',
        agents: [{ agentName: 'worker', snapshot: memberSnapshot('w') }],
      },
      workspaceId: WS,
      sessionId: SES,
      mintThreadId: seqThreadIds(),
      buildSubagent: () => {
        const next = queue.shift();
        if (next === undefined) throw new Error('too few held fakes');
        return {
          harness: next,
          startInput: { workspaceId: WS, sessionId: SES, agentSnapshot: {} },
        };
      },
    });

    let capError: Error | undefined;
    base.onSubmit = async (delegate) => {
      // Launch the cap-many delegations concurrently (do NOT await — each blocks on its
      // subagent's gate, so all stay in flight simultaneously).
      const inflight = held.map((_h, i) =>
        delegate({ agentName: 'worker', prompt: `t${i}` }).catch(() => undefined),
      );
      // Wait until all cap-many subagents have actually STARTED their turn (are in flight).
      await Promise.all(held.map((h) => h.started));
      // The (cap+1)-th delegation must be refused — the cap is saturated.
      try {
        await delegate({ agentName: 'worker', prompt: 'over' });
      } catch (e) {
        capError = e as Error;
      }
      // Release every held subagent so the in-flight delegations complete + threads free.
      for (const h of held) h.release();
      await Promise.all(inflight);
    };

    await coordinator.start({ workspaceId: WS, sessionId: SES, agentSnapshot: {} });
    const drain = drainEvents(coordinator);
    await coordinator.submit({ kind: 'user.message', payload: {} });
    await coordinator.stop('client.archived');
    await drain;

    expect(capError).toBeDefined();
    expect(capError!.message).toMatch(/concurren|limit|25/i);
    // Every subagent that was admitted actually ran (the cap refused only the extra one).
    for (const h of held) expect(h.didRun).toBe(true);
  });
});

describe('CoordinatorHarness — teardown terminates a live subagent thread', () => {
  it('emits session.thread_status_terminated for a delegation cut off by stop()', async () => {
    // A delegation is IN FLIGHT (its subagent blocks) when the coordinator is stopped.
    // The thread never reached its idle completion, so the coordinator emits a
    // `session.thread_status_terminated` for it (the forced-termination read-model signal).
    const base = new FakeCoordinatorHarness();
    const held = new HeldSubagentHarness('never finishes');
    const coordinator = new CoordinatorHarness({
      base,
      multiagent: {
        type: 'coordinator',
        primaryThreadId: 'sth_primary',
        agents: [{ agentName: 'worker', snapshot: memberSnapshot('w') }],
      },
      workspaceId: WS,
      sessionId: SES,
      mintThreadId: seqThreadIds(),
      buildSubagent: () => ({
        harness: held,
        startInput: { workspaceId: WS, sessionId: SES, agentSnapshot: {} },
      }),
    });

    // The coordinator's turn launches a delegation but does NOT await it (it blocks),
    // then resolves its own submit — so the delegation is still in flight at stop().
    base.onSubmit = async (delegate) => {
      void delegate({ agentName: 'worker', prompt: 'go' }).catch(() => undefined);
      await held.started; // ensure the subagent turn is actually in flight.
    };

    await coordinator.start({ workspaceId: WS, sessionId: SES, agentSnapshot: {} });
    const events: AgentEvent[] = [];
    const drain = (async () => {
      for await (const ev of coordinator.events()) events.push(ev);
    })();
    await coordinator.submit({ kind: 'user.message', payload: {} });
    await coordinator.stop('client.archived');
    await drain;

    // The still-live thread was terminated (not idled) on teardown.
    const terminated = events.filter((e) => e.kind === 'session.thread_status_terminated');
    expect(terminated).toHaveLength(1);
    expect(terminated[0]!.payload).toEqual({ session_thread_id: 'sth_t1' });
    expect(terminated[0]!.subpath).toBe('');
    expect(held.stopReason).toBe('client.archived');
  });
});

/**
 * A subagent fake whose delegated turn CRASHES: `submit` REJECTS, the way a real provider
 * unwinds when its model transport dies mid-turn. It emits NO events at all, so the
 * coordinator's accumulator collects the empty string — precisely the state that must not
 * be mistaken for a successful answer of no content.
 */
class CrashingSubagentHarness implements AgentHarness {
  private terminated = false;
  private readonly outResolvers: Array<(r: IteratorResult<AgentEvent>) => void> = [];

  constructor(private readonly reason: string) {}

  async start(_input: SessionStartInput): Promise<void> {
    this.terminated = false;
  }

  submit(_event: UserEvent): Promise<void> {
    return Promise.reject(new Error(this.reason));
  }

  interrupt(): void {
    /* no-op double */
  }

  async stop(_reason: TerminationReason): Promise<void> {
    this.terminated = true;
    while (this.outResolvers.length > 0) {
      this.outResolvers.shift()?.({ value: undefined, done: true });
    }
  }

  async *events(): AsyncIterable<AgentEvent> {
    while (!this.terminated) {
      const next = await new Promise<IteratorResult<AgentEvent>>((resolve) => {
        this.outResolvers.push(resolve);
      });
      if (next.done) return;
      yield next.value;
    }
  }
}

/**
 * A subagent fake whose turn BLOCKS on an external gate — used to hold many threads
 * in flight simultaneously so the concurrency cap can be exercised. Exposes `started`
 * (resolves once its turn began) and `release()` (unblocks the turn).
 */
class HeldSubagentHarness implements AgentHarness {
  startInput: SessionStartInput | undefined;
  didRun = false;
  stopReason: TerminationReason | undefined;
  readonly started: Promise<void>;
  private markStarted!: () => void;
  private gate: Promise<void>;
  private openGate!: () => void;
  private terminated = false;
  private readonly outQueue: AgentEvent[] = [];
  private outResolvers: Array<(r: IteratorResult<AgentEvent>) => void> = [];

  constructor(private readonly result: string) {
    this.started = new Promise<void>((r) => (this.markStarted = r));
    this.gate = new Promise<void>((r) => (this.openGate = r));
  }

  release(): void {
    this.openGate();
  }

  async start(input: SessionStartInput): Promise<void> {
    this.startInput = input;
    this.terminated = false;
  }

  async submit(_event: UserEvent): Promise<void> {
    if (this.terminated) return;
    this.didRun = true;
    this.markStarted();
    await this.gate; // hold the turn in flight until the test releases it.
    this.emit({
      kind: 'agent.message',
      payload: { content: [{ type: 'text', text: this.result }] },
    });
    this.emit({ kind: 'agent.turn_completed', payload: { stop_reason: 'end_turn' } });
  }

  interrupt(): void {
    this.openGate();
  }

  async stop(reason: TerminationReason): Promise<void> {
    this.stopReason = reason;
    this.terminated = true;
    this.openGate();
    while (this.outResolvers.length > 0) {
      this.outResolvers.shift()?.({ value: undefined, done: true });
    }
  }

  async *events(): AsyncIterable<AgentEvent> {
    while (!this.terminated || this.outQueue.length > 0) {
      const head = this.outQueue.shift();
      if (head !== undefined) {
        yield head;
        continue;
      }
      const next = await new Promise<IteratorResult<AgentEvent>>((resolve) => {
        this.outResolvers.push(resolve);
      });
      if (next.done) return;
      yield next.value;
    }
  }

  private emit(event: AgentEvent): void {
    const resolver = this.outResolvers.shift();
    if (resolver !== undefined) {
      resolver({ value: event, done: false });
      return;
    }
    this.outQueue.push(event);
  }
}

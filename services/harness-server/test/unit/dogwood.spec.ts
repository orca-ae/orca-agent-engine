// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0
import { describe, it, expect } from 'vitest';
import { DogwoodEnforcer, type DogwoodTransport } from '../../src/harness/dogwood.js';
import type { PolicyReadBody, PolicyAppendBody, PolicyAppendResponse } from '@orca/guardrails';
const allow = '@id("baseline") permit(principal,action,resource);';
const count =
  allow +
  ' @id("once") forbid(principal,action == Orca::Action::"bash",resource) when temporal {count_within(1h,Orca::Action::"bash"::request{}) > 1};';
function fixture(policy = allow, finite: number | null = null) {
  const now = Date.parse('2026-11-02T00:00:00Z');
  let monotonic = 100,
    boot = 'boot',
    epoch = 'process',
    lost = false,
    replayed = false;
  const view: PolicyReadBody = {
    session_id: 'ses_test',
    now: new Date(now).toISOString(),
    epoch: '1',
    bundle_id: 'bundle',
    core_version: '0.1.0',
    head: { session: '0', principal: null },
    principal: { type: 'Unattributed', id: 'ses_test' },
    resource: {
      type: 'Agent',
      id: 'agt_test',
      attributes: { name: 'agent' },
      parents: [{ type: 'Workspace', id: 'ws_test' }],
    },
    turn_event_id: 'evt_turn',
    day_window: '2026-11-02',
    month_window: '2026-11',
    state_revision: '0',
    events: [],
    guardrail_state: { session: {}, subject_window: {}, workspace_window: {} },
    guardrails: [
      { id: 'grd_test', name: 'test', mode: 'enforce', rule: { kind: 'dogwood', policy } },
    ],
  };
  const receipts = new Map<string, PolicyAppendResponse>();
  const writes: PolicyAppendBody[] = [];
  const transport: DogwoodTransport = {
    prepare: async () => ({ epoch: '1', bundle_id: 'bundle' }),
    read: async () => structuredClone(view),
    append: async (body) => {
      writes.push(body);
      const previous = receipts.get(body.batch_id);
      if (previous) return { ...previous, disposition: 'replayed' };
      const events = body.decision
        ? body.events
        : body.outcome
          ? [
              {
                ...view.events.find((e) => e.event.request_id === body.tool_use_id)!.event,
                kind: body.outcome === 'response' ? ('response' as const) : ('error' as const),
                ...(body.outcome === 'response' ? { output: {} } : { error_type: body.outcome }),
              },
            ]
          : [];
      const rows = events.map((event) => ({ seq: String(Number(view.head.session) + 1), event }));
      view.events.push(...rows);
      view.head.session = String(view.events.length);
      const result: PolicyAppendResponse = {
        disposition: replayed ? 'replayed' : 'created',
        receipt: {
          batch_id: body.batch_id,
          tool_use_id: body.tool_use_id,
          epoch: '1',
          bundle_id: 'bundle',
          committed_at: view.now,
          valid_before:
            body.decision && finite !== null ? new Date(now + finite).toISOString() : null,
        },
        now: view.now,
        head: { ...view.head },
        events: rows,
        guardrail_state: view.guardrail_state,
        state_revision: view.state_revision,
      };
      receipts.set(body.batch_id, result);
      if (lost) {
        lost = false;
        throw new Error('lost acknowledgment');
      }
      return result;
    },
  };
  const evidence: Record<string, unknown>[] = [];
  const mirrors: NonNullable<PolicyReadBody['guardrail_state']>[] = [];
  const build = (bound?: number) =>
    new DogwoodEnforcer(transport, {
      mirror: (state) => {
        mirrors.push(structuredClone(state));
      },
      evidence: (p) => evidence.push(p),
      clock: () => ({ boot_id: boot, epoch_id: epoch, monotonic_ms: monotonic }),
      ...(bound !== undefined ? { clockErrorBoundMs: bound } : {}),
    });
  const input = (id: string, signal = new AbortController().signal, group = 'message') => ({
    toolUseId: id,
    toolName: 'Bash',
    input: { command: 'ls' },
    publicToolUseId: `public-${id}`,
    signal,
    group,
    other: () => ({ verdict: 'allow' as const, updates: [] }),
  });
  return {
    view,
    transport,
    writes,
    evidence,
    mirrors,
    build,
    input,
    setTime: (m: number) => {
      monotonic = m;
    },
    resetClock: () => {
      boot = 'new-boot';
      epoch = 'new-process';
    },
    lose: () => {
      lost = true;
    },
    replay: () => {
      replayed = true;
    },
  };
}
describe('Dogwood first release and recovery', () => {
  it('allows an original null-bound receipt once after a long pause', async () => {
    const f = fixture();
    const e = f.build();
    await e.startTurn('evt_turn');
    const result = await e.decide(f.input('one'));
    f.setTime(99999999);
    expect(result.release()).toBe(true);
    expect(result.release()).toBe(false);
    expect(f.evidence[0]).toMatchObject({
      decision: 'allow',
      enforcer: 'harness',
      guardrails: [{ guardrail_id: 'grd_test', verdict: 'allow' }],
    });
  });
  it('uses the original pre-POST start and excludes equality at a finite deadline', async () => {
    const f = fixture(count, 100);
    const e = f.build(10);
    await e.startTurn('evt_turn');
    const result = await e.decide(f.input('one'));
    f.setTime(190);
    expect(result.release()).toBe(false);
    await result.interrupt();
    expect(f.view.events.at(-1)!.event.error_type).toBe('interrupted');
    expect(f.view.events.filter((v) => v.event.kind === 'request')).toHaveLength(1);
  });
  it('refuses a finite receipt with an unknown interval error bound or a reset clock', async () => {
    for (const reset of [false, true]) {
      const f = fixture(count, 100);
      const e = reset ? f.build(0) : f.build();
      await e.startTurn('evt_turn');
      const result = await e.decide(f.input('one'));
      if (reset) f.resetClock();
      expect(result.release()).toBe(false);
    }
  });
  it('checks local cancellation after a delayed created reply', async () => {
    const f = fixture();
    const original = f.transport.append;
    let resume!: () => void;
    let started!: () => void;
    const sent = new Promise<void>((r) => {
      started = r;
    });
    const blocked = new Promise<void>((r) => {
      resume = r;
    });
    f.transport.append = async (b) => {
      const result = await original(b);
      started();
      await blocked;
      return result;
    };
    const e = f.build();
    await e.startTurn('evt_turn');
    const controller = new AbortController();
    const pending = e.decide(f.input('one', controller.signal));
    await sent;
    controller.abort();
    resume();
    const result = await pending;
    expect(result.release()).toBe(false);
    await result.interrupt();
  });
  it('reconciles a lost acknowledgment without reviving the dispatch path', async () => {
    const f = fixture();
    const e = f.build();
    await e.startTurn('evt_turn');
    f.lose();
    await expect(e.decide(f.input('one'))).rejects.toThrow('lost acknowledgment');
    expect(f.view.events.map((v) => v.event.kind)).toEqual(['request', 'error']);
    expect(f.view.events.at(-1)!.event.error_type).toBe('interrupted');
    expect(f.writes.filter((w) => w.decision)).toHaveLength(2);
  });
  it('treats replay as history even when the original request never dispatched', async () => {
    const f = fixture();
    const e = f.build();
    await e.startTurn('evt_turn');
    f.replay();
    await expect(e.decide(f.input('one'))).rejects.toThrow('Historical');
    expect(f.view.events.filter((v) => v.event.kind === 'request')).toHaveLength(1);
  });
  it('serializes parallel sibling reservations through actual temporal evaluation', async () => {
    const f = fixture(count);
    const e = f.build();
    await e.startTurn('evt_turn');
    const results = await Promise.all([e.decide(f.input('one')), e.decide(f.input('two'))]);
    expect(results.map((r) => r.verdict)).toEqual(['allow', 'deny']);
    expect(f.view.events.map((v) => v.event.kind)).toEqual(['request', 'error']);
  });
  it('waits for the earlier message result, and stop does not interrupt completed calls', async () => {
    const f = fixture();
    const e = f.build();
    await e.startTurn('evt_turn');
    const one = await e.decide(f.input('one'));
    expect(one.release()).toBe(true);
    let finished = false;
    const second = e.decide({ ...f.input('two'), group: 'next-message' }).then((v) => {
      finished = true;
      return v;
    });
    await new Promise((r) => setTimeout(r, 20));
    expect(finished).toBe(false);
    await e.complete('one', 'response', {});
    expect((await second).release()).toBe(true);
    await e.stop();
    expect(
      f.view.events.filter((v) => v.event.request_id === 'one').map((v) => v.event.kind),
    ).toEqual(['request', 'response']);
    expect(f.view.events.at(-1)!.event.error_type).toBe('interrupted');
  });
  it('settles a lost result acknowledgment with its original batch and never interrupts that known result', async () => {
    const f = fixture(),
      e = f.build();
    await e.startTurn('evt_turn');
    const permit = await e.decide(f.input('result'));
    expect(permit.release()).toBe(true);
    f.lose();
    await e.complete('result', 'response', { raw: 'not persisted' });
    await e.stop();
    expect(f.view.events.map((e) => e.event.kind)).toEqual(['request', 'response']);
    const completions = f.writes.filter((b) => b.outcome === 'response');
    expect(completions).toHaveLength(2);
    expect(completions[1]).toEqual(completions[0]);
    expect(JSON.stringify(completions[0])).not.toContain('not persisted');
  });
  it('ignores a delayed completion snapshot after a newer GET while another call evaluates', async () => {
    const f = fixture(),
      e = f.build();
    await e.startTurn('evt_turn');
    expect((await e.decide(f.input('first'))).release()).toBe(true);
    const original = f.transport.append;
    let releaseAck!: () => void, committed!: () => void;
    const ack = new Promise<void>((r) => {
        releaseAck = r;
      }),
      committedSignal = new Promise<void>((r) => {
        committed = r;
      });
    f.transport.append = async (body) => {
      const response = await original(body);
      if (body.outcome === 'response') {
        committed();
        await ack;
        return {
          ...response,
          state_revision: '1',
          guardrail_state: { session: { version: 1 }, subject_window: {}, workspace_window: {} },
        };
      }
      return response;
    };
    const completion = e.complete('first', 'response', {});
    await committedSignal;
    f.view.state_revision = '2';
    f.view.guardrail_state = { session: { version: 2 }, subject_window: {}, workspace_window: {} };
    let read!: () => void;
    const readSignal = new Promise<void>((r) => {
      read = r;
    });
    const originalRead = f.transport.read;
    f.transport.read = async (history) => {
      const response = await originalRead(history);
      read();
      return response;
    };
    const next = e.decide({
      ...f.input('second'),
      other: () => {
        expect(f.mirrors.at(-1)!.session.version).toBe(2);
        return { verdict: 'allow', updates: [] };
      },
    });
    await readSignal;
    releaseAck();
    await completion;
    expect((await next).verdict).toBe('allow');
    expect(f.mirrors.at(-1)!.session.version).toBe(2);
  });
  it('retires queued callbacks before preparing another turn even if the epoch text is reused', async () => {
    const f = fixture(),
      e = f.build();
    await e.startTurn('first-turn');
    const original = f.transport.append;
    let sent!: () => void, resume!: () => void;
    const sentSignal = new Promise<void>((r) => {
        sent = r;
      }),
      blocked = new Promise<void>((r) => {
        resume = r;
      });
    f.transport.append = async (body) => {
      const response = await original(body);
      if (body.decision) {
        sent();
        await blocked;
      }
      return response;
    };
    const first = e.decide(f.input('first'));
    await sentSignal;
    const queued = e.decide(f.input('queued'));
    const queuedFailure = expect(queued).rejects.toThrow('ownership');
    await e.startTurn('next-turn');
    resume();
    expect((await first).release()).toBe(false);
    await queuedFailure;
    expect(f.writes.filter((w) => w.decision).map((w) => w.tool_use_id)).toEqual(['first']);
  });
});

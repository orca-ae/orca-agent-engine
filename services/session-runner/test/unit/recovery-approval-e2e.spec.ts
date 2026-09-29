// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// End-to-end spec for recovery-apply + tool-confirmation, driven THROUGH the runner
// loop over the real serve loop, in-process.
//
// This wires the REAL pieces together with only the two allowed test doubles: a fake
// registry-tunnel WS peer (the production runner-tunnel wire) and a fake AgentHarness
// (scripted turns; real claude runs in the self-hosted e2e). The registry peer PUSHES
// snapshot / turn / replay / confirmation frames exactly as the owner pod would, and the
// runner — SessionRunner → serve loop → RouteDispatcher → registerSessionHandlers →
// SessionLoop → FakeAgentHarness — serves them for real. Asserted here:
//
//   - RECOVERY: on (re)connect the runner advertises its resume cursor in the hello;
//     the registry pushes the ordered `after={cursor}` replay; the runner APPLIES it,
//     DEDUPS by event id, and a reconnect re-advertises the advanced cursor — at
//     various cursors (fresh full replay, mid-stream resume, caught-up).
//   - DEDUP: a re-pushed (overlapping) replay slice applies known ids zero times.
//   - APPROVAL: a turn parks on a gated tool call (the harness calls the confirmation
//     gate); the registry delivers `user.tool_confirmation` over the confirmation
//     route; the parked verdict resolves and the turn proceeds (allow) or the tool is
//     denied (deny) — the round-trip across the wire.

import { afterEach, beforeEach, describe, it, expect } from 'vitest';
import { tokenBoundRunnerId } from '@orca/harness-tunnel';
import { SessionRunner } from '../../src/runner.js';
import type { RunnerConfig } from '../../src/config.js';
import { SessionLoop } from '../../src/session-loop.js';
import { ProviderRegistry } from '../../src/harness/provider.js';
import { registerSessionHandlers } from '../../src/register-handlers.js';
import {
  RUNNER_CONFIRMATION_PATH,
  RUNNER_INTERRUPT_PATH,
  RUNNER_REPLAY_PATH,
  RUNNER_RESUME_CURSOR_HEADER,
  RUNNER_SESSION_HEADER,
  RUNNER_SNAPSHOT_PATH,
  RUNNER_TURN_PATH,
} from '../../src/protocol.js';

import {
  FakeRegistryRunnerTunnel,
  type LiveRunner,
} from './support/fake-registry-runner-tunnel.js';
import { FakeAgentHarness } from './support/fake-agent-harness.js';
import { GatedToolHarness } from './support/gated-tool-harness.js';

const BINDING_TOKEN = 'binding-token-e2e';
const RUNNER_ID = tokenBoundRunnerId(BINDING_TOKEN);
const WS = 'ws_e2e';
const SES = 'ses_e2e';

/** A runner-wiring config pointed at the fake registry (only the fields the runner reads). */
function runnerConfig(registryRunnerUrl: string): RunnerConfig {
  return {
    bindingToken: BINDING_TOKEN,
    registryRunnerUrl,
    workspace: '/tmp/runner-e2e',
    workspaceId: WS,
    sessionId: SES,
    idleTimeoutS: 0,
    provider: {
      modelDefault: 'claude-sonnet-4',
    },
  };
}

/** The snapshot body (one NDJSON line) the registry pushes for the `claude` provider. */
function snapshotBody(): string {
  return `${JSON.stringify({
    model: { provider: 'anthropic', id: 'claude-sonnet-4' },
    provider: 'claude',
    system: 'sys',
    allowed_tool_names: ['bash'],
    allowed_mcp_server_names: [],
    egress: { mode: 'gateway' },
  })}\n`;
}

/** Build an NDJSON replay body (one persisted-event JSON per line + trailing newline). */
function replayBody(objs: Array<Record<string, unknown>>): string {
  return objs.map((o) => JSON.stringify(o)).join('\n') + '\n';
}

const sessionHeaders: [string, string][] = [[RUNNER_SESSION_HEADER.toLowerCase(), SES]];

describe('recovery-apply + tool-confirmation — end-to-end over the serve loop', () => {
  let registry: FakeRegistryRunnerTunnel;
  let runner: SessionRunner;
  let loop: SessionLoop;
  let harness: FakeAgentHarness | GatedToolHarness;
  let runPromise: Promise<void> | undefined;

  beforeEach(async () => {
    registry = new FakeRegistryRunnerTunnel({ runnerId: RUNNER_ID, bindingToken: BINDING_TOKEN });
    await registry.listen();
  });

  afterEach(async () => {
    await runner?.stop();
    await runPromise?.catch(() => {});
    await loop?.stop();
    await registry.close();
  });

  /**
   * Construct the real runner stack (SessionLoop + handlers + SessionRunner) over the
   * fake registry, with the resume-cursor hook sourced from the loop (the recovery
   * client half). Returns once the first tunnel is live.
   */
  async function connect(h: FakeAgentHarness | GatedToolHarness): Promise<LiveRunner> {
    harness = h;
    const providers = new ProviderRegistry();
    providers.register('claude', () => harness);
    loop = new SessionLoop({ workspaceId: WS, providers });
    runner = new SessionRunner({
      config: runnerConfig(registry.baseUrl()),
      providers: loop.providerNames(),
      resumeCursors: () => loop.resumeCursors(),
      onActivity: () => loop.touchActivity(),
    });
    registerSessionHandlers(runner.dispatcher, loop);
    runPromise = runner.run();
    return registry.nextRunner();
  }

  it('fresh runner: hello carries an EMPTY cursor; applies a full replay; reconnect advertises the advanced cursor', async () => {
    const live = await connect(new FakeAgentHarness());
    // A fresh runner advertises no resume cursor.
    expect(live.hello.resumeCursors ?? {}).toEqual({});

    // The owner pod pushes the full replay (a fresh runner rebuilds from the start).
    const ack = await live.request({
      method: 'POST',
      path: RUNNER_REPLAY_PATH,
      headers: sessionHeaders,
      body: replayBody([
        { id: 'evt_1', type: 'user.message' },
        { id: 'evt_2', type: 'agent.message' },
        { id: 'evt_3', type: 'agent.turn_completed' },
      ]),
    });
    expect(ack.status).toBe(200);
    expect(JSON.parse(ack.body)).toEqual({ ok: true, applied: 3 });

    // Drop the tunnel; on reconnect the hello advertises the advanced cursor (the
    // last applied id) so the owner pod serves only `after=evt_3` next time.
    live.dropSocket(1012, 'recycle');
    const reconnected = await registry.nextRunner();
    expect(reconnected.hello.resumeCursors).toEqual({ [SES]: 'evt_3' });
  });

  it('mid-stream resume: a reconnect at a cursor applies only the after-slice and re-advertises', async () => {
    const live = await connect(new FakeAgentHarness());

    // First slice establishes the cursor at evt_2.
    await live.request({
      method: 'POST',
      path: RUNNER_REPLAY_PATH,
      headers: sessionHeaders,
      body: replayBody([{ id: 'evt_1' }, { id: 'evt_2' }]),
    });
    expect(loop.resumeCursors()).toEqual({ [SES]: 'evt_2' });

    live.dropSocket(1012, 'recycle');
    const reconnected = await registry.nextRunner();
    // The reconnect presents evt_2; the owner serves only the after-slice (evt_3+).
    expect(reconnected.hello.resumeCursors).toEqual({ [SES]: 'evt_2' });
    const ack = await reconnected.request({
      method: 'POST',
      path: RUNNER_REPLAY_PATH,
      headers: sessionHeaders,
      body: replayBody([{ id: 'evt_3' }, { id: 'evt_4' }]),
    });
    expect(JSON.parse(ack.body)).toEqual({ ok: true, applied: 2 });
    expect(loop.resumeCursors()).toEqual({ [SES]: 'evt_4' });
  });

  it('dedup: a re-pushed overlapping slice applies the known ids ZERO times', async () => {
    const live = await connect(new FakeAgentHarness());
    await live.request({
      method: 'POST',
      path: RUNNER_REPLAY_PATH,
      headers: sessionHeaders,
      body: replayBody([{ id: 'evt_1' }, { id: 'evt_2' }]),
    });
    // Re-push evt_2 (already held) + a new evt_3: only evt_3 is newly applied — the
    // dedup that makes a flapping reconnect's re-push idempotent.
    const ack = await live.request({
      method: 'POST',
      path: RUNNER_REPLAY_PATH,
      headers: sessionHeaders,
      body: replayBody([{ id: 'evt_2' }, { id: 'evt_3' }]),
    });
    expect(JSON.parse(ack.body)).toEqual({ ok: true, applied: 1 });
    expect(loop.resumeCursors()).toEqual({ [SES]: 'evt_3' });
  });

  it('caught-up frame: an empty replay applies nothing and the cursor is unchanged', async () => {
    const live = await connect(new FakeAgentHarness());
    await live.request({
      method: 'POST',
      path: RUNNER_REPLAY_PATH,
      headers: sessionHeaders,
      body: replayBody([{ id: 'evt_1' }]),
    });
    const ack = await live.request({
      method: 'POST',
      path: RUNNER_REPLAY_PATH,
      headers: sessionHeaders,
      body: '', // zero-line caught-up frame
    });
    expect(JSON.parse(ack.body)).toEqual({ ok: true, applied: 0 });
    expect(loop.resumeCursors()).toEqual({ [SES]: 'evt_1' });
  });

  it('resume-cursor HEADER is contract-matched but NOT consumed: the cursor derives from the BODY ids', async () => {
    // Locks the intentional design that the runner sources its resume position SOLELY
    // from applied body-event ids, never from the registry's resume-cursor HEADER.
    //
    // The header the registry actually sends is pushed here under
    // {@link RUNNER_RESUME_CURSOR_HEADER}. That the runner's literal MATCHES the
    // registry's own is asserted in `protocol-registry-pin.spec.ts`, which reads the
    // registry SOURCE — this spec used to re-declare the string by hand, which is the
    // shape that passes happily while the registry side drifts.
    const live = await connect(new FakeAgentHarness());
    // A replay whose HEADER names a DIFFERENT (further-ahead) cursor than the body's
    // last id. The runner must advance to the BODY's last id (evt_2), ignoring the
    // header — body-id derivation is exactly-once on its own.
    const ack = await live.request({
      method: 'POST',
      path: RUNNER_REPLAY_PATH,
      headers: [...sessionHeaders, [RUNNER_RESUME_CURSOR_HEADER.toLowerCase(), 'evt_999']],
      body: replayBody([{ id: 'evt_1' }, { id: 'evt_2' }]),
    });
    expect(JSON.parse(ack.body)).toEqual({ ok: true, applied: 2 });
    // The cursor is evt_2 (the body's last id), NOT evt_999 (the header) — the runner
    // trusts the replay body over the header.
    expect(loop.resumeCursors()).toEqual({ [SES]: 'evt_2' });

    // A caught-up ZERO-BYTE frame whose header names a further cursor (evt_999) must
    // NOT advance the runner's cursor: with no body ids there is nothing to derive
    // from, so the runner holds evt_2 and re-presents it on reconnect (the registry's
    // sliceAfterCursor re-serves idempotently). The header is informational only.
    const caughtUp = await live.request({
      method: 'POST',
      path: RUNNER_REPLAY_PATH,
      headers: [...sessionHeaders, [RUNNER_RESUME_CURSOR_HEADER.toLowerCase(), 'evt_999']],
      body: '',
    });
    expect(JSON.parse(caughtUp.body)).toEqual({ ok: true, applied: 0 });
    expect(loop.resumeCursors()).toEqual({ [SES]: 'evt_2' });

    // And the reconnect advertises evt_2 (the last truly-applied body id), proving the
    // header never leaked into the runner's resume bookkeeping.
    live.dropSocket(1012, 'recycle');
    const reconnected = await registry.nextRunner();
    expect(reconnected.hello.resumeCursors).toEqual({ [SES]: 'evt_2' });
  });

  it('a pushed replay ending in a pending user.message NEVER drives a turn (state-rebuild only, over the wire)', async () => {
    // The end-to-end form of "never re-run a finished turn" on the runner: the owner
    // pod PUSHES a replay whose last event is a trailing pending user.message — the
    // exact shape that would otherwise look like an unanswered turn to start — and
    // the runner applies it (dedup + cursor) but drives ZERO turns. The bridge (not
    // the runner) owns the turn decision, so a replay push is pure state rebuild.
    const harness = new FakeAgentHarness();
    const live = await connect(harness);
    const ack = await live.request({
      method: 'POST',
      path: RUNNER_REPLAY_PATH,
      headers: sessionHeaders,
      body: replayBody([
        { id: 'evt_1', type: 'user.message' },
        { id: 'evt_2', type: 'agent.turn_completed' },
        { id: 'evt_3', type: 'user.message' }, // trailing PENDING user turn
      ]),
    });
    expect(JSON.parse(ack.body)).toEqual({ ok: true, applied: 3 });
    expect(loop.resumeCursors()).toEqual({ [SES]: 'evt_3' });
    // The runner drove no turn: the harness was never submitted, no active work.
    expect(harness.submitted).toHaveLength(0);
    expect(loop.hasActiveWork()).toBe(false);
  });

  it('approval ALLOW: a turn parks on a gated tool, the delivered confirmation lets it proceed', async () => {
    // A harness whose turn calls the confirmation gate (canUseTool) mid-turn, parks,
    // and only emits its completed marker once the verdict ALLOWS.
    const gated = new GatedToolHarness('toolu_allow');
    const live = await connect(gated);

    // Configure the provider.
    await live.request({
      method: 'POST',
      path: RUNNER_SNAPSHOT_PATH,
      headers: sessionHeaders,
      body: snapshotBody(),
    });

    // Start the turn WITHOUT awaiting — it blocks on the parked verdict.
    const turn = live.startRequest({
      method: 'POST',
      path: RUNNER_TURN_PATH,
      headers: sessionHeaders,
      body: JSON.stringify({
        type: 'user.message',
        id: 'evt_u',
        content: [{ type: 'text', text: 'go' }],
      }),
    });
    await turn.headSeen();
    // The runner is parked on the human gate.
    await waitFor(() => loop.hasPendingApproval('toolu_allow'));

    // The registry delivers the user's allow verdict (it rode the transcript).
    const conf = await live.request({
      method: 'POST',
      path: RUNNER_CONFIRMATION_PATH,
      headers: sessionHeaders,
      body: JSON.stringify({
        type: 'user.tool_confirmation',
        tool_use_id: 'toolu_allow',
        decision: 'allow',
      }),
    });
    expect(JSON.parse(conf.body)).toEqual({ ok: true, resolved: true });

    // The turn unblocks and completes.
    await turn.ended;
    expect(gated.toolAllowed).toBe(true);
    const lines = live
      .bodySoFar(turn.id)
      .trim()
      .split('\n')
      .filter((l) => l.length > 0)
      .map((l) => JSON.parse(l) as { type: string });
    expect(lines.map((l) => l.type)).toContain('agent.turn_completed');
    expect(loop.hasPendingApproval()).toBe(false);
  });

  it('approval DENY: the delivered denial resolves the gate to a clean denial', async () => {
    const gated = new GatedToolHarness('toolu_deny');
    const live = await connect(gated);
    await live.request({
      method: 'POST',
      path: RUNNER_SNAPSHOT_PATH,
      headers: sessionHeaders,
      body: snapshotBody(),
    });

    const turn = live.startRequest({
      method: 'POST',
      path: RUNNER_TURN_PATH,
      headers: sessionHeaders,
      body: JSON.stringify({
        type: 'user.message',
        id: 'evt_u',
        content: [{ type: 'text', text: 'go' }],
      }),
    });
    await turn.headSeen();
    await waitFor(() => loop.hasPendingApproval('toolu_deny'));

    const conf = await live.request({
      method: 'POST',
      path: RUNNER_CONFIRMATION_PATH,
      headers: sessionHeaders,
      body: JSON.stringify({ tool_use_id: 'toolu_deny', decision: 'deny' }),
    });
    expect(JSON.parse(conf.body)).toEqual({ ok: true, resolved: true });

    await turn.ended;
    // The tool was denied — the harness recorded the clean denial, not an allow.
    expect(gated.toolAllowed).toBe(false);
    expect(gated.toolDenied).toBe(true);
  });

  it('approval idempotent re-push: a second confirmation for a settled id is a 200 no-op', async () => {
    const gated = new GatedToolHarness('toolu_idem');
    const live = await connect(gated);
    await live.request({
      method: 'POST',
      path: RUNNER_SNAPSHOT_PATH,
      headers: sessionHeaders,
      body: snapshotBody(),
    });
    const turn = live.startRequest({
      method: 'POST',
      path: RUNNER_TURN_PATH,
      headers: sessionHeaders,
      body: JSON.stringify({
        type: 'user.message',
        id: 'evt_u',
        content: [{ type: 'text', text: 'go' }],
      }),
    });
    await turn.headSeen();
    await waitFor(() => loop.hasPendingApproval('toolu_idem'));

    const first = await live.request({
      method: 'POST',
      path: RUNNER_CONFIRMATION_PATH,
      headers: sessionHeaders,
      body: JSON.stringify({ tool_use_id: 'toolu_idem', decision: 'allow' }),
    });
    expect(JSON.parse(first.body)).toEqual({ ok: true, resolved: true });
    await turn.ended;

    // A re-delivered confirmation (flapping reconnect) finds the verdict already
    // settled — resolved:false, a harmless 200. The first verdict wins.
    const second = await live.request({
      method: 'POST',
      path: RUNNER_CONFIRMATION_PATH,
      headers: sessionHeaders,
      body: JSON.stringify({ tool_use_id: 'toolu_idem', decision: 'deny' }),
    });
    expect(JSON.parse(second.body)).toEqual({ ok: true, resolved: false });
    expect(gated.toolAllowed).toBe(true);
  });

  it('interrupt OVER THE WIRE aborts a turn PARKED on a gated tool, and the harness stays alive', async () => {
    // The end-to-end form of the Major-finding fix: a turn parks on a gated tool (the
    // turn loop is blocked inside its streaming POST); the owner pod PUSHES a
    // `user.interrupt` to the interrupt route, which runs CONCURRENTLY with the turn
    // driver and aborts the in-flight turn — releasing the parked gate WITHOUT a
    // confirmation verdict and WITHOUT tearing down the harness. Proves the interrupt
    // is reachable against a real blocked turn (the gap the verifier flagged).
    const gated = new GatedToolHarness('toolu_interrupt');
    const live = await connect(gated);
    await live.request({
      method: 'POST',
      path: RUNNER_SNAPSHOT_PATH,
      headers: sessionHeaders,
      body: snapshotBody(),
    });

    // Start the turn WITHOUT awaiting — it blocks on the parked tool gate.
    const turn = live.startRequest({
      method: 'POST',
      path: RUNNER_TURN_PATH,
      headers: sessionHeaders,
      body: JSON.stringify({
        type: 'user.message',
        id: 'evt_u',
        content: [{ type: 'text', text: 'go' }],
      }),
    });
    await turn.headSeen();
    // The runner is parked on the human gate (the turn cannot proceed).
    await waitFor(() => loop.hasPendingApproval('toolu_interrupt'));

    // The owner pod delivers a `user.interrupt` (the client hit "stop"). NO verdict.
    const ack = await live.request({
      method: 'POST',
      path: RUNNER_INTERRUPT_PATH,
      headers: sessionHeaders,
      body: JSON.stringify({ type: 'user.interrupt' }),
    });
    expect(ack.status).toBe(200);
    expect(JSON.parse(ack.body)).toEqual({ ok: true, interrupted: true });

    // The parked turn unblocks and its stream ends (the gate released as a denial).
    await turn.ended;
    expect(gated.toolAllowed).toBe(false);
    expect(gated.toolInterrupted).toBe(true);
    // The pending approval cleared, and the harness was NOT torn down (interrupt, not stop).
    expect(loop.hasPendingApproval()).toBe(false);
    expect(gated.stopReason).toBeUndefined();
  });

  it('interrupt before any snapshot acks 200 interrupted:false (harmless, no harness yet)', async () => {
    const live = await connect(new FakeAgentHarness());
    const ack = await live.request({
      method: 'POST',
      path: RUNNER_INTERRUPT_PATH,
      headers: sessionHeaders,
      body: JSON.stringify({ type: 'user.interrupt' }),
    });
    expect(ack.status).toBe(200);
    expect(JSON.parse(ack.body)).toEqual({ ok: true, interrupted: false });
  });
});

/** Poll `cond` until true (bounded), yielding to the event loop between checks. */
async function waitFor(cond: () => boolean, tries = 200): Promise<void> {
  for (let i = 0; i < tries; i += 1) {
    if (cond()) {
      return;
    }
    await new Promise((r) => setTimeout(r, 2));
  }
  throw new Error('waitFor: condition not met in time');
}

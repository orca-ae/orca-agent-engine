// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * @orca/e2e-tests — the self-hosted (colocated) control flow, end to end.
 *
 *   client → registry → (durable claim) → environment-worker → session-runner →
 *   single-writer event bridge → client SSE
 *
 * This file is the PLUMBING half of the colocated suite and needs no model
 * credential. It runs the first-class `mock` provider — a deterministic, LLM-free
 * harness — against the minimal stack `start-self-hosted.sh` brings up: Postgres +
 * RustFS only, the registry on `TRANSCRIPT_STORE_BACKEND=postgres`, and a live
 * environment-worker already holding a claim on a `self_hosted` environment. No
 * Kafka, no Pulsar, no ai-gateway, no secret. That is what lets it be the CI default.
 *
 * What it asserts:
 *   1. RUN — a `harness=mock` agent, a session on the worker-attached environment,
 *      and a posted `user.message` drive the runner; `agent.message` +
 *      `agent.turn_completed` come back through the bridge over SSE. The message is
 *      exactly `mock: <text>`, which the mock harness can only produce by having
 *      actually run the turn.
 *   2. PENDING — a session on a DIFFERENT `self_hosted` environment with no worker
 *      stays `distribution_state=pending` with no runner, and the work queue counts
 *      it. Proves the distributor waits rather than failing or silently dropping.
 *   3. SINGLE-WRITER ORDERING — in the persisted transcript, `user.message` strictly
 *      precedes `agent.message` strictly precedes `agent.turn_completed`, with
 *      `produced_by` proving the client wrote one and the bridge wrote the others.
 *
 * The CAPABILITY half — tool dispatch, resources, skills, outputs, approvals — needs a
 * real model, because the mock provider never calls a tool. It lives in
 * `self-hosted-capabilities.spec.ts` and hard-fails without a credential.
 *
 * Splitting the two is deliberate. This file used to carry two real-Claude scenarios
 * behind `const maybe = hasKey ? it : it.skip`, which is precisely the silent-skip
 * pattern `real-agent-loop.spec.ts` forbids: "silent skips were the failure mode that
 * left nightly-e2b ineffective for months." Now each file hard-fails on its own
 * requirement and nothing passes by testing nothing.
 *
 * Prerequisite: `bash services/dev/scripts/start-self-hosted.sh`. Run via
 * `pnpm e2e:self-hosted`.
 */
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import {
  apiCall,
  buildClientFromConfig,
  ensureStackReachable,
  type OrcaClientConfig,
} from '../src/client.js';
import {
  createMockAgent,
  GUARDRAILS_PATH,
  createRequestPiiGuardrail,
  updateRequestPiiGuardrail,
  createSelfHostedEnvironment,
  createSession,
  getSession,
  getWorkStats,
  pollEventsUntil,
  postUserMessage,
  readEvents,
  readSelfHostedApiKey,
  readWorkerAttachedEnvironmentId,
  sleep,
  collectSseFrames,
  textOf,
  type EnvironmentResponse,
} from './self-hosted-helpers.js';

describe('self-hosted path (worker → claim → runner → mock provider → bridge → SSE)', () => {
  let cfg: OrcaClientConfig;
  let workerEnvId: string;
  // Per-test created ids, drained in reverse-dependency order in afterAll so a
  // re-run on a reused Postgres volume stays clean.
  const sessionIds: string[] = [];
  const agentIds: string[] = [];
  const environmentIds: string[] = [];
  const guardrailIds: string[] = [];

  beforeAll(async () => {
    cfg = buildClientFromConfig({ apiKey: readSelfHostedApiKey() });
    await ensureStackReachable(cfg);
    workerEnvId = readWorkerAttachedEnvironmentId();

    // Sanity: the recorded environment must exist, be self_hosted, and have a
    // connected worker. If the worker isn't connected the run scenario can't
    // pass — surface that precisely here instead of as a turn timeout below.
    const env = await apiCall(cfg, `/v1/environments/${workerEnvId}`, { method: 'GET' });
    expect(env.status, `worker-attached environment ${workerEnvId} not found`).toBe(200);
    // Read `config.type`, not `target`. Both name the same thing, but `target` is an
    // Orca extension gated behind `orca-beta` and this call does not send it, so it
    // comes back undefined; `config.type` is Anthropic's own field and is always
    // present. Asserting the default projection also means this check exercises the
    // wire an ordinary Anthropic-SDK client would see.
    expect(env.json<EnvironmentResponse>().config?.type).toBe('self_hosted');

    // The worker connects asynchronously after the stack script returns in CI;
    // give the claim heartbeat a brief window to be observed as live.
    let connected = false;
    for (let i = 0; i < 30; i++) {
      const stats = await getWorkStats(cfg, workerEnvId);
      if (stats.worker_connected) {
        connected = true;
        break;
      }
      await sleep(1_000);
    }
    expect(
      connected,
      `no worker connected to environment ${workerEnvId} — is start-self-hosted.sh's worker running? ` +
        'Check services/dev/logs/environment-worker.log.',
    ).toBe(true);
  }, 120_000);

  afterAll(async () => {
    for (const id of sessionIds.reverse()) {
      await apiCall(cfg, `/v1/sessions/${id}`, { method: 'DELETE' }).catch(() => {});
    }
    for (const id of agentIds.reverse()) {
      await apiCall(cfg, `/v1/agents/${id}`, { method: 'DELETE' }).catch(() => {});
    }
    for (const id of guardrailIds.reverse()) {
      await apiCall(cfg, `${GUARDRAILS_PATH}/${id}`, { method: 'DELETE' }).catch(() => {});
    }
    for (const id of environmentIds.reverse()) {
      await apiCall(cfg, `/v1/environments/${id}`, { method: 'DELETE' }).catch(() => {});
    }
  });

  it('pending-when-no-worker: a session on a worker-less self_hosted environment stays pending', async () => {
    // A fresh self_hosted environment the running worker is NOT attached to.
    const lonelyEnv = await createSelfHostedEnvironment(cfg, `self-hosted-no-worker-${Date.now()}`);
    environmentIds.push(lonelyEnv.id);

    // No worker on it — work_stats reports it.
    const stats = await getWorkStats(cfg, lonelyEnv.id);
    expect(stats.worker_connected).toBe(false);

    const agentId = await createMockAgent(cfg, `self-hosted-mock-pending-${Date.now()}`);
    agentIds.push(agentId);

    const session = await createSession(cfg, agentId, lonelyEnv.id);
    sessionIds.push(session.id);
    // The distributor persists PENDING for a self_hosted session; with no
    // connected worker it has no runner and stays pending. (Create already
    // reflects pending; assert it.)
    expect(session.distribution_state).toBe('pending');
    expect(session.runner_id).toBeNull();

    // Posting a user.message does not magically run it (still no worker). Give
    // it a moment, then confirm it is STILL pending + unassigned.
    await postUserMessage(cfg, session.id, 'are you there?');
    await sleep(3_000);
    const after = await getSession(cfg, session.id);
    expect(after.distribution_state).toBe('pending');
    expect(after.runner_id).toBeNull();
    expect(after.host_environment_id).toBeNull();
    const pendingEvents = await readEvents(cfg, session.id);
    expect(pendingEvents.some((event) => event.type.startsWith('agent.'))).toBe(false);

    // The work depth counts this stranded session (no worker picked it up).
    const depthStats = await getWorkStats(cfg, lonelyEnv.id);
    expect(depthStats.worker_connected).toBe(false);
    expect(depthStats.depth).toBeGreaterThanOrEqual(1);
  }, 60_000);

  it('run-when-worker-connected: the mock agent answers a turn through worker → runner → bridge → SSE', async () => {
    const agentId = await createMockAgent(cfg, `self-hosted-mock-run-${Date.now()}`);
    agentIds.push(agentId);

    const session = await createSession(cfg, agentId, workerEnvId);
    sessionIds.push(session.id);
    // On the worker-attached environment the distributor dispatched a launch —
    // the session is pending (spawning) or already assigned, never null.
    expect(session.distribution_state, 'self_hosted session should be distributed').not.toBeNull();
    expect(['pending', 'assigned']).toContain(session.distribution_state);

    const prompt = `hello from the self-hosted e2e ${Date.now()}`;
    await postUserMessage(cfg, session.id, prompt);

    // The runner's scripted turn flows back through the single-writer bridge to
    // SSE: an agent.message answering the prompt, then a terminal turn_completed.
    const frames = await collectSseFrames(cfg, session.id, {
      deadlineMs: 60_000,
      until: (f) => f.some((frame) => frame.type === 'agent.turn_completed'),
    });

    const kinds = frames.map((f) => f.type);
    expect(
      kinds,
      `expected agent.message + agent.turn_completed over SSE; saw: ${JSON.stringify(kinds)}`,
    ).toContain('agent.message');
    expect(kinds).toContain('agent.turn_completed');

    // The mock harness answers `mock: <user text>` — proof it actually ran the
    // turn (it can't produce the echoed prompt without seeing the user.message).
    const agentMessage = frames.find((f) => f.type === 'agent.message');
    expect(agentMessage, 'no agent.message frame').toBeDefined();
    expect(textOf(agentMessage!)).toBe(`mock: ${prompt}`);

    // Once the runner connected, the session moved off pending.
    const ran = await getSession(cfg, session.id);
    expect(ran.runner_id, 'a runner should be assigned after the turn').not.toBeNull();
    expect(ran.distribution_state).toBe('assigned');
  }, 90_000);

  it('guardrails: request deny and a live edit apply before the next colocated turn', async () => {
    const guardrailId = await createRequestPiiGuardrail(
      cfg,
      `self-hosted-request-guardrail-${Date.now()}`,
      ['ssn'],
    );
    guardrailIds.push(guardrailId);
    const agentId = await createMockAgent(cfg, `self-hosted-guarded-mock-${Date.now()}`, [
      guardrailId,
    ]);
    agentIds.push(agentId);
    const session = await createSession(cfg, agentId, workerEnvId);
    sessionIds.push(session.id);

    const allowedPrompt = `safe request ${Date.now()}`;
    await postUserMessage(cfg, session.id, allowedPrompt);
    let frames = await collectSseFrames(cfg, session.id, {
      deadlineMs: 60_000,
      until: (items) => items.filter((frame) => frame.type === 'agent.turn_completed').length >= 1,
    });
    expect(frames.some((frame) => textOf(frame) === `mock: ${allowedPrompt}`)).toBe(true);

    // Change the existing policy while the runner is live. The owner-pod bridge
    // must observe this fold before sending the next message and re-deliver the
    // snapshot; otherwise the mock harness would echo the email below.
    await updateRequestPiiGuardrail(cfg, guardrailId, ['email']);
    const blockedPrompt = `contact alice@example.com for guardrail ${Date.now()}`;
    await postUserMessage(cfg, session.id, blockedPrompt);
    frames = await collectSseFrames(cfg, session.id, {
      deadlineMs: 60_000,
      until: (items) => items.filter((frame) => frame.type === 'agent.turn_completed').length >= 2,
    });

    expect(frames.some((frame) => textOf(frame) === `mock: ${blockedPrompt}`)).toBe(false);
    const guardrailError = frames.find(
      (frame) =>
        frame.type === 'agent.error' &&
        typeof frame['message'] === 'string' &&
        frame['message'].includes('email'),
    );
    expect(
      guardrailError,
      'request guardrail denial did not reach the durable stream',
    ).toBeDefined();
  }, 120_000);

  it('single-writer ordering: user.message ≺ agent.message ≺ agent.turn_completed in the persisted log', async () => {
    const agentId = await createMockAgent(cfg, `self-hosted-mock-order-${Date.now()}`);
    agentIds.push(agentId);

    const session = await createSession(cfg, agentId, workerEnvId);
    sessionIds.push(session.id);

    const prompt = `ordering probe ${Date.now()}`;
    await postUserMessage(cfg, session.id, prompt);

    // Tail the durable transcript (SSE replays it from seq 0) until the turn
    // completes. The SSE stream serves the registry's single-writer transcript
    // verbatim — each frame carries the monotonic `seq` the writer assigned and
    // the `produced_by` provenance — so it is the authoritative ordering source
    // (the `GET /events` index is a SEPARATE, asynchronously-maintained
    // projection of the same transcript and lags the live tail by a poll cycle,
    // so asserting on it here would be racing an eventually-consistent read, not
    // the ordering guarantee itself).
    const frames = await collectSseFrames(cfg, session.id, {
      deadlineMs: 60_000,
      until: (f) => f.some((frame) => frame.type === 'agent.turn_completed'),
    });

    const userFrame = frames.find((f) => f.type === 'user.message' && textOf(f) === prompt);
    const msgFrame = frames.find(
      (f) => f.type === 'agent.message' && textOf(f) === `mock: ${prompt}`,
    );
    const doneFrame = frames.find((f) => f.type === 'agent.turn_completed');
    const kinds = frames.map((f) => f.type);
    expect(userFrame, `no user.message; saw ${JSON.stringify(kinds)}`).toBeDefined();
    expect(msgFrame, `no agent.message; saw ${JSON.stringify(kinds)}`).toBeDefined();
    expect(doneFrame, `no agent.turn_completed; saw ${JSON.stringify(kinds)}`).toBeDefined();
    expect(
      kinds.filter((kind) =>
        ['user.message', 'agent.message', 'agent.turn_completed'].includes(kind),
      ),
    ).toEqual(['user.message', 'agent.message', 'agent.turn_completed']);

    // Single-writer provenance: the agent events were written by the owner-pod
    // bridge (`produced_by=harness`), the user event by the client — exactly the
    // one-writer split the bridge enforces (the client appends user.*, the
    // bridge is the sole writer of the agent.* events it persists-before-forward).
    expect((userFrame as { produced_by?: string }).produced_by).toBe('client');
    expect((msgFrame as { produced_by?: string }).produced_by).toBe('harness');
    expect((doneFrame as { produced_by?: string }).produced_by).toBe('harness');

    // Strict in-order: the writer assigns a single monotonic seq, so
    // user.message ≺ agent.message ≺ agent.turn_completed by seq. The seqs are
    // string-encoded bigints; compare numerically.
    const userSeq = Number((userFrame as { seq?: string }).seq);
    const msgSeq = Number((msgFrame as { seq?: string }).seq);
    const doneSeq = Number((doneFrame as { seq?: string }).seq);
    expect(Number.isFinite(userSeq) && Number.isFinite(msgSeq) && Number.isFinite(doneSeq)).toBe(
      true,
    );
    expect(userSeq).toBeLessThan(msgSeq);
    expect(msgSeq).toBeLessThan(doneSeq);

    // The index projection (`GET /events`) eventually reflects the same turn.
    // Poll it (rather than read once) so the assertion rides the eventual
    // consistency instead of racing it — the durable transcript already proved
    // ordering above; this just confirms the read API converges to it.
    const indexed = await pollEventsUntil(
      cfg,
      session.id,
      (evts) =>
        evts.some((e) => e.type === 'agent.message' && textOf(e) === `mock: ${prompt}`) &&
        evts.some((e) => e.type === 'agent.turn_completed'),
      20_000,
    );
    const iUser = indexed.findIndex((e) => e.type === 'user.message' && textOf(e) === prompt);
    const iMsg = indexed.findIndex(
      (e) => e.type === 'agent.message' && textOf(e) === `mock: ${prompt}`,
    );
    const iDone = indexed.findIndex((e) => e.type === 'agent.turn_completed');
    expect(iUser).toBeGreaterThanOrEqual(0);
    expect(iMsg).toBeGreaterThanOrEqual(0);
    expect(iDone).toBeGreaterThanOrEqual(0);
    expect(iUser).toBeLessThan(iMsg);
    expect(iMsg).toBeLessThan(iDone);
  }, 90_000);
});

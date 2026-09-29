// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Unit spec for the handler-registration adapter — the bridge between the framed
// HTTP requests the registry pushes and the SessionLoop's methods.
//
// This exercises the routes the registry's owner pod drives THROUGH the
// RouteDispatcher (the same seam the serve loop dispatches into), asserting the
// status + body the runner frames back for each: the snapshot 200 ack (and the
// non-2xx on a bad snapshot / unknown provider), the turn NDJSON stream (and the
// 503 before a snapshot + the 400 on a bad body), and the replay 200 ack. The
// SessionLoop runs for real against a fake harness — only the tunnel socket is
// absent (the dispatcher is invoked directly), which is exactly the unit boundary.

import { describe, it, expect } from 'vitest';
import { ProviderRegistry } from '../../src/harness/provider.js';
import { SessionLoop } from '../../src/session-loop.js';
import { RouteDispatcher, type DispatchResponse } from '../../src/tunnel/request-dispatch.js';
import { registerSessionHandlers } from '../../src/register-handlers.js';
import {
  RUNNER_CONFIRMATION_PATH,
  RUNNER_CUSTOM_TOOL_RESULT_PATH,
  RUNNER_INTERRUPT_PATH,
  RUNNER_REPLAY_PATH,
  RUNNER_SESSION_HEADER,
  RUNNER_SKILLS_PATH,
  RUNNER_SNAPSHOT_PATH,
  RUNNER_TURN_PATH,
} from '../../src/protocol.js';
import { FakeAgentHarness } from './support/fake-agent-harness.js';
import { mkdtemp, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

const WS = 'ws_h';
const SES = 'ses_h';

/** One captured structured-logger call (level + payload + message). */
interface LogCall {
  level: 'info' | 'warn' | 'error';
  obj: unknown;
  // Explicit `| undefined` so a logger call with no message (the `msg?` arg is
  // `undefined`) is assignable under `exactOptionalPropertyTypes`.
  msg?: string | undefined;
}

/** Build a (dispatcher, loop, harness) trio wired together, capturing logger calls. */
function wired(harness = new FakeAgentHarness()): {
  dispatcher: RouteDispatcher;
  loop: SessionLoop;
  harness: FakeAgentHarness;
  logs: LogCall[];
} {
  const providers = new ProviderRegistry();
  providers.register('claude', () => harness);
  const loop = new SessionLoop({ workspaceId: WS, providers });
  const logs: LogCall[] = [];
  const logger = {
    info: (obj: unknown, msg?: string): void => void logs.push({ level: 'info', obj, msg }),
    warn: (obj: unknown, msg?: string): void => void logs.push({ level: 'warn', obj, msg }),
    error: (obj: unknown, msg?: string): void => void logs.push({ level: 'error', obj, msg }),
  };
  const dispatcher = registerSessionHandlers(new RouteDispatcher(), loop, { logger });
  return { dispatcher, loop, harness, logs };
}

/** Lower-cased session header (the codec lower-cases header names on the wire). */
function sessionHeader(sessionId = SES): Array<[string, string]> {
  return [[RUNNER_SESSION_HEADER.toLowerCase(), sessionId]];
}

/** Collect a dispatch response body into one utf8 string. */
async function bodyText(res: DispatchResponse): Promise<string> {
  const chunks: Uint8Array[] = [];
  for await (const chunk of res.body) {
    chunks.push(chunk);
  }
  return Buffer.concat(chunks.map((c) => Buffer.from(c))).toString('utf8');
}

/**
 * A snapshot delivery body (one NDJSON line). Carries the two fields the parser
 * REQUIRES — `provider` and `allowed_tool_names` (`[]` = no orca tool) — so these
 * routing specs exercise the route, not the allowlist contract.
 */
function snapshotBody(overrides: Record<string, unknown> = {}): Uint8Array {
  return new TextEncoder().encode(
    `${JSON.stringify({ provider: 'claude', allowed_tool_names: [], ...overrides })}\n`,
  );
}

describe('registerSessionHandlers — snapshot route', () => {
  it('applies the snapshot and acks 200', async () => {
    const { dispatcher, harness } = wired();
    const res = await dispatcher.dispatch({
      method: 'POST',
      path: RUNNER_SNAPSHOT_PATH,
      queryString: '',
      headers: sessionHeader(),
      body: snapshotBody(),
    });
    expect(res.status).toBe(200);
    expect(JSON.parse(await bodyText(res))).toEqual({ ok: true });
    expect(harness.startCount).toBe(1);
  });

  it('acks 400 on a malformed snapshot body (owner pod records it undelivered)', async () => {
    const { dispatcher } = wired();
    const res = await dispatcher.dispatch({
      method: 'POST',
      path: RUNNER_SNAPSHOT_PATH,
      queryString: '',
      headers: sessionHeader(),
      body: new TextEncoder().encode('not json'),
    });
    expect(res.status).toBe(400);
    expect(JSON.parse(await bodyText(res))).toEqual({ error: 'snapshot_apply_failed' });
  });

  it('acks 422 on an unknown provider (the runner-side capability mismatch)', async () => {
    const { dispatcher } = wired();
    const res = await dispatcher.dispatch({
      method: 'POST',
      path: RUNNER_SNAPSHOT_PATH,
      queryString: '',
      headers: sessionHeader(),
      body: snapshotBody({ provider: 'codex' }),
    });
    expect(res.status).toBe(422);
  });

  it('acks 422 on a guardrail phase this runner cannot enforce', async () => {
    const { dispatcher } = wired();
    const res = await dispatcher.dispatch({
      method: 'POST',
      path: RUNNER_SNAPSHOT_PATH,
      queryString: '',
      headers: sessionHeader(),
      body: snapshotBody({
        guardrails: [
          {
            id: 'grd_tool',
            name: 'block shell',
            tier: 'workspace',
            phases: ['tool_call'],
            rule: { kind: 'builtin', builtin: 'block_tools', params: { tools: ['Bash'] } },
            stateful: false,
          },
        ],
      }),
    });
    expect(res.status).toBe(422);
    expect(JSON.parse(await bodyText(res))).toEqual({ error: 'snapshot_apply_failed' });
  });
});

describe('registerSessionHandlers — turn route', () => {
  it('drives the harness and streams the agent events back as a 200 NDJSON body', async () => {
    const harness = new FakeAgentHarness([
      {
        events: [
          { kind: 'agent.message', payload: { text: 'hi' } },
          { kind: 'agent.turn_completed', payload: {} },
        ],
      },
    ]);
    const { dispatcher } = wired(harness);
    await dispatcher.dispatch({
      method: 'POST',
      path: RUNNER_SNAPSHOT_PATH,
      queryString: '',
      headers: sessionHeader(),
      body: snapshotBody(),
    });

    const res = await dispatcher.dispatch({
      method: 'POST',
      path: RUNNER_TURN_PATH,
      queryString: '',
      headers: sessionHeader(),
      body: new TextEncoder().encode(
        '{"type":"user.message","content":[{"type":"text","text":"hi"}]}',
      ),
    });
    expect(res.status).toBe(200);
    expect(res.headers).toContainEqual(['content-type', 'application/x-ndjson']);
    const lines = (await bodyText(res))
      .split('\n')
      .filter((l) => l.trim().length > 0)
      .map((l) => JSON.parse(l) as { type: string });
    expect(lines.map((l) => l.type)).toEqual(['agent.message', 'agent.turn_completed']);
    expect(harness.submitted).toHaveLength(1);
  });

  it('returns 503 when a turn races ahead of snapshot delivery (no harness yet)', async () => {
    const { dispatcher } = wired();
    const res = await dispatcher.dispatch({
      method: 'POST',
      path: RUNNER_TURN_PATH,
      queryString: '',
      headers: sessionHeader(),
      body: new TextEncoder().encode('{"type":"user.message"}'),
    });
    expect(res.status).toBe(503);
    expect(JSON.parse(await bodyText(res))).toEqual({ error: 'no_harness' });
  });

  it('returns 400 on a malformed turn body', async () => {
    const { dispatcher } = wired();
    await dispatcher.dispatch({
      method: 'POST',
      path: RUNNER_SNAPSHOT_PATH,
      queryString: '',
      headers: sessionHeader(),
      body: snapshotBody(),
    });
    const res = await dispatcher.dispatch({
      method: 'POST',
      path: RUNNER_TURN_PATH,
      queryString: '',
      headers: sessionHeader(),
      body: new TextEncoder().encode('[]'),
    });
    expect(res.status).toBe(400);
    expect(JSON.parse(await bodyText(res))).toEqual({ error: 'turn_body_rejected' });
  });

  it('reads the session id from the session header and scopes the turn to it', async () => {
    const harness = new FakeAgentHarness([
      { events: [{ kind: 'agent.turn_completed', payload: {} }] },
    ]);
    const { dispatcher } = wired(harness);
    await dispatcher.dispatch({
      method: 'POST',
      path: RUNNER_SNAPSHOT_PATH,
      queryString: '',
      headers: sessionHeader('ses_specific'),
      body: snapshotBody(),
    });
    // The harness was started for the header's session id.
    expect(harness.startInput?.sessionId).toBe('ses_specific');
    const res = await dispatcher.dispatch({
      method: 'POST',
      path: RUNNER_TURN_PATH,
      queryString: '',
      headers: sessionHeader('ses_specific'),
      body: new TextEncoder().encode('{"type":"user.message"}'),
    });
    // Draining the response body is what drives the turn (the body is the lazy
    // agent-event stream, exactly as the serve loop consumes it).
    await bodyText(res);
    expect(harness.submitted).toHaveLength(1);
  });
});

describe('registerSessionHandlers — replay route', () => {
  it('applies the replay and acks 200 with the applied count', async () => {
    const { dispatcher, loop } = wired();
    await dispatcher.dispatch({
      method: 'POST',
      path: RUNNER_SNAPSHOT_PATH,
      queryString: '',
      headers: sessionHeader(),
      body: snapshotBody(),
    });
    const res = await dispatcher.dispatch({
      method: 'POST',
      path: RUNNER_REPLAY_PATH,
      queryString: '',
      headers: sessionHeader(),
      body: new TextEncoder().encode('{"id":"evt_1"}\n{"id":"evt_2"}\n'),
    });
    expect(res.status).toBe(200);
    expect(JSON.parse(await bodyText(res))).toEqual({ ok: true, applied: 2 });
    // The runner's resume cursor advanced — what the next hello advertises.
    expect(loop.resumeCursors()).toEqual({ [SES]: 'evt_2' });
  });

  it('acks 200 for an empty caught-up replay frame', async () => {
    const { dispatcher } = wired();
    await dispatcher.dispatch({
      method: 'POST',
      path: RUNNER_SNAPSHOT_PATH,
      queryString: '',
      headers: sessionHeader(),
      body: snapshotBody(),
    });
    const res = await dispatcher.dispatch({
      method: 'POST',
      path: RUNNER_REPLAY_PATH,
      queryString: '',
      headers: sessionHeader(),
      body: new Uint8Array(0),
    });
    expect(res.status).toBe(200);
    expect(JSON.parse(await bodyText(res))).toEqual({ ok: true, applied: 0 });
  });
});

describe('registerSessionHandlers — confirmation route', () => {
  it('resolves a parked tool-confirmation (ALLOW) and acks 200', async () => {
    const { dispatcher, loop } = wired();
    await dispatcher.dispatch({
      method: 'POST',
      path: RUNNER_SNAPSHOT_PATH,
      queryString: '',
      headers: sessionHeader(),
      body: snapshotBody(),
    });
    // Park a verdict as the harness's canUseTool would.
    const gate = loop.confirmTool('Bash', { command: 'ls' }, { toolUseId: 'toolu_1' });

    const res = await dispatcher.dispatch({
      method: 'POST',
      path: RUNNER_CONFIRMATION_PATH,
      queryString: '',
      headers: sessionHeader(),
      body: new TextEncoder().encode(
        '{"type":"user.tool_confirmation","tool_use_id":"toolu_1","decision":"allow"}',
      ),
    });
    expect(res.status).toBe(200);
    expect(JSON.parse(await bodyText(res))).toEqual({ ok: true, resolved: true });
    // The parked tool call proceeded.
    expect((await gate).behavior).toBe('allow');
  });

  it('resolves a parked tool-confirmation (DENY) and acks 200', async () => {
    const { dispatcher, loop } = wired();
    await dispatcher.dispatch({
      method: 'POST',
      path: RUNNER_SNAPSHOT_PATH,
      queryString: '',
      headers: sessionHeader(),
      body: snapshotBody(),
    });
    const gate = loop.confirmTool('Bash', {}, { toolUseId: 'toolu_2' });
    const res = await dispatcher.dispatch({
      method: 'POST',
      path: RUNNER_CONFIRMATION_PATH,
      queryString: '',
      headers: sessionHeader(),
      body: new TextEncoder().encode('{"tool_use_id":"toolu_2","decision":"deny"}'),
    });
    expect(res.status).toBe(200);
    expect(JSON.parse(await bodyText(res))).toEqual({ ok: true, resolved: true });
    expect((await gate).behavior).toBe('deny');
  });

  it('resolves a routable but decision-LESS confirmation as a denial and logs it distinctly', async () => {
    // A body with a tool_use_id but no recognizable decision (no allow/deny string,
    // no `approved` boolean) is routable — so it resolves the parked verdict (200,
    // resolved:true) — but fail-closes to a DENY, and is logged distinctly so a
    // malformed/decision-less producer is observable apart from a deliberate deny.
    const { dispatcher, loop, logs } = wired();
    await dispatcher.dispatch({
      method: 'POST',
      path: RUNNER_SNAPSHOT_PATH,
      queryString: '',
      headers: sessionHeader(),
      body: snapshotBody(),
    });
    const gate = loop.confirmTool('Bash', {}, { toolUseId: 'toolu_amb' });
    const res = await dispatcher.dispatch({
      method: 'POST',
      path: RUNNER_CONFIRMATION_PATH,
      queryString: '',
      headers: sessionHeader(),
      // tool_use_id present (routable), but NO decision/approved field.
      body: new TextEncoder().encode('{"type":"user.tool_confirmation","tool_use_id":"toolu_amb"}'),
    });
    expect(res.status).toBe(200);
    expect(JSON.parse(await bodyText(res))).toEqual({ ok: true, resolved: true });
    // Fail-closed: the parked tool call was DENIED, never allowed.
    expect((await gate).behavior).toBe('deny');
    // The decision-less verdict was logged distinctly (a warn naming the tool-use id).
    const ambiguousLog = logs.find(
      (l) =>
        l.level === 'warn' &&
        typeof l.msg === 'string' &&
        l.msg.includes('no recognizable decision'),
    );
    expect(ambiguousLog).toBeDefined();
    expect((ambiguousLog?.obj as { toolUseId?: string }).toolUseId).toBe('toolu_amb');
  });

  it('does NOT log the ambiguous warning for an explicit deny (deliberate refusal is distinct)', async () => {
    // An explicit deny resolves + denies exactly like before, WITHOUT the ambiguous
    // log — so a real refusal is not noised up as a malformed verdict.
    const { dispatcher, loop, logs } = wired();
    await dispatcher.dispatch({
      method: 'POST',
      path: RUNNER_SNAPSHOT_PATH,
      queryString: '',
      headers: sessionHeader(),
      body: snapshotBody(),
    });
    const gate = loop.confirmTool('Bash', {}, { toolUseId: 'toolu_dn' });
    await dispatcher.dispatch({
      method: 'POST',
      path: RUNNER_CONFIRMATION_PATH,
      queryString: '',
      headers: sessionHeader(),
      body: new TextEncoder().encode('{"tool_use_id":"toolu_dn","decision":"deny"}'),
    });
    expect((await gate).behavior).toBe('deny');
    expect(
      logs.some((l) => typeof l.msg === 'string' && l.msg.includes('no recognizable decision')),
    ).toBe(false);
  });

  it('acks 200 with resolved:false for an unknown / already-resolved tool-use id (idempotent re-push)', async () => {
    const { dispatcher } = wired();
    await dispatcher.dispatch({
      method: 'POST',
      path: RUNNER_SNAPSHOT_PATH,
      queryString: '',
      headers: sessionHeader(),
      body: snapshotBody(),
    });
    // No park outstanding for this id — a re-delivered confirmation is a harmless no-op.
    const res = await dispatcher.dispatch({
      method: 'POST',
      path: RUNNER_CONFIRMATION_PATH,
      queryString: '',
      headers: sessionHeader(),
      body: new TextEncoder().encode('{"tool_use_id":"toolu_gone","decision":"allow"}'),
    });
    expect(res.status).toBe(200);
    expect(JSON.parse(await bodyText(res))).toEqual({ ok: true, resolved: false });
  });

  it('acks 400 on a confirmation body with no tool_use_id (cannot route the verdict)', async () => {
    const { dispatcher } = wired();
    const res = await dispatcher.dispatch({
      method: 'POST',
      path: RUNNER_CONFIRMATION_PATH,
      queryString: '',
      headers: sessionHeader(),
      body: new TextEncoder().encode('{"decision":"allow"}'),
    });
    expect(res.status).toBe(400);
    expect(JSON.parse(await bodyText(res))).toEqual({ error: 'tool_confirmation_rejected' });
  });
});

describe('registerSessionHandlers — interrupt route', () => {
  it('aborts the in-flight turn and acks 200 (interrupted:true when a harness is present)', async () => {
    const harness = new FakeAgentHarness([
      { events: [{ kind: 'agent.turn_completed', payload: {} }] },
    ]);
    const { dispatcher, harness: h } = wired(harness);
    await dispatcher.dispatch({
      method: 'POST',
      path: RUNNER_SNAPSHOT_PATH,
      queryString: '',
      headers: sessionHeader(),
      body: snapshotBody(),
    });
    const res = await dispatcher.dispatch({
      method: 'POST',
      path: RUNNER_INTERRUPT_PATH,
      queryString: '',
      headers: sessionHeader(),
      body: new TextEncoder().encode('{"type":"user.interrupt"}'),
    });
    expect(res.status).toBe(200);
    expect(JSON.parse(await bodyText(res))).toEqual({ ok: true, interrupted: true });
    // The harness's interrupt() ran (the out-of-band abort), without a teardown.
    expect(h.interruptCount).toBe(1);
    expect(h.stopReason).toBeUndefined();
  });

  it('acks 200 with interrupted:false when no snapshot has configured a harness yet', async () => {
    const { dispatcher } = wired();
    const res = await dispatcher.dispatch({
      method: 'POST',
      path: RUNNER_INTERRUPT_PATH,
      queryString: '',
      headers: sessionHeader(),
      body: new TextEncoder().encode('{"type":"user.interrupt"}'),
    });
    expect(res.status).toBe(200);
    expect(JSON.parse(await bodyText(res))).toEqual({ ok: true, interrupted: false });
  });

  it('acks 400 on a malformed interrupt body (empty / non-object)', async () => {
    const { dispatcher } = wired();
    await dispatcher.dispatch({
      method: 'POST',
      path: RUNNER_SNAPSHOT_PATH,
      queryString: '',
      headers: sessionHeader(),
      body: snapshotBody(),
    });
    const empty = await dispatcher.dispatch({
      method: 'POST',
      path: RUNNER_INTERRUPT_PATH,
      queryString: '',
      headers: sessionHeader(),
      body: new Uint8Array(0),
    });
    expect(empty.status).toBe(400);
    expect(JSON.parse(await bodyText(empty))).toEqual({ error: 'interrupt_rejected' });

    const arr = await dispatcher.dispatch({
      method: 'POST',
      path: RUNNER_INTERRUPT_PATH,
      queryString: '',
      headers: sessionHeader(),
      body: new TextEncoder().encode('[]'),
    });
    expect(arr.status).toBe(400);
  });

  it('a re-pushed interrupt is idempotent (each is a harmless 200)', async () => {
    const harness = new FakeAgentHarness([
      { events: [{ kind: 'agent.turn_completed', payload: {} }] },
    ]);
    const { dispatcher } = wired(harness);
    await dispatcher.dispatch({
      method: 'POST',
      path: RUNNER_SNAPSHOT_PATH,
      queryString: '',
      headers: sessionHeader(),
      body: snapshotBody(),
    });
    for (let i = 0; i < 2; i += 1) {
      const res = await dispatcher.dispatch({
        method: 'POST',
        path: RUNNER_INTERRUPT_PATH,
        queryString: '',
        headers: sessionHeader(),
        body: new TextEncoder().encode('{"type":"user.interrupt"}'),
      });
      expect(res.status).toBe(200);
      expect(JSON.parse(await bodyText(res))).toEqual({ ok: true, interrupted: true });
    }
  });
});

describe('registerSessionHandlers — wiring', () => {
  it('registers exactly the snapshot / turn / replay POST routes', async () => {
    const { dispatcher } = wired();
    // An unrouted path is a 404 (the dispatcher default); the three routes are 2xx/5xx.
    const notFound = await dispatcher.dispatch({
      method: 'POST',
      path: '/v1/runner/unknown',
      queryString: '',
      headers: sessionHeader(),
      body: new Uint8Array(0),
    });
    expect(notFound.status).toBe(404);
  });

  it('returns the same dispatcher so registration can chain', () => {
    const providers = new ProviderRegistry();
    providers.register('claude', () => new FakeAgentHarness());
    const loop = new SessionLoop({ workspaceId: WS, providers });
    const dispatcher = new RouteDispatcher();
    expect(registerSessionHandlers(dispatcher, loop)).toBe(dispatcher);
  });
});

describe('registerSessionHandlers — skills route', () => {
  /** A skills push body (manifest + one SKILL.md file). */
  function skillsBody(): Uint8Array {
    return new TextEncoder().encode(
      [
        JSON.stringify({ type: 'skills_manifest', dir: 'skills-plugin', skills: ['alpha'] }),
        JSON.stringify({
          type: 'skill_file',
          skill: 'alpha',
          path: 'SKILL.md',
          mode: 0o644,
          mime_type: null,
          content_base64: Buffer.from('# Alpha\n').toString('base64'),
        }),
      ].join('\n') + '\n',
    );
  }

  it('materializes the skills push and acks 200', async () => {
    const dir = await mkdtemp(join(tmpdir(), 'orca-h-skills-'));
    const providers = new ProviderRegistry();
    providers.register('claude', () => new FakeAgentHarness());
    const loop = new SessionLoop({ workspaceId: WS, providers, skillsWorkspaceDir: dir });
    const dispatcher = registerSessionHandlers(new RouteDispatcher(), loop);
    const res = await dispatcher.dispatch({
      method: 'POST',
      path: RUNNER_SKILLS_PATH,
      queryString: '',
      headers: sessionHeader(),
      body: skillsBody(),
    });
    expect(res.status).toBe(200);
    expect(JSON.parse(await bodyText(res))).toEqual({ ok: true });
  });

  it('rejects a malformed skills body with 400', async () => {
    const { dispatcher } = wired();
    const res = await dispatcher.dispatch({
      method: 'POST',
      path: RUNNER_SKILLS_PATH,
      queryString: '',
      headers: sessionHeader(),
      body: new TextEncoder().encode('not json\n'),
    });
    expect(res.status).toBe(400);
    expect(JSON.parse(await bodyText(res))).toEqual({ error: 'skills_apply_failed' });
  });

  it('reports a materialize/write failure as 500', async () => {
    // A workspace dir that is actually a FILE → the plugin mkdir fails (ENOTDIR),
    // a non-parse fault the handler maps to 500 (the owner pod re-pushes on reconnect).
    const filePath = join(await mkdtemp(join(tmpdir(), 'orca-h-skills-')), 'not-a-dir');
    await writeFile(filePath, 'x');
    const providers = new ProviderRegistry();
    providers.register('claude', () => new FakeAgentHarness());
    const loop = new SessionLoop({ workspaceId: WS, providers, skillsWorkspaceDir: filePath });
    const dispatcher = registerSessionHandlers(new RouteDispatcher(), loop);
    const res = await dispatcher.dispatch({
      method: 'POST',
      path: RUNNER_SKILLS_PATH,
      queryString: '',
      headers: sessionHeader(),
      body: skillsBody(),
    });
    expect(res.status).toBe(500);
    expect(JSON.parse(await bodyText(res))).toEqual({ error: 'skills_apply_failed' });
  });
});

describe('custom result independent handler', () => {
  it('checks session binding, validates content, resolves once, and never starts a turn', async () => {
    const { dispatcher, loop, harness } = wired();
    await loop.applySnapshot(SES, snapshotBody());
    const pending = harness.startInput!.awaitCustomToolResult!(
      'evt_custom',
      new AbortController().signal,
    );
    const result = {
      type: 'user.custom_tool_result',
      custom_tool_use_id: 'evt_custom',
      content: [{ type: 'text', text: 'ticket' }],
      is_error: true,
    };
    const push = async (body: unknown, session = SES) =>
      dispatcher.dispatch({
        method: 'POST',
        path: RUNNER_CUSTOM_TOOL_RESULT_PATH,
        queryString: '',
        headers: sessionHeader(session),
        body: Buffer.from(JSON.stringify(body)),
      });
    expect((await push(result, 'wrong_session')).status).toBe(403);
    expect((await push({ ...result, content: [{ type: 'text', text: 1 }] })).status).toBe(400);
    expect(JSON.parse(await bodyText(await push(result)))).toEqual({ ok: true, resolved: true });
    expect(await pending).toEqual(result);
    expect(JSON.parse(await bodyText(await push(result)))).toEqual({ ok: true, resolved: false });
    expect(
      JSON.parse(await bodyText(await push({ ...result, custom_tool_use_id: 'unknown' }))),
    ).toEqual({ ok: true, resolved: false });
    expect(harness.submitted).toEqual([]);
    await loop.stop();
  });
  it('snapshot replacement and stop reject outstanding callbacks; replay never resolves a new callback', async () => {
    const { loop, harness } = wired();
    await loop.applySnapshot(SES, snapshotBody());
    const oldGate = harness.startInput!.awaitCustomToolResult!;
    const old = oldGate('evt_old', new AbortController().signal);
    const rejected = expect(old).rejects.toThrow('abandoned');
    await loop.applySnapshot(SES, snapshotBody());
    await rejected;
    expect(() => oldGate('evt_stale', new AbortController().signal)).toThrow('generation retired');
    const next = harness.startInput!.awaitCustomToolResult!(
      'evt_next',
      new AbortController().signal,
    );
    const result = {
      id: 'evt_result',
      type: 'user.custom_tool_result',
      custom_tool_use_id: 'evt_old',
      content: [],
    };
    expect(loop.applyReplay(SES, Buffer.from(JSON.stringify(result) + '\n'))).toBe(1);
    expect(loop.applyReplay(SES, Buffer.from(JSON.stringify(result) + '\n'))).toBe(0);
    expect(
      loop.resolveCustomToolResult(SES, {
        type: 'user.custom_tool_result',
        custom_tool_use_id: 'evt_old',
      }),
    ).toBe(false);
    const stopped = expect(next).rejects.toThrow('abandoned');
    await loop.stop();
    await stopped;
  });
});

// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { parseCustomToolResult } from './custom-tools.js';
import { RUNNER_CUSTOM_TOOL_RESULT_PATH } from './protocol.js';
// Wire the {@link SessionLoop}'s behaviors onto the runner's request-dispatch seam
// — the adapter between the framed-HTTP requests the registry pushes and the loop's
// methods.
//
// The serve loop hands each pushed request to the {@link RouteDispatcher}; this
// registers ALL TEN routes the registry's owner pod drives (single-sourced from
// `protocol.ts` to match the registry exactly) and maps each to a loop call:
//
//   - POST /v1/runner/resources → applyResources, POST /v1/runner/resource-changes
//     → resourceChanges, and POST /v1/runner/resource-changes/ack →
//     acknowledgeResources → 200 + the loop's result (or `{ ok: true }`), or a 400
//     on any failure (an unparseable body included);
//   - POST /v1/runner/skills → applySkills → 200 ack (or a 400 on a malformed push
//     body, a 500 on a materialize failure; either way the owner pod records it
//     undelivered and re-pushes before the snapshot on the next reconnect);
//   - POST /v1/runner/snapshot → applySnapshot → 200 ack (or non-2xx on a bad
//     snapshot / unknown provider, so the owner pod records it undelivered);
//   - POST /v1/runner/turn → runTurn → 200 + the NDJSON agent-event stream (or a
//     503 when no snapshot has configured a harness yet, or a 400 on a bad body);
//   - POST /v1/runner/replay → applyReplay → 200 ack (the runner deduped + advanced
//     its resume cursor).
//   - POST /v1/runner/confirmation → resolveToolConfirmation → 200 ack (the runner
//     settled a parked tool-confirmation verdict; `resolved` reports whether a park
//     was actually pending, so an idempotent re-push is a harmless `resolved:false`),
//     or a 400 on a body with no `tool_use_id` (the verdict cannot be routed).
//   - POST /v1/runner/custom-tool-result → resolveCustomToolResult → 200 ack
//     (`resolved` reports whether a pending custom tool call took the result), a
//     403 when the session header does not match the snapshot's session, or a 400
//     on a malformed or rejected result.
//   - POST /v1/runner/interrupt → interruptTurn → 200 ack (the runner aborted the
//     in-flight turn WITHOUT tearing down the harness; `interrupted` reports whether a
//     harness was present, so an interrupt before any snapshot is a harmless
//     `interrupted:false`), or a 400 on a malformed body.
//
// Each handler reads the session id from the {@link RUNNER_SESSION_HEADER} and is
// framework-free: it returns a {@link DispatchResponse} the serve loop frames back.

import type {
  DispatchRequest,
  DispatchResponse,
  RouteDispatcher,
} from './tunnel/request-dispatch.js';
import {
  NDJSON_CONTENT_TYPE,
  RUNNER_CONFIRMATION_PATH,
  RUNNER_INTERRUPT_PATH,
  RUNNER_REPLAY_PATH,
  RUNNER_SESSION_HEADER,
  RUNNER_SKILLS_PATH,
  RUNNER_SNAPSHOT_PATH,
  RUNNER_TURN_PATH,
  RUNNER_RESOURCES_PATH,
  RUNNER_RESOURCE_CHANGES_PATH,
  RUNNER_RESOURCE_ACK_PATH,
} from './protocol.js';
import { SnapshotParseError } from './snapshot.js';
import { GuardrailUnsupportedError } from './guardrails.js';
import { SkillsPushParseError } from './skills-materialize.js';
import { UnknownProviderError } from './harness/provider.js';
import { SessionLoop, UserEventParseError } from './session-loop.js';
import {
  parseToolConfirmation,
  ToolConfirmationParseError,
  type ToolConfirmationVerdict,
} from './tool-confirmation.js';
import { parseUserInterrupt, InterruptParseError } from './interrupt.js';

/** Structured logger seam (a subset of the usual structured logger). All optional. */
export interface RegisterHandlersLogger {
  info?(obj: unknown, msg?: string): void;
  warn?(obj: unknown, msg?: string): void;
  error?(obj: unknown, msg?: string): void;
}

/** Options for {@link registerSessionHandlers}. */
export interface RegisterSessionHandlersOptions {
  /** Optional structured logger threaded into the handlers. */
  logger?: RegisterHandlersLogger;
}

/**
 * Register the snapshot / turn / replay handlers for `loop` on `dispatcher`.
 *
 * Returns the same `dispatcher` so the caller can chain. Registering twice for the
 * same path throws (the dispatcher rejects a duplicate route) — a wiring guard.
 */
export function registerSessionHandlers(
  dispatcher: RouteDispatcher,
  loop: SessionLoop,
  opts: RegisterSessionHandlersOptions = {},
): RouteDispatcher {
  const logger = opts.logger;

  // These handlers never enter the parked turn's serialized tool queue.
  for (const [path, handle] of [
    [RUNNER_RESOURCES_PATH, loop.applyResources.bind(loop)],
    [RUNNER_RESOURCE_CHANGES_PATH, loop.resourceChanges.bind(loop)],
    [RUNNER_RESOURCE_ACK_PATH, loop.acknowledgeResources.bind(loop)],
  ] as const) {
    dispatcher.register('POST', path, async (req) => {
      try {
        const result = await handle(
          sessionIdOf(req),
          JSON.parse(Buffer.from(req.body).toString('utf8')),
        );
        return jsonResponse(200, result ?? { ok: true });
      } catch {
        return jsonResponse(400, { error: 'resource_request_failed' });
      }
    });
  }

  // ── Snapshot: configure the provider from the credential-free snapshot. ──
  dispatcher.register(RUNNER_SNAPSHOT_PATH_METHOD, RUNNER_SNAPSHOT_PATH, async (req) => {
    const sessionId = sessionIdOf(req);
    try {
      await loop.applySnapshot(sessionId, req.body);
    } catch (err) {
      // A malformed snapshot / unknown provider / start failure: ack non-2xx so the
      // owner pod's delivery records it undelivered and retries on the next
      // reconnect (it self-heals; the bridge still starts).
      const status =
        err instanceof SnapshotParseError
          ? 400
          : err instanceof UnknownProviderError || err instanceof GuardrailUnsupportedError
            ? 422
            : 500;
      logger?.warn?.({ err, sessionId, status }, 'session runner snapshot apply failed');
      return jsonResponse(status, { error: 'snapshot_apply_failed' });
    }
    return jsonResponse(200, { ok: true });
  });

  // ── Skills: materialize the owner pod's Skill bundle push (before the snapshot). ──
  dispatcher.register(RUNNER_SKILLS_PATH_METHOD, RUNNER_SKILLS_PATH, async (req) => {
    const sessionId = sessionIdOf(req);
    try {
      await loop.applySkills(sessionId, req.body);
    } catch (err) {
      // A malformed push body → 400; a write/materialize failure → 500. Either way the
      // owner pod records it undelivered and re-pushes on the next reconnect (before the
      // snapshot), so it self-heals.
      const status = err instanceof SkillsPushParseError ? 400 : 500;
      logger?.warn?.({ err, sessionId, status }, 'session runner skills apply failed');
      return jsonResponse(status, { error: 'skills_apply_failed' });
    }
    return jsonResponse(200, { ok: true });
  });

  // ── Turn: drive the harness, stream agent events back as NDJSON. ──
  dispatcher.register(RUNNER_TURN_PATH_METHOD, RUNNER_TURN_PATH, async (req, signal) => {
    const sessionId = sessionIdOf(req);
    let stream: AsyncIterable<Uint8Array>;
    try {
      stream = loop.runTurn(sessionId, req.body, signal);
    } catch (err) {
      // A bad turn body fails synchronously (before any stream): 400, no harness
      // drive. Any other synchronous fault is a 500.
      const status = err instanceof UserEventParseError ? 400 : 500;
      logger?.warn?.({ err, sessionId, status }, 'session runner turn body rejected');
      return jsonResponse(status, { error: 'turn_body_rejected' });
    }
    if (!loop.hasHarness()) {
      // No snapshot has configured a harness yet — the turn raced ahead of
      // delivery. 503 so the owner pod retries the turn after the snapshot lands.
      logger?.warn?.({ sessionId }, 'session runner turn before snapshot; 503');
      return jsonResponse(503, { error: 'no_harness' });
    }
    return {
      status: 200,
      headers: [['content-type', NDJSON_CONTENT_TYPE]],
      body: stream,
    };
  });

  // ── Replay: apply the pushed resume slice (dedup + cursor advance). ──
  dispatcher.register(RUNNER_REPLAY_PATH_METHOD, RUNNER_REPLAY_PATH, async (req) => {
    const sessionId = sessionIdOf(req);
    let applied: number;
    try {
      applied = loop.applyReplay(sessionId, req.body);
    } catch (err) {
      // applyReplay is defensive (it skips malformed lines), so a throw here is an
      // unexpected fault: 500 so the owner pod records the replay undelivered.
      logger?.error?.({ err, sessionId }, 'session runner replay apply failed');
      return jsonResponse(500, { error: 'replay_apply_failed' });
    }
    return jsonResponse(200, { ok: true, applied });
  });

  // ── Confirmation: resolve a parked tool-confirmation verdict (allow/deny). ──
  dispatcher.register(RUNNER_CONFIRMATION_PATH_METHOD, RUNNER_CONFIRMATION_PATH, async (req) => {
    const sessionId = sessionIdOf(req);
    let verdict: ToolConfirmationVerdict;
    try {
      loop.assertResourceSession(sessionId);
      verdict = parseToolConfirmation(req.body);
    } catch (err) {
      // A confirmation body with no tool_use_id (or malformed) cannot be routed to a
      // parked tool call: 400. The owner pod treats it as a bad push.
      const status = err instanceof ToolConfirmationParseError ? 400 : 500;
      logger?.warn?.({ err, sessionId, status }, 'session runner tool confirmation rejected');
      return jsonResponse(status, { error: 'tool_confirmation_rejected' });
    }
    // The body was routable (had a tool_use_id) but carried NO recognizable decision
    // (no allow/deny `decision` string, no `approved` boolean). The verdict still
    // fail-closed to a denial — but log it distinctly so a producer sending a
    // malformed/decision-less verdict is observable apart from a deliberate deny.
    // (This is a 200: the verdict routed and resolved; the ambiguity is operational,
    // not a wire fault, and the durable transcript remains the source of truth.)
    if (verdict.decision === 'ambiguous') {
      logger?.warn?.(
        { sessionId, toolUseId: verdict.toolUseId },
        'session runner tool confirmation carried no recognizable decision; denying (fail-closed)',
      );
    }
    // Resolve the parked verdict. `resolved` is false for an unknown / already-settled
    // tool-use id — an idempotent re-push (a flapping reconnect re-delivering the same
    // `user.tool_confirmation`) is then a harmless 200 no-op, since the durable
    // transcript is the verdict's source of truth and the first verdict already won.
    const resolved = loop.resolveToolConfirmation(verdict.toolUseId, verdict.approved);
    return jsonResponse(200, { ok: true, resolved });
  });

  dispatcher.register('POST', RUNNER_CUSTOM_TOOL_RESULT_PATH, async (req) => {
    const sessionId = sessionIdOf(req);
    try {
      loop.assertCallbackSession(sessionId);
    } catch {
      return jsonResponse(403, { error: 'custom_tool_result_session_mismatch' });
    }
    try {
      const result = parseCustomToolResult(req.body);
      const resolved = loop.resolveCustomToolResult(sessionId, result);
      return jsonResponse(200, { ok: true, resolved });
    } catch {
      return jsonResponse(400, { error: 'custom_tool_result_rejected' });
    }
  });

  // ── Interrupt: abort the in-flight turn (keep the harness alive). ──
  dispatcher.register(RUNNER_INTERRUPT_PATH_METHOD, RUNNER_INTERRUPT_PATH, async (req) => {
    const sessionId = sessionIdOf(req);
    try {
      loop.assertResourceSession(sessionId);
      parseUserInterrupt(req.body);
    } catch (err) {
      // A malformed interrupt body (empty / non-object / bad JSON): 400. The owner pod
      // treats it as a bad push.
      const status = err instanceof InterruptParseError ? 400 : 500;
      logger?.warn?.({ err, sessionId, status }, 'session runner interrupt rejected');
      return jsonResponse(status, { error: 'interrupt_rejected' });
    }
    // Abort the in-flight turn (no-op + `interrupted:false` when no harness yet, or
    // no turn in flight). The harness stays alive — the next turn reuses it. A
    // re-pushed interrupt (a flapping reconnect) is idempotent: aborting an already-
    // finished turn is a harmless 200 no-op.
    const interrupted = loop.interruptTurn();
    return jsonResponse(200, { ok: true, interrupted });
  });

  return dispatcher;
}

const RUNNER_SNAPSHOT_PATH_METHOD = 'POST';
const RUNNER_SKILLS_PATH_METHOD = 'POST';
const RUNNER_TURN_PATH_METHOD = 'POST';
const RUNNER_REPLAY_PATH_METHOD = 'POST';
const RUNNER_CONFIRMATION_PATH_METHOD = 'POST';
const RUNNER_INTERRUPT_PATH_METHOD = 'POST';

/** The session id from the request's session header, or `''` when absent. */
function sessionIdOf(req: DispatchRequest): string {
  return req.header(RUNNER_SESSION_HEADER) ?? '';
}

/** A single-object JSON response with the given status (one body chunk, then end). */
function jsonResponse(status: number, value: unknown): DispatchResponse {
  const bytes = new TextEncoder().encode(JSON.stringify(value));
  return {
    status,
    headers: [['content-type', 'application/json']],
    body: (async function* () {
      yield bytes;
    })(),
  };
}

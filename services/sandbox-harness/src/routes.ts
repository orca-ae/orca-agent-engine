// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// routes.ts — the HTTP request handlers, one per managed-agents endpoint.
//
// Each handler is the `(req, res, ctx, params)` shape the router in `server.ts`
// dispatches; an `HttpError` thrown here is caught there and turned into a JSON
// error body.
//
// The shared context (`HarnessContext`), the body-parsed request
// (`HarnessRequest`), the params bag (`RouteParams`), and the handler signature
// (`RouteHandler`) are all OWNED BY `server.ts` — this module imports them so
// the wiring stays single-sourced. Spawn-arg resolution (`resolveHarness`) and
// the per-session runtime (`createManagedSession`) live in `session-manager.ts`;
// the provider catalog used by `listHarnesses` comes from the provider registry.
//
// Resume: POST /v1/sessions accepts an optional `replay` history. When present it
// is threaded through `resolveHarness` into the subprocess's launch env, and the
// subprocess's Session seeds it as a preamble first user message — letting a
// fresh subprocess rehydrate prior context. It is provider-agnostic (it only
// seeds Session history, never a provider).
//
// The event store stays in-memory / ephemeral (see `store.ts`). Durability is
// an external concern: the host tails the SSE stream produced by `streamEvents`
// (harness-server's in-sandbox harness), which is why the replay-then-subscribe
// ordering below is load-bearing — the handoff from stored history to the live
// feed must have no gap and no duplication.

import type { ServerResponse } from 'node:http';

import {
  HttpError,
  sendJson,
  userMessageEvent,
  sessionErrorEvent,
  type ContentBlock,
  type MessageContent,
} from './core.js';
import type { BareEvent, StoredEvent } from './store.js';
import { resolveHarness, type SpawnArgs } from './session-manager.js';
import type { HarnessContext, HarnessRequest, RouteHandler, RouteParams } from './server.js';
import {
  listProviderMetadata,
  MODEL_EFFORTS,
  MODEL_SPEEDS,
  type CustomToolDefinition,
  type ModelEffort,
  type ModelSpeed,
  type ProviderMetadata,
  type RuntimeAgentDefinitions,
} from './providers/index.js';
import { parseSandboxWritePolicy, probeSandboxWritePolicy } from './write-policy.js';

// ── request body shapes ─────────────────────────────────────────────────────

/**
 * A single prior turn supplied via `replay` on session create. Mirrors the loose
 * shape the resume seam accepts: either `text` (a plain string) or `parts`
 * (structured content blocks). Both collapse to plain text inside the session.
 */
export interface ReplayMessage {
  role: 'user' | 'assistant';
  parts?: ContentBlock[];
  text?: string;
}

/** Parsed body of `POST /v1/sessions`. Fields are validated per-handler. */
export interface CreateSessionBody {
  agent?: unknown;
  model?: unknown;
  modelSpeed?: unknown;
  model_speed?: unknown;
  modelEffort?: unknown;
  model_effort?: unknown;
  replay?: unknown;
  agents?: unknown;
  forwardSubagentText?: unknown;
  customTools?: unknown;
  custom_tools?: unknown;
  systemPrompt?: unknown;
  system_prompt?: unknown;
  tools?: unknown;
  allowedTools?: unknown;
  allowed_tools?: unknown;
  runtimeTools?: unknown;
}

/** One element of the `events` array on `POST /v1/sessions/:id/events`. */
export interface InboundEvent {
  type?: unknown;
  content?: unknown;
  [key: string]: unknown;
}

/** Parsed body of `POST /v1/sessions/:id/events`. */
export interface SendEventBody {
  events?: unknown;
}

// ── validation helpers (pure) ───────────────────────────────────────────────

/** Treat any non-object body as an empty object so field access stays total. */
function asRecord(value: unknown): Record<string, unknown> {
  return typeof value === 'object' && value !== null ? (value as Record<string, unknown>) : {};
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

/** Narrow one loose replay element, dropping anything malformed. */
function isReplayMessage(value: unknown): value is ReplayMessage {
  if (typeof value !== 'object' || value === null) return false;
  const v = value as Record<string, unknown>;
  if (v['role'] !== 'user' && v['role'] !== 'assistant') return false;
  if (v['parts'] !== undefined && !Array.isArray(v['parts'])) return false;
  if (v['text'] !== undefined && typeof v['text'] !== 'string') return false;
  return true;
}

/**
 * Coerce an unknown `replay` field into a typed history. Returns `undefined`
 * (not `[]`) when there is nothing usable, so the caller can omit the field
 * entirely under `exactOptionalPropertyTypes`.
 */
function parseReplay(value: unknown): ReplayMessage[] | undefined {
  if (!Array.isArray(value)) return undefined;
  const history = value.filter(isReplayMessage);
  return history.length > 0 ? history : undefined;
}

/** Coerce an unknown `model` field into a non-empty string, or `undefined`. */
function parseModel(value: unknown): string | undefined {
  return typeof value === 'string' && value.length > 0 ? value : undefined;
}

function parseString(value: unknown): string | undefined {
  return typeof value === 'string' ? value : undefined;
}

function parseStringArray(value: unknown): string[] | undefined {
  return Array.isArray(value) && value.every((item) => typeof item === 'string')
    ? value
    : undefined;
}

function parseModelSpeed(value: unknown): ModelSpeed | undefined {
  if (value === undefined) return undefined;
  if (MODEL_SPEEDS.includes(value as ModelSpeed)) return value as ModelSpeed;
  throw new HttpError(400, "modelSpeed must be 'standard' or 'fast'");
}

function parseModelEffort(value: unknown): ModelEffort | undefined {
  if (value === undefined) return undefined;
  const effort =
    typeof value === 'string'
      ? value
      : isRecord(value) && typeof value.type === 'string'
        ? value.type
        : undefined;
  if (effort !== undefined && MODEL_EFFORTS.includes(effort as ModelEffort)) {
    return effort as ModelEffort;
  }
  throw new HttpError(400, `modelEffort must be one of ${MODEL_EFFORTS.join(', ')}`);
}

function parseAgents(value: unknown): RuntimeAgentDefinitions | undefined {
  if (!isRecord(value)) return undefined;
  const agents: RuntimeAgentDefinitions = {};
  for (const [name, raw] of Object.entries(value)) {
    if (!name || !isRecord(raw)) continue;
    if (typeof raw.description !== 'string' || typeof raw.prompt !== 'string') continue;
    const definition: RuntimeAgentDefinitions[string] = {
      description: raw.description,
      prompt: raw.prompt,
    };
    if (typeof raw.managedAgentId === 'string' && raw.managedAgentId.length > 0) {
      definition.managedAgentId = raw.managedAgentId;
    }
    if (typeof raw.model === 'string' && raw.model.length > 0) definition.model = raw.model;
    const modelSpeed = parseModelSpeed(raw.modelSpeed ?? raw.model_speed ?? raw.speed);
    if (modelSpeed !== undefined) definition.modelSpeed = modelSpeed;
    const effort = parseModelEffort(raw.effort);
    if (effort !== undefined) definition.effort = effort;
    if (Array.isArray(raw.tools) && raw.tools.every((tool) => typeof tool === 'string')) {
      definition.tools = raw.tools;
    }
    if (
      Array.isArray(raw.disallowedTools) &&
      raw.disallowedTools.every((tool) => typeof tool === 'string')
    ) {
      definition.disallowedTools = raw.disallowedTools;
    }
    agents[name] = definition;
  }
  return Object.keys(agents).length > 0 ? agents : undefined;
}

function parseForwardSubagentText(value: unknown): boolean | undefined {
  return typeof value === 'boolean' ? value : undefined;
}

function parseCustomTools(value: unknown): CustomToolDefinition[] | undefined {
  if (!Array.isArray(value)) return undefined;
  const customTools: CustomToolDefinition[] = [];
  for (const raw of value) {
    if (!isRecord(raw) || typeof raw.name !== 'string' || raw.name.length === 0) continue;
    const customTool: CustomToolDefinition = { name: raw.name };
    if (typeof raw.description === 'string') customTool.description = raw.description;
    if (isRecord(raw.input_schema)) customTool.input_schema = raw.input_schema;
    customTools.push(customTool);
  }
  return customTools.length > 0 ? customTools : undefined;
}

/** Serialize one stored event as a single SSE `data:` record. */
function writeSseEvent(res: ServerResponse, event: StoredEvent): void {
  res.write(`data: ${JSON.stringify(event)}\n\n`);
}

/**
 * Adapt a `core` event-factory result to the event store's `BareEvent`.
 *
 * The factories in `core.ts` return precise, closed discriminated-union types
 * (e.g. `UserMessageEvent`); the store models a bare event as an open
 * `{ type; [k]: unknown }`. A factory result IS a bare event — the only gap is
 * the index signature — so widen it here, in one place, rather than scattering
 * the conversion across call sites.
 */
function toBareEvent(event: { type: string }): BareEvent {
  return event as BareEvent;
}

// ── handlers ────────────────────────────────────────────────────────────────

/** GET /v1/harnesses — advertise the harnesses this server can spawn. */
export const listHarnesses: RouteHandler = (_req: HarnessRequest, res: ServerResponse): void => {
  const data = listProviderMetadata().map((harness: ProviderMetadata) => ({ id: harness.id }));
  sendJson(res, 200, { object: 'list', data });
};

/**
 * POST /v1/sessions — body `{ agent, model?, replay? }`.
 *
 * Requires a non-empty `agent` (400 otherwise). Resolves spawn args via
 * `resolveHarness`, threading any `model` / `replay` through so the session
 * manager can inject the replay as a preamble. Creates the session record, then
 * starts the subprocess. Responds 201 with the created session.
 */
export const createSession: RouteHandler = (
  req: HarnessRequest,
  res: ServerResponse,
  ctx: HarnessContext,
): void => {
  const body = asRecord(req.body) as CreateSessionBody;

  const agent = body.agent;
  if (typeof agent !== 'string' || agent.length === 0) {
    throw new HttpError(400, 'agent is required');
  }

  const model = parseModel(body.model);
  const modelSpeed = parseModelSpeed(body.modelSpeed ?? body.model_speed);
  const modelEffort = parseModelEffort(body.modelEffort ?? body.model_effort);
  const replay = parseReplay(body.replay);
  const agents = parseAgents(body.agents);
  const forwardSubagentText = parseForwardSubagentText(body.forwardSubagentText);
  const customTools = parseCustomTools(body.customTools ?? body.custom_tools);
  const systemPrompt = parseString(body.systemPrompt ?? body.system_prompt);
  const tools = parseStringArray(body.tools);
  const allowedTools = parseStringArray(body.allowedTools ?? body.allowed_tools);
  const runtimeTools = parseStringArray(body.runtimeTools);

  try {
    const writePolicy = parseSandboxWritePolicy(ctx.env);
    if (writePolicy) probeSandboxWritePolicy(writePolicy);
  } catch (error) {
    throw new HttpError(
      503,
      error instanceof Error ? error.message : 'sandbox write policy is unavailable',
    );
  }

  // Pass `model` / `replay` only when present: under exactOptionalPropertyTypes
  // an explicit `undefined` is not assignable to an optional parameter, and
  // omission is what the resume seam expects for "no prior history".
  const spawnArgs: SpawnArgs = resolveHarness(
    agent,
    model,
    replay,
    agents,
    forwardSubagentText,
    customTools,
    systemPrompt,
    tools,
    allowedTools,
    runtimeTools,
    modelSpeed,
    modelEffort,
  );

  const session = ctx.sessionStore.create({ agent });
  ctx.spawnManagedSession(session.id, spawnArgs).start();

  sendJson(res, 201, session);
};

/** GET /v1/sessions/:id — fetch one session record (404 if unknown). */
export const getSession: RouteHandler = (
  _req: HarnessRequest,
  res: ServerResponse,
  ctx: HarnessContext,
  params: RouteParams,
): void => {
  const session = ctx.sessionStore.get(sessionId(params));
  if (!session) throw new HttpError(404, 'session not found');
  sendJson(res, 200, session);
};

/**
 * DELETE /v1/sessions/:id — tear a session down.
 *
 * Kills the runtime subprocess, drops the runtime handle, clears the event
 * history, and removes the session record. 404 if the session is unknown.
 */
export const deleteSession: RouteHandler = (
  _req: HarnessRequest,
  res: ServerResponse,
  ctx: HarnessContext,
  params: RouteParams,
): void => {
  const id = sessionId(params);
  if (!ctx.sessionStore.get(id)) throw new HttpError(404, 'session not found');

  ctx.getRuntime(id)?.kill();
  ctx.deleteRuntime(id);
  ctx.eventStore.deleteSession(id);
  ctx.sessionStore.delete(id);

  sendJson(res, 200, { id, object: 'session', deleted: true });
};

/**
 * POST /v1/sessions/:id/events — fire-and-forget user turns.
 *
 * 404 if the session is unknown. A dead/absent runtime cannot deliver the turn,
 * so we surface 409 instead of returning a misleading `{ ok: true }` over an
 * undelivered `user.message`.
 *
 * For each `user.message` event we publish the user message and flip status to
 * `running` BEFORE awaiting delivery, so the SSE stream and `GET .../events`
 * reflect the turn immediately. Delivery itself is fire-and-forget; a rejection
 * is reported as a `session.status_error` event (whose publish also flips the
 * record to `error` via the session-manager emit wiring).
 */
export const sendEvent: RouteHandler = (
  req: HarnessRequest,
  res: ServerResponse,
  ctx: HarnessContext,
  params: RouteParams,
): void => {
  const id = sessionId(params);
  if (!ctx.sessionStore.get(id)) throw new HttpError(404, 'session not found');

  const runtime = ctx.getRuntime(id);
  if (!runtime || !runtime.isAlive()) {
    throw new HttpError(409, 'session runtime is not available');
  }

  const body = asRecord(req.body) as SendEventBody;
  const events: unknown[] = Array.isArray(body.events) ? body.events : [];

  for (const raw of events) {
    const ev = asRecord(raw) as InboundEvent;
    if (ev.type === 'user.message') {
      const content = ev.content as MessageContent | undefined;

      ctx.eventStore.publish(id, toBareEvent(userMessageEvent(content ?? [])));
      ctx.sessionStore.setStatus(id, 'running');

      // Fire-and-forget: output streams back via SSE / history. A delivery failure
      // becomes a session.status_error event (and an "error" status).
      void runtime.sendUserMessage(content ?? []).catch((err: unknown) => {
        const message = err instanceof Error ? err.message : String(err);
        ctx.sessionStore.setStatus(id, 'error');
        ctx.eventStore.publish(
          id,
          toBareEvent(sessionErrorEvent(`failed to deliver message: ${message}`)),
        );
      });
      continue;
    }
    if (ev.type === 'user.custom_tool_result') {
      ctx.sessionStore.setStatus(id, 'running');
      void runtime.sendCustomToolResult(ev).catch((err: unknown) => {
        const message = err instanceof Error ? err.message : String(err);
        ctx.sessionStore.setStatus(id, 'error');
        ctx.eventStore.publish(
          id,
          toBareEvent(sessionErrorEvent(`failed to deliver custom tool result: ${message}`)),
        );
      });
    }
  }

  sendJson(res, 200, { ok: true });
};

/** GET /v1/sessions/:id/events — the full event history (404 if unknown). */
export const listEvents: RouteHandler = (
  _req: HarnessRequest,
  res: ServerResponse,
  ctx: HarnessContext,
  params: RouteParams,
): void => {
  const id = sessionId(params);
  if (!ctx.sessionStore.get(id)) throw new HttpError(404, 'session not found');
  sendJson(res, 200, { object: 'list', data: ctx.eventStore.list(id) });
};

/**
 * GET /v1/sessions/:id/events/stream — replay history, then live SSE.
 *
 * Writes the SSE headers, replays the full stored history first, and only THEN
 * subscribes to live events. That order — with no interleaving point between the
 * replay loop and the subscribe call — guarantees a consumer (including the
 * host that persists the stream) sees every event exactly once, with no gap between
 * the historical tail and the live head. On client disconnect we unsubscribe and
 * end the response.
 */
export const streamEvents: RouteHandler = (
  req: HarnessRequest,
  res: ServerResponse,
  ctx: HarnessContext,
  params: RouteParams,
): void => {
  const id = sessionId(params);
  if (!ctx.sessionStore.get(id)) throw new HttpError(404, 'session not found');

  res.writeHead(200, {
    'content-type': 'text/event-stream',
    'cache-control': 'no-cache',
    connection: 'keep-alive',
  });

  // 1) Replay everything already recorded, in insertion order.
  for (const event of ctx.eventStore.list(id)) writeSseEvent(res, event);

  // 2) THEN go live. Subscribing after the replay (and never between an
  //    individual replayed event) is what makes the handoff gap-free.
  const unsubscribe = ctx.eventStore.subscribe(id, (event) => writeSseEvent(res, event));

  req.on('close', () => {
    unsubscribe();
    try {
      res.end();
    } catch {
      // already closed
    }
  });

  // Intentionally do not end the response here: the stream stays open until the
  // client disconnects.
};

/** Internal SDK control channel; command payloads (credentials/checkpoints) never enter history. */
export const sdkCommand: RouteHandler = async (req, res, ctx, params) => {
  const id = sessionId(params);
  if (!ctx.sessionStore.get(id)) throw new HttpError(404, 'session not found');
  const runtime = ctx.getRuntime(id);
  if (!runtime?.isAlive() || !runtime.sendSdkCommand)
    throw new HttpError(409, 'SDK command channel unavailable');
  const body = asRecord(req.body);
  if (body.afterSequence !== undefined) {
    if (!Number.isSafeInteger(body.afterSequence) || (body.afterSequence as number) < 0)
      throw new HttpError(400, 'invalid SDK event acknowledgement');
    ctx.eventStore.acknowledgeSdkEvents(id, body.afterSequence as number);
  }
  // Turns can wait on a client tool result for minutes. Whitespace keeps this
  // JSON response alive without accepting the command before the child does.
  res.writeHead(200, { 'content-type': 'application/json' });
  res.write(' ');
  const heartbeat = setInterval(() => res.write(' '), 10_000);
  try {
    const sequence = await runtime.sendSdkCommand(body.command);
    res.end(JSON.stringify({ ok: true, sequence }));
  } catch {
    res.end(JSON.stringify({ ok: false, error: 'SDK command failed' }));
  } finally {
    clearInterval(heartbeat);
  }
};

// ── params ──────────────────────────────────────────────────────────────────

/**
 * Extract the `:id` path param. The router only ever matches routes whose
 * pattern carries `:id`, so it is present for every handler here; this helper
 * keeps that assumption in one place under `noUncheckedIndexedAccess`.
 */
function sessionId(params: RouteParams): string {
  return params['id'] ?? '';
}

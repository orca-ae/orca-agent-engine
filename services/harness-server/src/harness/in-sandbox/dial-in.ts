// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * DialInTransport — HarnessTransport that dials a live @orca/sandbox-harness
 * HTTP endpoint.
 *
 * Wire contract (from @orca/sandbox-harness):
 *   POST   /v1/sessions                      → 201 { id, object, agent, status, created_at }
 *   POST   /v1/sessions/:id/events            → 200 { ok: true }
 *   GET    /v1/sessions/:id/events/stream     → SSE; data: <JSON StoredEvent>\n\n
 *   DELETE /v1/sessions/:id                   → 200
 *
 * The SSE stream REPLAYS FULL HISTORY on every (re)connect, so this transport
 * maintains a bounded Set<string> of recently seen event ids to deduplicate
 * across reconnects. That is the single most important correctness property
 * here — a reconnect MUST NOT double-emit events to upstream consumers, while
 * long-running sessions must not retain an unbounded id set.
 */

import type {
  HarnessChannel,
  HarnessTransport,
  OpenSessionOptions,
  RawSandboxEvent,
} from './transport.js';
import type { UserEvent } from '../agent-harness.js';

export interface DialInTransportOptions {
  /** Base URL of the in-sandbox harness server (e.g. `http://127.0.0.1:9000`). */
  baseUrl: string;
  /** Optional headers forwarded on every request (e.g. Authorization). */
  headers?: Record<string, string>;
}

/** Minimal delay between SSE reconnect attempts (ms). */
const RECONNECT_DELAY_MS = 100;
const HARNESS_READY_TIMEOUT_MS = 30_000;
const HARNESS_READY_POLL_MS = 100;
const HARNESS_READY_REQUEST_TIMEOUT_MS = 1_000;
const MAX_SEEN_EVENT_IDS = 10_000;

function rememberSeen(seen: Set<string>, eventId: string): boolean {
  if (seen.has(eventId)) return false;
  seen.add(eventId);
  if (seen.size > MAX_SEEN_EVENT_IDS) {
    const oldest = seen.values().next().value;
    if (typeof oldest === 'string') seen.delete(oldest);
  }
  return true;
}

export class DialInTransport implements HarnessTransport {
  private readonly baseUrl: string;
  private readonly headers: Record<string, string>;

  constructor(opts: DialInTransportOptions) {
    this.baseUrl = opts.baseUrl.replace(/\/$/, '');
    this.headers = opts.headers ?? {};
  }

  async open(opts: OpenSessionOptions): Promise<HarnessChannel> {
    // OpenSandbox exposes the requested port before the image entrypoint has
    // necessarily finished creating its bubblewrap namespace and starting the
    // HTTP server. Probe the idempotent health endpoint before issuing the
    // non-idempotent session-creation request.
    await waitForHarnessReady(this.baseUrl, this.headers);

    const body: Record<string, unknown> = { agent: opts.agent };
    if (opts.model !== undefined) body['model'] = opts.model;
    if (opts.modelSpeed !== undefined) body['modelSpeed'] = opts.modelSpeed;
    if (opts.modelEffort !== undefined) body['modelEffort'] = opts.modelEffort;
    if (opts.systemPrompt !== undefined) body['systemPrompt'] = opts.systemPrompt;
    if (opts.tools !== undefined) body['tools'] = opts.tools;
    if (opts.allowedTools !== undefined) body['allowedTools'] = opts.allowedTools;
    if (opts.runtimeTools !== undefined) body['runtimeTools'] = opts.runtimeTools;
    if (opts.replay !== undefined) body['replay'] = opts.replay;
    if (opts.agents !== undefined) body['agents'] = opts.agents;
    if (opts.forwardSubagentText !== undefined) {
      body['forwardSubagentText'] = opts.forwardSubagentText;
    }
    if (opts.customTools !== undefined) body['customTools'] = opts.customTools;

    const resp = await fetch(`${this.baseUrl}/v1/sessions`, {
      method: 'POST',
      headers: { ...this.headers, 'content-type': 'application/json' },
      body: JSON.stringify(body),
    });

    if (!resp.ok) {
      throw new Error(`POST /v1/sessions failed: ${resp.status} ${await resp.text()}`);
    }

    const session = (await resp.json()) as { id: string };
    const sessionId = session.id;
    const baseUrl = this.baseUrl;
    const commonHeaders = this.headers;

    // Shared abort controller so stop() cancels the in-flight SSE fetch.
    const abort = new AbortController();
    // Track recent event ids across reconnects — THE key dedup set.
    const seen = new Set<string>();

    const channel: HarnessChannel = {
      async *events(): AsyncIterable<RawSandboxEvent> {
        while (!abort.signal.aborted) {
          let reader: ReadableStreamDefaultReader<Uint8Array> | undefined;
          let decoder: TextDecoder | undefined;
          let buffer = '';

          try {
            const sseResp = await fetch(`${baseUrl}/v1/sessions/${sessionId}/events/stream`, {
              method: 'GET',
              headers: { ...commonHeaders, accept: 'text/event-stream' },
              signal: abort.signal,
            });

            if (!sseResp.ok || !sseResp.body) {
              // Non-2xx or no body: wait and reconnect.
              await delay(RECONNECT_DELAY_MS, abort.signal);
              continue;
            }

            reader = sseResp.body.getReader();
            decoder = new TextDecoder();

            // Read chunks, accumulate text, split on blank lines (SSE record delimiter).
            while (true) {
              let result: ReadableStreamReadResult<Uint8Array>;
              try {
                result = await reader.read();
              } catch {
                // Stream read error (e.g. connection reset) — reconnect.
                break;
              }

              if (result.done) {
                // Server closed the stream (e.g. for reconnect test) — reconnect.
                break;
              }

              buffer += decoder.decode(result.value, { stream: true });

              // Process complete SSE records (delimited by \n\n).
              let boundary: number;
              while ((boundary = buffer.indexOf('\n\n')) !== -1) {
                const record = buffer.slice(0, boundary);
                buffer = buffer.slice(boundary + 2);

                // Extract the `data:` line from the record.
                for (const line of record.split('\n')) {
                  if (!line.startsWith('data:')) continue;
                  const raw = line.slice(5).trim();
                  if (!raw) continue;

                  let event: RawSandboxEvent;
                  try {
                    event = JSON.parse(raw) as RawSandboxEvent;
                  } catch {
                    // Malformed JSON: skip.
                    continue;
                  }

                  // DEDUP: the sandbox-harness replays full history on every
                  // reconnect, so we must skip any id we've already delivered.
                  if (!rememberSeen(seen, event.id)) continue;

                  yield event;
                }
              }
            }
          } catch (err) {
            // Abort means stop() was called — do not reconnect.
            if (abort.signal.aborted) return;
            // Other error: brief pause, then reconnect.
            void err;
          } finally {
            // Always release the reader on exit.
            try {
              await reader?.cancel();
            } catch {
              // ignore
            }
          }

          if (abort.signal.aborted) return;

          // Brief back-off before reconnect.
          await delay(RECONNECT_DELAY_MS, abort.signal);
        }
      },

      async submit(event: UserEvent): Promise<void> {
        const resp = await fetch(`${baseUrl}/v1/sessions/${sessionId}/events`, {
          method: 'POST',
          headers: { ...commonHeaders, 'content-type': 'application/json' },
          body: JSON.stringify({ events: [wireEventOf(event)] }),
        });
        if (!resp.ok) {
          throw new Error(`POST /v1/sessions/${sessionId}/events failed: ${resp.status}`);
        }
      },

      async sdkCommand(command: unknown, afterSequence?: number): Promise<number> {
        const response = await fetch(`${baseUrl}/v1/sessions/${sessionId}/sdk-command`, {
          method: 'POST',
          headers: { ...commonHeaders, 'content-type': 'application/json' },
          body: JSON.stringify({ command, afterSequence }),
          signal: AbortSignal.any([abort.signal, AbortSignal.timeout(660_000)]),
        });
        if (!response.ok)
          throw new Error(`SDK command failed: ${response.status} ${await response.text()}`);
        const result = (await response.json()) as { ok?: boolean; sequence?: number };
        if (result.ok !== true || !Number.isSafeInteger(result.sequence) || result.sequence! < 0)
          throw new Error('SDK command was not acknowledged');
        return result.sequence!;
      },

      async stop(): Promise<void> {
        abort.abort();
        // Best-effort DELETE.
        try {
          await fetch(`${baseUrl}/v1/sessions/${sessionId}`, {
            method: 'DELETE',
            signal: AbortSignal.timeout(5_000),
            headers: { ...commonHeaders },
          });
        } catch {
          // Ignore: the sandbox may already be down.
        }
      },
    };

    return channel;
  }
}

// ── helpers ──────────────────────────────────────────────────────────────────

async function waitForHarnessReady(
  baseUrl: string,
  headers: Record<string, string>,
): Promise<void> {
  const deadline = Date.now() + HARNESS_READY_TIMEOUT_MS;
  let lastError: unknown = new Error('no health response received');

  while (Date.now() < deadline) {
    let response: Response;
    try {
      const remainingMs = Math.max(1, deadline - Date.now());
      response = await fetch(`${baseUrl}/healthz`, {
        method: 'GET',
        headers,
        signal: AbortSignal.timeout(Math.min(HARNESS_READY_REQUEST_TIMEOUT_MS, remainingMs)),
      });
    } catch (error) {
      lastError = error;
      await sleepBeforeReadyRetry(deadline);
      continue;
    }

    if (response.ok) {
      await response.arrayBuffer();
      return;
    }

    const detail = (await response.text()).trim().slice(0, 256);
    const error = new Error(`GET /healthz failed: ${response.status}${detail ? ` ${detail}` : ''}`);
    if (response.status >= 400 && response.status < 500) {
      throw error;
    }
    lastError = error;
    await sleepBeforeReadyRetry(deadline);
  }

  throw new Error(
    `sandbox harness did not become ready within ${HARNESS_READY_TIMEOUT_MS}ms: ${errorMessage(lastError)}`,
  );
}

async function sleepBeforeReadyRetry(deadline: number): Promise<void> {
  const remainingMs = deadline - Date.now();
  if (remainingMs <= 0) return;
  await new Promise((resolve) => setTimeout(resolve, Math.min(HARNESS_READY_POLL_MS, remainingMs)));
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

/**
 * Extract a content string (or ContentBlock array) from a UserEvent payload.
 * Mirrors the textOf() helper in the Claude harness but returns the value the
 * sandbox-harness expects for `user.message.content`:  we pass the raw content
 * array when available, or a plain string as fallback.
 */
function contentOf(payload: unknown): string | Array<{ type: string; text?: string }> {
  if (!payload || typeof payload !== 'object') return '';
  const arr = (payload as { content?: unknown }).content;
  if (Array.isArray(arr)) return arr as Array<{ type: string; text?: string }>;
  if (typeof arr === 'string') return arr;
  return '';
}

function wireEventOf(event: UserEvent): Record<string, unknown> {
  if (event.kind === 'user.custom_tool_result') {
    return { type: event.kind, ...recordOf(event.payload) };
  }
  if (event.kind === 'user.message') {
    return { type: event.kind, content: contentOf(event.payload) };
  }
  throw new Error(`DialInTransport does not support ${event.kind}`);
}

function recordOf(value: unknown): Record<string, unknown> {
  return value && typeof value === 'object' && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
}

/**
 * Promise that resolves after `ms` milliseconds, but rejects immediately when
 * the AbortSignal fires — so `stop()` cancels a reconnect delay.
 */
function delay(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    if (signal.aborted) {
      reject(new DOMException('Aborted', 'AbortError'));
      return;
    }
    const timer = setTimeout(resolve, ms);
    signal.addEventListener(
      'abort',
      () => {
        clearTimeout(timer);
        reject(new DOMException('Aborted', 'AbortError'));
      },
      { once: true },
    );
  }).catch((err: unknown) => {
    if ((err as { name?: string })?.name === 'AbortError') return; // stop() called
    throw err;
  });
}

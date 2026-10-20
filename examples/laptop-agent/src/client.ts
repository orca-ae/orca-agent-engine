// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// The entire Orca Agent Engine surface this example uses, in one file.
//
// It is deliberately dependency-free -- `fetch` and the standard library, no SDK,
// nothing from `@orca/*`. Copy it into an unrelated codebase and it will work. That
// is the point of the example: the engine is an ordinary HTTP API, and driving an
// agent on your own hardware does not need a client library.
//
// Two headers matter, and one of them is easy to get wrong:
//
//   `x-api-key`   the workspace key. It must start with `orca_` -- the registry
//                 rejects anything else without a database round-trip, so a key in
//                 the wrong format fails as 401, not as "no such key".
//
//   `orca-beta`   NOT optional here. The default stream and event list are filtered
//                 to the documented Anthropic session-event vocabulary, and the two
//                 frames this client is built on are not in it: `agent.turn_completed`
//                 (the turn boundary) and `agent.requires_action` (the tool-approval
//                 gate) are Orca runner signals. Without the header a turn never
//                 appears to finish and an approval request never arrives -- the
//                 client hangs rather than erroring, which is the worst way to learn
//                 about a missing header. It also gates the one-time `env_key` on
//                 environment create and the `distribution_state` / `runner_id`
//                 fields that show a session was actually picked up.
//
// Note the two SDKs published for this API send `Authorization: Bearer` instead of
// `x-api-key`. Against a local engine that routes to the OIDC verifier and 401s with
// no issuer configured, so this file uses the header the registry's own tests use.

/** Where the engine is and who we are. */
export interface EngineConfig {
  /** e.g. `http://localhost:8080`. No trailing slash. */
  readonly baseUrl: string;
  /** Workspace api key. Starts with `orca_`. */
  readonly apiKey: string;
}

/** One frame off the SSE stream, plus the cursor it arrived with. */
export interface StreamFrame {
  readonly type: string;
  /** The SSE `id:` line -- the resume cursor. See `streamSession`. */
  readonly cursor: string | null;
  readonly body: Record<string, unknown>;
}

export interface CreatedEnvironment {
  readonly id: string;
  /**
   * Echoed exactly once, at create, and never persisted in recoverable form. Lose it
   * and the only way to attach a worker is `POST /v1/environments/:id/rotate-key`.
   */
  readonly envKey: string | null;
}

export interface SessionState {
  readonly id: string;
  readonly status: string;
  /** `pending` while the registry is launching a runner, `assigned` once one dialed in. */
  readonly distributionState: string | null;
  /** The runner that took the work -- the visible proof your machine picked it up. */
  readonly runnerId: string | null;
}

export interface WorkStats {
  readonly workerConnected: boolean;
  readonly depth: number;
}

/** Thrown for any non-2xx, carrying enough to diagnose without a debugger. */
export class EngineError extends Error {
  constructor(
    readonly method: string,
    readonly path: string,
    readonly status: number,
    readonly body: string,
  ) {
    super(`${method} ${path} -> ${status}: ${body.slice(0, 400)}`);
    this.name = 'EngineError';
  }
}

export class EngineClient {
  constructor(private readonly cfg: EngineConfig) {}

  private headers(extra: Record<string, string> = {}): Record<string, string> {
    return {
      'x-api-key': this.cfg.apiKey,
      'orca-beta': 'true',
      ...extra,
    };
  }

  private async call(
    method: string,
    path: string,
    body?: unknown,
  ): Promise<Record<string, unknown>> {
    const res = await fetch(`${this.cfg.baseUrl}${path}`, {
      method,
      headers: this.headers(body === undefined ? {} : { 'content-type': 'application/json' }),
      ...(body === undefined ? {} : { body: JSON.stringify(body) }),
    });
    const text = await res.text();
    if (res.status < 200 || res.status >= 300) {
      throw new EngineError(method, path, res.status, text);
    }
    return text.length > 0 ? (JSON.parse(text) as Record<string, unknown>) : {};
  }

  /**
   * `POST /v1/agents` -- returns 200, not 201. Anthropic's shape.
   *
   * `metadata.harness` + `metadata.mode` are the whole of the harness selection. The
   * registry validates the pair on write and rejects an unsupported combination with
   * a 400, so a typo here fails at create rather than at the first turn.
   */
  async createAgent(input: {
    name: string;
    model: string;
    system?: string;
    harness: string;
    mode: string;
    tools?: unknown[];
  }): Promise<string> {
    const out = await this.call('POST', '/v1/agents', {
      name: input.name,
      model: input.model,
      ...(input.system === undefined ? {} : { system: input.system }),
      tools: input.tools ?? [],
      mcp_servers: [],
      skills: [],
      metadata: { harness: input.harness, mode: input.mode },
    });
    return out['id'] as string;
  }

  /**
   * `POST /v1/environments` with `target: self_hosted`.
   *
   * The response carries `env_key` ONLY for an `orca-beta` caller, which is why this
   * client always sends the header.
   */
  async createSelfHostedEnvironment(name: string): Promise<CreatedEnvironment> {
    const out = await this.call('POST', '/v1/environments', {
      name,
      target: 'self_hosted',
      egress_mode: 'sidecar',
    });
    return { id: out['id'] as string, envKey: (out['env_key'] as string | undefined) ?? null };
  }

  /**
   * `POST /v1/sessions`.
   *
   * The environment MUST be one a worker has claimed. Point a session at an
   * environment with no worker and it sits `pending` for ever -- there is no error,
   * because "no worker yet" is a legitimate transient state.
   */
  async createSession(agentId: string, environmentId: string): Promise<SessionState> {
    const out = await this.call('POST', '/v1/sessions', {
      agent_id: agentId,
      environment_id: environmentId,
    });
    return toSessionState(out);
  }

  async getSession(sessionId: string): Promise<SessionState> {
    return toSessionState(await this.call('GET', `/v1/sessions/${encodeURIComponent(sessionId)}`));
  }

  async deleteSession(sessionId: string): Promise<void> {
    await this.call('DELETE', `/v1/sessions/${encodeURIComponent(sessionId)}`);
  }

  /**
   * `GET /v1/sessions/:id/events` -- the PERSISTED transcript, from a cursor.
   *
   * The stream is the live view; this is the durable one, and the two are not
   * interchangeable. A turn that completes faster than the SSE subscribe round-trip
   * can be missed entirely: the catch-up read runs before the append commits, and the
   * stream then tails live from a point already past it. Reading the transcript is how
   * a client makes itself independent of that race.
   *
   * Both projections return `{ data, next_page }` — `orca-beta` adds `has_more`, it
   * does not rename the array.
   */
  async listEvents(sessionId: string, fromCursor?: string): Promise<StreamFrame[]> {
    const qs = fromCursor === undefined ? '' : `&from_cursor=${encodeURIComponent(fromCursor)}`;
    const out = await this.call(
      'GET',
      `/v1/sessions/${encodeURIComponent(sessionId)}/events?limit=1000${qs}`,
    );
    const rows = Array.isArray(out['data']) ? (out['data'] as Array<Record<string, unknown>>) : [];
    return rows.map((body) => ({
      type: String(body['type'] ?? ''),
      cursor: body['seq'] === undefined ? null : String(body['seq']),
      body,
    }));
  }

  /** `GET /v1/environments/:id/work_stats` -- the public read of worker liveness. */
  async workStats(environmentId: string): Promise<WorkStats> {
    const out = await this.call(
      'GET',
      `/v1/environments/${encodeURIComponent(environmentId)}/work_stats`,
    );
    return {
      workerConnected: out['worker_connected'] === true,
      depth: typeof out['depth'] === 'number' ? out['depth'] : 0,
    };
  }

  /** `POST /v1/sessions/:id/events` with one `user.message` -- this starts a turn. */
  async sendMessage(sessionId: string, text: string, requestId: string): Promise<void> {
    await this.call('POST', `/v1/sessions/${encodeURIComponent(sessionId)}/events`, {
      events: [{ type: 'user.message', content: [{ type: 'text', text }] }],
      request_id: requestId,
    });
  }

  /**
   * Answer an `agent.requires_action` gate.
   *
   * The field is `result`, not `decision`, and the input schema is `.strict()` -- an
   * undeclared key is a 400, not something the server ignores. `deny_message` is
   * valid only alongside a deny.
   */
  async confirmTool(
    sessionId: string,
    toolUseId: string,
    result: 'allow' | 'deny',
    denyMessage?: string,
  ): Promise<void> {
    const event: Record<string, unknown> = {
      type: 'user.tool_confirmation',
      tool_use_id: toolUseId,
      result,
    };
    if (result === 'deny' && denyMessage !== undefined) event['deny_message'] = denyMessage;
    await this.call('POST', `/v1/sessions/${encodeURIComponent(sessionId)}/events`, {
      events: [event],
    });
  }

  /**
   * `GET /v1/sessions/:id/events/stream` -- the live transcript, as SSE.
   *
   * The path is `/events/stream`. There is no `/v1/sessions/:id/stream`.
   *
   * The resume cursor is the SSE `id:` line, NOT a field in the JSON body: the
   * default projection strips `seq`, so a client that reads the cursor out of the
   * payload gets `undefined` and silently restarts from 0 on every reconnect. Pass
   * the last cursor back as `fromCursor` (or as a `Last-Event-ID` header, which takes
   * precedence) to resume. `from_cursor` is INCLUSIVE, so the frame at that cursor is
   * redelivered -- dedupe by cursor if that matters to you.
   */
  async *streamSession(
    sessionId: string,
    opts: { fromCursor?: string; signal?: AbortSignal } = {},
  ): AsyncGenerator<StreamFrame, void, void> {
    const url = new URL(
      `${this.cfg.baseUrl}/v1/sessions/${encodeURIComponent(sessionId)}/events/stream`,
    );
    url.searchParams.set('from_cursor', opts.fromCursor ?? '0');

    const res = await fetch(url, {
      method: 'GET',
      headers: this.headers({ accept: 'text/event-stream' }),
      ...(opts.signal === undefined ? {} : { signal: opts.signal }),
    });
    if (res.status !== 200 || res.body === null) {
      throw new EngineError('GET', url.pathname, res.status, await res.text());
    }

    const decoder = new TextDecoder();
    const reader = res.body.getReader();
    let buffered = '';

    // The `finally` is load-bearing, not tidiness. A consumer that stops early —
    // `break`, `return`, or a thrown error inside its `for await` — closes this
    // generator, and without an explicit cancel the reader keeps the response body,
    // and therefore the socket, open. Node then never exits: the process prints its
    // last line and hangs forever, which is exactly what `--once` did before this.
    try {
      for (;;) {
        const chunk = await reader.read();
        if (chunk.done) return;
        buffered += decoder.decode(chunk.value, { stream: true });

        // SSE frames are separated by a blank line. A block starting with `:` is a
        // heartbeat comment and carries nothing.
        let idx: number;
        while ((idx = buffered.indexOf('\n\n')) >= 0) {
          const block = buffered.slice(0, idx);
          buffered = buffered.slice(idx + 2);
          if (block.startsWith(':')) continue;

          let cursor: string | null = null;
          let data: string | null = null;
          for (const line of block.split('\n')) {
            if (line.startsWith('id: ')) cursor = line.slice(4);
            else if (line.startsWith('data: ')) data = line.slice(6);
          }
          if (data === null) continue;
          try {
            const body = JSON.parse(data) as Record<string, unknown>;
            yield { type: String(body['type'] ?? ''), cursor, body };
          } catch {
            // A frame we cannot parse is not worth killing the stream over.
          }
        }
      }
    } finally {
      await reader.cancel().catch(() => {});
      reader.releaseLock();
    }
  }
}

function toSessionState(out: Record<string, unknown>): SessionState {
  return {
    id: out['id'] as string,
    status: String(out['status'] ?? ''),
    distributionState: (out['distribution_state'] as string | undefined) ?? null,
    runnerId: (out['runner_id'] as string | undefined) ?? null,
  };
}

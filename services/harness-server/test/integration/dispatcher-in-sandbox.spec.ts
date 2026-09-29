// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * Hermetic e2e for the dispatcher's colocated branch.
 *
 * Proves end-to-end that a `colocated` agent is routed through:
 *   annotation → selectHarness(buildInSandbox) → InSandboxHarness
 *   → dispatcher shared sandbox/resource setup
 *   → sandboxRuntime.acquire(spec) [spec carries harness image + exposePorts]
 *   → SandboxHandle.endpoint(port) → fake wire server URL
 *   → events mapped + appended to transcript store
 *   → user.message echo DROPPED; the sandbox's session.status_idle frame fans
 *     out into agent.usage (diverted to the usage sink, NOT
 *     appended) + span.model_request_end + a terminal session.status_idle. The
 *     bridge also injects the leading session.status_running +
 *     span.model_request_start at submit time. So the APPENDED turn is:
 *     session.status_running, span.model_request_start, agent.message,
 *     span.model_request_end, session.status_idle{end_turn}.
 *   → valid raw sandbox IDs become Transcript Event.id and remain matching
 *     idempotencyKey metadata; fanned-out span/idle use derived Event.id values
 *     (`${frameId}:model_end`, `${frameId}:idle`) — the raw frame ID itself
 *     rides on the DIVERTED agent.usage.
 *
 * No real child processes, no LLM, no cloud services. All traffic 127.0.0.1.
 */

import { describe, it, expect, afterEach } from 'vitest';
import { createServer, type Server, type ServerResponse, type IncomingMessage } from 'node:http';
import { Readable } from 'node:stream';
import type { AddressInfo } from 'node:net';
import { isAgentEventId, isAgentEventSubpath } from '@orca/agent-event-contract';
import { Dispatcher, type SessionEventSource } from '../../src/runner/dispatcher.js';
import type { Event, ReadOptions, TailOptions, TranscriptStore } from '@orca/transcript-store';
import type { FileStore } from '@orca/file-store';
import type {
  EnvironmentSpec,
  SandboxHandle,
  SandboxRuntime,
  ToolCall,
  ToolResult,
} from '../../src/sandbox/sandbox-runtime.js';
import type {
  AgentRecord,
  PreparedExecutionV2,
  RegistryClient,
  SessionRecord,
} from '../../src/clients/registry.js';
import type { RawSandboxEvent } from '../../src/harness/in-sandbox/transport.js';
import type {
  AgentEvent,
  AgentEventInput,
  AgentHarness,
  SessionStartInput,
  TerminationReason,
  UserEvent,
} from '../../src/harness/agent-harness.js';
import { withCanonicalAgentEventEnvelope } from '../../src/harness/agent-harness.js';

// ── fake sandbox-harness wire server ─────────────────────────────────────────

/** Build a stamped sandbox event as the sandbox-harness wire emits. */
function makeEvent(
  id: string,
  sessionId: string,
  type: string,
  extra: Record<string, unknown> = {},
): RawSandboxEvent {
  return {
    id,
    session_id: sessionId,
    created_at: new Date().toISOString(),
    type,
    ...extra,
  };
}

/** Read and JSON-parse a POST body from an IncomingMessage. */
function readBody(req: IncomingMessage): Promise<unknown> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    req.on('data', (c: Buffer) => chunks.push(c));
    req.on('end', () => {
      try {
        resolve(JSON.parse(Buffer.concat(chunks).toString('utf8') || '{}'));
      } catch (e) {
        reject(e);
      }
    });
    req.on('error', reject);
  });
}

/** Write an SSE data line. */
function writeSse(res: ServerResponse, event: RawSandboxEvent): void {
  res.write(`data: ${JSON.stringify(event)}\n\n`);
}

interface FakeWireServer {
  baseUrl: string;
  port: number;
  server: Server;
  /** Captured POST /v1/sessions request body. */
  capturedSessionBody: unknown;
  close: () => Promise<void>;
}

/**
 * Minimal in-process fake sandbox-harness wire server.
 * Implements: POST /v1/sessions, POST /v1/sessions/:id/events, GET stream SSE, DELETE.
 * Scripted to emit: user.message echo → agent.message → session.status_idle.
 */
function startFakeWireServer(sessionEvents: RawSandboxEvent[]): Promise<FakeWireServer> {
  return new Promise((resolve) => {
    let sessionId: string | null = null;
    let capturedSessionBody: unknown = undefined;

    const server = createServer(async (req, res) => {
      const url = req.url ?? '/';
      const method = req.method ?? 'GET';

      if (method === 'GET' && url === '/healthz') {
        res.writeHead(200, { 'content-type': 'application/json' });
        res.end(JSON.stringify({ status: 'ok' }));
        return;
      }

      // POST /v1/sessions → create session
      if (method === 'POST' && url === '/v1/sessions') {
        capturedSessionBody = await readBody(req);
        sessionId = `session_insandbox_${Date.now()}`;
        res.writeHead(201, { 'content-type': 'application/json' });
        res.end(
          JSON.stringify({
            id: sessionId,
            object: 'session',
            agent: 'claude',
            status: 'idle',
            created_at: new Date().toISOString(),
          }),
        );
        return;
      }

      // POST /v1/sessions/:id/events → accept user message
      if (method === 'POST' && sessionId && url === `/v1/sessions/${sessionId}/events`) {
        await readBody(req);
        res.writeHead(200, { 'content-type': 'application/json' });
        res.end(JSON.stringify({ ok: true }));
        return;
      }

      // GET /v1/sessions/:id/events/stream → SSE
      if (method === 'GET' && sessionId && url === `/v1/sessions/${sessionId}/events/stream`) {
        res.writeHead(200, {
          'content-type': 'text/event-stream',
          'cache-control': 'no-cache',
          connection: 'keep-alive',
        });

        for (const ev of sessionEvents) {
          writeSse(res, ev);
        }

        // Keep open until client disconnects
        req.on('close', () => {
          try {
            res.end();
          } catch {
            /* already ended */
          }
        });
        return;
      }

      // DELETE /v1/sessions/:id → tear down
      if (method === 'DELETE' && sessionId && url === `/v1/sessions/${sessionId}`) {
        res.writeHead(200, { 'content-type': 'application/json' });
        res.end(JSON.stringify({ id: sessionId, object: 'session', deleted: true }));
        return;
      }

      res.writeHead(404, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ error: 'not found', url, method }));
    });

    server.listen({ host: '127.0.0.1', port: 0 }, () => {
      const addr = server.address() as AddressInfo;
      const srv: FakeWireServer = {
        baseUrl: `http://127.0.0.1:${addr.port}`,
        port: addr.port,
        server,
        get capturedSessionBody() {
          return capturedSessionBody;
        },
        close: () =>
          new Promise<void>((r, j) => {
            server.closeAllConnections();
            server.close((e) => (e ? j(e) : r()));
          }),
      };
      resolve(srv);
    });
  });
}

// ── fake sandbox runtime ──────────────────────────────────────────────────────

class FakeInSandboxRuntime implements SandboxRuntime {
  readonly capabilities = { supportsFuse: false };
  readonly acquiredSpecs: EnvironmentSpec[] = [];
  readonly acquiredHandles: FakeInSandboxHandle[] = [];
  private serverUrl: string;
  private serverPort: number;
  private outputFiles: Record<string, Buffer>;
  private requiredFileBeforeEndpoint: { path: string; content: Buffer } | undefined;

  constructor(
    serverUrl: string,
    serverPort: number,
    outputFiles: Record<string, Buffer> = {},
    requiredFileBeforeEndpoint?: { path: string; content: Buffer },
  ) {
    this.serverUrl = serverUrl;
    this.serverPort = serverPort;
    this.outputFiles = outputFiles;
    this.requiredFileBeforeEndpoint = requiredFileBeforeEndpoint;
  }

  async acquire(spec: EnvironmentSpec): Promise<SandboxHandle> {
    // Assert the spec carries the harness image + exposePorts (proves the harness image was resolved).
    expect(spec.image).toBeDefined();
    expect(typeof spec.image).toBe('string');
    expect(spec.image!.length).toBeGreaterThan(0);
    expect(spec.exposePorts).toBeDefined();
    expect(Array.isArray(spec.exposePorts)).toBe(true);
    expect(spec.exposePorts!).toContain(4096);

    this.acquiredSpecs.push(spec);
    const serverUrl = this.serverUrl;
    const serverPort = this.serverPort;
    const handle = new FakeInSandboxHandle(
      `sbx_insandbox_1`,
      serverUrl,
      serverPort,
      this.outputFiles,
      this.requiredFileBeforeEndpoint,
    );
    this.acquiredHandles.push(handle);
    return handle;
  }
}

class FakeInSandboxHandle implements SandboxHandle {
  private readonly fileContents = new Map<string, Buffer>();
  readonly files = {
    write: async (path: string, content: Buffer | NodeJS.ReadableStream): Promise<void> => {
      if (Buffer.isBuffer(content)) {
        this.fileContents.set(path, content);
        return;
      }
      const chunks: Buffer[] = [];
      for await (const chunk of content) {
        chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk as Uint8Array));
      }
      this.fileContents.set(path, Buffer.concat(chunks));
    },
    read: async (path: string): Promise<Buffer> => {
      const content = this.fileContents.get(path);
      if (!content) throw new Error(`not found: ${path}`);
      return content;
    },
    readUtf8Page: async () => {
      throw new Error('files.readUtf8Page not used');
    },
    list: async (path: string): Promise<string[]> => {
      const prefix = path.endsWith('/') ? path : `${path}/`;
      const entries = new Set<string>();
      for (const filePath of this.fileContents.keys()) {
        if (!filePath.startsWith(prefix)) continue;
        const name = filePath.slice(prefix.length).split('/')[0];
        if (name) entries.add(name);
      }
      return [...entries];
    },
    chmod: async (_path: string, _mode: number): Promise<void> => {},
    delete: async (path: string): Promise<void> => {
      this.fileContents.delete(path);
    },
  };

  constructor(
    readonly id: string,
    private readonly serverUrl: string,
    private readonly serverPort: number,
    initialFiles: Record<string, Buffer>,
    private readonly requiredFileBeforeEndpoint?: { path: string; content: Buffer },
  ) {
    for (const [path, content] of Object.entries(initialFiles)) {
      this.fileContents.set(path, content);
    }
  }

  async run(_call: ToolCall): Promise<ToolResult> {
    return { stdout: '', stderr: '', exit_code: 0 };
  }

  async prepareFilesystemRoots(_paths: readonly string[]): Promise<void> {}

  async canonicalizePathForPolicy(path: string): Promise<string> {
    return path;
  }

  async runPrivileged(_cmd: string): Promise<ToolResult> {
    return { stdout: '', stderr: '', exit_code: 0 };
  }

  async pause(): Promise<void> {}
  async resume(): Promise<void> {}
  async destroy(): Promise<void> {}

  async endpoint(port: number): Promise<{ url: string; headers?: Record<string, string> }> {
    expect(this.fileContents.has('/tmp/orca-sandbox-harness-ready')).toBe(true);
    if (this.requiredFileBeforeEndpoint) {
      expect(this.fileContents.get(this.requiredFileBeforeEndpoint.path)).toEqual(
        this.requiredFileBeforeEndpoint.content,
      );
    }
    // Return the fake wire server URL regardless of the exact port requested.
    // (The fake server listens on a random port; the dispatcher passed the catalog port.)
    void port; // port is asserted in the runtime acquire above
    return { url: this.serverUrl };
  }
}

// ── fake registry ─────────────────────────────────────────────────────────────

class FakeInSandboxRegistry {
  readonly states: Array<{
    sessionId: string;
    status: 'idle' | 'running' | 'rescheduling' | 'terminated';
    sandboxHandleId: string | null | undefined;
  }> = [];
  readonly createdFiles: Array<{
    workspaceId: string;
    sessionId: string;
    filename: string;
    content: Buffer;
  }> = [];
  readonly llmJwtMints: Array<{
    workspaceId: string;
    sessionId: string;
  }> = [];
  readonly usageReports: unknown[] = [];

  constructor(
    private readonly resources: SessionRecord['resources'] = [],
    private readonly usageWriter: 'harness' | 'ai-gateway' = 'harness',
  ) {}

  async mintLlmGatewayJwt(
    workspaceId: string,
    sessionId: string,
  ): Promise<{ token: string; expiresAt: number }> {
    this.llmJwtMints.push({ workspaceId, sessionId });
    return { token: 'jwt_llm_test', expiresAt: Date.now() + 3600_000 };
  }

  async getExecutionOwner(): Promise<'harness-server'> {
    return 'harness-server';
  }

  async prepareExecution(input: {
    workspaceId: string;
    sessionId: string;
  }): Promise<PreparedExecutionV2> {
    const resources = this.resources ?? [];
    const session: SessionRecord = {
      id: input.sessionId,
      agent_id: 'agt_insandbox',
      agent_version: 1,
      workspace_id: 'ws_insandbox',
      usage_writer: this.usageWriter,
      vault_ids: [],
      resources,
    };
    const primaryAgent: AgentRecord = {
      id: 'agt_insandbox',
      name: 'in-sandbox test agent',
      workspace_id: 'ws_insandbox',
      version: 1,
      model: {
        provider: 'anthropic',
        id: 'claude-opus-5',
        speed: 'fast',
        effort: 'high',
      },
      system: 'You are a test agent.',
      tools: [],
      mcp_servers: [],
      skills: [],
      multiagent: {
        type: 'coordinator',
        agents: [{ type: 'agent', id: 'agt_worker', version: 2 }],
      },
      // harness=claude_code, mode=colocated → routes to InSandboxHarness
      metadata: { harness: 'claude_code', mode: 'colocated' },
    };
    const workerAgent: AgentRecord = {
      id: 'agt_worker',
      name: 'Worker',
      workspace_id: 'ws_insandbox',
      version: 2,
      model: {
        provider: 'anthropic',
        id: 'claude-opus-4-8',
        speed: 'fast',
        effort: 'low',
      },
      system: 'Handle delegated work.',
      tools: [],
      mcp_servers: [],
      skills: [],
      metadata: {},
      multiagent: null,
    };
    return {
      schema_version: 2,
      workspace_id: input.workspaceId,
      session,
      primary_agent: primaryAgent,
      subagents: [workerAgent],
      environment: null,
      vault_credentials: [],
      resources,
    };
  }

  async updateSessionStateInternal(input: {
    sessionId: string;
    status: 'idle' | 'running' | 'rescheduling' | 'terminated';
    sandboxHandleId?: string | null;
  }): Promise<SessionRecord | null> {
    this.states.push({
      sessionId: input.sessionId,
      status: input.status,
      sandboxHandleId: input.sandboxHandleId,
    });
    return null;
  }

  async recordSessionUsageInternal(input: { sessionId: string }): Promise<SessionRecord> {
    this.usageReports.push(input);
    return {
      id: input.sessionId,
      agent_id: 'agt_insandbox',
      agent_version: 1,
      workspace_id: 'ws_insandbox',
      usage_writer: this.usageWriter,
      guardrail_usage_state: {},
    };
  }

  /**
   * The dispatcher refreshes the subject window at the start of every turn, so
   * a fake without it fails the turn rather than the assertion under test.
   * An empty record is the shape a session with no cross-session counters has.
   */
  async refreshGuardrailSubjectWindowInternal(_input: {
    workspaceId: string;
    sessionId: string;
    turnEventId: string;
  }): Promise<Record<string, unknown>> {
    return {};
  }

  async applyGuardrailStateInternal(_input: unknown): Promise<Record<string, unknown>> {
    return {};
  }

  async createFile(input: {
    workspaceId: string;
    sessionId: string;
    filename: string;
    content: Buffer;
  }): Promise<{ id: string; sha256: string; size_bytes: number }> {
    this.createdFiles.push(input);
    return {
      id: `file_output_${this.createdFiles.length}`,
      sha256: 'test-sha256',
      size_bytes: input.content.length,
    };
  }
}

// ── recording transcript store ────────────────────────────────────────────────

class RecordingStore implements TranscriptStore {
  readonly appended: Event[] = [];

  async append(workspaceId: string, sessionId: string, events: Event[]): Promise<string[]> {
    this.appended.push(
      ...events.map((event, i) => ({
        ...event,
        workspaceId,
        sessionId,
        seq: this.appended.length + 1 + i,
      })),
    );
    return events.map((e) => e.id);
  }

  async *read(_workspaceId: string, _sessionId: string, _opts: ReadOptions): AsyncIterable<Event> {
    yield* [];
  }

  async *tail(_workspaceId: string, _sessionId: string, _opts: TailOptions): AsyncIterable<Event> {
    yield* [];
  }

  async archive(_workspaceId: string, _sessionId: string): Promise<void> {}
  async close(): Promise<void> {}
}

/**
 * RecordingStore variant that yields pre-seeded events from `read()`.
 * Used by the replay test to seed prior turn entries so the dispatcher
 * picks them up via `readInSandboxReplay`.
 */
class SeededStore extends RecordingStore {
  private readonly seedEvents: Event[];

  constructor(seedEvents: Event[]) {
    super();
    this.seedEvents = seedEvents;
  }

  override async *read(
    _workspaceId: string,
    _sessionId: string,
    _opts: ReadOptions,
  ): AsyncIterable<Event> {
    yield* this.seedEvents;
  }
}

// ── fake event source ─────────────────────────────────────────────────────────

class FakeEventSource implements SessionEventSource {
  private handler: ((event: Event) => Promise<void>) | null = null;

  async start(handler: (event: Event) => Promise<void>): Promise<void> {
    this.handler = handler;
  }

  async stop(): Promise<void> {
    this.handler = null;
  }

  async emit(event: Event): Promise<void> {
    if (!this.handler) throw new Error('event source not started');
    await this.handler(event);
  }
}

// ── helpers ───────────────────────────────────────────────────────────────────

function userMessageEvent(
  id: string,
  workspaceId: string,
  sessionId: string,
  text: string = 'hello',
): Event {
  return {
    id,
    workspaceId,
    sessionId,
    subpath: '',
    seq: 1,
    producedAt: new Date().toISOString(),
    producedBy: 'client',
    kind: 'user.message',
    payload: new TextEncoder().encode(JSON.stringify({ content: [{ type: 'text', text }] })),
    idempotencyKey: '',
  };
}

/**
 * Build a transcript Event whose payload is the UTF-8 JSON of the given entry.
 * Used to seed prior turns into the SeededStore for replay tests.
 */
function transcriptEvent(
  kind: string,
  entry: Record<string, unknown>,
  overrides: Partial<Event> = {},
): Event {
  return {
    id: `seed_${kind}_${Date.now()}_${Math.random()}`,
    workspaceId: 'ws_insandbox',
    sessionId: 'ses_insandbox_replay',
    subpath: '',
    seq: 1,
    producedAt: new Date().toISOString(),
    producedBy: kind.startsWith('user') ? 'client' : 'harness',
    kind,
    payload: new TextEncoder().encode(JSON.stringify({ ...entry, type: kind })),
    idempotencyKey: '',
    ...overrides,
  };
}

async function waitFor(predicate: () => boolean, timeoutMs = 5000): Promise<void> {
  const started = Date.now();
  while (Date.now() - started < timeoutMs) {
    if (predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  throw new Error(`waitFor condition not met after ${timeoutMs}ms`);
}

// ── test cleanup ──────────────────────────────────────────────────────────────

let _dispatcher: Dispatcher | undefined;
let _wireServer: FakeWireServer | undefined;

afterEach(async () => {
  if (_dispatcher) {
    await _dispatcher.stop().catch(() => undefined);
    _dispatcher = undefined;
  }
  if (_wireServer) {
    await _wireServer.close().catch(() => undefined);
    _wireServer = undefined;
  }
});

// ── tests ─────────────────────────────────────────────────────────────────────

describe('dispatcher colocated routing', () => {
  it('routes colocated agent through InSandboxHarness, acquires sandbox with harness image+port, maps events', async () => {
    const WORKSPACE_ID = 'ws_insandbox';
    const SESSION_ID = 'ses_insandbox_1';
    const SANDBOX_SESSION_ID = `${SESSION_ID}_inner`;

    // Script the wire server to emit a turn with user-echo, agent.message, and session.status_idle.
    const wireEvents: RawSandboxEvent[] = [
      makeEvent('evt_echo', SANDBOX_SESSION_ID, 'user.message', { content: 'hello' }),
      makeEvent('evt_agent1', SANDBOX_SESSION_ID, 'agent.message', {
        content: [{ type: 'text', text: 'Hello from in-sandbox!' }],
      }),
      makeEvent('evt_idle1', SANDBOX_SESSION_ID, 'session.status_idle', {
        usage: { input_tokens: 5 },
        total_cost_usd: 0.0,
      }),
    ];

    _wireServer = await startFakeWireServer(wireEvents);

    const runtime = new FakeInSandboxRuntime(_wireServer.baseUrl, _wireServer.port);
    const registry = new FakeInSandboxRegistry([], 'ai-gateway');
    const store = new RecordingStore();
    const source = new FakeEventSource();

    _dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'test-insandbox',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      registry: registry as unknown as RegistryClient,
      sandboxRuntime: runtime,
      sessionIdleTimeoutMs: 1_000,
      gatewayLlmUrl: 'http://gw.local/v1/llm',
    });

    await _dispatcher.start();

    // Drive a user.message from the client.
    await source.emit(userMessageEvent('evt_client_1', WORKSPACE_ID, SESSION_ID, 'hello'));

    // Wait for agent.message to be appended.
    await waitFor(() => store.appended.some((e) => e.kind === 'agent.message'));

    // Wait for the terminal session.status_idle to be appended.
    await waitFor(() => store.appended.some((e) => e.kind === 'session.status_idle'));

    // ── assertions ────────────────────────────────────────────────────────────

    // 1. The fake runtime was acquired with an EnvironmentSpec carrying the harness image + exposePorts.
    //    (Assertions are inline in FakeInSandboxRuntime.acquire — if they fail the test fails here.)
    expect(runtime.acquiredSpecs.length).toBe(1);
    const acquiredSpec = runtime.acquiredSpecs[0]!;
    // HARNESS_CATALOG['claude_code'].defaultImage is the expected image.
    expect(acquiredSpec.image).toBe('ghcr.io/orca-ae/sandbox-harness-claude-code:latest');
    expect(acquiredSpec.exposePorts).toEqual([4096]);

    // 1b. harnessEnv carries the gateway LLM base URL and the minted token.
    expect(acquiredSpec.harnessEnv).toBeDefined();
    expect(acquiredSpec.harnessEnv!['LITELLM_API_BASE']).toBe('http://gw.local/v1/llm');
    expect(acquiredSpec.harnessEnv!['LITELLM_API_KEY']).toBe('jwt_llm_test');
    expect(acquiredSpec.harnessEnv!['ANTHROPIC_CUSTOM_HEADERS']).toBe(
      `X-Orca-Session-Id: ${SESSION_ID}`,
    );
    // Resolved agent model is injected as the gateway's default model.
    expect(acquiredSpec.harnessEnv!['LITELLM_DEFAULT_MODEL']).toBe('claude-opus-5');
    expect(registry.llmJwtMints).toEqual([
      {
        workspaceId: WORKSPACE_ID,
        sessionId: SESSION_ID,
      },
    ]);
    // Gateway is the authoritative usage writer for colocated sessions. The
    // sandbox's terminal usage frame must not produce a second Harness write.
    expect(registry.usageReports).toEqual([]);
    expect(acquiredSpec.harnessEnv!['ORCA_OUTPUT_CAPTURE_DIRECTORY']).toBe('/mnt/session/outputs');
    expect(JSON.parse(acquiredSpec.harnessEnv!['ORCA_SANDBOX_WRITE_POLICY']!)).toEqual({
      writablePaths: [{ path: '/mnt/session/outputs', kind: 'session_output' }],
      readonlyPaths: [],
    });

    // 2. The user.message echo from the sandbox (evt_echo) is NOT appended.
    //    The client's user.message (evt_client_1) is also NOT expected to come back
    //    through the harness path (it was forwarded to submit, not emitted as an AgentEvent).
    const appendedKinds = store.appended.map((e) => e.kind);
    expect(appendedKinds).not.toContain('user.message');

    // 3. agent.message (evt_agent1) was appended — exactly once (wire emits one).
    const agentMsgEvents = store.appended.filter((e) => e.kind === 'agent.message');
    expect(agentMsgEvents.length).toBe(1);

    // 4. The sandbox's single session.status_idle frame (evt_idle1) fans out.
    //    What lands in the store for that frame is:
    //      - span.model_request_end (once)
    //      - a terminal session.status_idle{end_turn} (once)
    //    The agent.usage sibling is DIVERTED by SessionRunner.pumpEvents to the
    //    usage sink and is NEVER appended.
    const idleEvents = store.appended.filter((e) => e.kind === 'session.status_idle');
    expect(idleEvents.length).toBe(1);
    const modelRequestEndEvents = store.appended.filter((e) => e.kind === 'span.model_request_end');
    expect(modelRequestEndEvents.length).toBe(1);
    expect(store.appended.some((e) => e.kind === 'agent.usage')).toBe(false);

    // 4b. The bridge opens the turn with a leading session.status_running +
    //     span.model_request_start (emitted at submit time); both are appended.
    expect(store.appended.some((e) => e.kind === 'session.status_running')).toBe(true);
    expect(store.appended.some((e) => e.kind === 'span.model_request_start')).toBe(true);
    expect(store.appended.every((event) => isAgentEventId(event.id))).toBe(true);
    expect(store.appended.every((event) => isAgentEventSubpath(event.subpath))).toBe(true);
    expect(store.appended.every((event) => event.subpath === '')).toBe(true);
    expect(new Set(store.appended.map((event) => event.id)).size).toBe(store.appended.length);

    // 5. Valid raw sandbox IDs become Transcript Event.id; the raw ID remains
    //    idempotencyKey metadata, and routing context is forwarded.
    const agentMsg = agentMsgEvents[0]!;
    expect(agentMsg.id).toBe('evt_agent1');
    expect(agentMsg.idempotencyKey).toBe('evt_agent1');
    expect(agentMsg.workspaceId).toBe(WORKSPACE_ID);
    expect(agentMsg.sessionId).toBe(SESSION_ID);

    // Every fan-out sibling of the sandbox's session.status_idle frame (evt_idle1)
    // carries a valid derived Event.id (`${frameId}:model_end`, `${frameId}:idle`)
    // plus matching idempotencyKey metadata. Transcript-store dedups Event.id;
    // the raw frame ID itself still rides on the DIVERTED agent.usage event.
    const modelRequestEnd = modelRequestEndEvents[0]!;
    expect(modelRequestEnd.id).toBe('evt_idle1:model_end');
    expect(modelRequestEnd.idempotencyKey).toBe('evt_idle1:model_end');
    const idle = idleEvents[0]!;
    expect(idle.id).toBe('evt_idle1:idle');
    expect(idle.idempotencyKey).toBe('evt_idle1:idle');
    const idlePayload = JSON.parse(new TextDecoder().decode(idle.payload)) as {
      stop_reason?: { type?: string };
    };
    expect(idlePayload.stop_reason?.type).toBe('end_turn');

    // 6. Registry was asked to mark the session running (from markSessionRunning).
    expect(registry.states.some((s) => s.status === 'running')).toBe(true);

    // 7. Fresh session (RecordingStore.read() yields nothing) → no replay in POST /v1/sessions.
    const sessionBody = _wireServer.capturedSessionBody as Record<string, unknown>;
    expect(sessionBody['replay']).toBeUndefined();
    expect(sessionBody).toMatchObject({
      model: 'claude-opus-5',
      modelSpeed: 'fast',
      modelEffort: 'high',
      agents: {
        worker: {
          description: 'Managed agent Worker (agt_worker v2)',
          prompt: 'Handle delegated work.',
          model: 'claude-opus-4-8',
          modelSpeed: 'fast',
          effort: 'low',
        },
      },
    });

    await _dispatcher.stop();
    _dispatcher = undefined;
  });

  it('indexes in-sandbox outputs after tool result without duplicating them on stop', async () => {
    const WORKSPACE_ID = 'ws_insandbox';
    const SESSION_ID = 'ses_insandbox_output';
    const SANDBOX_SESSION_ID = `${SESSION_ID}_inner`;
    _wireServer = await startFakeWireServer([
      makeEvent('evt_output_tool', SANDBOX_SESSION_ID, 'agent.tool_result', {
        tool_use_id: 'toolu_write_poem',
        content: 'wrote /mnt/session/outputs/ai_coding_poem.txt',
        is_error: false,
      }),
      makeEvent('evt_output_agent', SANDBOX_SESSION_ID, 'agent.message', {
        content: [{ type: 'text', text: 'Saved the poem.' }],
      }),
      makeEvent('evt_output_idle', SANDBOX_SESSION_ID, 'session.status_idle', {
        usage: {},
        total_cost_usd: 0,
      }),
    ]);

    const poem = Buffer.from('A compiler dreams in measured rhyme.\n', 'utf8');
    const runtime = new FakeInSandboxRuntime(_wireServer.baseUrl, _wireServer.port, {
      '/mnt/session/outputs/ai_coding_poem.txt': poem,
    });
    const registry = new FakeInSandboxRegistry();
    const store = new RecordingStore();
    const source = new FakeEventSource();
    _dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'test-insandbox-output',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      registry: registry as unknown as RegistryClient,
      sandboxRuntime: runtime,
      sessionIdleTimeoutMs: 60_000,
    });

    await _dispatcher.start();
    await source.emit(
      userMessageEvent('evt_output_client', WORKSPACE_ID, SESSION_ID, 'write a poem'),
    );
    await waitFor(() => registry.createdFiles.length === 1);

    // The file is visible before the 60-second idle timeout and before an
    // explicit dispatcher stop.
    expect(store.appended.some((event) => event.kind === 'session.output_indexed')).toBe(true);
    expect(registry.createdFiles).toEqual([
      expect.objectContaining({
        workspaceId: WORKSPACE_ID,
        sessionId: SESSION_ID,
        filename: 'ai_coding_poem.txt',
        content: poem,
      }),
    ]);

    await _dispatcher.stop();
    _dispatcher = undefined;

    // The shutdown safety scan sees identical bytes and does not create a
    // duplicate immutable File record.
    expect(registry.createdFiles).toHaveLength(1);
  });

  it('mounts attached file resources before starting the in-sandbox harness', async () => {
    const WORKSPACE_ID = 'ws_insandbox';
    const SESSION_ID = 'ses_insandbox_resource';
    const SANDBOX_SESSION_ID = `${SESSION_ID}_inner`;
    const FILE_ID = 'file_attached';
    const MOUNT_PATH = '/workspace/context.txt';
    const content = Buffer.from('mounted session context\n', 'utf8');
    _wireServer = await startFakeWireServer([
      makeEvent('evt_resource_agent', SANDBOX_SESSION_ID, 'agent.message', {
        content: [{ type: 'text', text: 'I can read the attached context.' }],
      }),
      makeEvent('evt_resource_idle', SANDBOX_SESSION_ID, 'session.status_idle', {
        usage: {},
        total_cost_usd: 0,
      }),
    ]);

    const runtime = new FakeInSandboxRuntime(
      _wireServer.baseUrl,
      _wireServer.port,
      {},
      {
        path: MOUNT_PATH,
        content,
      },
    );
    const registry = new FakeInSandboxRegistry([
      {
        id: 'sesrsc_attached',
        type: 'file',
        file_id: FILE_ID,
        memory_store_id: null,
        repo_ref: null,
        mount_path: MOUNT_PATH,
        access: 'read_only',
        instructions: null,
        attached_at: new Date().toISOString(),
        detached_at: null,
        mount_strategy: 'tarball_prefetch',
      },
    ]);
    const fileStore = {
      open: async (workspaceId: string, fileId: string) => {
        expect(workspaceId).toBe(WORKSPACE_ID);
        expect(fileId).toBe(FILE_ID);
        return {
          stream: Readable.from([content]),
          sizeBytes: content.length,
          sha256: 'test-sha256',
        };
      },
    } as unknown as FileStore;
    const store = new RecordingStore();
    const source = new FakeEventSource();
    _dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'test-insandbox-resource',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      registry: registry as unknown as RegistryClient,
      sandboxRuntime: runtime,
      fileStore,
      sessionIdleTimeoutMs: 60_000,
    });

    await _dispatcher.start();
    await source.emit(
      userMessageEvent('evt_resource_client', WORKSPACE_ID, SESSION_ID, 'read the context'),
    );
    await waitFor(() => store.appended.some((event) => event.kind === 'session.status_idle'));

    const sandbox = runtime.acquiredHandles[0]!;
    await expect(sandbox.files.read(MOUNT_PATH)).resolves.toEqual(content);
    expect(JSON.parse(runtime.acquiredSpecs[0]!.harnessEnv!['ORCA_SANDBOX_WRITE_POLICY']!)).toEqual(
      {
        writablePaths: [{ path: '/mnt/session/outputs', kind: 'session_output' }],
        readonlyPaths: [MOUNT_PATH],
      },
    );
    expect(store.appended).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          kind: 'session.resource_mounted',
          workspaceId: WORKSPACE_ID,
          sessionId: SESSION_ID,
        }),
      ]),
    );
  });

  it('sends prior transcript turns as replay on cold resume (SeededStore)', async () => {
    const WORKSPACE_ID = 'ws_insandbox';
    const SESSION_ID = 'ses_insandbox_replay';
    const SANDBOX_SESSION_ID = `${SESSION_ID}_inner`;

    // Wire events for the resumed turn.
    const wireEvents: RawSandboxEvent[] = [
      makeEvent('evt_resume_agent', SANDBOX_SESSION_ID, 'agent.message', {
        content: [{ type: 'text', text: 'context restored, hello again' }],
      }),
      makeEvent('evt_resume_idle', SANDBOX_SESSION_ID, 'session.status_idle', {
        usage: { input_tokens: 12 },
        total_cost_usd: 0.0,
      }),
    ];

    _wireServer = await startFakeWireServer(wireEvents);

    // Seed prior user.message + agent.message turns in the store.
    const seedEvents: Event[] = [
      transcriptEvent('user.message', { content: 'prior user turn' }),
      transcriptEvent('agent.message', {
        content: [{ type: 'text', text: 'prior assistant reply' }],
      }),
      // tool_use and the terminal session.status_idle should be skipped by
      // buildReplayTurns (only user.message + agent.message become turns).
      transcriptEvent('agent.tool_use', { name: 'Bash', input: { command: 'ls' } }),
      transcriptEvent('session.status_idle', { stop_reason: { type: 'end_turn' } }),
    ];

    const runtime = new FakeInSandboxRuntime(_wireServer.baseUrl, _wireServer.port);
    const registry = new FakeInSandboxRegistry();
    const store = new SeededStore(seedEvents);
    const source = new FakeEventSource();

    _dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'test-insandbox-replay',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      registry: registry as unknown as RegistryClient,
      sandboxRuntime: runtime,
      sessionIdleTimeoutMs: 1_000,
      gatewayLlmUrl: 'http://gw.local/v1/llm',
    });

    await _dispatcher.start();

    // Drive a user.message (the resumed turn).
    await source.emit(userMessageEvent('evt_resume_client', WORKSPACE_ID, SESSION_ID, 'continue'));

    // Wait for the terminal session.status_idle to be appended.
    await waitFor(() => store.appended.some((e) => e.kind === 'session.status_idle'));

    // Assert POST /v1/sessions body contained replay with ONLY user+assistant turns
    // (tool_use and session.status_idle entries are skipped by buildReplayTurns).
    const sessionBody = _wireServer.capturedSessionBody as Record<string, unknown>;
    expect(sessionBody['replay']).toBeDefined();
    expect(sessionBody['replay']).toEqual([
      { role: 'user', text: 'prior user turn' },
      { role: 'assistant', text: 'prior assistant reply' },
    ]);

    // Session still ran correctly: agent.message + terminal session.status_idle appended.
    expect(store.appended.some((e) => e.kind === 'agent.message')).toBe(true);
    expect(store.appended.some((e) => e.kind === 'session.status_idle')).toBe(true);
    await waitFor(() => registry.usageReports.length === 1);
    expect(registry.usageReports[0]).toMatchObject({
      sessionId: SESSION_ID,
      usage: { input_tokens: 12 },
    });

    await _dispatcher.stop();
    _dispatcher = undefined;
  });

  it('keeps separate agents on the existing Claude harness path', async () => {
    // A `separate` agent still uses the injected Claude-path harness rather
    // than InSandboxHarness, even though both modes share sandbox setup.
    const WORKSPACE_ID = 'ws_separate';
    const SESSION_ID = 'ses_separate_1';

    const store = new RecordingStore();
    const source = new FakeEventSource();
    let harnessFactoryCalled = false;

    _dispatcher = new Dispatcher({
      eventSource: source,
      groupId: 'test-separate',
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      // harnessFactory overrides selectHarness entirely and confirms that the
      // `separate` path reaches the override.
      harnessFactory: (_wsId, _sesId, _adapter) => {
        harnessFactoryCalled = true;
        return new MinimalEchoHarness();
      },
    });

    await _dispatcher.start();
    await source.emit(userMessageEvent('evt_sep_1', WORKSPACE_ID, SESSION_ID, 'hello'));
    await waitFor(() => store.appended.some((e) => e.kind === 'agent.message'));

    // The harness factory was called (separate path).
    expect(harnessFactoryCalled).toBe(true);

    // agent.message was appended.
    expect(store.appended.some((e) => e.kind === 'agent.message')).toBe(true);

    await _dispatcher.stop();
    _dispatcher = undefined;
  });
});

// ── minimal echo harness for the "separate" path guard test ──────────────────

class MinimalEchoHarness implements AgentHarness {
  private queue: AgentEvent[] = [];
  private waiters: Array<(v: IteratorResult<AgentEvent>) => void> = [];
  private done = false;

  async start(_input: SessionStartInput): Promise<void> {}

  async submit(event: UserEvent): Promise<void> {
    if (event.kind !== 'user.message') return;
    this.push({ kind: 'agent.message', payload: { content: [{ type: 'text', text: 'echo' }] } });
    // session.status_idle is the unified terminal.
    this.push({ kind: 'session.status_idle', payload: { stop_reason: { type: 'end_turn' } } });
  }

  async stop(_reason: TerminationReason): Promise<void> {
    this.done = true;
    for (const w of this.waiters.splice(0)) {
      w({ value: undefined as never, done: true });
    }
  }

  async *events(): AsyncIterable<AgentEvent> {
    while (!this.done || this.queue.length > 0) {
      const item = this.queue.shift();
      if (item !== undefined) {
        yield item;
        continue;
      }
      const next = await new Promise<IteratorResult<AgentEvent>>((r) => {
        this.waiters.push(r);
      });
      if (next.done) return;
      yield next.value;
    }
  }

  private push(input: AgentEventInput): void {
    const event = withCanonicalAgentEventEnvelope(input);
    const w = this.waiters.shift();
    if (w) w({ value: event, done: false });
    else this.queue.push(event);
  }
}

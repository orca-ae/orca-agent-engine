// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// End-to-end proof of the `target=cloud` managed-launch lifecycle (A3b) via
// the Local launcher — no cloud creds, no external provider:
//
//   client -> registry (in-process buildApp) -> EnvironmentLaunchLifecycle
//     -> LocalEnvironmentLauncher (REAL ChildProcessSpawner)
//     -> REAL `environment-worker` process dials the REAL worker tunnel with
//        the minted Environment Token
//     -> (now un-gated) SessionDistributor.onWorkerConnect drives the
//        create-time-stranded session -> `worker.launch_runner`
//     -> the worker spawns a REAL `session-runner` process that dials the
//        REAL runner tunnel
//     -> the owner-pod single-writer bridge delivers the turn -> SSE
//
// Every layer is real EXCEPT the registry running in-process (`buildApp` +
// `app.listen`) rather than as a separately-spawned OS process — the same
// scope the self-hosted three-layer integration spec already exercises for
// the `target=self_hosted` path (see `sessions-tunnel-three-layer.spec.ts`),
// except THIS spec's "worker" and "runner" are not hand-rolled `ws` frame
// clients — they are the actual `environment-worker` / `session-runner`
// binaries, actually spawned as child processes by the actual
// `LocalEnvironmentLauncher`. The turn drives the credential-free `mock`
// provider (see `packages/e2e-tests/test/self-hosted-e2e.spec.ts`'s doc for
// why `mock` is what makes this runnable with zero external credentials).
//
// Prerequisite: `pnpm -r build` (this spec spawns
// `services/environment-worker/dist/main.js` and
// `services/session-runner/dist/main.js` — it fails loud with the fix if
// either is missing, mirroring `start-self-hosted.sh`). DB-touching (real
// Postgres via `getTestDb()` + the Postgres transcript backend), so it lives
// under test/integration and is gated on the dev Postgres stack, exactly like
// its self-hosted sibling.

import { existsSync, mkdtempSync, rmSync } from 'node:fs';
import net from 'node:net';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import type { FastifyInstance } from 'fastify';
import type { Pool } from 'pg';
import type { PostgresTranscriptStore } from '@orca/transcript-store';
import {
  getTestDb,
  closeTestDb,
  buildTestPostgresStore,
  deleteTranscriptRowsForWorkspace,
  STUB_SSE_CONFIG,
  buildTestJwtMinter,
  buildStubFileStore,
} from './setup.js';
import { uniqueWorkspace, createTestApiKey } from './fixtures.js';
import { buildApp } from '../../src/server.js';
import { WorkerRegistry } from '../../src/tunnel/worker-registry.js';
import { EnvironmentTokenStore } from '../../src/domain/environment-token-store.js';
import { EnvironmentLaunchLifecycle } from '../../src/environment/launch/environment-launch-lifecycle.js';
import { LocalEnvironmentLauncher } from '../../src/environment/launcher/local-environment-launcher.js';
import { AgentSnapshotResolver } from '../../src/domain/agent-snapshot-resolver.js';
import { buildSnapshotRecordLoader } from '../../src/api/snapshot-loader.js';

const here = dirname(fileURLToPath(import.meta.url));
// here = .../services/registry-service-ts/test/integration -> up 3 = .../services
const SERVICES_DIR = resolve(here, '../../..');
const ENVIRONMENT_WORKER_MAIN = join(SERVICES_DIR, 'environment-worker/dist/main.js');
const SESSION_RUNNER_MAIN = join(SERVICES_DIR, 'session-runner/dist/main.js');

/** Generous ceiling: real child-process spawns (worker, then runner) + a live WS handshake. */
const WAIT_ONLINE_TIMEOUT_MS = 30_000;
const ASSIGNED_TIMEOUT_MS = 45_000;
const TURN_TIMEOUT_MS = 60_000;

interface EnvironmentResponse {
  id: string;
  target: string | null;
}

interface SessionResponse {
  id: string;
  distribution_state: string | null;
  runner_id: string | null;
  host_environment_id: string | null;
}

interface SseFrame {
  type: string;
  content?: Array<{ type: string; text?: string }>;
  [key: string]: unknown;
}

function apiHeaders(apiKey: string): Record<string, string> {
  return { 'x-api-key': apiKey, 'content-type': 'application/json' };
}

/**
 * Session create/read headers. `orca-beta` is required, not decoration: the
 * DEFAULT session view is the Claude-shaped one, which omits
 * `distribution_state`, `runner_id` and `host_environment_id` — the three fields
 * this test follows the launch chain with. `loadSessionView` only emits them
 * (via `toApi`) on the `orca-beta` branch. Scoped to the session calls so the
 * agent/environment creates keep asserting the default public wire.
 */
function sessionApiHeaders(apiKey: string): Record<string, string> {
  return { ...apiHeaders(apiKey), 'orca-beta': 'true' };
}

/** Reserve a free loopback TCP port synchronously-enough for test setup (a throwaway listen+close). */
async function reserveFreePort(): Promise<number> {
  return new Promise((resolvePort, reject) => {
    const srv = net.createServer();
    srv.once('error', reject);
    srv.listen(0, '127.0.0.1', () => {
      const addr = srv.address();
      if (addr === null || typeof addr === 'string') {
        reject(new Error('reserveFreePort: expected a TCP address'));
        return;
      }
      const { port } = addr;
      srv.close(() => resolvePort(port));
    });
  });
}

/** Read a fetch Response's body exactly once as text, parsed as JSON when non-empty. */
async function readBody(res: Response): Promise<unknown> {
  const text = await res.text();
  if (text.length === 0) return undefined;
  try {
    return JSON.parse(text);
  } catch {
    return text;
  }
}

async function createCloudEnvironment(
  baseURL: string,
  apiKey: string,
  name: string,
): Promise<EnvironmentResponse> {
  const res = await fetch(`${baseURL}/v1/environments`, {
    method: 'POST',
    headers: apiHeaders(apiKey),
    body: JSON.stringify({ name, target: 'cloud', egress_mode: 'sidecar' }),
  });
  const body = await readBody(res);
  expect(res.status, `create cloud environment failed: ${res.status} ${JSON.stringify(body)}`).toBe(
    200,
  );
  return body as EnvironmentResponse;
}

async function createMockAgent(baseURL: string, apiKey: string, name: string): Promise<string> {
  const res = await fetch(`${baseURL}/v1/agents`, {
    method: 'POST',
    headers: apiHeaders(apiKey),
    body: JSON.stringify({
      name,
      model: { provider: 'mock', id: 'mock-1' },
      system: 'You are a deterministic mock agent.',
      tools: [],
      mcp_servers: [],
      skills: [],
      // The ONLY supported mode for the `mock` harness is `colocated` — the
      // session-create route's environment-launch trigger explicitly gates on
      // resolveAgentMode(...) === 'colocated' before kicking the lifecycle.
      metadata: { harness: 'mock', mode: 'colocated' },
    }),
  });
  const body = await readBody(res);
  expect(res.status, `create agent failed: ${res.status} ${JSON.stringify(body)}`).toBe(200);
  return (body as { id: string }).id;
}

async function createSession(
  baseURL: string,
  apiKey: string,
  agentId: string,
  environmentId: string,
): Promise<SessionResponse> {
  const res = await fetch(`${baseURL}/v1/sessions`, {
    method: 'POST',
    headers: sessionApiHeaders(apiKey),
    body: JSON.stringify({ agent_id: agentId, environment_id: environmentId }),
  });
  const body = await readBody(res);
  expect(res.status, `create session failed: ${res.status} ${JSON.stringify(body)}`).toBe(200);
  return body as SessionResponse;
}

async function getSession(
  baseURL: string,
  apiKey: string,
  sessionId: string,
): Promise<SessionResponse> {
  const res = await fetch(`${baseURL}/v1/sessions/${sessionId}`, {
    method: 'GET',
    headers: sessionApiHeaders(apiKey),
  });
  const body = await readBody(res);
  expect(res.status, `get session failed: ${res.status} ${JSON.stringify(body)}`).toBe(200);
  return body as SessionResponse;
}

async function postUserMessage(
  baseURL: string,
  apiKey: string,
  sessionId: string,
  text: string,
): Promise<void> {
  const res = await fetch(`${baseURL}/v1/sessions/${sessionId}/events`, {
    method: 'POST',
    headers: apiHeaders(apiKey),
    body: JSON.stringify({
      events: [{ type: 'user.message', content: [{ type: 'text', text }] }],
      request_id: `cloud-e2e-${Date.now()}`,
    }),
  });
  const body = await readBody(res);
  expect(res.status, `post user.message failed: ${res.status} ${JSON.stringify(body)}`).toBe(200);
}

const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

/** Poll a session GET until `predicate(session)` holds or the budget elapses. */
async function pollSessionUntil(
  baseURL: string,
  apiKey: string,
  sessionId: string,
  predicate: (s: SessionResponse) => boolean,
  timeoutMs: number,
): Promise<SessionResponse> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const session = await getSession(baseURL, apiKey, sessionId);
    if (predicate(session)) return session;
    if (Date.now() > deadline) {
      throw new Error(
        `session ${sessionId} predicate not met within ${timeoutMs}ms: ${JSON.stringify(session)}`,
      );
    }
    await sleep(250);
  }
}

/** Tail the SSE stream (replayed from_cursor=0) until `until(frames)` holds or the deadline passes. */
async function collectSseFrames(
  baseURL: string,
  apiKey: string,
  sessionId: string,
  opts: { until: (frames: SseFrame[]) => boolean; deadlineMs: number },
): Promise<SseFrame[]> {
  const ac = new AbortController();
  const timer = setTimeout(() => ac.abort(), opts.deadlineMs + 5_000);
  try {
    // The SSE route is `/events/stream`, not `/stream`; a bare `/stream` path
    // does not exist and simply 404s.
    //
    // `orca-beta` again: a DEFAULT stream emits only Claude event types
    // (`sse.ts` drops anything outside `CLAUDE_SESSION_EVENT_TYPES`), and
    // `agent.turn_completed` — the turn-boundary marker this test waits on — is
    // deliberately not one of them. Without the header the stream delivers
    // `agent.message` and then simply never completes.
    const res = await fetch(`${baseURL}/v1/sessions/${sessionId}/events/stream?from_cursor=0`, {
      method: 'GET',
      headers: { ...sessionApiHeaders(apiKey), accept: 'text/event-stream' },
      signal: ac.signal,
    });
    if (res.status !== 200) {
      throw new Error(`SSE handshake failed: ${res.status}`);
    }
    const reader = res.body!.getReader();
    const decoder = new TextDecoder();
    let buffered = '';
    const frames: SseFrame[] = [];
    const deadline = Date.now() + opts.deadlineMs;

    while (Date.now() < deadline) {
      let chunk: ReadableStreamReadResult<Uint8Array>;
      try {
        chunk = await reader.read();
      } catch {
        break;
      }
      if (chunk.done) break;
      buffered += decoder.decode(chunk.value, { stream: true });

      let idx;
      while ((idx = buffered.indexOf('\n\n')) >= 0) {
        const block = buffered.slice(0, idx);
        buffered = buffered.slice(idx + 2);
        if (block.startsWith(':')) continue; // heartbeat comment
        const dataLine = block.split('\n').find((l) => l.startsWith('data: '));
        if (!dataLine) continue;
        try {
          frames.push(JSON.parse(dataLine.slice(6)) as SseFrame);
        } catch {
          /* ignore non-JSON */
        }
      }
      if (opts.until(frames)) break;
    }
    await reader.cancel().catch(() => {});
    return frames;
  } finally {
    clearTimeout(timer);
    ac.abort();
  }
}

function textOf(frame: SseFrame | undefined): string {
  const blocks = Array.isArray(frame?.content) ? frame!.content! : [];
  return blocks
    .filter((b) => b?.type === 'text' && typeof b.text === 'string')
    .map((b) => b.text!)
    .join('');
}

describe('target=cloud colocated session — Local launcher, real spawned worker+runner (end-to-end)', () => {
  let app: FastifyInstance;
  let baseURL: string;
  let apiKey: string;
  let workspaceId: string;
  let workDir: string;
  let pgPool: Pool;
  let pgStore: PostgresTranscriptStore;
  let lifecycle: EnvironmentLaunchLifecycle;
  const launchedEnvironmentIds: string[] = [];

  beforeAll(async () => {
    if (!existsSync(ENVIRONMENT_WORKER_MAIN)) {
      throw new Error(
        `cloud-local-launch-e2e: ${ENVIRONMENT_WORKER_MAIN} is missing — run \`pnpm -r build\` first.`,
      );
    }
    if (!existsSync(SESSION_RUNNER_MAIN)) {
      throw new Error(
        `cloud-local-launch-e2e: ${SESSION_RUNNER_MAIN} is missing — run \`pnpm -r build\` first.`,
      );
    }

    const { db } = await getTestDb();
    const built = await buildTestPostgresStore();
    pgPool = built.pool;
    pgStore = built.store;

    workDir = mkdtempSync(join(tmpdir(), 'orca-cloud-e2e-'));
    const port = await reserveFreePort();
    baseURL = `http://127.0.0.1:${port}`;

    const workerRegistry = new WorkerRegistry();
    const launcher = new LocalEnvironmentLauncher({
      baseDir: workDir,
      workerLaunchCommand: ['node', ENVIRONMENT_WORKER_MAIN],
      runnerLaunchCommand: ['node', SESSION_RUNNER_MAIN],
    });
    lifecycle = new EnvironmentLaunchLifecycle({
      launcher,
      tokens: new EnvironmentTokenStore(db),
      workerOnline: workerRegistry,
      registryTunnelUrl: baseURL,
      waitOnlineTimeoutMs: WAIT_ONLINE_TIMEOUT_MS,
      pollIntervalMs: 250,
      logger: {
        warn: (obj, msg) => console.warn('[cloud-e2e lifecycle]', msg, obj),
        error: (obj, msg) => console.error('[cloud-e2e lifecycle]', msg, obj),
      },
    });

    const jwtMinter = buildTestJwtMinter();
    app = buildApp({
      db,
      oidc: { allowedIssuers: [], audience: 'orca-managed-agents' },
      store: pgStore,
      sse: STUB_SSE_CONFIG,
      jwtMinter,
      fileStore: buildStubFileStore(),
      workerRegistry,
      environmentLaunchLifecycle: lifecycle,
      // Required for a REAL session-runner to do anything: it waits on the
      // owner-pod bridge's credential-free snapshot delivery (model/provider/
      // system/tools) before it can process a turn. `sessions-tunnel-three-layer.spec.ts`
      // never needs this because its "runner" is a hand-rolled ws frame
      // responder with no snapshot dependency; a REAL session-runner process
      // genuinely blocks without it. `main.ts` always wires this in production
      // (unconditionally) — mirrored here. No gateway URL: `egress_mode:
      // 'sidecar'` (set on the environment below) resolves without one.
      agentSnapshotProvider: new AgentSnapshotResolver({
        loader: buildSnapshotRecordLoader(db),
        minter: jwtMinter,
      }),
    });
    await app.ready();
    await app.listen({ host: '127.0.0.1', port });

    workspaceId = uniqueWorkspace('cloude2e');
    apiKey = await createTestApiKey(db, workspaceId);
  }, 60_000);

  afterAll(async () => {
    for (const id of launchedEnvironmentIds) {
      await lifecycle?.terminate(id).catch(() => {});
    }
    if (app) await app.close();
    await closeTestDb();
    if (pgPool && workspaceId) {
      await deleteTranscriptRowsForWorkspace(pgPool, workspaceId).catch(() => {});
    }
    if (pgStore) await pgStore.close();
    if (workDir) rmSync(workDir, { recursive: true, force: true });
  });

  it(
    'creating a target=cloud colocated session provisions a Local Environment, the worker dials in with the Environment Token, the distributor dispatches, session-runner runs, and a mock turn round-trips',
    async () => {
      const env = await createCloudEnvironment(baseURL, apiKey, `cloud-e2e-${Date.now()}`);
      launchedEnvironmentIds.push(env.id);
      const agentId = await createMockAgent(baseURL, apiKey, `cloud-e2e-mock-${Date.now()}`);

      // The registry provisions before the box exists: the create returns 200
      // promptly, PENDING, with no runner yet ("return before the box exists" —
      // see sessions.routes.ts's trigger).
      const session = await createSession(baseURL, apiKey, agentId, env.id);
      expect(session.distribution_state).toBe('pending');

      // Wait for the FULL chain to settle: Local launcher spawns a REAL
      // environment-worker -> it mints+presents the Environment Token over the
      // REAL worker tunnel -> wait-online observes it -> the (un-gated)
      // distributor's onWorkerConnect dispatches this create-time-stranded
      // session -> the worker spawns a REAL session-runner -> it dials the
      // runner tunnel -> the session flips ASSIGNED.
      const assigned = await pollSessionUntil(
        baseURL,
        apiKey,
        session.id,
        (s) => s.distribution_state === 'assigned',
        ASSIGNED_TIMEOUT_MS,
      );
      expect(assigned.runner_id, 'a runner should be bound once assigned').not.toBeNull();
      expect(
        assigned.host_environment_id,
        'the worker environment id should be the cloud env',
      ).toBe(env.id);

      // A real turn, through the real single-writer bridge, to SSE.
      const prompt = `hello from the cloud e2e ${Date.now()}`;
      await postUserMessage(baseURL, apiKey, session.id, prompt);

      const frames = await collectSseFrames(baseURL, apiKey, session.id, {
        deadlineMs: TURN_TIMEOUT_MS,
        until: (f) =>
          f.some(
            (frame) => frame.type === 'agent.turn_completed' || frame.type === 'agent.turn_failed',
          ),
      });
      const kinds = frames.map((f) => f.type);

      const failure = frames.find((f) => f.type === 'agent.turn_failed');
      expect(
        failure,
        failure ? `agent.turn_failed: ${JSON.stringify(failure)}` : undefined,
      ).toBeUndefined();
      expect(kinds, `expected agent.message; saw ${JSON.stringify(kinds)}`).toContain(
        'agent.message',
      );
      expect(kinds, `expected agent.turn_completed; saw ${JSON.stringify(kinds)}`).toContain(
        'agent.turn_completed',
      );

      // The mock harness answers `mock: <user text>` — proof it actually ran
      // the turn (it cannot produce the echoed prompt without the message
      // having reached a REAL session-runner process through the REAL
      // worker-launched-by-the-registry path).
      const agentMessage = frames.find((f) => f.type === 'agent.message');
      expect(textOf(agentMessage)).toBe(`mock: ${prompt}`);
    },
    TURN_TIMEOUT_MS + ASSIGNED_TIMEOUT_MS + 30_000,
  );
});

// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import type { FastifyInstance } from 'fastify';
import type { AddressInfo } from 'node:net';
import { createHash } from 'node:crypto';
import { Kafka } from 'kafkajs';
import { Pool } from 'pg';
import { S3Client } from '@aws-sdk/client-s3';
import { KafkaTranscriptStore, sessionTopicName } from '@orca/transcript-store';
import { LocalFileStore, S3BlobStore, applyMigrations } from '@orca/file-store';

import { Dispatcher } from '../../src/runner/dispatcher.js';
import { InMemorySandboxRuntime } from '../../src/sandbox/in-memory/runtime.js';
import { RegistryClient } from '../../src/clients/registry.js';
import type {
  AgentEvent,
  AgentEventInput,
  AgentHarness,
  SessionStartInput,
  TerminationReason,
  UserEvent,
} from '../../src/harness/agent-harness.js';
import { withCanonicalAgentEventEnvelope } from '../../src/harness/agent-harness.js';
import type { SandboxHandle } from '../../src/sandbox/sandbox-runtime.js';

import { buildCombinedTestApp } from '../../../registry-service-ts/src/server.ts';
import {
  getTestDb,
  closeTestDb,
  buildTestJwtMinter,
  buildTestMemoryStore,
  closeTestMemoryStore,
} from '../../../registry-service-ts/test/integration/setup.ts';
import {
  uniqueWorkspace,
  createTestApiKey,
  createTestAgent,
  createTestEnvironment,
} from '../../../registry-service-ts/test/integration/fixtures.ts';

const FILESTORE_DB =
  process.env['FILESTORE_DATABASE_URL'] ?? 'postgres://orca:orca@localhost:5432/filestore';
const S3_ENDPOINT = process.env['S3_ENDPOINT'] ?? 'http://localhost:9000';
const S3_ACCESS_KEY = process.env['S3_ACCESS_KEY'] ?? 'minioadmin';
const S3_SECRET_KEY = process.env['S3_SECRET_KEY'] ?? 'minioadmin';
const S3_BUCKET = process.env['S3_BUCKET'] ?? 'orca-files';
const KAFKA_BROKERS = process.env['KAFKA_BROKERS'] ?? 'localhost:9092';

interface ApiMemoryStoreResp {
  id: string;
  name: string;
}

interface ApiMemoryResp {
  id: string;
  memory_store_id: string;
  path: string;
  content_sha256: string;
  content_size_bytes: number;
}

interface ApiMemoryVersionResp {
  id: string;
  memory_id: string;
  content_sha256: string | null;
  content_size_bytes: number | null;
  created_at: string;
  created_by?: { type: 'session_actor'; session_id: string };
  path: string | null;
  redacted_at: string | null;
}

function sha256Hex(buf: Buffer): string {
  return createHash('sha256').update(buf).digest('hex');
}

/**
 * Full end-to-end test of cross-session memory persistence
 * against the `InMemorySandboxRuntime` + `LocalMemoryStrategy` path.
 *
 * The test exercises the entire memory pipeline that doesn't require FUSE:
 *
 *   1. Pre-create a memory_store via `POST /v1/memory_stores`.
 *   2. **Session A**: create a session attaching the memory_store. Submit a
 *      `user.message`; the FakeHarness writes "hello-A" to
 *      `/mnt/memory/{store_name}/foo.txt` via `sandbox.files.write`. Wait for
 *      the watcher to fire (configured `intervalMs=200` so the test runs
 *      well under the 30s suite budget). Stop the runner so the watcher's
 *      final tick can drain.
 *   3. Assert: the registry has one memory at path `/foo.txt` with sha equal
 *      to `sha256("hello-A")`, and one version row whose
 *      `created_by` actor matches Session A.
 *   4. **Session B**: create a NEW session attached to the SAME memory_store.
 *      Submit a `user.message`; the FakeHarness reads the same path and
 *      verifies the bytes are "hello-A" (cross-session-persistence proof —
 *      the dispatcher's seed step in `dispatcher.ts` ensures the InMemory
 *      sandbox tmpdir starts with the existing memory's bytes BEFORE the
 *      watcher boots; without that seed Session B's read would ENOENT
 *      because the InMemory runtime's tmpdir is fresh per `acquire()`).
 *   5. Session B then writes "hello-B" to the same path. Wait for the
 *      watcher. Stop the runner. Assert the version chain has TWO versions
 *      ordered DESC by `created_at`.
 *   6. Redact Session A's version via `POST /redact`. Assert the version row
 *      has `redacted_at` set + the version blob is gone (registry side).
 *
 * No FUSE / E2B dependency: the test runs entirely on the InMemory runtime
 * (`supportsFuse=false`), so the dispatcher's auto-pick selects
 * `LocalMemoryStrategy` for the mount and the sandbox-walking branch for the
 * watcher. This is deliberate: the test proves the local-tmpdir path works
 * end-to-end including the seed-on-spawn behavior the FUSE path doesn't need.
 */

interface MemoryE2EAction {
  /**
   * Per-session closure executed when the harness's `submit()` receives the
   * test's `user.message`. The harness exposes the live sandbox handle so
   * the closure can read/write into `/mnt/memory/{store_name}/...`.
   */
  (sandbox: SandboxHandle): Promise<void>;
}

class MemoryE2EFakeHarness implements AgentHarness {
  private q: AgentEvent[] = [];
  private resolvers: Array<(v: IteratorResult<AgentEvent>) => void> = [];
  private done = false;
  private sandbox: SandboxHandle | undefined;
  /**
   * Set when this harness's `start()` saw the test's TARGET sessionId. The
   * dispatcher's regex consumer subscribes from-beginning, so it replays
   * messages from sibling session topics whose workspaces still exist in the
   * registry (e.g. session A's topic is still alive when session B's
   * dispatcher boots). Those harnesses DO get a sandbox (since the registry
   * row exists), so plain "hasSandbox" isn't enough to filter — we also gate
   * on the matching sessionId.
   */
  isTargetSession = false;
  hasSandbox = false;
  /**
   * Resolved once the test action has finished running (success or failure).
   * Tests await this before stopping the runner so the watcher tick can
   * observe the write.
   */
  readonly acted: Promise<void>;
  private resolveActed!: () => void;
  private rejectActed!: (err: Error) => void;
  private actionRan = false;

  constructor(
    private readonly targetSessionId: string,
    private readonly action: MemoryE2EAction,
  ) {
    this.acted = new Promise<void>((resolve, reject) => {
      this.resolveActed = resolve;
      this.rejectActed = reject;
    });
  }

  async start(input: SessionStartInput): Promise<void> {
    this.sandbox = input.sandbox;
    this.hasSandbox = input.sandbox !== undefined;
    this.isTargetSession = input.sessionId === this.targetSessionId;
  }

  async submit(ev: UserEvent): Promise<void> {
    if (this.done || ev.kind !== 'user.message') return;
    if (!this.sandbox) {
      // Stale-message replay path: dispatcher's regex consumer subscribes
      // from-beginning and will replay topics from earlier test runs whose
      // workspaces were wiped from the registry. Those harnesses get
      // instantiated without a sandbox; we no-op so the consumer commits
      // the offset and moves on.
      return;
    }
    if (!this.isTargetSession) {
      // Cross-session replay path: the regex consumer also picks up sibling
      // session topics whose workspaces still exist — e.g. session A's topic
      // when session B's dispatcher boots. We DO get a sandbox (the registry
      // record is real), but if we ran the action here we'd write into the
      // wrong session's memory mount + the wrong watcher would attribute
      // the write to the wrong sessionId. No-op + emit a benign agent.message
      // so the consumer commits and moves on.
      this.emit({
        kind: 'agent.message',
        payload: {
          type: 'assistant',
          content: [{ type: 'text', text: 'sibling-session-noop' }],
        },
      });
      return;
    }
    if (this.actionRan) return;
    this.actionRan = true;
    try {
      await this.action(this.sandbox);
      this.emit({
        kind: 'agent.message',
        payload: { type: 'assistant', content: [{ type: 'text', text: 'memory-action-done' }] },
      });
      this.resolveActed();
    } catch (e) {
      this.rejectActed(e as Error);
      throw e;
    }
  }

  async stop(reason: TerminationReason): Promise<void> {
    void reason;
    this.done = true;
    for (const r of this.resolvers.splice(0)) {
      r({ value: undefined, done: true });
    }
  }

  async *events(): AsyncIterable<AgentEvent> {
    while (!this.done || this.q.length > 0) {
      if (this.q.length > 0) {
        const head = this.q.shift();
        if (head !== undefined) {
          yield head;
        }
        continue;
      }
      const next = await new Promise<IteratorResult<AgentEvent>>((resolve) => {
        this.resolvers.push(resolve);
      });
      if (next.done) return;
      yield next.value;
    }
  }

  private emit(e: AgentEventInput): void {
    const event = withCanonicalAgentEventEnvelope(e);
    const r = this.resolvers.shift();
    if (r) r({ value: event, done: false });
    else this.q.push(event);
  }
}

describe('Phase 6 cross-session memory persistence (InMemory)', () => {
  let app: FastifyInstance;
  let baseURL: string;
  let apiKey: string;
  let environmentId: string;
  let workspaceId: string;
  let kafka: Kafka;
  let store: KafkaTranscriptStore;
  let fileStorePool: Pool;
  let fileStore: LocalFileStore;
  let s3Client: S3Client;
  let topicPrefix: string;

  beforeAll(async () => {
    fileStorePool = new Pool({ connectionString: FILESTORE_DB });
    await applyMigrations(fileStorePool);
    s3Client = new S3Client({
      endpoint: S3_ENDPOINT,
      region: process.env['S3_REGION'] ?? 'us-east-1',
      credentials: { accessKeyId: S3_ACCESS_KEY, secretAccessKey: S3_SECRET_KEY },
      forcePathStyle: true,
    });
    fileStore = new LocalFileStore({
      pool: fileStorePool,
      blobStore: new S3BlobStore({
        client: s3Client,
        bucket: S3_BUCKET,
        keyPrefix: 'test/memory-cross-session/',
      }),
    });

    kafka = new Kafka({
      clientId: 'phase6-memory-cross-session',
      brokers: [KAFKA_BROKERS],
      metadataMaxAge: 1000,
    });
    topicPrefix = `phase6_memory_${Date.now()}_${Math.random().toString(36).slice(2, 8)}.`;
    store = new KafkaTranscriptStore({ kafka, topicPrefix });

    const { db } = await getTestDb();
    const memoryStore = await buildTestMemoryStore();
    app = buildCombinedTestApp({
      db,
      oidc: { allowedIssuers: [], audience: 'orca-managed-agents' },
      store,
      sse: { bufferSize: 256, dropAgeMs: 5000, heartbeatMs: 15000 },
      jwtMinter: buildTestJwtMinter(),
      fileStore,
      memoryStore,
    });
    await app.listen({ host: '127.0.0.1', port: 0 });
    baseURL = `http://127.0.0.1:${(app.server.address() as AddressInfo).port}`;
    workspaceId = uniqueWorkspace('memxsession');
    apiKey = await createTestApiKey(db, workspaceId);
    environmentId = await createTestEnvironment(baseURL, apiKey);
  }, 60_000);

  afterAll(async () => {
    await store.close();
    await fileStore.close();
    if (fileStorePool) await fileStorePool.end().catch(() => {});
    await app.close();
    await closeTestMemoryStore();
    await closeTestDb();
  });

  /** Pre-create each session topic before its dispatcher starts for deterministic broker readiness. */
  async function preCreateTopic(ws: string, ses: string): Promise<void> {
    const admin = kafka.admin();
    const topic = sessionTopicName(ws, ses, topicPrefix);
    await admin.connect();
    try {
      await admin.createTopics({
        waitForLeaders: true,
        topics: [{ topic, numPartitions: 1 }],
      });
      const deadline = Date.now() + 10_000;
      while (true) {
        try {
          await admin.fetchTopicOffsets(topic);
          return;
        } catch (e) {
          if (Date.now() >= deadline) throw e;
          await new Promise((r) => setTimeout(r, 250));
        }
      }
    } finally {
      await admin.disconnect();
    }
  }

  function exactSessionTopicPattern(ws: string, ses: string): RegExp {
    return new RegExp(`^${escapeRegex(sessionTopicName(ws, ses))}$`);
  }

  /**
   * Boot a Dispatcher wired with a `MemoryE2EFakeHarness` whose `submit()`
   * runs the supplied closure. Returns once the dispatcher's regex consumer
   * has had time to rebalance + the action has been observed against a LIVE
   * (sandbox-attached) harness instance, so callers can safely stop the
   * runner immediately afterwards.
   *
   * The dispatcher is wired with `s3Client` + `s3Bucket` + the canonical S3 root
   * so the watcher's S3 branch is ELIGIBLE — but because the runtime is
   * `InMemorySandboxRuntime` (`supportsFuse=false`), the dispatcher selects
   * the sandbox/InMemory branch instead. This proves the dispatch logic in
   * `dispatcher.ts` gates on `supportsFuse`, not on the
   * presence of the S3 wiring alone.
   */
  async function runOneSession(opts: {
    sessionId: string;
    action: MemoryE2EAction;
    /** ms to wait after action completes for watcher tick to observe writes. */
    watcherDrainMs: number;
  }): Promise<void> {
    const { sessionId, action, watcherDrainMs } = opts;
    const liveHarnesses: MemoryE2EFakeHarness[] = [];
    const sandboxRuntime = new InMemorySandboxRuntime();
    const registryClient = new RegistryClient(
      baseURL,
      async () => 'test-internal-service-token-at-least-32-chars',
    );

    const dispatcher = new Dispatcher({
      kafka,
      groupId: `phase6-memory-${sessionId}-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`,
      topicPattern: exactSessionTopicPattern(workspaceId, sessionId),
      store,
      topicPrefix,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => {
        const h = new MemoryE2EFakeHarness(sessionId, action);
        liveHarnesses.push(h);
        return h;
      },
      registry: registryClient,
      gatewayMcpUrl: 'http://127.0.0.1:1/mcp',
      fileStore,
      sandboxRuntime,
      // Wire the S3 path so the dispatcher's `useS3Branch` check has all
      // inputs but rejects on `supportsFuse=false`. This proves the
      // dispatch logic picks the sandbox path when FUSE is unavailable
      // even with S3 wiring present.
      s3Bucket: S3_BUCKET,
      s3Endpoint: S3_ENDPOINT,
      s3KeyPrefix: 'test/memory-cross-session/',
      s3Client,
      // Speed the watcher up so the test runs in <30s.
      // Production keeps the 2000ms default; here we want sub-second
      // detection of the FakeHarness's write.
      memoryWatcherIntervalMs: 200,
    });

    try {
      await dispatcher.start();
      // Allow consumer-group rebalance to settle.
      await new Promise((r) => setTimeout(r, 4500));

      const eventResp = await fetch(`${baseURL}/v1/sessions/${sessionId}/events`, {
        method: 'POST',
        headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
        body: JSON.stringify({
          events: [{ type: 'user.message', content: [{ type: 'text', text: 'go' }] }],
        }),
      });
      expect(eventResp.status).toBe(200);

      // Wait for the LIVE harness (sandbox-attached AND matching the target
      // sessionId) to finish its action. The dispatcher is topic-filtered to
      // this session, but the extra guards keep failures diagnostic if the
      // topic filter or registry lookup wiring regresses.
      const actedPromise = new Promise<MemoryE2EFakeHarness>((resolve, reject) => {
        const start = Date.now();
        const tick = (): void => {
          const live = liveHarnesses.find((h) => h.hasSandbox && h.isTargetSession);
          if (live) {
            live.acted.then(() => resolve(live)).catch(reject);
            return;
          }
          if (Date.now() - start > 60_000) {
            reject(new Error('live (sandbox-attached) harness was never observed within 60s'));
            return;
          }
          setTimeout(tick, 100);
        };
        tick();
      });
      await actedPromise;

      // Give the watcher's polling loop time to fire after the write. With
      // `intervalMs=200`, two-three polls within ~700ms is enough; we wait
      // a bit longer to absorb scheduling jitter.
      await new Promise((r) => setTimeout(r, watcherDrainMs));
    } finally {
      await dispatcher.stop().catch(() => {});
    }
  }

  it('persists writes across sessions; version chain + redact behave correctly', async () => {
    // ----- Pre-create the memory_store -----------------------------------
    const storeName = `xsess-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    const createStoreRes = await fetch(`${baseURL}/v1/memory_stores`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({ name: storeName }),
    });
    expect(createStoreRes.status).toBe(200);
    const memStore = (await createStoreRes.json()) as ApiMemoryStoreResp;

    // ----- Session A: write "hello-A" via the FakeHarness ----------------
    const agentId = await createTestAgent(baseURL, apiKey);
    const sessionARes = await fetch(`${baseURL}/v1/sessions`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({
        environment_id: environmentId,
        agent_id: agentId,
        resources: [{ type: 'memory_store', memory_store_id: memStore.id, access: 'read_write' }],
      }),
    });
    expect(sessionARes.status).toBe(200);
    const sessionA = (await sessionARes.json()) as { id: string };
    await preCreateTopic(workspaceId, sessionA.id);

    const helloA = Buffer.from('hello-A', 'utf8');
    const helloASha = sha256Hex(helloA);
    const memoryPath = `/mnt/memory/${storeName}/foo.txt`;

    await runOneSession({
      sessionId: sessionA.id,
      action: async (sandbox) => {
        await sandbox.files.write(memoryPath, helloA);
      },
      watcherDrainMs: 1500,
    });

    // ----- Assert: registry has the memory + version --------------------
    const memListRes = await fetch(`${baseURL}/v1/memory_stores/${memStore.id}/memories`, {
      headers: { 'x-api-key': apiKey },
    });
    expect(memListRes.status).toBe(200);
    const memList = (await memListRes.json()) as { data: ApiMemoryResp[] };
    expect(memList.data).toHaveLength(1);
    const memory = memList.data[0]!;
    expect(memory.path).toBe('/foo.txt');
    expect(memory.content_sha256).toBe(helloASha);
    expect(memory.content_size_bytes).toBe(helloA.length);

    const versionsAfterARes = await fetch(
      `${baseURL}/v1/memory_stores/${memStore.id}/memory_versions?memory_id=${memory.id}`,
      { headers: { 'x-api-key': apiKey } },
    );
    expect(versionsAfterARes.status).toBe(200);
    const versionsAfterA = (await versionsAfterARes.json()) as {
      data: ApiMemoryVersionResp[];
      next_page: string | null;
    };
    expect(versionsAfterA.data).toHaveLength(1);
    const versionA = versionsAfterA.data[0]!;
    expect(versionA.content_sha256).toBe(helloASha);
    expect(versionA.created_by).toEqual({ type: 'session_actor', session_id: sessionA.id });
    expect(versionA.redacted_at).toBeNull();

    // ----- Session B: read the bytes (cross-session-persistence proof) ---
    // and write "hello-B" -------------------------------------------------
    const sessionBRes = await fetch(`${baseURL}/v1/sessions`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({
        environment_id: environmentId,
        agent_id: agentId,
        resources: [{ type: 'memory_store', memory_store_id: memStore.id, access: 'read_write' }],
      }),
    });
    expect(sessionBRes.status).toBe(200);
    const sessionB = (await sessionBRes.json()) as { id: string };
    await preCreateTopic(workspaceId, sessionB.id);

    const helloB = Buffer.from('hello-B', 'utf8');
    const helloBSha = sha256Hex(helloB);
    let bytesReadInB: Buffer | null = null;

    await runOneSession({
      sessionId: sessionB.id,
      action: async (sandbox) => {
        // CRITICAL: this read MUST return what Session A wrote. Without the
        // dispatcher's seed step (in dispatcher.ts), the
        // InMemory tmpdir would be empty and this read would throw ENOENT.
        bytesReadInB = await sandbox.files.read(memoryPath);
        // Now overwrite with "hello-B" to extend the version chain.
        await sandbox.files.write(memoryPath, helloB);
      },
      watcherDrainMs: 1500,
    });

    expect(bytesReadInB).not.toBeNull();
    expect(bytesReadInB!.toString('utf8')).toBe('hello-A');

    // ----- Assert: version chain now has TWO versions, DESC by created_at -
    const versionsAfterBRes = await fetch(
      `${baseURL}/v1/memory_stores/${memStore.id}/memory_versions?memory_id=${memory.id}`,
      { headers: { 'x-api-key': apiKey } },
    );
    expect(versionsAfterBRes.status).toBe(200);
    const versionsAfterB = (await versionsAfterBRes.json()) as {
      data: ApiMemoryVersionResp[];
      next_page: string | null;
    };
    expect(versionsAfterB.data).toHaveLength(2);
    // DESC by created_at: index 0 is the newest (Session B's hello-B).
    const newest = versionsAfterB.data[0]!;
    const oldest = versionsAfterB.data[1]!;
    expect(newest.content_sha256).toBe(helloBSha);
    expect(newest.created_by).toEqual({ type: 'session_actor', session_id: sessionB.id });
    expect(oldest.content_sha256).toBe(helloASha);
    expect(oldest.created_by).toEqual({ type: 'session_actor', session_id: sessionA.id });
    expect(newest.created_at >= oldest.created_at).toBe(true);

    // The current memory state must reflect Session B's write.
    const memAfterRes = await fetch(
      `${baseURL}/v1/memory_stores/${memStore.id}/memories/${memory.id}`,
      { headers: { 'x-api-key': apiKey } },
    );
    expect(memAfterRes.status).toBe(200);
    const memAfter = (await memAfterRes.json()) as ApiMemoryResp;
    expect(memAfter.content_sha256).toBe(helloBSha);
    expect(memAfter.content_size_bytes).toBe(helloB.length);

    // ----- Redact Session A's version -------------------------------------
    const redactRes = await fetch(
      `${baseURL}/v1/memory_stores/${memStore.id}/memory_versions/${oldest.id}/redact`,
      {
        method: 'POST',
        headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
        body: '{}',
      },
    );
    expect(redactRes.status).toBe(200);
    const redacted = (await redactRes.json()) as ApiMemoryVersionResp;
    expect(redacted.id).toBe(oldest.id);
    expect(redacted.redacted_at).not.toBeNull();
    expect(redacted.content_sha256).toBeNull();
    expect(redacted.path).toBeNull();

    // GET versions again: the redacted_at field on the oldest row is now set.
    const versionsAfterRedactRes = await fetch(
      `${baseURL}/v1/memory_stores/${memStore.id}/memory_versions?memory_id=${memory.id}`,
      { headers: { 'x-api-key': apiKey } },
    );
    expect(versionsAfterRedactRes.status).toBe(200);
    const versionsAfterRedact = (await versionsAfterRedactRes.json()) as {
      data: ApiMemoryVersionResp[];
      next_page: string | null;
    };
    expect(versionsAfterRedact.data).toHaveLength(2);
    const oldestAfterRedact = versionsAfterRedact.data.find((v) => v.id === oldest.id);
    expect(oldestAfterRedact).toBeDefined();
    expect(oldestAfterRedact!.redacted_at).not.toBeNull();
    // Newest version is unaffected.
    const newestAfterRedact = versionsAfterRedact.data.find((v) => v.id === newest.id);
    expect(newestAfterRedact).toBeDefined();
    expect(newestAfterRedact!.redacted_at).toBeNull();
  }, 240_000);
});

function escapeRegex(input: string): string {
  return input.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

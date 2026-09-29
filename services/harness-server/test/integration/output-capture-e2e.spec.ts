// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import type { FastifyInstance } from 'fastify';
import type { AddressInfo } from 'node:net';
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

interface FileResp {
  id: string;
  created_at: string;
  filename: string;
  mime_type: string;
  size_bytes: number;
  type: 'file';
  downloadable: boolean;
  scope: { type: 'session'; id: string } | null;
}

interface ListResp {
  data: FileResp[];
  next_page: string | null;
}

/**
 * End-to-end output-capture test against the `InMemorySandboxRuntime`.
 *
 * The test boots a real registry app + a `Dispatcher` wired with an
 * `OutputCaptureFakeHarness`. The fake harness, when it receives a
 * `user.message`, writes a JSON blob into the sandbox at
 * `/mnt/session/outputs/result.json` and emits `agent.tool_result`. The runner
 * triggers `OutputIndexer.indexSession(...)` while the session remains live,
 * and its final stop pass runs before deactivating mounts. The indexer walks
 * the InMemory sandbox FS at `/mnt/session/outputs/`, registers the blob via
 * the registry's internal session-files endpoint (which derives the session
 * scope and `downloadable=true` from the path), and the test polls
 * `GET /v1/files?scope_id=…` until the artifact appears.
 *
 * No E2B dependency: the test runs entirely against the InMemory runtime, so
 * it gates on Postgres + RustFS + Kafka only (same dev stack the rest of the
 * harness integration suite needs).
 */
class OutputCaptureFakeHarness implements AgentHarness {
  private q: AgentEvent[] = [];
  private resolvers: Array<(v: IteratorResult<AgentEvent>) => void> = [];
  private done = false;
  private sandbox: SandboxHandle | undefined;
  /**
   * Promise resolved after the harness has written the output and emitted the
   * corresponding tool result. The event pump may still be indexing when it
   * resolves, so the test polls the Files API while the runner remains live.
   */
  readonly written: Promise<void>;
  private resolveWritten!: () => void;
  private rejectWritten!: (err: Error) => void;
  // Set when the test's REAL session boots (sandbox present). Stale-message
  // harnesses leave this false so the test's poll-loop can pick out the
  // active one.
  hasSandbox = false;

  constructor() {
    this.written = new Promise<void>((resolve, reject) => {
      this.resolveWritten = resolve;
      this.rejectWritten = reject;
    });
  }

  async start(input: SessionStartInput): Promise<void> {
    // The dispatcher hands the per-session sandbox handle in via
    // SessionStartInput. We capture it so `submit()` can write into the
    // session's `/mnt/session/outputs/` directory directly.
    //
    // Sandbox may be absent if the dispatcher's `getSession` returned null
    // (e.g. stale Kafka messages from prior test runs whose workspaces have
    // been wiped from the registry DB). Those sessions go through chat-only
    // mode — `submit()` no-ops below so stale-message replay doesn't crash
    // the consumer.
    this.sandbox = input.sandbox;
    this.hasSandbox = input.sandbox !== undefined;
  }

  async submit(ev: UserEvent): Promise<void> {
    if (this.done) return;
    if (ev.kind !== 'user.message') return;
    if (!this.sandbox) {
      // Stale-message replay path — no sandbox means the session isn't
      // recognized by the registry. Log + return so Kafka commits the offset
      // and stops retrying, freeing the consumer to process the live session.
      console.warn('OutputCaptureFakeHarness: skipping stale message (no sandbox attached)');
      return;
    }
    try {
      // Mirror what the agent's `write` tool would do — land bytes straight
      // at /mnt/session/outputs/result.json. The InMemory runtime backs
      // this via tmpdir; mountSessionOutputs already created the dir
      // (the dispatcher's spawnRunner calls it).
      await this.sandbox.files.write(
        '/mnt/session/outputs/result.json',
        Buffer.from('{"ok":true}', 'utf8'),
      );

      this.emit({
        kind: 'agent.tool_result',
        payload: { tool_use_id: 'write_result', content: 'wrote result.json' },
      });
      this.emit({
        kind: 'agent.message',
        payload: { type: 'assistant', content: [{ type: 'text', text: 'wrote result.json' }] },
      });

      this.resolveWritten();
    } catch (e) {
      this.rejectWritten(e as Error);
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

describe('Phase 5.1 output-capture end-to-end (InMemory)', () => {
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
        keyPrefix: 'test/output-capture-e2e/',
      }),
    });

    kafka = new Kafka({
      clientId: 'phase5.1-output-capture-e2e',
      brokers: [KAFKA_BROKERS],
      metadataMaxAge: 1000,
    });
    topicPrefix = `phase51_output_${Date.now()}_${Math.random().toString(36).slice(2, 8)}.`;
    store = new KafkaTranscriptStore({ kafka, topicPrefix });

    const { db } = await getTestDb();
    app = buildCombinedTestApp({
      db,
      oidc: { allowedIssuers: [], audience: 'orca-managed-agents' },
      store,
      sse: { bufferSize: 256, dropAgeMs: 5000, heartbeatMs: 15000 },
      jwtMinter: buildTestJwtMinter(),
      fileStore,
    });
    await app.listen({ host: '127.0.0.1', port: 0 });
    baseURL = `http://127.0.0.1:${(app.server.address() as AddressInfo).port}`;
    workspaceId = uniqueWorkspace('outcapture');
    apiKey = await createTestApiKey(db, workspaceId);
    environmentId = await createTestEnvironment(baseURL, apiKey);
  }, 60_000);

  afterAll(async () => {
    await store.close();
    await fileStore.close();
    await app.close();
    await closeTestDb();
  });

  /** Pre-create the session topic before starting dispatcher for deterministic broker readiness. */
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

  it('captures /mnt/session/outputs/result.json into /v1/files?scope_id=ses_…', async () => {
    // 1) Create an agent + session via the public registry HTTP API. No file
    //    resources are needed — output capture is independent of mounted
    //    files.
    const agentId = await createTestAgent(baseURL, apiKey);
    const sessionResp = await fetch(`${baseURL}/v1/sessions`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({ environment_id: environmentId, agent_id: agentId }),
    });
    expect(sessionResp.status).toBe(200);
    const session = (await sessionResp.json()) as { id: string };

    // Pre-create the topic before dispatcher starts.
    await preCreateTopic(workspaceId, session.id);

    // 2) Build the dispatcher. The `OutputCaptureFakeHarness` writes the
    //    output blob inside `submit()`, so we hold a reference to it via the
    //    factory closure — the dispatcher constructs one harness per session,
    //    but this test only ever runs a single session.
    // Use the prefixed dispatcher path so this spec consumes only its own
    // session topic. The shared CI Kafka broker can retain topics from earlier
    // integration suites, and replaying those would hide the live session
    // behind unrelated stale work.
    const liveHarnesses: OutputCaptureFakeHarness[] = [];
    const sandboxRuntime = new InMemorySandboxRuntime();
    const registryClient = new RegistryClient(
      baseURL,
      async () => 'test-internal-service-token-at-least-32-chars',
    );

    const dispatcher = new Dispatcher({
      kafka,
      groupId: `phase5.1-output-capture-e2e-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`,
      store,
      topicPrefix,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => {
        const h = new OutputCaptureFakeHarness();
        liveHarnesses.push(h);
        return h;
      },
      registry: registryClient,
      gatewayMcpUrl: 'http://127.0.0.1:1/mcp',
      fileStore,
      sandboxRuntime,
      // S3/output config — the InMemory path doesn't actually use the S3
      // client (output indexer walks the sandbox FS), but the dispatcher
      // gates `mountSessionOutputs` on `s3Bucket && s3Endpoint &&
      // s3KeyPrefix` being set, and `outputMount` is required for the
      // indexer wiring closure to fire.
      s3Bucket: S3_BUCKET,
      s3Endpoint: S3_ENDPOINT,
      s3KeyPrefix: 'test/output-capture-e2e/',
      s3Client,
    });

    let listedBeforeStop: ListResp | null = null;
    try {
      await dispatcher.start();
      // Allow consumer rebalance so the first POST-event lands in this
      // dispatcher's subscription range.
      await new Promise((r) => setTimeout(r, 4500));

      // 3) POST a `user.message` — this wakes the dispatcher's spawnRunner
      //    path, which mounts /mnt/session/outputs (InMemory: just mkdir),
      //    builds the agent_toolset, starts the FakeHarness, and forwards the
      //    user event into `submit()`. The harness's submit() writes the
      //    output blob.
      const eventResp = await fetch(`${baseURL}/v1/sessions/${session.id}/events`, {
        method: 'POST',
        headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
        body: JSON.stringify({
          events: [{ type: 'user.message', content: [{ type: 'text', text: 'go' }] }],
        }),
      });
      expect(eventResp.status).toBe(200);

      // Wait for the LIVE harness (the one whose `start()` saw a sandbox) to
      // finish writing — bounded so a wiring bug surfaces as a clear timeout
      // instead of an infinite hang.
      const writePromise = new Promise<OutputCaptureFakeHarness>((resolve, reject) => {
        const start = Date.now();
        const tick = (): void => {
          const live = liveHarnesses.find((h) => h.hasSandbox);
          if (live) {
            live.written.then(() => resolve(live)).catch(reject);
            return;
          }
          // Keep a generous CI bound even though topic-prefix isolation should
          // make this near-immediate after the dispatcher's initial join.
          if (Date.now() - start > 45_000) {
            reject(new Error('live (sandbox-attached) harness was never observed within 45s'));
            return;
          }
          setTimeout(tick, 100);
        };
        tick();
      });
      await writePromise;

      // 4) The tool result should trigger indexing while the runner is still
      //    live. Poll before dispatcher.stop() to prove capture does not
      //    depend on the idle timeout or shutdown path.
      const start = Date.now();
      while (Date.now() - start < 30_000) {
        const list = await fetch(`${baseURL}/v1/files?scope_id=${session.id}`, {
          headers: { 'x-api-key': apiKey },
        });
        expect(list.status).toBe(200);
        const body = (await list.json()) as ListResp;
        if (body.data.length > 0) {
          listedBeforeStop = body;
          break;
        }
        await new Promise((r) => setTimeout(r, 200));
      }
      expect(listedBeforeStop).not.toBeNull();

      // The final stop scan is a safety pass for background writes and must
      // not create a duplicate for the unchanged result.json.
      await dispatcher.stop();
    } finally {
      // Idempotent — `dispatcher.stop()` above is the success path; this
      // catches the failure cases.
      await dispatcher.stop().catch(() => {});
    }

    // 5) After the shutdown fallback, the session still has exactly the one
    //    file that became visible during the live turn.
    const afterStop = await fetch(`${baseURL}/v1/files?scope_id=${session.id}`, {
      headers: { 'x-api-key': apiKey },
    });
    expect(afterStop.status).toBe(200);
    const listed = (await afterStop.json()) as ListResp;
    expect(listed.data).toHaveLength(1);
    expect(listed.data[0]?.id).toBe(listedBeforeStop!.data[0]?.id);
    const indexed = listed.data[0]!;
    expect(indexed.scope).toEqual({ type: 'session', id: session.id });
    expect(indexed.downloadable).toBe(true);
    expect(indexed.filename).toBe('result.json');
    expect(indexed.size_bytes).toBe(11); // length of '{"ok":true}'

    // 6) `GET /v1/files/:id/content` must return the exact bytes the harness
    //    wrote — closes the loop the SDK relies on
    //    (`client.beta.files.list({ scope_id }) → files[].content`).
    const content = await fetch(`${baseURL}/v1/files/${indexed.id}/content`, {
      headers: { 'x-api-key': apiKey },
    });
    expect(content.status).toBe(200);
    const back = await content.text();
    expect(back).toBe('{"ok":true}');
  }, 120_000);
});

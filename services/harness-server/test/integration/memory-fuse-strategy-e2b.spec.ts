// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import type { FastifyInstance } from 'fastify';
import type { AddressInfo } from 'node:net';
import { createHash } from 'node:crypto';
import { Kafka } from 'kafkajs';
import { Pool } from 'pg';
import {
  DeleteObjectCommand,
  ListObjectsV2Command,
  S3Client,
  type _Object,
} from '@aws-sdk/client-s3';
import { nanoid } from 'nanoid';
import { KafkaTranscriptStore, sessionTopicName } from '@orca/transcript-store';
import { LocalFileStore, S3BlobStore, applyMigrations } from '@orca/file-store';

import { Dispatcher } from '../../src/runner/dispatcher.js';
import { E2BSandboxRuntime } from '../../src/sandbox/e2b/runtime.js';
import { RegistryClient } from '../../src/clients/registry.js';
import { SessionCredsMinter } from '../../src/auth/sts-creds.js';
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
import { readPublicS3Config, logSkipReason } from './_e2b-helpers.js';

const E2B_API_KEY = process.env['E2B_API_KEY'];
const E2B_TEMPLATE_ID = process.env['E2B_TEMPLATE_ID'];

const FILESTORE_DB =
  process.env['FILESTORE_DATABASE_URL'] ?? 'postgres://orca:orca@localhost:5432/filestore';
const KAFKA_BROKERS = process.env['KAFKA_BROKERS'] ?? 'localhost:9092';

/**
 * Public S3 config (S3_PUBLIC_*). Required because the E2B sandbox can't
 * reach the runner's localhost RustFS; s3fs inside the sandbox needs an
 * endpoint reachable from E2B's network.
 */
const publicS3 = readPublicS3Config();

/**
 * Gated end-to-end test of the real `MemoryFuseStrategy` path
 * against a live E2B sandbox built from the operator-published `orca-default`
 * template. Mirrors `file-prefetch-outputs-e2b.spec.ts` (file resources) for
 * `memory_store` resources.
 *
 * Flow:
 *   1. POST `/v1/memory_stores` to create a per-test-run unique store name
 *      (so concurrent runs don't clash within the same workspace).
 *   2. POST a session attaching the memory store as a resource (default
 *      mount path `/mnt/memory/{store_name}/`).
 *   3. Wire a `Dispatcher` with the real `E2BSandboxRuntime`. The
 *      `MemoryLiveBashHarness` test double captures the per-session sandbox
 *      handle, runs `bash` to write `hello-from-agent` into
 *      `/mnt/memory/{store_name}/note.txt`, then waits.
 *   4. Wait up to 30s for the harness-side `MemoryVersionWatcher` to detect
 *      the write and register a version. Poll `/v1/memory_stores/:id/memories`
 *      and `/v1/memory_stores/:id/memory_versions?memory_id=...` until the
 *      version row appears.
 *   5. Assert the registered version's `sha256` matches sha256(hello-from-agent),
 *      and `GET /v1/memory_stores/:id/memories/:memory_id?view=full` returns
 *      the same content.
 *   6. afterAll best-effort deletes every object the test created under
 *      `BLOB_KEY_PREFIX` (the registry's S3MemoryBlobStore prefix). The
 *      memory_store itself is deleted via `DELETE /v1/memory_stores/:id`.
 *
 * Triple-gate: skip when E2B_API_KEY OR E2B_TEMPLATE_ID OR the public-S3
 * envs are missing. Logging the reason makes it obvious in CI logs why the
 * suite no-ops.
 */

interface ApiMemoryStoreResp {
  id: string;
  name: string;
}

interface ApiMemoryResp {
  id: string;
  store_id: string;
  path: string;
  current_sha256: string;
  size_bytes: number;
}

interface ApiMemoryVersionResp {
  id: string;
  memory_id: string;
  sha256: string;
  written_at: string;
  redacted_at: string | null;
  written_by_session_id: string | null;
}

function sha256Hex(buf: Buffer): string {
  return createHash('sha256').update(buf).digest('hex');
}

/**
 * Test harness double: captures the per-session sandbox handle, then on the
 * priming `user.message` runs `bash` to write a note into the memory mount.
 * Mirrors `LiveBashHarness` from `file-prefetch-outputs-e2b.spec.ts` but for memory
 * resources (no FUSE-mount-of-blobs symlink dance — the FUSE mount IS the
 * agent-visible state).
 */
class MemoryLiveBashHarness implements AgentHarness {
  private q: AgentEvent[] = [];
  private resolvers: Array<(v: IteratorResult<AgentEvent>) => void> = [];
  private done = false;
  private sandbox: SandboxHandle | undefined;
  hasSandbox = false;

  /** Bash exit code from the write, populated by `submit()`. */
  writeExitCode = -1;
  /** Captures bash stdout from the post-write read-back. */
  readBack = '';
  /** Resolved once `submit()` finishes its sequence. */
  readonly ready: Promise<void>;
  private resolveReady!: () => void;
  private rejectReady!: (err: Error) => void;

  /** Path inside the sandbox to write into. */
  notePath = '';
  /** Bytes to write. */
  noteContent = '';

  constructor() {
    this.ready = new Promise<void>((resolve, reject) => {
      this.resolveReady = resolve;
      this.rejectReady = reject;
    });
  }

  async start(input: SessionStartInput): Promise<void> {
    this.sandbox = input.sandbox;
    this.hasSandbox = input.sandbox !== undefined;
  }

  async submit(ev: UserEvent): Promise<void> {
    if (this.done) return;
    if (ev.kind !== 'user.message') return;
    if (!this.sandbox) {
      console.warn('MemoryLiveBashHarness: skipping stale message (no sandbox)');
      return;
    }
    try {
      // 1) Write the note via bash (echo > path). The mount path's parent dir
      //    is created by MemoryFuseStrategy.activate (mkdir -p mountPath).
      const write = await this.sandbox.run({
        tool: 'bash',
        args: {
          command: `echo -n '${shellEscape(this.noteContent)}' > '${shellEscape(this.notePath)}'`,
        },
      });
      this.writeExitCode = write.exit_code ?? -1;

      // 2) Read it back inside the sandbox so the FUSE round-trip is proven
      //    locally (independent of the watcher's S3 scan path).
      const read = await this.sandbox.run({
        tool: 'bash',
        args: { command: `cat '${shellEscape(this.notePath)}'` },
      });
      this.readBack = read.stdout ?? '';

      this.emit({
        kind: 'agent.message',
        payload: {
          type: 'assistant',
          content: [{ type: 'text', text: 'memory write done' }],
        },
      });

      this.resolveReady();
    } catch (e) {
      this.rejectReady(e as Error);
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

function shellEscape(s: string): string {
  return s.replace(/'/g, `'\\''`);
}

const skipReason = !E2B_API_KEY
  ? 'E2B_API_KEY unset'
  : !E2B_TEMPLATE_ID
    ? 'E2B_TEMPLATE_ID unset (operator must build + push orca-default first)'
    : !publicS3
      ? 'S3_PUBLIC_* envs unset (need a real S3 endpoint reachable from E2B)'
      : null;
if (skipReason) {
  logSkipReason('memory-fuse-strategy-e2b.spec', skipReason);
}

describe.skipIf(!E2B_API_KEY || !E2B_TEMPLATE_ID || !publicS3)(
  'Phase 8 MemoryFuseStrategy end-to-end (E2B + orca-default)',
  () => {
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

    // All object kinds share one deployment root. Memory blobs MUST match the
    // registry's `S3MemoryBlobStore`
    // (otherwise the harness's FUSE write goes to one prefix and the
    // registry's `/content` route + the watcher's S3 list both look at a
    // different one). The registry's setup.ts hardcodes `test/registry-memory/`
    // for `S3MemoryBlobStore.keyPrefix`; mirror it here. Per-run uniqueness
    // is achieved via the per-store workspace/store segments below
    // — the workspaceId is `uniqueWorkspace('memfuse')` = guaranteed unique.
    const MEMORY_KEY_PREFIX = 'test/registry-memory/';
    const PUBLIC_S3 = publicS3!;

    beforeAll(async () => {
      fileStorePool = new Pool({ connectionString: FILESTORE_DB });
      await applyMigrations(fileStorePool);
      s3Client = new S3Client({
        endpoint: PUBLIC_S3.endpoint,
        region: PUBLIC_S3.region,
        credentials: {
          accessKeyId: PUBLIC_S3.accessKey,
          secretAccessKey: PUBLIC_S3.secretKey,
        },
        forcePathStyle: true,
      });
      // FileStore remains wired because separate mode preserves its historical
      // sandbox-runtime dependency gate. The memory strategy itself does not
      // consume FileStore when no file resources are attached.
      fileStore = new LocalFileStore({
        pool: fileStorePool,
        blobStore: new S3BlobStore({
          client: s3Client,
          bucket: PUBLIC_S3.bucket,
          keyPrefix: MEMORY_KEY_PREFIX,
        }),
      });

      kafka = new Kafka({
        clientId: 'phase8-memory-fuse-e2b',
        brokers: [KAFKA_BROKERS],
        metadataMaxAge: 1000,
      });
      store = new KafkaTranscriptStore({ kafka });

      // The registry's `buildTestMemoryStore` reads `S3_*` (NOT `S3_PUBLIC_*`)
      // for its blob store. That's fine when the harness + registry both
      // reach the same RustFS instance, but for this test the harness writes
      // memory blobs via FUSE to the PUBLIC bucket while the registry must
      // read the SAME bucket so the watcher's S3 scan + the
      // full Memory view observe the agent's writes. Override by
      // pointing the env at the public S3 just for the duration of the
      // singleton-build call. The S3 client is constructed at build time and
      // captured into the LocalMemoryStore closure, so resetting env after
      // doesn't affect future reads — they'll keep using the public-S3 client.
      const savedEnv = {
        S3_ENDPOINT: process.env['S3_ENDPOINT'],
        S3_REGION: process.env['S3_REGION'],
        S3_BUCKET: process.env['S3_BUCKET'],
        S3_ACCESS_KEY: process.env['S3_ACCESS_KEY'],
        S3_SECRET_KEY: process.env['S3_SECRET_KEY'],
      };
      process.env['S3_ENDPOINT'] = PUBLIC_S3.endpoint;
      process.env['S3_REGION'] = PUBLIC_S3.region;
      process.env['S3_BUCKET'] = PUBLIC_S3.bucket;
      process.env['S3_ACCESS_KEY'] = PUBLIC_S3.accessKey;
      process.env['S3_SECRET_KEY'] = PUBLIC_S3.secretKey;
      const memoryStore = await buildTestMemoryStore();
      for (const [k, v] of Object.entries(savedEnv)) {
        if (v === undefined) delete process.env[k];
        else process.env[k] = v;
      }

      const { db } = await getTestDb();
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
      workspaceId = uniqueWorkspace('memfuse');
      apiKey = await createTestApiKey(db, workspaceId);
      environmentId = await createTestEnvironment(baseURL, apiKey);
    }, 120_000);

    afterAll(async () => {
      // Best-effort cleanup. MEMORY_KEY_PREFIX is shared with the registry
      // (the singleton hardcodes `test/registry-memory/`), so scope cleanup to
      // `${MEMORY_KEY_PREFIX}workspaces/${workspaceId}/` — uniqueWorkspace('memfuse')
      // guarantees no other test uses that subprefix. Wrap in try/catch so a
      // partial cleanup never fails the test.
      const cleanupPrefixes = [`${MEMORY_KEY_PREFIX}workspaces/${workspaceId}/`];
      for (const prefix of cleanupPrefixes) {
        try {
          let token: string | undefined;
          do {
            const params: ConstructorParameters<typeof ListObjectsV2Command>[0] = {
              Bucket: PUBLIC_S3.bucket,
              Prefix: prefix,
            };
            if (token !== undefined) params.ContinuationToken = token;
            const listed = await s3Client.send(new ListObjectsV2Command(params));
            const objects: _Object[] = listed.Contents ?? [];
            for (const obj of objects) {
              if (!obj.Key) continue;
              await s3Client
                .send(new DeleteObjectCommand({ Bucket: PUBLIC_S3.bucket, Key: obj.Key }))
                .catch(() => {});
            }
            token = listed.IsTruncated ? listed.NextContinuationToken : undefined;
          } while (token);
        } catch (e) {
          console.warn(`memory-fuse-strategy-e2b.spec: cleanup of ${prefix} failed`, e);
        }
      }

      // Guard each cleanup so a half-finished beforeAll surfaces the original
      // error instead of cascading TypeErrors from undefined refs.
      if (store) await store.close().catch(() => {});
      if (fileStore) await fileStore.close().catch(() => {});
      if (fileStorePool) await fileStorePool.end().catch(() => {});
      if (app) await app.close().catch(() => {});
      await closeTestMemoryStore().catch(() => {});
      await closeTestDb().catch(() => {});
    });

    /**
     * Pre-create the session topic before the dispatcher's regex consumer
     * subscribes — same rationale as the file/output e2e specs.
     */
    async function preCreateTopic(ws: string, ses: string): Promise<void> {
      const admin = kafka.admin();
      await admin.connect();
      try {
        await admin.createTopics({
          waitForLeaders: true,
          topics: [{ topic: sessionTopicName(ws, ses), numPartitions: 1 }],
        });
      } finally {
        await admin.disconnect();
      }
    }

    it('mounts memory_store via FUSE; agent write surfaces as a registered memory version', async () => {
      // 1) Create the memory store. Per-test-run unique name keeps concurrent
      //    runs in the same workspace from clashing on the (workspace_id, name)
      //    uniqueness constraint.
      const storeName = `memstore-test-${Date.now()}-${nanoid(6)}`;
      const createStoreRes = await fetch(`${baseURL}/v1/memory_stores`, {
        method: 'POST',
        headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
        body: JSON.stringify({ name: storeName }),
      });
      expect(createStoreRes.status).toBe(200);
      const memStore = (await createStoreRes.json()) as ApiMemoryStoreResp;

      // 2) Create an agent + session attaching the memory store.
      const agentId = await createTestAgent(baseURL, apiKey);
      const sessionResp = await fetch(`${baseURL}/v1/sessions`, {
        method: 'POST',
        headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
        body: JSON.stringify({
          environment_id: environmentId,
          agent_id: agentId,
          resources: [
            {
              type: 'memory_store',
              memory_store_id: memStore.id,
              access: 'read_write',
            },
          ],
        }),
      });
      expect(sessionResp.status).toBe(200);
      const session = (await sessionResp.json()) as { id: string };

      await preCreateTopic(workspaceId, session.id);

      // 3) Build the dispatcher with the real E2B runtime + the operator-pushed
      //    orca-default template. The canonical S3 root tells both the FUSE
      //    strategy and watcher where the workspace namespaces are rooted.
      const liveHarnesses: MemoryLiveBashHarness[] = [];
      const harness = new MemoryLiveBashHarness();
      const noteContent = 'hello-from-agent';
      const noteContentBuf = Buffer.from(noteContent, 'utf8');
      const expectedSha = sha256Hex(noteContentBuf);
      harness.notePath = `/mnt/memory/${storeName}/note.txt`;
      harness.noteContent = noteContent;

      const sandboxRuntime = new E2BSandboxRuntime({
        apiKey: E2B_API_KEY!,
        templateId: E2B_TEMPLATE_ID!,
      });
      const credsMinter = new SessionCredsMinter({
        bucket: PUBLIC_S3.bucket,
        outputsRoot: MEMORY_KEY_PREFIX,
        memoryRoot: MEMORY_KEY_PREFIX,
        staticAccessKey: PUBLIC_S3.accessKey,
        staticSecretKey: PUBLIC_S3.secretKey,
        stsEndpoint: PUBLIC_S3.endpoint,
      });
      const registryClient = new RegistryClient(
        baseURL,
        async () => 'test-internal-service-token-at-least-32-chars',
      );

      const dispatcher = new Dispatcher({
        kafka,
        groupId: `phase8-memory-fuse-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`,
        store,
        anthropicApiKey: 'unused',
        modelDefault: 'fake',
        harnessFactory: () => {
          if (liveHarnesses.length === 0) {
            liveHarnesses.push(harness);
            return harness;
          }
          const stale = new MemoryLiveBashHarness();
          stale.notePath = harness.notePath;
          stale.noteContent = harness.noteContent;
          liveHarnesses.push(stale);
          return stale;
        },
        registry: registryClient,
        gatewayMcpUrl: 'http://127.0.0.1:1/mcp',
        fileStore,
        sandboxRuntime,
        credsMinter,
        s3Bucket: PUBLIC_S3.bucket,
        s3Endpoint: PUBLIC_S3.endpoint,
        s3KeyPrefix: MEMORY_KEY_PREFIX,
        s3Region: PUBLIC_S3.region,
        // Speed the watcher up so the test runs in <30s. Production keeps
        // the 2000ms default.
        memoryWatcherIntervalMs: 1000,
        s3Client,
      });

      try {
        await dispatcher.start();
        // Allow rebalance to settle so the first POST-event lands.
        await new Promise((r) => setTimeout(r, 4500));

        // 4) POST a `user.message` — the dispatcher acquires a real E2B
        //    sandbox, mints creds, mounts /mnt/memory/{store_name}/ via s3fs
        //    (RW), then hands the sandbox to `MemoryLiveBashHarness.submit()`
        //    which writes the note.
        const eventResp = await fetch(`${baseURL}/v1/sessions/${session.id}/events`, {
          method: 'POST',
          headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
          body: JSON.stringify({
            events: [{ type: 'user.message', content: [{ type: 'text', text: 'mem-write' }] }],
          }),
        });
        expect(eventResp.status).toBe(200);

        // Wait for the live (sandbox-attached) harness to finish. Bounded so
        // a wiring bug surfaces as a timeout rather than a hang.
        await new Promise<void>((resolve, reject) => {
          const start = Date.now();
          const tick = (): void => {
            const live = liveHarnesses.find((h) => h.hasSandbox);
            if (live) {
              live.ready.then(() => resolve()).catch(reject);
              return;
            }
            if (Date.now() - start > 90_000) {
              reject(new Error('live harness was never observed within 90s'));
              return;
            }
            setTimeout(tick, 250);
          };
          tick();
        });

        const live = liveHarnesses.find((h) => h.hasSandbox);
        expect(live).toBeDefined();
        expect(live!.writeExitCode).toBe(0);
        expect(live!.readBack).toBe(noteContent);

        // 5) Wait up to 30s for the registry-side `MemoryVersionWatcher` to
        //    detect the write and register a version. We poll
        //    `GET /v1/memory_stores/:id/memories` because that's the agent-
        //    visible API surface; the underlying watcher's S3 ListObjectsV2
        //    sweeps every memoryWatcherIntervalMs (set to 1000ms above), so
        //    this normally fires within 1-2 polls.
        const start = Date.now();
        let memory: ApiMemoryResp | null = null;
        while (Date.now() - start < 30_000) {
          const memListRes = await fetch(`${baseURL}/v1/memory_stores/${memStore.id}/memories`, {
            headers: { 'x-api-key': apiKey },
          });
          expect(memListRes.status).toBe(200);
          const body = (await memListRes.json()) as { data: ApiMemoryResp[] };
          if (body.data.length > 0) {
            const found = body.data.find((m) => m.path === 'note.txt');
            if (found && found.current_sha256 === expectedSha) {
              memory = found;
              break;
            }
          }
          await new Promise((r) => setTimeout(r, 500));
        }
        expect(memory).not.toBeNull();
        expect(memory!.size_bytes).toBe(noteContentBuf.length);

        // 6) Versions: there should be exactly one, with the matching sha256.
        const versionsRes = await fetch(
          `${baseURL}/v1/memory_stores/${memStore.id}/memory_versions?memory_id=${memory!.id}`,
          { headers: { 'x-api-key': apiKey } },
        );
        expect(versionsRes.status).toBe(200);
        const versions = (await versionsRes.json()) as {
          data: ApiMemoryVersionResp[];
          next_page: string | null;
        };
        expect(versions.data).toHaveLength(1);
        const version = versions.data[0]!;
        expect(version.sha256).toBe(expectedSha);
        expect(version.written_by_session_id).toBe(session.id);
        expect(version.redacted_at).toBeNull();

        // 7) Round-trip the content via the full Memory view to close the SDK loop.
        const contentRes = await fetch(
          `${baseURL}/v1/memory_stores/${memStore.id}/memories/${memory!.id}?view=full`,
          { headers: { 'x-api-key': apiKey } },
        );
        expect(contentRes.status).toBe(200);
        const back = (await contentRes.json()) as { content: string | null };
        expect(back.content).toBe(noteContent);

        // 8) Stop the dispatcher (drains the watcher + tears down the sandbox).
        await dispatcher.stop();
      } finally {
        // Idempotent — covers the failure path.
        await dispatcher.stop().catch(() => {});

        // Best-effort delete of the memory_store. The afterAll bucket sweep
        // covers the blob side; this drops the metadata row + cascade.
        try {
          await fetch(`${baseURL}/v1/memory_stores/${memStore.id}`, {
            method: 'DELETE',
            headers: { 'x-api-key': apiKey },
          });
        } catch {
          // ignore — afterAll handles the bucket cleanup; per-test workspace
          // is unique so a leftover row doesn't affect other tests.
        }
      }
    }, 300_000);
  },
);

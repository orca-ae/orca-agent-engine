// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import type { FastifyInstance } from 'fastify';
import type { AddressInfo } from 'node:net';
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
 * The E2B sandbox lives in E2B's network and cannot reach `localhost:9000`
 * (the GitHub Actions runner's RustFS). The output FUSE mount therefore needs
 * a publicly reachable S3 endpoint — surfaced via `S3_PUBLIC_*` env. When any
 * of the five required envs is missing, the suite skips with a clear warn
 * rather than fails. The local RustFS envs (`S3_ENDPOINT`, `S3_BUCKET`, …) are
 * still consumed by the harness's other integration specs but NOT by this one.
 */
const publicS3 = readPublicS3Config();

/**
/**
 * Gated end-to-end test of host-side file prefetch plus the real output FUSE path
 * against a live E2B sandbox built from the operator-published
 * `orca-default` template (`E2B_TEMPLATE_ID`).
 *
 * The default GitHub CI workflow does NOT run this — the suite is triple-gated
 * on `E2B_API_KEY` (required for any live sandbox), `E2B_TEMPLATE_ID`
 * (required for the FUSE-capable image; the upstream e2bdev/code-interpreter
 * template lacks `s3fs-fuse` and would fail), and the `S3_PUBLIC_*` envs
 * (required because the E2B sandbox cannot reach the runner's localhost
 * RustFS; s3fs needs an endpoint reachable from E2B's network). The
 * `nightly-e2b.yml` workflow is the only place all three are wired up; that
 * workflow runs once a day on master and surfaces failures via the standard
 * GitHub run notifications.
 *
 * Flow:
 *   1. Pre-seed two workspace-isolated blobs into the public S3 bucket via
 *      `S3BlobStore.put`.
 *      Register both as `purpose=agent` File rows in the registry so the
 *      harness's strategy factory can resolve sha256 on activate.
 *   2. POST a session with two file resources pinned to
 *      `mount_strategy=tarball_prefetch`; sandbox credentials never include
 *      the workspace file namespace.
 *   3. Wire a `Dispatcher` with the real `E2BSandboxRuntime`. The
 *      `LiveBashHarness` test double captures the per-session sandbox handle,
 *      then runs `bash` to `cat` each mount_path and to write a file under
 *      `/mnt/session/outputs/`.
 *   4. Stop the dispatcher; the `OutputIndexer` runs against the S3 prefix
 *      and registers the written file as `purpose=agent_output`,
 *      `scope_id=ses_…`, `downloadable=true`.
 *   5. Assert each cat returns the original bytes; assert
 *      `GET /v1/files?scope_id=…` lists `note.md` with `downloadable=true`.
 *   6. afterAll best-effort deletes every object under the per-test KEY_PREFIX
 *      so the shared bucket doesn't accumulate leftover blobs.
 */

interface FileResp {
  id: string;
  filename: string;
  size_bytes: number;
  sha256: string;
  purpose: 'agent' | 'agent_output';
  scope_id: string | null;
  downloadable: boolean;
}

interface ListResp {
  data: FileResp[];
  next_page: string | null;
}

/**
 * Test double that captures the live sandbox + executes a sequence of `bash`
 * tool calls when it receives the priming `user.message`. The bash output is
 * stashed on the harness so the test body can assert against it after the
 * write completes.
 */
class LiveBashHarness implements AgentHarness {
  private q: AgentEvent[] = [];
  private resolvers: Array<(v: IteratorResult<AgentEvent>) => void> = [];
  private done = false;
  private sandbox: SandboxHandle | undefined;
  hasSandbox = false;

  /** Bytes read from the small + large blobs, populated by `submit()`. */
  readResults: { small: Buffer; large: Buffer } = {
    small: Buffer.alloc(0),
    large: Buffer.alloc(0),
  };
  /** Bash exit codes from each cat + the write, populated by `submit()`. */
  readExitCodes: { small: number; large: number; write: number } = {
    small: -1,
    large: -1,
    write: -1,
  };
  securityExitCode = -1;
  /** Resolved when `submit()` finishes its sequence (cat both, write note). */
  readonly ready: Promise<void>;
  private resolveReady!: () => void;
  private rejectReady!: (err: Error) => void;

  /** Mount paths to cat (set by the test before the priming message lands). */
  mountPaths: { small: string; large: string } = { small: '', large: '' };

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
      // Stale-message replay path (no sandbox attached). Skip silently so the
      // Kafka offset commits and the live consumer keeps moving.
      console.warn('LiveBashHarness: skipping stale message (no sandbox)');
      return;
    }
    try {
      // 1) Prove the agent boundary cannot reuse the outer template's mount
      //    privilege or inherited sudo configuration.
      const security = await this.sandbox.run({
        tool: 'bash',
        args: {
          command:
            'test "$(id -u):$(id -g)" = 1000:1000 && ' +
            "grep -Eq '^CapEff:[[:space:]]+0+$' /proc/self/status && " +
            "grep -Eq '^CapBnd:[[:space:]]+0+$' /proc/self/status && " +
            "grep -Eq '^NoNewPrivs:[[:space:]]+1$' /proc/self/status && " +
            'test "$PPID" -eq 1 && test "$(cat /proc/1/comm)" = bwrap && ' +
            'test ! -e /dev/fuse && ' +
            'if sudo -n /bin/sh -c id >/dev/null 2>&1; then exit 90; fi',
        },
      });
      this.securityExitCode = security.exit_code ?? -1;

      // 2) Verify the small blob via sha256 + byte count. Raw `cat` over the
      //    E2B SDK's stdout pipe loses fidelity for non-ASCII bytes (the SDK
      //    UTF-8-decodes stdout), so we sidestep the round-trip entirely and
      //    compare digests instead — same pattern as the large blob below.
      const small = await this.sandbox.run({
        tool: 'bash',
        args: {
          command: `wc -c < '${shellEscape(this.mountPaths.small)}' && sha256sum '${shellEscape(this.mountPaths.small)}' | awk '{print $1}'`,
        },
      });
      this.readResults.small = Buffer.from(small.stdout ?? '', 'utf8');
      this.readExitCodes.small = small.exit_code ?? -1;

      // 3) cat the large blob. Use `wc -c` + a sha256sum so we don't pull
      //    50 MB through the SDK's stdout pipe; we compare against the
      //    expected sha256 (computed test-side) instead.
      const large = await this.sandbox.run({
        tool: 'bash',
        args: {
          command: `wc -c < '${shellEscape(this.mountPaths.large)}' && sha256sum '${shellEscape(this.mountPaths.large)}' | awk '{print $1}'`,
        },
      });
      this.readResults.large = Buffer.from(large.stdout ?? '', 'utf8');
      this.readExitCodes.large = large.exit_code ?? -1;

      // 4) Write a tiny note into /mnt/session/outputs/. The output indexer
      //    picks it up at session-end via ListObjectsV2 against the s3fs
      //    prefix.
      const writeOut = await this.sandbox.run({
        tool: 'bash',
        args: {
          command: `echo -n 'phase-5.1 e2b-fuse note' > /mnt/session/outputs/note.md`,
        },
      });
      this.readExitCodes.write = writeOut.exit_code ?? -1;

      this.emit({
        kind: 'agent.message',
        payload: {
          type: 'assistant',
          content: [{ type: 'text', text: 'cat + write done' }],
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

/**
 * Triple-gate: skip when E2B_API_KEY OR E2B_TEMPLATE_ID OR the public-S3
 * envs are missing.
 *
 * The template id matters because the FUSE path requires the `orca-default`
 * image (s3fs-fuse + constrained mount helper) — using
 * e2bdev/code-interpreter would fail at the first FUSE mount.
 *
 * The public-S3 envs matter because the E2B sandbox cannot reach the
 * runner-local RustFS at `localhost:9000` — s3fs inside the sandbox needs an
 * endpoint reachable from E2B's network (e.g. `https://s3.us-east-1.amazonaws.com`).
 * Logging the reason makes it obvious in CI logs why the suite no-ops.
 */
const skipReason = !E2B_API_KEY
  ? 'E2B_API_KEY unset'
  : !E2B_TEMPLATE_ID
    ? 'E2B_TEMPLATE_ID unset (operator must build + push orca-default first)'
    : !publicS3
      ? 'S3_PUBLIC_* envs unset (need a real S3 endpoint reachable from E2B)'
      : null;
if (skipReason) {
  logSkipReason('file-prefetch-outputs-e2b.spec', skipReason);
}

describe.skipIf(!E2B_API_KEY || !E2B_TEMPLATE_ID || !publicS3)(
  'file prefetch + output FUSE end-to-end (E2B + orca-default)',
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
    let s3BlobStore: S3BlobStore;

    // Per-test-run unique prefix: avoids object-key clashes when multiple
    // concurrent CI runs (e.g. retries) hit the same shared bucket. The
    // `nanoid(8)` suffix is short enough to keep keys readable while still
    // collision-resistant in practice.
    const KEY_PREFIX = `test/file-prefetch-outputs/${Date.now()}-${nanoid(8)}/`;
    // describe.skipIf above guarantees publicS3 is non-null inside the body.
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
      s3BlobStore = new S3BlobStore({
        client: s3Client,
        bucket: PUBLIC_S3.bucket,
        keyPrefix: KEY_PREFIX,
      });
      fileStore = new LocalFileStore({ pool: fileStorePool, blobStore: s3BlobStore });

      kafka = new Kafka({
        clientId: 'file-prefetch-outputs-e2b',
        brokers: [KAFKA_BROKERS],
        metadataMaxAge: 1000,
      });
      store = new KafkaTranscriptStore({ kafka });

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
      workspaceId = uniqueWorkspace('prefetch');
      apiKey = await createTestApiKey(db, workspaceId);
      environmentId = await createTestEnvironment(baseURL, apiKey);
    }, 120_000);

    afterAll(async () => {
      // Best-effort: rm every object under KEY_PREFIX so the test's blobs +
      // outputs don't accumulate in the shared bucket. Wrapped in try/catch
      // so a partial cleanup (e.g. a single 403 mid-list) never fails the
      // test — the prefix is per-run unique anyway, so leftover objects only
      // cost storage, not correctness.
      try {
        let token: string | undefined;
        do {
          const params: ConstructorParameters<typeof ListObjectsV2Command>[0] = {
            Bucket: PUBLIC_S3.bucket,
            Prefix: KEY_PREFIX,
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
        console.warn(`file-prefetch-outputs-e2b.spec: cleanup of ${KEY_PREFIX} failed`, e);
      }

      // Guard each cleanup so a half-finished beforeAll surfaces the original
      // error instead of cascading TypeErrors from undefined refs.
      if (store) await store.close().catch(() => {});
      if (fileStore) await fileStore.close().catch(() => {});
      if (fileStorePool) await fileStorePool.end().catch(() => {});
      if (app) await app.close().catch(() => {});
      await closeTestDb().catch(() => {});
    });

    /**
     * Pre-create the session topic before the dispatcher's regex consumer
     * subscribes — same rationale as `output-capture-e2e.spec.ts` and
     * `file-mount-e2e.spec.ts`: kafkajs evaluates the regex once at
     * `subscribe()` time.
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

    /**
     * Pre-seed the blob into S3 via the same `S3BlobStore` the production
     * strategy reads through. The two-level fanout key
     * `{prefix}{sha[0:2]}/{sha[2:4]}/{sha}` is computed inside `S3BlobStore.put`
     * — there is no key-layout drift between this seed path and what
     * the host-side file materializer expects.
     */
    async function seedBlob(content: Buffer): Promise<{ fileId: string; sha256: string }> {
      const f = new FormData();
      f.append(
        'file',
        new Blob([new Uint8Array(content)], { type: 'application/octet-stream' }),
        'seed',
      );
      const res = await fetch(`${baseURL}/v1/files`, {
        method: 'POST',
        headers: { 'x-api-key': apiKey },
        body: f,
      });
      if (res.status !== 200) {
        throw new Error(`seed file POST failed: ${res.status} ${await res.text()}`);
      }
      const file = (await res.json()) as { id: string; sha256: string };
      return { fileId: file.id, sha256: file.sha256 };
    }

    it('prefetches file resources + captures /mnt/session/outputs into the registry', async () => {
      // 1) Pre-seed two blobs:
      //    - small: 1 KB — fast to read, easy to cat back inline.
      //    - large: 50 MB — exercises the FUSE read path under realistic
      //      sustained throughput so a "works for 1 KB only" regression
      //      surfaces here instead of in production.
      const smallBytes = Buffer.alloc(1024);
      for (let i = 0; i < smallBytes.length; i += 1) smallBytes[i] = i % 251;
      const small = await seedBlob(smallBytes);

      const largeBytes = Buffer.alloc(50 * 1024 * 1024);
      // Fill with a deterministic non-trivial pattern so the sha256
      // assertion is meaningful (an all-zero buffer would falsely match
      // any same-size all-zero blob).
      for (let i = 0; i < largeBytes.length; i += 1) largeBytes[i] = (i * 31 + 7) & 0xff;
      const large = await seedBlob(largeBytes);
      // The seed path round-trips through the store's hasher; we use the
      // returned sha256 to assert what s3fs serves matches.
      const expectedLargeSha = large.sha256;

      // 2) Create an agent + session pinning both resources to the only
      // public file strategy: host-side tarball prefetch.
      const agentId = await createTestAgent(baseURL, apiKey);
      const sessionResp = await fetch(`${baseURL}/v1/sessions`, {
        method: 'POST',
        headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
        body: JSON.stringify({
          environment_id: environmentId,
          agent_id: agentId,
          resources: [
            {
              type: 'file',
              file_id: small.fileId,
              mount_path: '/mnt/inputs/small.bin',
              access: 'read_only',
              mount_strategy: 'tarball_prefetch',
            },
            {
              type: 'file',
              file_id: large.fileId,
              mount_path: '/mnt/inputs/large.bin',
              access: 'read_only',
              mount_strategy: 'tarball_prefetch',
            },
          ],
        }),
      });
      expect(sessionResp.status).toBe(200);
      const session = (await sessionResp.json()) as {
        id: string;
        resources: Array<{ mount_strategy: string | null }>;
      };
      // Sanity-check the contract round-trip — if this drops to null, the
      // dispatcher would no longer be proving the file namespace stays out of
      // sandbox S3 credentials.
      expect(session.resources.every((r) => r.mount_strategy === 'tarball_prefetch')).toBe(true);

      await preCreateTopic(workspaceId, session.id);

      // 3) Build the dispatcher with the REAL E2B runtime + the
      //    operator-pushed orca-default template. Static-key STS minter is
      //    the dev path; production swaps in `s3StsRoleArn` but the FUSE
      //    write path is identical.
      const liveHarnesses: LiveBashHarness[] = [];
      const harness = new LiveBashHarness();
      harness.mountPaths = {
        small: '/mnt/inputs/small.bin',
        large: '/mnt/inputs/large.bin',
      };

      const sandboxRuntime = new E2BSandboxRuntime({
        apiKey: E2B_API_KEY!,
        templateId: E2B_TEMPLATE_ID!,
      });
      const credsMinter = new SessionCredsMinter({
        bucket: PUBLIC_S3.bucket,
        outputsRoot: KEY_PREFIX,
        memoryRoot: KEY_PREFIX,
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
        groupId: `file-prefetch-outputs-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`,
        store,
        anthropicApiKey: 'unused',
        modelDefault: 'fake',
        harnessFactory: () => {
          // The dispatcher constructs one harness per session; replay of any
          // stale topics from prior runs gets a sandbox-less harness whose
          // `submit()` returns early. Track them so we can pick the live
          // (sandbox-attached) one in the wait-for-write loop below.
          if (liveHarnesses.length === 0) {
            liveHarnesses.push(harness);
            return harness;
          }
          const stale = new LiveBashHarness();
          stale.mountPaths = harness.mountPaths;
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
        s3KeyPrefix: KEY_PREFIX,
        s3Region: PUBLIC_S3.region,
        s3Client,
      });

      try {
        await dispatcher.start();
        await new Promise((r) => setTimeout(r, 4500));

        // 4) POST a `user.message` — the dispatcher acquires a real E2B
        //    sandbox, mints output-only creds, mounts
        //    /mnt/session/outputs (read-write) via s3fs, prefetches the two
        //    file resources host-side under /mnt/inputs/, then hands
        //    the sandbox to `LiveBashHarness.submit()` which cats both
        //    files and writes note.md.
        const eventResp = await fetch(`${baseURL}/v1/sessions/${session.id}/events`, {
          method: 'POST',
          headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
          body: JSON.stringify({
            events: [{ type: 'user.message', content: [{ type: 'text', text: 'fuse-it' }] }],
          }),
        });
        expect(eventResp.status).toBe(200);

        // Wait for the live (sandbox-attached) harness to finish its bash
        // sequence. Bounded so a wiring bug surfaces as a timeout rather
        // than a hang.
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

        // 5) Stop the dispatcher — runs the OutputIndexer BEFORE
        //    deactivating mounts (so s3fs is still serving). The indexer
        //    lists `outputs/{ws}/{ses}/` in S3 and registers note.md as a
        //    File row scoped to this session.
        await dispatcher.stop();
      } finally {
        // Idempotent — covers the failure path.
        await dispatcher.stop().catch(() => {});
      }

      // 6) Assert the cat results.
      const live = liveHarnesses.find((h) => h.hasSandbox);
      expect(live).toBeDefined();
      expect(live!.securityExitCode).toBe(0);
      expect(live!.readExitCodes.small).toBe(0);
      expect(live!.readExitCodes.large).toBe(0);
      expect(live!.readExitCodes.write).toBe(0);

      // Small blob: verified via wc + sha256sum inside the sandbox so the
      // E2B SDK's UTF-8 stdout decoding can't corrupt non-ASCII bytes
      // mid-roundtrip. First line is the byte count, second is the sha256.
      const smallOutput = live!.readResults.small.toString('utf8').trim().split('\n');
      expect(smallOutput).toHaveLength(2);
      expect(parseInt(smallOutput[0]!.trim(), 10)).toBe(smallBytes.length);
      expect(smallOutput[1]!.trim()).toBe(small.sha256);

      // Large blob: same pattern — sha256sum'd inside the sandbox so we
      // never pull 50 MB through the SDK stdout pipe.
      const largeOutput = live!.readResults.large.toString('utf8').trim().split('\n');
      expect(largeOutput).toHaveLength(2);
      expect(parseInt(largeOutput[0]!.trim(), 10)).toBe(largeBytes.length);
      expect(largeOutput[1]!.trim()).toBe(expectedLargeSha);

      // 7) Poll `/v1/files?scope_id=<session.id>` until note.md is indexed.
      //    The indexer ran inside `dispatcher.stop()` so this is normally
      //    immediate; a generous bound absorbs any S3 list-after-write
      //    eventual-consistency lag.
      const start = Date.now();
      let listed: ListResp | null = null;
      while (Date.now() - start < 30_000) {
        const list = await fetch(`${baseURL}/v1/files?scope_id=${session.id}`, {
          headers: { 'x-api-key': apiKey },
        });
        expect(list.status).toBe(200);
        const body = (await list.json()) as ListResp;
        if (body.data.length > 0) {
          listed = body;
          break;
        }
        await new Promise((r) => setTimeout(r, 500));
      }

      expect(listed).not.toBeNull();
      const noteFile = listed!.data.find((f) => f.filename === 'note.md');
      expect(noteFile).toBeDefined();
      expect(noteFile!.purpose).toBe('agent_output');
      expect(noteFile!.scope_id).toBe(session.id);
      expect(noteFile!.downloadable).toBe(true);
      expect(noteFile!.size_bytes).toBe(Buffer.byteLength('phase-5.1 e2b-fuse note', 'utf8'));

      // 8) Round-trip the bytes via /v1/files/:id/content to close the loop
      //    the SDK relies on (`client.beta.files.list({ scope_id })` →
      //    `client.beta.files.content`).
      const content = await fetch(`${baseURL}/v1/files/${noteFile!.id}/content`, {
        headers: { 'x-api-key': apiKey },
      });
      expect(content.status).toBe(200);
      const back = await content.text();
      expect(back).toBe('phase-5.1 e2b-fuse note');
    }, 300_000);
  },
);

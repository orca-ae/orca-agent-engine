// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { existsSync, mkdirSync, mkdtempSync, readdirSync, rmSync, writeFileSync } from 'node:fs';
import { copyFile, mkdir, readdir } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
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
import { WorkDirManager } from '../../src/git/work-dir.js';
import type { CloneInput, GitWorker } from '../../src/git/git-worker.js';
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
import { LocalSecretStore } from '../../../registry-service-ts/src/secrets/index.ts';
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

/**
 * End-to-end github_repository mount test against the
 * `InMemorySandboxRuntime`.
 *
 * The test boots a real registry app + a `Dispatcher` wired with a fake
 * `GitWorker` (copies a seeded source dir into the work dir verbatim — no
 * actual `git clone` invocation), a real `WorkDirManager` against a tmp
 * baseDir, and a `RepoE2EFakeHarness`. The flow:
 *
 *   1. Seed a working tree with `README.md`. The "clone" itself is faked —
 *      the seeded working tree IS what shows up in the sandbox.
 *   2. Create a registry app with a write-capable SecretStore.
 *   3. Create an agent + a session with a valid HTTPS `github_repository`
 *      URL and a raw write-only `authorization_token`.
 *   4. Submit a `user.message`. The dispatcher's `spawnRunner` path:
 *        - mints a `git-creds` JWT,
 *        - writes `/etc/profile.d/orca-git-creds.sh` with URL + token,
 *        - picks `GitCloneStrategy`,
 *        - acquires a host-side work dir,
 *        - calls the fake `cloneInto` (which copies the seed),
 *        - streams the working tree (and `.git/`) into the sandbox at the
 *          resolved `mount_path`.
 *   5. Assert the FakeHarness saw the sandbox; read `/workspace/repo/README.md`
 *      from the sandbox and confirm it matches the seed bytes.
 *   6. Trigger `dispatcher.stop()`. Assert the per-session work dir was
 *      rm-rf'd.
 *
 * The `git push` round-trip (in-sandbox `git push origin master`) is NOT
 * validated here — that path requires `bash + git` inside the sandbox runtime,
 * which the InMemorySandboxRuntime's tmpdir-on-host shell can technically do
 * but the credential-helper protocol round-trip (`git push` → bash helper →
 * /v1/git-creds → vault) is covered separately by the `orca-git-creds`
 * unit test (`test/unit/orca-git-creds.spec.ts`).
 */

class RepoE2EFakeHarness implements AgentHarness {
  private q: AgentEvent[] = [];
  private resolvers: Array<(v: IteratorResult<AgentEvent>) => void> = [];
  private done = false;
  private sandbox: SandboxHandle | undefined;
  /**
   * Promise resolved once `submit()` has run for THIS harness — i.e. once
   * the FakeHarness has observed the user.message that wakes the runner.
   * Tests await this before reading from the sandbox so the assertions
   * happen after the dispatcher's strategy.activate has streamed bytes in.
   */
  readonly submitted: Promise<void>;
  private resolveSubmitted!: () => void;
  /**
   * Set when the dispatcher calls `start()` with a non-undefined sandbox.
   * Stale-message harnesses (instantiated for prior test runs' topics whose
   * workspaces no longer exist) leave this false; the test's poll-loop picks
   * out the live one by `hasSandbox === true`.
   */
  hasSandbox = false;

  constructor() {
    this.submitted = new Promise<void>((resolve) => {
      this.resolveSubmitted = resolve;
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
      console.warn('RepoE2EFakeHarness: skipping stale message (no sandbox attached)');
      return;
    }
    // Echo a single agent.message so the SSE stream has activity (matches the
    // pattern from output-capture-e2e). Then resolve the submitted promise so
    // the test can run its assertions against the live sandbox.
    this.emit({
      kind: 'agent.message',
      payload: { type: 'assistant', content: [{ type: 'text', text: 'cloned' }] },
    });
    this.resolveSubmitted();
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
        if (head !== undefined) yield head;
        continue;
      }
      const next = await new Promise<IteratorResult<AgentEvent>>((resolve) => {
        this.resolvers.push(resolve);
      });
      if (next.done) return;
      yield next.value;
    }
  }

  /** Test-only: lets the test reach into the sandbox once `start()` ran. */
  liveSandbox(): SandboxHandle | undefined {
    return this.sandbox;
  }

  private emit(e: AgentEventInput): void {
    const event = withCanonicalAgentEventEnvelope(e);
    const r = this.resolvers.shift();
    if (r) r({ value: event, done: false });
    else this.q.push(event);
  }
}

class InspectableInMemorySandboxRuntime extends InMemorySandboxRuntime {
  private readonly acquiredSandboxes = new Map<string, SandboxHandle>();

  override async acquire(
    ...args: Parameters<InMemorySandboxRuntime['acquire']>
  ): Promise<SandboxHandle> {
    const sandbox = await super.acquire(...args);
    this.acquiredSandboxes.set(sandbox.id, sandbox);
    return sandbox;
  }

  controlPlaneSandbox(id: string): SandboxHandle {
    const sandbox = this.acquiredSandboxes.get(id);
    if (!sandbox) throw new Error(`sandbox ${id} has not been acquired`);
    return sandbox;
  }
}

/**
 * Build a fake `GitWorker` that copies the contents of `seedDir` into the
 * clone's `dest` instead of running `git clone`. Includes `.git/` if present
 * under the seed so the strategy's working-tree streaming covers both the
 * tracked files and the `.git/HEAD` etc.
 *
 * This bypasses network access while preserving the dispatcher → credential
 * resolution → strategy chain. The actual GitWorker is exercised by
 * `test/unit/git-worker.spec.ts` against a `file://` remote with
 * `filterBlobs: false`; this fake is the minimal contract surface the
 * dispatcher → strategy chain needs.
 */
function makeFakeGitWorker(seedDir: string, onClone?: (input: CloneInput) => void): GitWorker {
  return {
    async cloneInto(input) {
      onClone?.(input);
      mkdirSync(input.dest, { recursive: true });
      await recursiveCopy(seedDir, input.dest);
      return { commit: 'a'.repeat(40) };
    },
    async listWorkingTree(_dest) {
      void _dest;
      return [];
    },
  };
}

async function recursiveCopy(src: string, dst: string): Promise<void> {
  const entries = await readdir(src, { withFileTypes: true });
  await mkdir(dst, { recursive: true });
  for (const e of entries) {
    const s = join(src, e.name);
    const d = join(dst, e.name);
    if (e.isDirectory()) {
      await recursiveCopy(s, d);
    } else if (e.isFile()) {
      await copyFile(s, d);
    }
  }
}

describe('Phase 7 github_repository end-to-end (InMemory + fake GitWorker)', () => {
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

  // Per-suite tmp dirs (seed working tree + harness work-dir baseDir).
  // Cleaned up in afterAll.
  let seedWorktree: string;
  let workDirBase: string;

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
        keyPrefix: 'test/github-repo-e2e/',
      }),
    });

    kafka = new Kafka({
      clientId: 'phase7-github-repo-e2e',
      brokers: [KAFKA_BROKERS],
      metadataMaxAge: 1000,
    });
    store = new KafkaTranscriptStore({ kafka });

    // ----- Seed working tree -----
    seedWorktree = mkdtempSync(join(tmpdir(), 'gh-e2e-seed-'));
    workDirBase = mkdtempSync(join(tmpdir(), 'gh-e2e-workdir-'));

    // Seed a working tree with a single README.md. This is what the
    //    fake GitWorker will copy into the sandbox at activate time.
    writeFileSync(join(seedWorktree, 'README.md'), '# orca-test-e2e\n');
    // Add a tiny .git/ subdir so the strategy's working-tree walk also
    // covers the .git/ branch (the production clone always ships .git/ for
    // in-sandbox git operations).
    mkdirSync(join(seedWorktree, '.git'), { recursive: true });
    writeFileSync(join(seedWorktree, '.git', 'HEAD'), 'ref: refs/heads/main\n');

    // ----- Registry app + api key -----
    const { db } = await getTestDb();
    app = buildCombinedTestApp({
      db,
      oidc: { allowedIssuers: [], audience: 'orca-managed-agents' },
      store,
      sse: { bufferSize: 256, dropAgeMs: 5000, heartbeatMs: 15000 },
      jwtMinter: buildTestJwtMinter(),
      fileStore,
      secretStore: new LocalSecretStore(),
    });
    await app.listen({ host: '127.0.0.1', port: 0 });
    baseURL = `http://127.0.0.1:${(app.server.address() as AddressInfo).port}`;
    workspaceId = uniqueWorkspace('githubrepoe2e');
    apiKey = await createTestApiKey(db, workspaceId);
    environmentId = await createTestEnvironment(baseURL, apiKey);
  }, 60_000);

  afterAll(async () => {
    await store.close();
    await fileStore.close();
    await app.close();
    await closeTestDb();
    for (const d of [seedWorktree, workDirBase]) {
      try {
        rmSync(d, { recursive: true, force: true });
      } catch {
        // ignore
      }
    }
  });

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

  function exactSessionTopicPattern(ws: string, ses: string): RegExp {
    return new RegExp(`^${escapeRegex(sessionTopicName(ws, ses))}$`);
  }

  it('clones github_repository → working tree appears at mount_path → work dir is cleaned up on stop', async () => {
    // 1) Use the Claude-compatible raw-token path. Registry stores the token
    //    behind a resource-owned SecretStore reference; dispatcher resolves it
    //    through the internal credential endpoint before invoking GitWorker.
    const repoUrl = `https://github.com/orca-ae/github-repo-e2e-${workspaceId}.git`;
    const authorizationToken = 'ghp_fake-pat-not-actually-used';

    // 2) Create the agent.
    const agentId = await createTestAgent(baseURL, apiKey);

    // 3) Create the session with a github_repository resource pointing at the
    //    bare remote URL.
    const sessionResp = await fetch(`${baseURL}/v1/sessions`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({
        environment_id: environmentId,
        agent_id: agentId,
        resources: [
          {
            type: 'github_repository',
            url: repoUrl,
            authorization_token: authorizationToken,
            access: 'read_write',
          },
        ],
      }),
    });
    expect(sessionResp.status).toBe(200);
    const session = (await sessionResp.json()) as {
      id: string;
      resources: Array<{ type: string; mount_path: string }>;
    };
    // Default mount_path for the HTTPS repo URL is `/workspace/<repo-name>/`.
    // Read the registry's resolved path back so the test stays independent of
    // the URL → mount-path derivation logic.
    const repoResource = session.resources.find((r) => r.type === 'github_repository');
    expect(repoResource).toBeDefined();
    const mountPath = repoResource!.mount_path;
    expect(mountPath.startsWith('/workspace/')).toBe(true);
    expect(mountPath.endsWith('/')).toBe(true);

    // Pre-create the session's Kafka topic so the dispatcher's regex
    // subscription picks it up at initial join.
    await preCreateTopic(workspaceId, session.id);

    // 4) Build the dispatcher. The harness factory captures every
    //    `RepoE2EFakeHarness` instance; we identify the live one (the one
    //    whose `start()` saw a non-undefined sandbox) at assertion time.
    const liveHarnesses: RepoE2EFakeHarness[] = [];
    const sandboxRuntime = new InspectableInMemorySandboxRuntime();
    const registryClient = new RegistryClient(
      baseURL,
      async () => 'test-internal-service-token-at-least-32-chars',
    );
    const workDir = new WorkDirManager({ baseDir: workDirBase });
    let observedClone: CloneInput | undefined;
    const fakeWorker = makeFakeGitWorker(seedWorktree, (input) => {
      observedClone = input;
    });

    const dispatcher = new Dispatcher({
      kafka,
      groupId: `phase7-github-repo-e2e-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`,
      topicPattern: exactSessionTopicPattern(workspaceId, session.id),
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => {
        const h = new RepoE2EFakeHarness();
        liveHarnesses.push(h);
        return h;
      },
      registry: registryClient,
      gatewayMcpUrl: 'http://127.0.0.1:1/mcp',
      fileStore,
      sandboxRuntime,
      gitWorker: fakeWorker,
      workDir,
      // The dispatcher fails the github_repository setup branch if this is
      // unset; we point it at the in-process registry so the JWT minting
      // (and the embedded URL) round-trip through the real route.
      gitCredsPublicUrl: `${baseURL}/v1/git-creds`,
    });

    try {
      await dispatcher.start();
      // Allow consumer rebalance to settle.
      await new Promise((r) => setTimeout(r, 4500));

      // 5) POST a user.message — wakes spawnRunner → activates GitCloneStrategy
      //    → fake worker copies seed → strategy streams the working tree
      //    into the sandbox.
      const eventResp = await fetch(`${baseURL}/v1/sessions/${session.id}/events`, {
        method: 'POST',
        headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
        body: JSON.stringify({
          events: [{ type: 'user.message', content: [{ type: 'text', text: 'go' }] }],
        }),
      });
      expect(eventResp.status).toBe(200);

      // 6) Wait for the live harness (the one whose `start()` saw a sandbox)
      //    AND for its `submit()` to fire on the user.message we just POSTed.
      //    This guarantees the dispatcher's strategy.activate has streamed
      //    the working tree into the sandbox before we read.
      const live = await new Promise<RepoE2EFakeHarness>((resolve, reject) => {
        const start = Date.now();
        const tick = (): void => {
          const found = liveHarnesses.find((h) => h.hasSandbox);
          if (found) {
            found.submitted.then(() => resolve(found)).catch(reject);
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

      const sandbox = live.liveSandbox();
      expect(sandbox).toBeDefined();
      expect(observedClone).toMatchObject({ url: repoUrl, pat: authorizationToken });

      // 7) The strategy should have streamed the seed working tree into
      //    the sandbox at `mount_path`. Read README.md back and compare.
      const readme = await sandbox!.files.read(`${mountPath}README.md`);
      expect(readme.toString()).toBe('# orca-test-e2e\n');

      // The .git/ subdir is also streamed (in-sandbox git ops depend on it).
      const gitHead = await sandbox!.files.read(`${mountPath}.git/HEAD`);
      expect(gitHead.toString()).toBe('ref: refs/heads/main\n');

      // 8) Sanity: the agent-facing handle cannot read outside its session
      //    resource roots. Inspect the trusted control-plane handle instead
      //    to confirm the JWT minting + profile injection ran end-to-end.
      await expect(sandbox!.files.read('/etc/profile.d/orca-git-creds.sh')).rejects.toThrow(
        /outside session resource roots/,
      );
      const profileBytes = await sandboxRuntime
        .controlPlaneSandbox(sandbox!.id)
        .files.read('/etc/profile.d/orca-git-creds.sh');
      const profileText = profileBytes.toString();
      expect(profileText).toContain('export ORCA_GIT_CREDS_URL=');
      expect(profileText).toContain('export ORCA_GIT_CREDS_TOKEN=');
      expect(profileText).toContain(`${baseURL}/v1/git-creds`);

      // 9) Sanity: WorkDirManager actually created the per-session work dir
      //    on the host. We'll assert it's gone after `dispatcher.stop()`.
      const sessionWorkDir = join(workDirBase, 'sessions', workspaceId, session.id);
      expect(existsSync(sessionWorkDir)).toBe(true);

      // 10) Trigger stop. The runner's stop sequence runs
      //     `workDirReleases` (which rm -rf's the session work dir) BEFORE
      //     deactivating mounts.
      await dispatcher.stop();

      // 11) Verify the work dir is gone.
      expect(existsSync(sessionWorkDir)).toBe(false);

      // Also assert the workspace-level dir contains no leftover sessions
      // (the only one we created should have been cleaned up).
      const workspaceDir = join(workDirBase, 'sessions', workspaceId);
      if (existsSync(workspaceDir)) {
        const remaining = readdirSync(workspaceDir);
        expect(remaining).toHaveLength(0);
      }
    } finally {
      // Idempotent — `dispatcher.stop()` above is the success path.
      await dispatcher.stop().catch(() => {});
    }
  }, 120_000);
});

function escapeRegex(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

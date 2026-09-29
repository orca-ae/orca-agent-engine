// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { mkdtempSync, rmSync } from 'node:fs';
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
import { E2BSandboxRuntime } from '../../src/sandbox/e2b/runtime.js';
import { RegistryClient } from '../../src/clients/registry.js';
import { WorkDirManager } from '../../src/git/work-dir.js';
import { makeGitWorker } from '../../src/git/git-worker.js';
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
import { gitCredentials } from '../../../registry-service-ts/src/persistence/postgres/schema.ts';
import { newId } from '../../../registry-service-ts/src/domain/versioning.ts';
import { DefaultSecretProvider } from '../../../registry-service-ts/src/secrets/default.ts';
import { EnvSecretProvider } from '../../../registry-service-ts/src/secrets/env.ts';
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
import { logSkipReason } from './_e2b-helpers.js';

const E2B_API_KEY = process.env['E2B_API_KEY'];
const E2B_TEMPLATE_ID = process.env['E2B_TEMPLATE_ID'];

const FILESTORE_DB =
  process.env['FILESTORE_DATABASE_URL'] ?? 'postgres://orca:orca@localhost:5432/filestore';
const S3_ENDPOINT = process.env['S3_ENDPOINT'] ?? 'http://localhost:9000';
const S3_ACCESS_KEY = process.env['S3_ACCESS_KEY'] ?? 'minioadmin';
const S3_SECRET_KEY = process.env['S3_SECRET_KEY'] ?? 'minioadmin';
const S3_BUCKET = process.env['S3_BUCKET'] ?? 'orca-files';
const KAFKA_BROKERS = process.env['KAFKA_BROKERS'] ?? 'localhost:9092';

/**
 * Public repo used for the test. ~1 KB, single file `README` with content
 * `Hello World!\n`. Public + unauth'd, so the read path doesn't actually
 * exercise the credential helper — but the env-injection pathway
 * (`/etc/profile.d/orca-git-creds.sh`) is still wired up by the dispatcher,
 * which is what we assert here.
 */
const TEST_REPO_URL = 'https://github.com/octocat/Hello-World';
const TEST_REPO_MOUNT = '/workspace/Hello-World/';

/**
 * Gated end-to-end test of the real `GitCloneStrategy` path
 * against a live E2B sandbox built from the operator-published `orca-default`
 * template. Mirrors the existing `github-repo-e2e.spec.ts` (InMemory + fake
 * GitWorker) but runs the REAL clone against `github.com/octocat/Hello-World`
 * and streams the working tree into a real E2B sandbox.
 *
 * Flow:
 *   1. Create a git credential bound to the public repo URL with a dummy `git_credentials.secret_ref`
 *      that resolves to a benign string (the env-secret provider just echoes
 *      `process.env['ORCA_E2B_TEST_PAT']`). github.com will ignore the
 *      malformed PAT for read operations.
 *   2. POST a session attaching the github_repository resource.
 *   3. Wire a `Dispatcher` with the real `E2BSandboxRuntime` + the real
 *      `makeGitWorker()` + a fresh `WorkDirManager` rooted at a per-test
 *      tmpdir.
 *   4. Submit a `user.message`. The dispatcher's `spawnRunner` path:
 *      - mints a `git-creds` JWT,
 *      - writes `/etc/profile.d/orca-git-creds.sh` with the URL + token,
 *      - clones the public repo host-side (via simple-git),
 *      - streams the working tree (incl. `.git/`) into the E2B sandbox at
 *        `/workspace/Hello-World/`.
 *   5. Once the harness sees the sandbox, run live bash to verify:
 *      - `cat /workspace/Hello-World/README` == `Hello World!\n`
 *      - `git -C /workspace/Hello-World status` exits 0 (.git/ shipped + safe.directory works)
 *      - `cat /etc/profile.d/orca-git-creds.sh` shows ORCA_GIT_CREDS_URL +
 *        ORCA_GIT_CREDS_TOKEN exports.
 *
 * The `git push` round-trip is NOT validated here — that requires a writable
 * repo + a real PAT, which is the operator-spike test in
 * `sandbox-templates/orca-default/README.md`.
 *
 * Double-gate: skip when E2B_API_KEY OR E2B_TEMPLATE_ID is missing. (No public
 * S3 dependency — the working tree is streamed via the SDK's file API.)
 */

class GitRepoLiveBashHarness implements AgentHarness {
  private q: AgentEvent[] = [];
  private resolvers: Array<(v: IteratorResult<AgentEvent>) => void> = [];
  private done = false;
  private sandbox: SandboxHandle | undefined;
  hasSandbox = false;

  /** README content read back from the sandbox via bash `cat`. */
  readmeContent = '';
  /** Exit code from `git -C <mount> status`. */
  gitStatusExitCode = -1;
  /** Profile script bytes read from the sandbox. */
  profileScript = '';
  /** Resolved when `submit()` finishes its sequence. */
  readonly ready: Promise<void>;
  private resolveReady!: () => void;
  private rejectReady!: (err: Error) => void;

  /** Repo mount path inside the sandbox. */
  mountPath = '';

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
      console.warn('GitRepoLiveBashHarness: skipping stale message (no sandbox)');
      return;
    }
    try {
      const mount = stripTrailingSlash(this.mountPath);
      // 1) Read README from the cloned working tree.
      const readme = await this.sandbox.run({
        tool: 'bash',
        args: { command: `cat '${shellEscape(mount)}/README'` },
      });
      this.readmeContent = readme.stdout ?? '';

      // 2) Run `git status` to prove `.git/` shipped + safe.directory works.
      const status = await this.sandbox.run({
        tool: 'bash',
        args: { command: `git -C '${shellEscape(mount)}' status --porcelain` },
      });
      this.gitStatusExitCode = status.exit_code ?? -1;

      // 3) Read the profile script to confirm env injection.
      const prof = await this.sandbox.run({
        tool: 'bash',
        args: { command: `cat /etc/profile.d/orca-git-creds.sh 2>/dev/null || true` },
      });
      this.profileScript = prof.stdout ?? '';

      this.emit({
        kind: 'agent.message',
        payload: {
          type: 'assistant',
          content: [{ type: 'text', text: 'git read done' }],
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

function stripTrailingSlash(s: string): string {
  return s.endsWith('/') ? s.slice(0, -1) : s;
}

const skipReason = !E2B_API_KEY
  ? 'E2B_API_KEY unset'
  : !E2B_TEMPLATE_ID
    ? 'E2B_TEMPLATE_ID unset (operator must build + push orca-default first)'
    : null;
if (skipReason) {
  logSkipReason('github-repo-strategy-e2b.spec', skipReason);
}

describe.skipIf(!E2B_API_KEY || !E2B_TEMPLATE_ID)(
  'Phase 8 GitCloneStrategy end-to-end (E2B + orca-default + public repo)',
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
    let workDirBase: string;
    /**
     * Set as `process.env['ORCA_E2B_TEST_PAT']` so the env secret provider
     * resolves git_credentials.secret_ref to this dummy value. github.com ignores
     * malformed PATs for public-repo read operations, so the clone still
     * succeeds. Cleared in afterAll.
     */
    const DUMMY_PAT = 'dummy-not-a-real-pat';

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
          keyPrefix: 'test/github-repo-strategy-e2b/',
        }),
      });

      kafka = new Kafka({
        clientId: 'phase8-github-repo-e2b',
        brokers: [KAFKA_BROKERS],
        metadataMaxAge: 1000,
      });
      store = new KafkaTranscriptStore({ kafka });

      workDirBase = mkdtempSync(join(tmpdir(), 'gh-e2b-workdir-'));
      process.env['ORCA_E2B_TEST_PAT'] = DUMMY_PAT;

      const { db } = await getTestDb();
      const env = new EnvSecretProvider((k) => process.env[k] ?? null);
      const secretProvider = new DefaultSecretProvider(env, []);
      app = buildCombinedTestApp({
        db,
        oidc: { allowedIssuers: [], audience: 'orca-managed-agents' },
        store,
        sse: { bufferSize: 256, dropAgeMs: 5000, heartbeatMs: 15000 },
        jwtMinter: buildTestJwtMinter(),
        fileStore,
        secretProvider,
      });
      await app.listen({ host: '127.0.0.1', port: 0 });
      baseURL = `http://127.0.0.1:${(app.server.address() as AddressInfo).port}`;
      workspaceId = uniqueWorkspace('ghrepoe2b');
      apiKey = await createTestApiKey(db, workspaceId);
      environmentId = await createTestEnvironment(baseURL, apiKey);
    }, 120_000);

    afterAll(async () => {
      // Guard each cleanup so a half-finished beforeAll (e.g. Postgres
      // unreachable, missing per-service DB) surfaces the original error
      // instead of cascading TypeErrors from undefined refs.
      if (store) await store.close().catch(() => {});
      if (fileStore) await fileStore.close().catch(() => {});
      if (fileStorePool) await fileStorePool.end().catch(() => {});
      if (app) await app.close().catch(() => {});
      await closeTestDb().catch(() => {});
      if (workDirBase) {
        try {
          rmSync(workDirBase, { recursive: true, force: true });
        } catch {
          /* ignore */
        }
      }
      delete process.env['ORCA_E2B_TEST_PAT'];
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

    it('clones a public github.com repo into the E2B sandbox; bash read + git status succeed', async () => {
      // 1) Create the git credential bound to the public repo URL.
      const gitCredentialId = newId('gitcred');
      const now = new Date();
      const { db } = await getTestDb();
      await db.insert(gitCredentials).values({
        id: gitCredentialId,
        workspaceId,
        provider: 'github',
        repoUrl: TEST_REPO_URL,
        secretRef: 'env:ORCA_E2B_TEST_PAT',
        metadata: {},
        archivedAt: null,
        createdAt: now,
        updatedAt: now,
      });

      // 2) Create the agent + session with a github_repository resource.
      const agentId = await createTestAgent(baseURL, apiKey);
      const sessionResp = await fetch(`${baseURL}/v1/sessions`, {
        method: 'POST',
        headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
        body: JSON.stringify({
          environment_id: environmentId,
          agent_id: agentId,
          resources: [
            {
              type: 'github_repository',
              url: TEST_REPO_URL,
              authorization_token: `git_cred://${gitCredentialId}`,
              access: 'read_only',
            },
          ],
        }),
      });
      expect(sessionResp.status).toBe(200);
      const session = (await sessionResp.json()) as {
        id: string;
        resources: Array<{ type: string; mount_path: string }>;
      };
      const repoResource = session.resources.find((r) => r.type === 'github_repository');
      expect(repoResource).toBeDefined();
      expect(repoResource!.mount_path).toBe(TEST_REPO_MOUNT);

      await preCreateTopic(workspaceId, session.id);

      // 3) Build the dispatcher with the real E2B runtime + real GitWorker.
      const liveHarnesses: GitRepoLiveBashHarness[] = [];
      const harness = new GitRepoLiveBashHarness();
      harness.mountPath = TEST_REPO_MOUNT;

      const sandboxRuntime = new E2BSandboxRuntime({
        apiKey: E2B_API_KEY!,
        templateId: E2B_TEMPLATE_ID!,
      });
      const registryClient = new RegistryClient(
        baseURL,
        async () => 'test-internal-service-token-at-least-32-chars',
      );
      const workDir = new WorkDirManager({ baseDir: workDirBase });
      const gitWorker = makeGitWorker();

      const dispatcher = new Dispatcher({
        kafka,
        groupId: `phase8-github-repo-e2b-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`,
        store,
        anthropicApiKey: 'unused',
        modelDefault: 'fake',
        harnessFactory: () => {
          if (liveHarnesses.length === 0) {
            liveHarnesses.push(harness);
            return harness;
          }
          const stale = new GitRepoLiveBashHarness();
          stale.mountPath = harness.mountPath;
          liveHarnesses.push(stale);
          return stale;
        },
        registry: registryClient,
        gatewayMcpUrl: 'http://127.0.0.1:1/mcp',
        fileStore,
        sandboxRuntime,
        gitWorker,
        workDir,
        // Fake URL — the sandbox helper would call it on `git push`, but the
        // read-only test never triggers a push. The dispatcher only needs
        // this for env-injection (writing `ORCA_GIT_CREDS_URL` into the
        // profile script).
        gitCredsPublicUrl: 'http://example.invalid/git-creds',
      });

      try {
        await dispatcher.start();
        await new Promise((r) => setTimeout(r, 4500));

        // 4) POST a `user.message` — wakes spawnRunner → mints JWT → writes
        //    profile script → clones host-side → streams working tree into
        //    sandbox → hands sandbox to harness.submit().
        const eventResp = await fetch(`${baseURL}/v1/sessions/${session.id}/events`, {
          method: 'POST',
          headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
          body: JSON.stringify({
            events: [{ type: 'user.message', content: [{ type: 'text', text: 'go' }] }],
          }),
        });
        expect(eventResp.status).toBe(200);

        // Wait for the live harness's submit to finish. Bounded so a wiring
        // bug surfaces as a timeout rather than a hang.
        await new Promise<void>((resolve, reject) => {
          const start = Date.now();
          const tick = (): void => {
            const live = liveHarnesses.find((h) => h.hasSandbox);
            if (live) {
              live.ready.then(() => resolve()).catch(reject);
              return;
            }
            if (Date.now() - start > 120_000) {
              reject(new Error('live harness was never observed within 120s'));
              return;
            }
            setTimeout(tick, 250);
          };
          tick();
        });

        const live = liveHarnesses.find((h) => h.hasSandbox);
        expect(live).toBeDefined();

        // 5) README content matches.
        expect(live!.readmeContent).toBe('Hello World!\n');

        // 6) `git -C <mount> status` exited 0 — proves `.git/` shipped and
        //    `safe.directory` is configured (the orca-default template's
        //    /etc/gitconfig sets * for safe.directory).
        expect(live!.gitStatusExitCode).toBe(0);

        // 7) The profile script embeds the URL we configured + a non-empty
        //    token. Confirms the JWT minting + env injection ran end-to-end.
        expect(live!.profileScript).toContain('export ORCA_GIT_CREDS_URL=');
        expect(live!.profileScript).toContain('export ORCA_GIT_CREDS_TOKEN=');
        expect(live!.profileScript).toContain('http://example.invalid/git-creds');

        await dispatcher.stop();
      } finally {
        await dispatcher.stop().catch(() => {});
      }
    }, 300_000);
  },
);

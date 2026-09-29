// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import {
  CreateBucketCommand,
  DeleteBucketCommand,
  DeleteObjectCommand,
  ListObjectsV2Command,
  S3Client,
} from '@aws-sdk/client-s3';
import { spawn, type ChildProcess } from 'node:child_process';
import { generateKeyPairSync, randomBytes } from 'node:crypto';
import { createWriteStream, type WriteStream } from 'node:fs';
import { access, mkdir, mkdtemp, readdir, rm, writeFile } from 'node:fs/promises';
import { createServer } from 'node:http';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { Pool } from 'pg';
import { closeServer, listen, readRequest, ScriptedMessages } from './scripted-messages-helpers.js';

export interface UsageReport {
  url: string;
  body: {
    usage_event_id: string;
    model?: string;
    usage: { input_tokens: number; output_tokens: number };
    [key: string]: unknown;
  };
  status: number;
  response: {
    guardrail_usage_state: {
      session_cost_usd?: number;
      session_usage_has_unpriced?: boolean;
      total_tokens: number;
    };
    guardrail_subject_window_state: { daily_cost_usd?: number };
  };
  dropped: boolean;
}

interface OwnedProcess {
  child: ChildProcess;
  log: WriteStream;
  name: string;
  exited: Promise<void>;
  stopped: boolean;
  error?: Error;
}

const root = resolve(dirname(fileURLToPath(import.meta.url)), '../../..');
const delay = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

/**
 * Owns private Registry + Harness data and credentials on explicitly selected infrastructure.
 * Fixture/proxy listeners bind loopback; built services retain their normal bind address,
 * accessed through loopback URLs and ephemeral ports.
 */
export class SpendStack {
  readonly model = new ScriptedMessages();
  readonly reports: UsageReport[] = [];
  readonly prepareFailures: string[] = [];
  readonly databases: string[] = [];
  readonly processes: OwnedProcess[] = [];
  readonly tag = randomBytes(6).toString('hex');
  readonly bucket = `orca-spend-${this.tag}`;
  publicBaseURL = '';
  adminBaseURL = '';
  databaseUrl = '';
  logDirectory = '';
  registryLogPath = '';
  private workDirectory = '';
  private harnessURL = '';
  private internalURL = '';
  private harnessEnv: NodeJS.ProcessEnv = {};
  private harness?: OwnedProcess;
  private bucketCreated = false;
  private closed = false;
  private dropSession: string | undefined;
  private failPrepareSession: string | undefined;
  private readonly adminUrl =
    process.env['ORCA_SPEND_POSTGRES_ADMIN_URL'] ?? 'postgres://orca:orca@127.0.0.1:5432/postgres';
  private readonly pool = new Pool({
    connectionString: this.adminUrl,
    max: 1,
    connectionTimeoutMillis: 5000,
  });
  private readonly s3Endpoint = process.env['ORCA_SPEND_S3_ENDPOINT'] ?? 'http://127.0.0.1:9000';
  private readonly s3AccessKey = process.env['ORCA_SPEND_S3_ACCESS_KEY_ID'] ?? 'minioadmin';
  private readonly s3SecretKey = process.env['ORCA_SPEND_S3_SECRET_ACCESS_KEY'] ?? 'minioadmin';
  private readonly s3 = new S3Client({
    endpoint: this.s3Endpoint,
    region: 'us-east-1',
    forcePathStyle: true,
    maxAttempts: 1,
    credentials: { accessKeyId: this.s3AccessKey, secretAccessKey: this.s3SecretKey },
  });
  private readonly proxy = createServer((req, res) => {
    void (async () => {
      const raw = await readRequest(req);
      const path = req.url ?? '/';
      if (
        this.failPrepareSession &&
        path.endsWith(`/sessions/${this.failPrepareSession}/executions:prepare`)
      ) {
        this.prepareFailures.push(path);
        res.writeHead(503, { 'content-type': 'application/json' });
        res.end(JSON.stringify({ error: 'spend_fixture_restore_unavailable' }));
        return;
      }
      const headers: Record<string, string> = {};
      for (const [name, value] of Object.entries(req.headers)) {
        if (
          !['host', 'connection', 'content-length', 'transfer-encoding'].includes(name) &&
          typeof value === 'string'
        )
          headers[name] = value;
      }
      const upstream = await fetch(`${this.internalURL}${path}`, {
        method: req.method ?? 'GET',
        headers,
        ...(raw ? { body: raw } : {}),
        signal: AbortSignal.timeout(15000),
      });
      const body = await upstream.text();
      if (path.endsWith('/usage')) {
        const dropped =
          upstream.status === 200 &&
          !!this.dropSession &&
          path.includes(`/sessions/${this.dropSession}/`);
        this.reports.push({
          url: path,
          body: JSON.parse(raw) as UsageReport['body'],
          status: upstream.status,
          response: JSON.parse(body) as UsageReport['response'],
          dropped,
        });
        // Lose only the response, after the real Registry has acknowledged its transaction.
        if (dropped) {
          this.dropSession = undefined;
          res.destroy();
          return;
        }
      }
      res.writeHead(upstream.status, {
        'content-type': upstream.headers.get('content-type') ?? 'application/json',
      });
      res.end(body);
    })().catch((error: unknown) => {
      if (!res.headersSent) res.writeHead(502);
      res.end(String(error));
    });
  });

  async start(options: { harness?: boolean } = {}): Promise<void> {
    try {
      for (const entry of [
        'registry-service-ts/dist/migrate.js',
        'registry-service-ts/dist/main.js',
        'harness-server/dist/main.js',
      ]) {
        await access(join(root, 'services', entry)).catch(() => {
          throw new Error(
            `Missing built service ${entry}. Run pnpm -r build before the spend suite.`,
          );
        });
      }
      this.workDirectory = await mkdtemp(join(tmpdir(), 'orca-spend-work-'));
      this.logDirectory = await mkdtemp(join(tmpdir(), 'orca-spend-logs-'));
      await mkdir(join(this.workDirectory, 'home'));
      const urls: Record<string, string> = {};
      for (const kind of ['registry', 'transcript', 'files', 'memory']) {
        const db = `orca_spend_${this.tag}_${kind}`;
        await this.pool.query(`CREATE DATABASE "${db}"`).catch((cause: unknown) => {
          throw new Error(
            'Spend suite needs reachable Postgres with CREATEDB; set ORCA_SPEND_POSTGRES_ADMIN_URL.',
            { cause },
          );
        });
        this.databases.push(db);
        const url = new URL(this.adminUrl);
        url.pathname = `/${db}`;
        urls[kind] = url.toString();
      }
      this.databaseUrl = urls.registry!;
      await this.s3
        .send(new CreateBucketCommand({ Bucket: this.bucket }), {
          abortSignal: AbortSignal.timeout(10000),
        })
        .catch((cause: unknown) => {
          throw new Error(
            'Spend suite needs S3 create/list/delete access; set ORCA_SPEND_S3_ENDPOINT and ORCA_SPEND_S3_ACCESS_KEY_ID/SECRET_ACCESS_KEY.',
            { cause },
          );
        });
      this.bucketCreated = true;
      // Hold all four reservations together so the OS cannot allocate one port twice.
      const reservations = Array.from({ length: 4 }, () => createServer());
      let ports: number[];
      let fixturePort: number;
      let proxyPort: number;
      try {
        ports = await Promise.all(reservations.map(listen));
        // Keep service ports reserved while binding our own fixture listeners;
        // otherwise the OS can give the model/proxy a just-released service port.
        fixturePort = await listen(this.model.server);
        proxyPort = await listen(this.proxy);
      } finally {
        await Promise.all(reservations.map(closeServer));
      }
      const [publicPort, internalPort, adminPort, harnessPort] = ports;
      this.publicBaseURL = `http://127.0.0.1:${publicPort}`;
      this.adminBaseURL = `http://127.0.0.1:${adminPort}`;
      this.internalURL = `http://127.0.0.1:${internalPort}`;
      this.harnessURL = `http://127.0.0.1:${harnessPort}`;
      const { privateKey } = generateKeyPairSync('rsa', { modulusLength: 2048 });
      // Deliberate allowlist: no provider credentials, cloud profiles, proxy variables, or real HOME.
      const common: NodeJS.ProcessEnv = {
        PATH: `${dirname(process.execPath)}:/usr/bin:/bin:/usr/sbin:/sbin`,
        HOME: join(this.workDirectory, 'home'),
        TMPDIR: this.workDirectory,
        NODE_ENV: 'test',
        REGISTRY_LOG_LEVEL: 'error',
        DATABASE_URL: urls.registry,
        TRANSCRIPT_STORE_BACKEND: 'postgres',
        TRANSCRIPT_STORE_DATABASE_URL: urls.transcript,
        FILESTORE_DATABASE_URL: urls.files,
        MEMORYSTORE_DATABASE_URL: urls.memory,
        S3_ENDPOINT: this.s3Endpoint,
        S3_STS_ENDPOINT: this.s3Endpoint,
        S3_BUCKET: this.bucket,
        S3_REGION: 'us-east-1',
        S3_ACCESS_KEY_ID: this.s3AccessKey,
        S3_SECRET_ACCESS_KEY: this.s3SecretKey,
        ALLOW_INSECURE_STATIC_S3_CREDS: 'true',
        INTERNAL_AUTH_MODE: 'static_token',
        INTERNAL_SERVICE_TOKEN: randomBytes(32).toString('hex'),
        SESSION_JWT_PRIVATE_KEY_PEM: String(privateKey.export({ type: 'pkcs8', format: 'pem' })),
        HTTP_PORT: String(publicPort),
        INTERNAL_HTTP_PORT: String(internalPort),
        ADMIN_HTTP_PORT: String(adminPort),
        TRIGGER_SCHEDULER_ENABLED: 'false',
        SSE_HEARTBEAT_MS: '1000',
        DATABASE_POOL_MAX: '2',
        TRANSCRIPT_STORE_POOL_MAX: '2',
        FILESTORE_POOL_MAX: '2',
        MEMORYSTORE_POOL_MAX: '2',
      };
      const migration = this.launch('registry-service-ts', 'migrate', common);
      await this.waitExit(migration, 30000);
      if (migration.child.exitCode !== 0 || migration.error)
        throw new Error('Registry migration failed');
      migration.stopped = true;
      const registry = this.launch('registry-service-ts', 'main', common);
      this.registryLogPath = join(this.logDirectory, `${registry.name}.log`);
      await this.health(registry, this.publicBaseURL);
      // Agent admission requires native SDK models. Remove fallback prices only
      // in this stack's private database so each scenario's organization prices
      // remain explicit and on_unpriced cases still exercise real missing prices.
      const registryPool = new Pool({ connectionString: this.databaseUrl, max: 1 });
      try {
        // /healthz becomes available before main.ts seeds the catalog. Waiting
        // for its atomic replace to commit prevents a late startup write from
        // restoring prices after our DELETE and breaking on_unpriced cases.
        const deadline = Date.now() + 30000;
        let seeded = false;
        while (Date.now() < deadline) {
          if (
            registry.error ||
            registry.child.exitCode !== null ||
            registry.child.signalCode !== null
          )
            throw new Error(
              `Registry exited before pricing initialization; logs: ${this.logDirectory}`,
            );
          const rows = await registryPool.query(
            "SELECT 1 FROM model_prices WHERE source = 'seed' AND deleted_at IS NULL LIMIT 1",
          );
          if (rows.rowCount) {
            seeded = true;
            break;
          }
          await delay(100);
        }
        if (!seeded)
          throw new Error(`Registry pricing initialization timed out; logs: ${this.logDirectory}`);
        await registryPool.query("DELETE FROM model_prices WHERE source = 'seed'");
      } finally {
        await registryPool.end();
      }
      this.harnessEnv = {
        ...common,
        HTTP_PORT: String(harnessPort),
        REGISTRY_INTERNAL_BASE_URL: `http://127.0.0.1:${proxyPort}`,
        SANDBOX_RUNTIME: 'in-memory',
        ANTHROPIC_API_KEY: 'spend-fixture-key',
        ANTHROPIC_BASE_URL: `http://127.0.0.1:${fixturePort}`,
        HARNESS_WORK_DIR: join(this.workDirectory, 'harness'),
        SESSION_IDLE_TIMEOUT_MS: '600000',
      };
      if (options.harness !== false) await this.restartHarness();
      await this.manifest(false);
    } catch (error) {
      await this.close().catch((cleanupError: unknown) => {
        throw new AggregateError(
          [error, cleanupError],
          `Spend setup and cleanup failed; logs: ${this.logDirectory}`,
        );
      });
      throw new Error(`Spend stack setup failed; logs: ${this.logDirectory}`, { cause: error });
    }
  }

  private launch(service: string, entry: string, env: NodeJS.ProcessEnv): OwnedProcess {
    const name = `${service}-${entry}-${this.processes.length}`;
    const log = createWriteStream(join(this.logDirectory, `${name}.log`));
    const child = spawn(process.execPath, [`services/${service}/dist/${entry}.js`], {
      cwd: root,
      env,
      detached: true,
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    const owned: OwnedProcess = { child, log, name, exited: Promise.resolve(), stopped: false };
    owned.exited = new Promise<void>((resolve) => {
      child.once('error', (error) => {
        owned.error = error;
        resolve();
      });
      child.once('close', () => resolve());
    });
    child.stdout!.pipe(log, { end: false });
    child.stderr!.pipe(log, { end: false });
    this.processes.push(owned);
    return owned;
  }

  private async waitExit(owned: OwnedProcess, timeout: number): Promise<void> {
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      await Promise.race([
        owned.exited,
        new Promise<never>((_, reject) => {
          timer = setTimeout(
            () => reject(new Error(`${owned.name} did not exit; logs: ${this.logDirectory}`)),
            timeout,
          );
        }),
      ]);
    } finally {
      clearTimeout(timer);
    }
  }

  private async health(owned: OwnedProcess, baseURL: string): Promise<void> {
    const deadline = Date.now() + 30000;
    while (Date.now() < deadline) {
      if (owned.error || owned.child.exitCode !== null || owned.child.signalCode !== null) {
        throw new Error(`${owned.name} exited before health; logs: ${this.logDirectory}`, {
          cause: owned.error,
        });
      }
      try {
        const response = await fetch(`${baseURL}/healthz`, { signal: AbortSignal.timeout(1000) });
        if (response.ok && ((await response.json()) as { status?: string }).status === 'ok') return;
      } catch {
        /* startup is polled only while this exact process remains alive */
      }
      await delay(100);
    }
    throw new Error(`${owned.name} health timeout; logs: ${this.logDirectory}`);
  }

  private async stop(owned: OwnedProcess): Promise<void> {
    if (owned.stopped) return;
    // Each detached service owns its process group, including Claude SDK descendants.
    const signalGroup = (signal: NodeJS.Signals) => {
      if (!owned.child.pid) return;
      try {
        process.kill(-owned.child.pid, signal);
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== 'ESRCH') throw error;
      }
    };
    signalGroup('SIGTERM');
    try {
      await this.waitExit(owned, 5000);
    } catch {
      signalGroup('SIGKILL');
      await this.waitExit(owned, 5000);
    }
    signalGroup('SIGKILL');
    owned.stopped = true;
  }

  async restartHarness(): Promise<void> {
    if (this.harness) await this.stop(this.harness);
    this.harness = this.launch('harness-server', 'main', this.harnessEnv);
    await this.health(this.harness, this.harnessURL);
  }

  /** Launch the production worker and runner in one owned process group. */
  async startWorker(environmentId: string, environmentKey: string): Promise<string> {
    for (const service of ['environment-worker', 'session-runner']) {
      await access(join(root, 'services', service, 'dist/main.js'));
    }
    const directory = join(this.workDirectory, `worker-${this.processes.length}`);
    await mkdir(directory);
    const runnerLog = join(this.logDirectory, `session-runner-${this.processes.length}.log`);
    const quote = (value: string) => "'" + value.replaceAll("'", "'\\''") + "'";
    await writeFile(
      join(directory, 'runner.sh'),
      `#!/bin/sh\nexec ${quote(process.execPath)} ${quote(join(root, 'services/session-runner/dist/main.js'))} >>${quote(runnerLog)} 2>&1\n`,
      { mode: 0o700 },
    );
    // parseLaunchCommand splits whitespace, so the shell expands this single
    // quoted argument. Neither the repository nor HOME needs a space-free path.
    this.launch('environment-worker', 'main', {
      PATH: `${dirname(process.execPath)}:/usr/bin:/bin:/usr/sbin:/sbin`,
      HOME: directory,
      TMPDIR: this.workDirectory,
      NODE_ENV: 'test',
      ENVIRONMENT_ID: environmentId,
      ENVIRONMENT_KEY: environmentKey,
      REGISTRY_TUNNEL_BASE_URL: this.publicBaseURL,
      WORKSPACE_DIR: join(directory, 'sessions'),
      RUNNER_LAUNCH_COMMAND: '/bin/sh -c "$HOME/runner.sh"',
      ENVIRONMENT_WORKER_NAME: `spend-${this.tag}`,
    });
    await this.manifest(false);
    return runnerLog;
  }

  dropNextUsageResponse(sessionId: string): void {
    this.dropSession = sessionId;
  }
  failRuntimePreparation(sessionId?: string): void {
    this.failPrepareSession = sessionId;
  }
  usageReports(sessionId: string): UsageReport[] {
    return this.reports.filter((report) => report.url.includes(`/sessions/${sessionId}/`));
  }

  async localOutputFiles(filename: string): Promise<string[]> {
    const paths = await readdir(this.workDirectory, { recursive: true });
    return paths.filter(
      (path) => path.includes('/mnt/session/outputs/') && path.endsWith(`/${filename}`),
    );
  }

  private async manifest(cleaned: boolean): Promise<void> {
    if (!this.logDirectory) return;
    await writeFile(
      join(this.logDirectory, 'resources.json'),
      JSON.stringify(
        {
          tag: this.tag,
          databases: this.databases,
          bucket: this.bucket,
          workDirectory: this.workDirectory,
          pids: this.processes.map(({ child, name }) => ({
            name,
            pid: child.pid,
            exitCode: child.exitCode,
            signal: child.signalCode,
          })),
          cleaned,
        },
        null,
        2,
      ),
    );
  }

  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    const errors: unknown[] = [];
    const attempt = async (operation: () => Promise<unknown>) => {
      try {
        await operation();
      } catch (error) {
        errors.push(error);
      }
    };
    for (const owned of [...this.processes].reverse()) {
      await attempt(() => this.stop(owned));
      await attempt(() => new Promise<void>((resolve) => owned.log.end(resolve)));
    }
    await attempt(() => closeServer(this.model.server));
    await attempt(() => closeServer(this.proxy));
    if (this.bucketCreated)
      await attempt(async () => {
        // Single-object delete works against older MinIO releases that require
        // Content-MD5 for the bulk-delete API. List from the beginning after each batch.
        while (true) {
          const listed = await this.s3.send(new ListObjectsV2Command({ Bucket: this.bucket }), {
            abortSignal: AbortSignal.timeout(10000),
          });
          if (!listed.Contents?.length) break;
          for (const object of listed.Contents) {
            await this.s3.send(new DeleteObjectCommand({ Bucket: this.bucket, Key: object.Key! }), {
              abortSignal: AbortSignal.timeout(10000),
            });
          }
        }
        await this.s3.send(new DeleteBucketCommand({ Bucket: this.bucket }), {
          abortSignal: AbortSignal.timeout(10000),
        });
      });
    this.s3.destroy();
    for (const db of [...this.databases].reverse())
      await attempt(() => this.pool.query(`DROP DATABASE "${db}" WITH (FORCE)`));
    await attempt(() => this.pool.end());
    if (this.workDirectory)
      await attempt(() => rm(this.workDirectory, { recursive: true, force: true }));
    if (this.logDirectory)
      await attempt(() =>
        writeFile(
          join(this.logDirectory, 'exchanges.json'),
          JSON.stringify(
            {
              messages: this.model.history,
              usage: this.reports,
              unexpected: this.model.unexpected,
              prepareFailures: this.prepareFailures,
            },
            null,
            2,
          ),
        ),
      );
    await attempt(() => this.manifest(errors.length === 0));
    if (errors.length)
      throw new AggregateError(errors, `Spend cleanup failed; logs: ${this.logDirectory}`);
  }
}

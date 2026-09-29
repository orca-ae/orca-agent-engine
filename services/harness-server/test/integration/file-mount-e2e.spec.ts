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
import { InMemorySandboxRuntime, asInMemoryHandle } from '../../src/sandbox/in-memory/runtime.js';
import { RegistryClient } from '../../src/clients/registry.js';
import type { EnvironmentSpec, SandboxHandle } from '../../src/sandbox/sandbox-runtime.js';
import { FakeHarness } from './fake-harness.js';

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
import { existsSync, readFileSync } from 'node:fs';
import { join } from 'node:path';

const FILESTORE_DB =
  process.env['FILESTORE_DATABASE_URL'] ?? 'postgres://orca:orca@localhost:5432/filestore';
const S3_ENDPOINT = process.env['S3_ENDPOINT'] ?? 'http://localhost:9000';
const S3_ACCESS_KEY = process.env['S3_ACCESS_KEY'] ?? 'minioadmin';
const S3_SECRET_KEY = process.env['S3_SECRET_KEY'] ?? 'minioadmin';
const S3_BUCKET = process.env['S3_BUCKET'] ?? 'orca-files';
const KAFKA_BROKERS = process.env['KAFKA_BROKERS'] ?? 'localhost:9092';

describe('Phase 5 file-mount end-to-end', () => {
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

  beforeAll(async () => {
    // ----- Postgres + file store -----
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
        keyPrefix: 'test/file-mount-e2e/',
      }),
    });

    // ----- Kafka transcript store -----
    // `metadataMaxAge: 1000` so metadata refreshes happen quickly. Default is
    // 5 minutes — far too long for an integration test.
    kafka = new Kafka({
      clientId: 'phase5-e2e',
      brokers: [KAFKA_BROKERS],
      metadataMaxAge: 1000,
    });
    store = new KafkaTranscriptStore({ kafka });

    // ----- Registry app -----
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
    workspaceId = uniqueWorkspace('filemount');
    apiKey = await createTestApiKey(db, workspaceId);
    environmentId = await createTestEnvironment(baseURL, apiKey);
  }, 60_000);

  afterAll(async () => {
    await store.close();
    await fileStore.close();
    await app.close();
    await closeTestDb();
  });

  /**
   * Pre-create the session topic for deterministic broker readiness and so
   * transcript-store's exact-topic SSE consumer does not trip UNKNOWN_TOPIC.
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

  function exactSessionTopicPattern(ws: string, ses: string): RegExp {
    return new RegExp(`^${escapeRegex(sessionTopicName(ws, ses))}$`);
  }

  it('mounts an attached file resource at mount_path inside the sandbox', async () => {
    // 1) POST a file through the registry.
    const payload = `phase-5 mount end-to-end ${Date.now()}`;
    const f = new FormData();
    f.append('file', new Blob([payload], { type: 'text/plain' }), 'mounted.txt');
    const fileResp = await fetch(`${baseURL}/v1/files`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey },
      body: f,
    });
    expect(fileResp.status).toBe(200);
    const file = (await fileResp.json()) as { id: string };

    // 2) Create an agent + session with that file as a resource.
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
            file_id: file.id,
            mount_path: '/mnt/test/mounted.txt',
            access: 'read_only',
          },
        ],
      }),
    });
    expect(sessionResp.status).toBe(200);
    const session = (await sessionResp.json()) as { id: string };

    // Pre-create the topic BEFORE starting the dispatcher, so the regex
    // subscription picks it up on its initial join.
    await preCreateTopic(workspaceId, session.id);

    // 3) Construct + start the dispatcher AFTER the topic exists.
    const acquiredHandles: SandboxHandle[] = [];
    const sandboxRuntime = new InMemorySandboxRuntime();
    // The dispatcher reads `runtime.capabilities` (FUSE and write-policy
    // support) during session setup, so the shim must forward them from the
    // underlying InMemorySandboxRuntime.
    const trackedRuntime = {
      capabilities: sandboxRuntime.capabilities,
      acquire: async (env: EnvironmentSpec): Promise<SandboxHandle> => {
        const h = await sandboxRuntime.acquire(env);
        acquiredHandles.push(h);
        return h;
      },
    };

    // The dispatcher only enters the resource-materialization path when it
    // can fetch the session record from the registry — wire a `RegistryClient`
    // that points at the in-process app. The agent has zero MCP servers so the
    // gateway URL is never actually contacted; the value just needs to be a
    // valid string so the MCP-rewrite branch is reachable.
    const registryClient = new RegistryClient(
      baseURL,
      async () => 'test-internal-service-token-at-least-32-chars',
    );

    const dispatcher = new Dispatcher({
      kafka,
      groupId: `phase5-e2e-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`,
      topicPattern: exactSessionTopicPattern(workspaceId, session.id),
      store,
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      harnessFactory: () => new FakeHarness(),
      registry: registryClient,
      gatewayMcpUrl: 'http://127.0.0.1:1/mcp',
      fileStore,
      sandboxRuntime: trackedRuntime,
    });

    const sseAbort = new AbortController();
    let sseReader: ReadableStreamDefaultReader<Uint8Array> | null = null;

    try {
      await dispatcher.start();
      // Allow rebalance to settle so the first POST-event lands.
      await new Promise((r) => setTimeout(r, 4500));

      // 4) Open SSE first so we don't miss session.resource_mounted.
      const ssePromise = (async () => {
        const r = await fetch(`${baseURL}/v1/sessions/${session.id}/events/stream`, {
          // session.resource_mounted is an Orca lifecycle extension and is
          // intentionally hidden from the default Claude-compatible stream.
          headers: { 'x-api-key': apiKey, 'orca-beta': '1' },
          signal: sseAbort.signal,
        });
        sseReader = r.body!.getReader();
        const decoder = new TextDecoder();
        let buf = '';
        const start = Date.now();
        while (Date.now() - start < 90000) {
          let chunk: ReadableStreamReadResult<Uint8Array>;
          try {
            chunk = await sseReader.read();
          } catch {
            return null;
          }
          if (chunk.done) return null;
          buf += decoder.decode(chunk.value);
          let idx;
          while ((idx = buf.indexOf('\n\n')) >= 0) {
            const block = buf.slice(0, idx);
            buf = buf.slice(idx + 2);
            if (block.includes('event: session.resource_mounted')) {
              const dataLine = block.split('\n').find((l) => l.startsWith('data: '))!;
              return JSON.parse(dataLine.slice(6));
            }
          }
        }
        return null;
      })();

      // Allow SSE to subscribe before we trigger the dispatcher.
      await new Promise((r) => setTimeout(r, 500));

      // 5) POST a user.message — that's what wakes the dispatcher's spawnRunner
      //    path and triggers the materialization.
      const eventResp = await fetch(`${baseURL}/v1/sessions/${session.id}/events`, {
        method: 'POST',
        headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
        body: JSON.stringify({
          events: [{ type: 'user.message', content: [{ type: 'text', text: 'mount my file' }] }],
        }),
      });
      expect(eventResp.status).toBe(200);

      // 6) Assert SSE emits session.resource_mounted with the right file_id + mount_path.
      const mounted = (await ssePromise) as {
        type: string;
        file_id?: string;
        mount_path?: string;
      } | null;
      expect(mounted).not.toBeNull();
      expect(mounted!.type).toBe('session.resource_mounted');
      expect(mounted!.mount_path).toBe('/mnt/test/mounted.txt');
      expect(mounted!.file_id).toBe(file.id);

      // 7) Assert the file is actually present inside the sandbox at mount_path.
      expect(acquiredHandles.length).toBeGreaterThan(0);
      const handle = acquiredHandles[acquiredHandles.length - 1]!;
      const inMem = asInMemoryHandle(handle);
      const fsPath = join(inMem.rootDir(), 'mnt/test/mounted.txt');
      expect(existsSync(fsPath)).toBe(true);
      expect(readFileSync(fsPath, 'utf8')).toBe(payload);
    } finally {
      // Tear the SSE stream down so the server's tail() consumer disconnects
      // before app.close() in afterAll, which keeps cleanup fast.
      sseAbort.abort();
      if (sseReader) {
        try {
          await (sseReader as ReadableStreamDefaultReader<Uint8Array>).cancel();
        } catch {
          /* ignore */
        }
      }
      await dispatcher.stop();
    }
  }, 120_000);
});

function escapeRegex(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

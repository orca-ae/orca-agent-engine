// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { randomUUID } from 'node:crypto';
import type { Event, TranscriptStore } from '@orca/transcript-store';
import {
  KafkaTranscriptStore,
  PostgresTranscriptStore,
  PulsarTranscriptStore,
  applyPostgresTranscriptMigrations,
} from '@orca/transcript-store';
import { Kafka } from 'kafkajs';
import { Pool } from 'pg';
import { FrameKind, decodeFrame, encodeFrame } from '@orca/harness-tunnel';
import { getTestDb } from './setup.js';
import { TunnelRegistry } from '../../src/tunnel/tunnel-registry.js';
import { SessionEventBridge, RUNNER_TURN_PATH } from '../../src/tunnel/session-event-bridge.js';

/** Keep the stack's Kafka/Postgres/Pulsar matrix; bus-free integration defaults to Postgres. */
export async function visibilityTranscriptStore() {
  const backend = (process.env['TRANSCRIPT_STORE_BACKEND'] ?? 'postgres').toLowerCase();
  // The stack harness must not consume fixture inputs and append real runner output.
  const namespace = 'visibility_' + randomUUID().replaceAll('-', '');
  if (backend === 'postgres') {
    const database = await getTestDb();
    await database.pool.query('CREATE SCHEMA "' + namespace + '"');
    const pool = new Pool({
      connectionString:
        process.env['DATABASE_URL'] ?? 'postgres://orca:orca@localhost:5432/registry',
      options: '-c search_path=' + namespace,
      max: 5,
    });
    try {
      await applyPostgresTranscriptMigrations(pool);
    } catch (error) {
      await pool.end();
      await database.pool.query('DROP SCHEMA "' + namespace + '" CASCADE');
      throw error;
    }
    return {
      pool,
      store: new PostgresTranscriptStore({ pool, tailPollIntervalMs: 50 }),
      cleanup: () => database.pool.query('DROP SCHEMA "' + namespace + '" CASCADE'),
    };
  }
  let store: TranscriptStore;
  if (backend === 'kafka') {
    store = new KafkaTranscriptStore({
      topicPrefix: namespace + '.',
      kafka: new Kafka({
        clientId: 'visibility-' + process.pid,
        brokers: (process.env['KAFKA_BROKERS'] ?? 'localhost:9092')
          .split(',')
          .map((s) => s.trim())
          .filter(Boolean),
      }),
    });
  } else if (backend === 'pulsar') {
    store = new PulsarTranscriptStore({
      serviceUrl: process.env['PULSAR_SERVICE_URL'] ?? 'pulsar://localhost:6650',
      tenant: process.env['PULSAR_TENANT'] ?? 'public',
      namespace: process.env['PULSAR_NAMESPACE'] ?? 'default',
      topicPrefix: namespace,
    });
  } else throw new Error('unsupported TRANSCRIPT_STORE_BACKEND=' + backend);
  return { store, pool: undefined, cleanup: async () => {} };
}

/** In-process wire peer; production transport, frame codec, bridge and persistence. */
export async function streamRunnerEvents(
  store: TranscriptStore,
  workspaceId: string,
  sessionId: string,
  events: Event[],
): Promise<void> {
  const registry = new TunnelRegistry();
  const runnerId = randomUUID();
  const session = registry.register(
    runnerId,
    {
      async sendText(raw) {
        const frame = decodeFrame(raw);
        // Responses below are queued synchronously. Bridge shutdown may still
        // cancel while the client is draining the final response frame.
        if (frame.kind === FrameKind.RequestCancel) return;
        if (frame.kind !== FrameKind.Request)
          throw new Error('Unexpected tunnel frame: ' + frame.kind);
        if (frame.path !== RUNNER_TURN_PATH)
          throw new Error('Unexpected runner route: ' + frame.path);
        const route = (response: Parameters<typeof encodeFrame>[0]) => {
          if (!registry.routeResponseFrame(runnerId, decodeFrame(encodeFrame(response))))
            throw new Error('Runner response was not routed');
        };
        route({
          kind: FrameKind.ResponseHead,
          id: frame.id,
          status: 200,
          headers: [['content-type', 'application/x-ndjson']],
        });
        for (const event of events)
          route({
            kind: FrameKind.ResponseBody,
            id: frame.id,
            encoding: 'utf-8',
            body:
              JSON.stringify({
                ...JSON.parse(Buffer.from(event.payload).toString('utf8')),
                id: event.id,
                type: event.kind,
              }) + '\n',
          });
        route({ kind: FrameKind.ResponseEnd, id: frame.id });
      },
      async receiveText() {
        throw new Error('Responses are pushed by the frame peer');
      },
    },
    {
      kind: FrameKind.Hello,
      runnerVersion: 'visibility-fixture',
      frameProtocolVersion: 1,
      harnesses: [],
      envs: [],
    },
    { owner: workspaceId },
  );
  const drain = (async () => {
    for (;;) {
      const frame = await session.outboundQueue.get();
      if (frame === null) return;
      await session.ws.sendText(frame);
    }
  })();
  const bridge = new SessionEventBridge({ workspaceId, sessionId, runnerId, store, registry });
  bridge.start();
  try {
    const last = events.at(-1)!;
    const persisted = (async () => {
      for await (const event of store.tail(workspaceId, sessionId, {
        fromCursor: '0',
        subpath: '*',
        signal: AbortSignal.timeout(10_000),
      }))
        if (event.id === last.id) return;
      throw new Error('Runner output was not persisted');
    })();
    await Promise.race([
      persisted,
      drain.then(() => {
        throw new Error('Tunnel closed early');
      }),
    ]);
  } finally {
    await bridge.stop();
    registry.deregister(runnerId);
    await drain;
  }
}

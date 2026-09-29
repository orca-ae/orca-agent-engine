// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { Kafka } from 'kafkajs';
import { Pool } from 'pg';
import { KafkaTranscriptStore } from '../../src/kafka-store.js';
import {
  PostgresTranscriptStore,
  applyPostgresTranscriptMigrations,
} from '../../src/postgres-store.js';

const KAFKA_BROKER = process.env['KAFKA_BROKERS'] ?? 'localhost:9092';

export function makeKafka(clientId = 'transcript-store-it'): Kafka {
  return new Kafka({ clientId, brokers: [KAFKA_BROKER] });
}

export function makeStore(): KafkaTranscriptStore {
  return new KafkaTranscriptStore({ kafka: makeKafka() });
}

export async function makePostgresStore(): Promise<{
  pool: Pool;
  store: PostgresTranscriptStore;
}> {
  const pool = new Pool({
    connectionString:
      process.env['TRANSCRIPT_STORE_DATABASE_URL'] ??
      process.env['DATABASE_URL'] ??
      'postgres://orca:orca@localhost:5432/transcriptstore',
  });
  await applyPostgresTranscriptMigrations(pool);
  return { pool, store: new PostgresTranscriptStore({ pool, tailPollIntervalMs: 50 }) };
}

export async function deletePostgresRowsForWorkspace(
  pool: Pool,
  workspaceId: string,
): Promise<void> {
  await pool.query('DELETE FROM transcript_events WHERE workspace_id = $1', [workspaceId]);
}

export async function deleteTopicsForSession(
  workspaceId: string,
  sessionId: string,
): Promise<void> {
  const admin = makeKafka().admin();
  await admin.connect();
  try {
    await admin.deleteTopics({
      topics: [`orca.${workspaceId}.sessions.${sessionId}.events`],
      timeout: 5000,
    });
  } catch {
    /* topic might not exist; ignore */
  } finally {
    await admin.disconnect();
  }
}

export function uniqueIds(prefix: string): { ws: string; ses: string } {
  const suffix = Date.now().toString(36) + Math.random().toString(36).slice(2, 6);
  return { ws: `ws_${prefix}_${suffix}`, ses: `ses_${prefix}_${suffix}` };
}

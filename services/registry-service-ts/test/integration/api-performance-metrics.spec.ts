// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import Fastify, { type FastifyInstance } from 'fastify';
import { Pool, Query, type PoolClient } from 'pg';
import { registry } from '../../src/metrics.js';
import {
  measureAuthStage,
  observePostgresPool,
  registerApiPerformance,
} from '../../src/observability/api-performance.js';

describe('API performance instrumentation (real pg driver)', () => {
  let pool: Pool;
  let app: FastifyInstance;
  let stop: () => void;
  beforeAll(async () => {
    pool = new Pool({
      connectionString:
        process.env['DATABASE_URL'] ?? 'postgres://orca:orca@localhost:5432/registry',
      max: 1,
    });
    stop = observePostgresPool(pool, 'metadata');
    app = Fastify();
    registerApiPerformance(app, 'public');
    app.get('/metrics-test/:privateId', async (req) => {
      const count = Number((req.query as { count: string }).count);
      for (let i = 0; i < count; i++) await pool.query('select pg_sleep(0.001)');
      return { ok: true };
    });
    app.get('/metrics-stream', (_req, reply) => {
      reply.hijack();
      reply.raw.writeHead(200, { 'content-type': 'text/event-stream' });
      reply.raw.end('data: hello\n\n');
    });
    await app.ready();
  });
  afterAll(async () => {
    await app?.close();
    await pool?.end();
    stop?.();
  });

  it('preserves Promise and callback queries, checked-out transactions, errors and release', async () => {
    expect((await pool.query('select 1 as n')).rows[0].n).toBe(1);
    const callbackValue = await new Promise<number>((resolve, reject) => {
      pool.query('select $1::int as n', [2], (error, result) =>
        error ? reject(error) : resolve(result.rows[0].n),
      );
    });
    expect(callbackValue).toBe(2);
    const client = await pool.connect();
    try {
      await client.query('begin');
      await client.query('select 3');
      await client.query('rollback');
    } finally {
      client.release();
    }
    await new Promise<void>((resolve, reject) => {
      pool.connect((error, client, release) => {
        if (error || !client) return reject(error);
        client.query('select 4', (queryError) => {
          release();
          if (queryError) reject(queryError);
          else resolve();
        });
      });
    });
    await expect(pool.query('select nonexistent_performance_column')).rejects.toThrow();
    await new Promise<void>((resolve) => {
      pool.query('select nonexistent_performance_column', (error) => {
        expect(error).toBeTruthy();
        resolve();
      });
    });
    expect(pool.waitingCount).toBe(0);
    // pg discards the client when a pool.query callback reports an error.
    await pool.query('select 1');
    expect(pool.idleCount).toBe(1);
    const metrics = await registry.metrics();
    expect(metrics).toContain(
      'registry_service_db_round_trip_seconds_count{pool="metadata",result="error"} 2',
    );
    expect(metrics).not.toContain('nonexistent_performance_column');
  });

  it('keeps request query counts isolated despite sharing a one-connection pool', async () => {
    await Promise.all(
      [1, 3].map((count) => app.inject(`/metrics-test/private-workspace-and-key?count=${count}`)),
    );
    const metrics = await registry.metrics();
    const labels = 'surface="public",method="GET",route="/metrics-test/:privateId",status="200"';
    expect(metrics).toContain(`registry_service_http_db_queries_sum{${labels}} 4`);
    expect(metrics).toContain(`registry_service_http_db_queries_count{${labels}} 2`);
    expect(metrics).toContain(`registry_service_http_db_queries_bucket{le="1",${labels}} 1`);
    expect(metrics).toContain(`registry_service_http_payload_bytes_sum{${labels}} 22`);
    expect(metrics).not.toContain('private-workspace-and-key');
  });

  it('preserves Query-object event errors and synchronous driver errors', async () => {
    const client = await pool.connect();
    try {
      const query = new Query('select 5 as n');
      const completion = new Promise<void>((resolve, reject) => {
        query.once('end', (result) => {
          expect(result.rows[0].n).toBe(5);
          resolve();
        });
        query.once('error', reject);
      });
      expect(client.query(query)).toBe(query);
      // Metrics must not turn an unhandled driver error into a handled one.
      expect(query.listenerCount('error')).toBe(1);
      await completion;
      const failedQuery = new Query('select nonexistent_performance_column');
      const failure = new Promise<void>((resolve) => {
        failedQuery.once('error', (error) => {
          expect(error).toBeTruthy();
          resolve();
        });
      });
      client.query(failedQuery);
      expect(failedQuery.listenerCount('error')).toBe(1);
      await failure;
      expect(() => client.query(null as unknown as string)).toThrow();
    } finally {
      client.release();
    }
  });

  it('separates stream lifetime, records unmatched routes with a bounded label and does not swallow auth errors', async () => {
    expect((await app.inject('/metrics-stream')).body).toBe('data: hello\n\n');
    await app.inject('/private-unmatched-key');
    await expect(
      measureAuthStage('api_key_verify', async () => {
        throw new Error('rejected');
      }),
    ).rejects.toThrow('rejected');
    const metrics = await registry.metrics();
    expect(metrics).toContain(
      'registry_service_http_stream_duration_seconds_count{surface="public",method="GET",route="/metrics-stream",status="200"} 1',
    );
    expect(metrics).not.toMatch(
      /registry_service_http_duration_seconds_count\{[^\n]*route="\/metrics-stream"/,
    );
    expect(metrics).toContain('route="<unmatched>"');
    expect(metrics).not.toContain('private-unmatched-key');
    expect(metrics).toContain(
      'registry_service_auth_stage_seconds_count{stage="api_key_verify",result="error"} 1',
    );
  });

  it('distinguishes acquisition wait from a fast query round trip', async () => {
    const held = await pool.connect();
    let acquired = false;
    const waiting = pool.connect().then((client) => {
      acquired = true;
      return client;
    });
    await new Promise((resolve) => setTimeout(resolve, 30));
    expect(acquired).toBe(false);
    expect(pool.waitingCount).toBe(1);
    held.release();
    const client: PoolClient = await waiting;
    try {
      await client.query('select 1');
    } finally {
      client.release();
    }
    const metric = await registry.getSingleMetric('registry_service_db_acquire_seconds')!.get();
    const sum = metric.values.find(
      (entry) =>
        'metricName' in entry &&
        typeof entry.metricName === 'string' &&
        entry.metricName.endsWith('_sum') &&
        entry.labels.result === 'ok',
    );
    expect(sum!.value).toBeGreaterThan(0.025);
  });
});

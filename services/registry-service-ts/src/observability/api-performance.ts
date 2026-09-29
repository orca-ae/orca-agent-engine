// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { AsyncLocalStorage } from 'node:async_hooks';
import { errorMonitor } from 'node:events';
import type { FastifyInstance } from 'fastify';
import type { PoolClient, Pool } from 'pg';
import { Gauge, Histogram } from 'prom-client';
import { registry, requestTotal } from '../metrics.js';

const secondsBuckets = [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30];
const httpLabels = ['surface', 'method', 'route', 'status'] as const;
const httpDuration = new Histogram({
  name: 'registry_service_http_duration_seconds',
  help: 'Completed non-SSE HTTP responses, from onRequest through socket finish.',
  labelNames: httpLabels,
  buckets: secondsBuckets,
  registers: [registry],
});
const streamDuration = new Histogram({
  name: 'registry_service_http_stream_duration_seconds',
  help: 'SSE response lifetime, separate from ordinary HTTP latency.',
  labelNames: httpLabels,
  buckets: [1, 10, 60, 300, 1800, 3600],
  registers: [registry],
});
const responseBytes = new Histogram({
  name: 'registry_service_http_payload_bytes',
  help: 'Content-Length of completed non-SSE responses when present; excludes transport framing.',
  labelNames: httpLabels,
  buckets: [0, 1024, 16384, 65536, 262144, 1048576, 4194304],
  registers: [registry],
});
const requestQueries = new Histogram({
  name: 'registry_service_http_db_queries',
  help: 'Database query calls initiated in a request context before the response closes.',
  labelNames: httpLabels,
  buckets: [0, 1, 2, 4, 8, 16, 32, 64, 128, 512, 1024],
  registers: [registry],
});
const acquireDuration = new Histogram({
  name: 'registry_service_db_acquire_seconds',
  help: 'Pool connect wait, including connection establishment when a new connection is needed.',
  labelNames: ['pool', 'result'] as const,
  buckets: secondsBuckets,
  registers: [registry],
});
const queryDuration = new Histogram({
  name: 'registry_service_db_round_trip_seconds',
  help: 'Client query call to completion, including network, server time and client queueing; excludes pool acquisition.',
  labelNames: ['pool', 'result'] as const,
  buckets: secondsBuckets,
  registers: [registry],
});
const poolConnections = new Gauge({
  name: 'registry_service_db_pool_connections',
  help: 'Current pool connections or waiting callers.',
  labelNames: ['pool', 'state'] as const,
  registers: [registry],
});
const authDuration = new Histogram({
  name: 'registry_service_auth_stage_seconds',
  help: 'Authentication stage wall time; rejected credentials may be successful stage executions.',
  labelNames: ['stage', 'result'] as const,
  buckets: secondsBuckets,
  registers: [registry],
});

interface RequestTiming {
  queries: number;
}
const requestTiming = new AsyncLocalStorage<RequestTiming>();
const elapsed = (start: number) => (performance.now() - start) / 1000;

export function registerApiPerformance(
  app: FastifyInstance,
  surface: 'public' | 'internal' | 'admin' | 'combined',
): void {
  app.addHook('onRequest', (req, reply, done) => {
    const start = performance.now();
    const timing = { queries: 0 };
    let recorded = false;
    const record = (completed: boolean) => {
      if (recorded || reply.raw.statusCode === 101) return;
      recorded = true;
      const method = ['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'HEAD', 'OPTIONS'].includes(
        req.method,
      )
        ? req.method
        : 'OTHER';
      const route = req.routeOptions?.url ?? '<unmatched>';
      const status = completed ? String(reply.statusCode) : 'aborted';
      const labels = { surface, method, route, status };
      requestTotal.inc({ method, route, status });
      const isStream = String(reply.raw.getHeader('content-type') ?? '').startsWith(
        'text/event-stream',
      );
      (isStream ? streamDuration : httpDuration).observe(labels, elapsed(start));
      requestQueries.observe(labels, timing.queries);
      const length = reply.raw.getHeader('content-length');
      if (length !== undefined && !isStream && completed) {
        const size = Number(length);
        if (Number.isFinite(size) && size >= 0) responseBytes.observe(labels, size);
      }
    };
    reply.raw.once('finish', () => record(true));
    reply.raw.once('close', () => record(reply.raw.writableFinished));
    // run, not enterWith: concurrent requests must not inherit each other's counters.
    requestTiming.run(timing, done);
  });
}

export type AuthStage =
  | 'api_key_lookup'
  | 'api_key_verify'
  | 'api_key_last_used'
  | 'workspace_lookup';
export async function measureAuthStage<T>(
  stage: AuthStage,
  work: () => PromiseLike<T>,
): Promise<T> {
  const start = performance.now();
  try {
    const result = await work();
    authDuration.observe({ stage, result: 'ok' }, elapsed(start));
    return result;
  } catch (error) {
    authDuration.observe({ stage, result: 'error' }, elapsed(start));
    throw error;
  }
}

type PoolName = 'metadata' | 'file' | 'memory' | 'transcript';
type Invocation = (this: unknown, ...args: unknown[]) => unknown;

/** Preserve pg's callback and Promise overloads without inspecting SQL or parameters. */
function timedInvocation(
  original: Invocation,
  observe: (seconds: number, failed: boolean) => void,
  onStart?: () => void,
): Invocation {
  return function (...args) {
    onStart?.();
    const start = performance.now();
    let finished = false;
    const finish = (failed: boolean) => {
      if (finished) return;
      finished = true;
      observe(elapsed(start), failed);
    };
    const callback = args.at(-1);
    if (typeof callback === 'function') {
      // pg-pool can fulfill a queued checkout from another request's release
      // callback. Restore the acquiring caller's context before it issues SQL.
      const runInCaller = AsyncLocalStorage.snapshot();
      args[args.length - 1] = function (this: unknown, ...result: unknown[]) {
        finish(result[0] != null);
        return runInCaller(() => Reflect.apply(callback, this, result));
      };
    }
    try {
      const result = Reflect.apply(original, this, args);
      if (
        result &&
        typeof result === 'object' &&
        'then' in result &&
        typeof result.then === 'function'
      ) {
        return result.then(
          (value: unknown) => {
            finish(false);
            return value;
          },
          (error: unknown) => {
            finish(true);
            throw error;
          },
        );
      }
      // pg also accepts Query objects with an event-emitter completion API.
      if (
        result &&
        typeof result === 'object' &&
        'once' in result &&
        typeof result.once === 'function'
      ) {
        result.once('end', () => finish(false));
        // Observe without consuming an otherwise unhandled EventEmitter error.
        result.once(errorMonitor, () => finish(true));
      }
      return result;
    } catch (error) {
      finish(true);
      throw error;
    }
  };
}

/** Install before the pool's first connection. One owner per named pool. */
export function observePostgresPool(pool: Pool, name: PoolName): () => void {
  const clients = new Map<PoolClient, PoolClient['query']>();
  const updateGauge = () => {
    poolConnections.set({ pool: name, state: 'total' }, pool.totalCount);
    poolConnections.set({ pool: name, state: 'idle' }, pool.idleCount);
    poolConnections.set({ pool: name, state: 'waiting' }, pool.waitingCount);
  };
  const connect = pool.connect;
  pool.connect = timedInvocation(connect as unknown as Invocation, (seconds, failed) => {
    acquireDuration.observe({ pool: name, result: failed ? 'error' : 'ok' }, seconds);
    updateGauge();
  }) as Pool['connect'];
  const instrument = (client: PoolClient) => {
    if (clients.has(client)) return;
    clients.set(client, client.query);
    client.query = timedInvocation(
      client.query as unknown as Invocation,
      (seconds, failed) => {
        queryDuration.observe({ pool: name, result: failed ? 'error' : 'ok' }, seconds);
      },
      () => {
        const timing = requestTiming.getStore();
        if (timing) timing.queries++;
      },
    ) as PoolClient['query'];
  };
  const remove = (client: PoolClient) => {
    clients.delete(client);
    updateGauge();
  };
  pool.on('connect', instrument);
  pool.on('remove', remove);
  // Sample gauges every second: an event-only snapshot misses queued callers
  // and pg's release event fires before the client enters its idle list.
  const timer = setInterval(updateGauge, 1000);
  timer.unref();
  updateGauge();
  return () => {
    clearInterval(timer);
    pool.connect = connect;
    pool.off('connect', instrument);
    pool.off('remove', remove);
    for (const [client, query] of clients) client.query = query;
    clients.clear();
    for (const state of ['total', 'idle', 'waiting']) poolConnections.remove({ pool: name, state });
  };
}

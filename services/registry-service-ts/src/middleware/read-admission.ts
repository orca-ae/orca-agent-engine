// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { FastifyInstance, FastifyReply, FastifyRequest } from 'fastify';
import { Counter, Gauge, Histogram } from 'prom-client';
import { registry } from '../metrics.js';

export interface ReadAdmissionOptions {
  /** Zero disables admission control. Tune against measured capacity before enabling. */
  maxConcurrent: number;
  maxPerWorkspace: number;
  maxQueued: number;
  maxQueuedPerWorkspace: number;
  queueTimeoutMs: number;
}

const pendingGauge = new Gauge({
  name: 'registry_service_heavy_reads',
  help: 'Heavy-read admission slots and queued requests per process.',
  labelNames: ['state'] as const,
  registers: [registry],
});
const rejectedTotal = new Counter({
  name: 'registry_service_heavy_read_rejected_total',
  help: 'Heavy reads refused or removed before execution.',
  labelNames: ['reason'] as const,
  registers: [registry],
});
const waitDuration = new Histogram({
  name: 'registry_service_heavy_read_queue_seconds',
  help: 'Heavy-read admission queue wait.',
  buckets: [0.001, 0.01, 0.1, 0.5, 1, 2, 5, 10],
  registers: [registry],
});

export class ReadAdmissionError extends Error {
  constructor(readonly reason: 'capacity' | 'timeout' | 'aborted' | 'closed') {
    super(`heavy read admission ${reason}`);
  }
}

type Release = () => void;
interface Waiter {
  resolve: (release: Release) => void;
  reject: (error: ReadAdmissionError) => void;
  cleanup: () => void;
  startedAt: number;
}

/** FIFO within each workspace, round-robin across workspaces, bounded at both levels. */
export function createReadAdmission(options: ReadAdmissionOptions) {
  for (const [key, value] of Object.entries(options)) {
    if (
      !Number.isSafeInteger(value) ||
      value <
        (key === 'maxQueued' || key === 'maxQueuedPerWorkspace' || key === 'maxConcurrent' ? 0 : 1)
    ) {
      throw new Error(`invalid read admission ${key}`);
    }
  }
  if (options.queueTimeoutMs > 2_147_483_647)
    throw new Error('invalid read admission queueTimeoutMs');
  const activeByWorkspace = new Map<string, number>();
  const queues = new Map<string, Waiter[]>();
  let active = 0;
  let queued = 0;
  let closed = false;
  const publish = () => {
    pendingGauge.set({ state: 'active' }, active);
    pendingGauge.set({ state: 'queued' }, queued);
  };
  const capacity = (workspace: string) =>
    active < options.maxConcurrent &&
    (activeByWorkspace.get(workspace) ?? 0) < options.maxPerWorkspace;
  const refuse = (reason: ReadAdmissionError['reason']) => {
    rejectedTotal.inc({ reason });
    return new ReadAdmissionError(reason);
  };
  const grant = (workspace: string): Release => {
    active++;
    activeByWorkspace.set(workspace, (activeByWorkspace.get(workspace) ?? 0) + 1);
    publish();
    let released = false;
    return () => {
      if (released) return;
      released = true;
      active--;
      const count = activeByWorkspace.get(workspace)! - 1;
      if (count === 0) activeByWorkspace.delete(workspace);
      else activeByWorkspace.set(workspace, count);
      drain();
      publish();
    };
  };
  function drain() {
    if (closed) return;
    while (active < options.maxConcurrent) {
      let dispatched = false;
      for (const [workspace, queue] of queues) {
        if (!capacity(workspace)) continue;
        const waiter = queue.shift()!;
        queues.delete(workspace);
        if (queue.length > 0) queues.set(workspace, queue); // rotate to the back
        queued--;
        waiter.cleanup();
        waitDuration.observe((performance.now() - waiter.startedAt) / 1000);
        waiter.resolve(grant(workspace));
        dispatched = true;
        break;
      }
      if (!dispatched) break;
    }
    publish();
  }
  publish();
  return {
    acquire(workspace: string, signal?: AbortSignal): Promise<Release> {
      if (closed) return Promise.reject(refuse('closed'));
      if (signal?.aborted) return Promise.reject(refuse('aborted'));
      if (options.maxConcurrent === 0) return Promise.resolve(() => {});
      drain();
      if (!queues.has(workspace) && capacity(workspace)) return Promise.resolve(grant(workspace));
      const queue = queues.get(workspace) ?? [];
      if (queued >= options.maxQueued || queue.length >= options.maxQueuedPerWorkspace) {
        return Promise.reject(refuse('capacity'));
      }
      return new Promise<Release>((resolve, reject) => {
        const remove = (reason: ReadAdmissionError['reason']) => {
          const position = queue.indexOf(waiter);
          if (position < 0) return;
          queue.splice(position, 1);
          queued--;
          if (queue.length === 0) queues.delete(workspace);
          waiter.cleanup();
          reject(refuse(reason));
          drain();
        };
        const abort = () => remove('aborted');
        const timer = setTimeout(() => remove('timeout'), options.queueTimeoutMs);
        timer.unref();
        const waiter: Waiter = {
          resolve,
          reject,
          startedAt: performance.now(),
          cleanup: () => {
            clearTimeout(timer);
            signal?.removeEventListener('abort', abort);
          },
        };
        queue.push(waiter);
        queues.set(workspace, queue);
        queued++;
        signal?.addEventListener('abort', abort, { once: true });
        drain();
      });
    },
    close() {
      closed = true;
      for (const queue of queues.values())
        for (const waiter of queue) {
          waiter.cleanup();
          waiter.reject(refuse('closed'));
        }
      queues.clear();
      queued = 0;
      publish();
    },
  };
}

const heavyRoutes = new Set([
  '/v1/sessions',
  '/v1/sessions/:id/threads',
  '/v1/triggers/:id/sessions',
  '/v1/memory_stores/:id/memories',
  '/v1/memory_stores/:id/memory_versions',
]);

/** A disconnect signal for read-only work. Already-running SQL still settles normally. */
export function requestReadSignal(req: FastifyRequest, reply: FastifyReply): AbortSignal {
  const controller = new AbortController();
  const abort = () => controller.abort();
  if (req.raw.aborted || reply.raw.destroyed) abort();
  else reply.raw.once('close', abort);
  return controller.signal;
}

export function registerReadAdmission(app: FastifyInstance, options: ReadAdmissionOptions): void {
  if (options.maxConcurrent === 0) return;
  const admission = createReadAdmission(options);
  const leases = new WeakMap<
    FastifyRequest,
    { running: boolean; closed: boolean; release: Release }
  >();
  app.addHook('onRoute', (route) => {
    const methods = Array.isArray(route.method) ? route.method : [route.method];
    if (
      !methods.some((method) => method === 'GET' || method === 'HEAD') ||
      !heavyRoutes.has(route.url)
    )
      return;
    const handler = route.handler;
    route.handler = async function (req, reply) {
      const lease = leases.get(req);
      if (!lease) return handler.call(this, req, reply);
      if (reply.raw.destroyed) {
        lease.release();
        return reply.hijack();
      }
      lease.running = true;
      try {
        return await handler.call(this, req, reply);
      } finally {
        lease.running = false;
        // A disconnected caller cannot free capacity while its SQL or object
        // read is still running. Cooperative cancellation stops subsequent work.
        if (lease.closed) lease.release();
      }
    };
  });
  app.addHook('preHandler', async (req, reply) => {
    if (
      (req.method !== 'GET' && req.method !== 'HEAD') ||
      !heavyRoutes.has(req.routeOptions.url ?? '') ||
      !req.auth?.workspaceId
    )
      return;
    const signal = requestReadSignal(req, reply);
    try {
      const release = await admission.acquire(req.auth.workspaceId, signal);
      if (signal.aborted) {
        release();
        return reply.hijack();
      }
      const lease = {
        running: false,
        closed: false,
        release: () => {
          reply.raw.off('finish', closed);
          reply.raw.off('close', closed);
          leases.delete(req);
          release();
        },
      };
      const closed = () => {
        lease.closed = true;
        if (!lease.running) lease.release();
      };
      leases.set(req, lease);
      reply.raw.once('finish', closed);
      reply.raw.once('close', closed);
    } catch (error) {
      if (!(error instanceof ReadAdmissionError)) throw error;
      if (error.reason === 'aborted') return reply.hijack();
      reply.header('retry-after', '1');
      if (error.reason === 'capacity') reply.code(429);
      else reply.code(503);
      return reply.send({ error: 'read capacity is temporarily exhausted; retry with backoff' });
    }
  });
  app.addHook('preClose', async () => {
    admission.close();
  });
}

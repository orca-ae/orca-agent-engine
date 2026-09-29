// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import Fastify from 'fastify';
import { get } from 'node:http';
import type { TranscriptStore } from '@orca/transcript-store';
import { registerApiPerformance } from '../../src/observability/api-performance.js';
import { registry } from '../../src/metrics.js';
import { streamSession } from '../../src/streaming/sse.js';

describe('SSE performance metrics over real HTTP sockets', () => {
  for (const subpath of ['', 'threads/child']) {
    for (const mode of ['complete', 'disconnect']) {
      it(`separates ${subpath || 'session'} streams on ${mode}`, async () => {
        const app = Fastify();
        registerApiPerformance(app, 'public');
        const route = `/metrics-${subpath ? 'thread' : 'session'}-${mode}`;
        let closed!: () => void;
        const serverClosed = new Promise<void>((resolve) => {
          closed = resolve;
        });
        const store = {
          async *tail(_workspace: string, _session: string, opts: { signal: AbortSignal }) {
            if (mode === 'disconnect') {
              await new Promise<void>((resolve) => {
                if (opts.signal.aborted) resolve();
                else opts.signal.addEventListener('abort', () => resolve(), { once: true });
              });
              return;
            }
            yield {
              id: 'evt_metrics_deleted',
              workspaceId: 'ws_metrics',
              sessionId: 'ses_metrics',
              subpath: '',
              seq: 1,
              producedAt: '2026-09-12T00:00:00Z',
              producedBy: 'registry',
              kind: 'session.deleted',
              payload: new Uint8Array(),
              idempotencyKey: 'evt_metrics_deleted',
            };
          },
        } as unknown as TranscriptStore;
        app.get(route, async (_req, reply) => {
          reply.hijack();
          reply.raw.once('close', closed);
          await streamSession(reply, store, 'ws_metrics', 'ses_metrics', '', subpath, {
            bufferSize: 4,
            dropAgeMs: 1000,
            heartbeatMs: 5,
          });
        });
        const address = await app.listen({ host: '127.0.0.1', port: 0 });
        try {
          const contentType = await new Promise<string | undefined>((resolve, reject) => {
            const client = get(`${address}${route}`, (response) => {
              const type = response.headers['content-type'];
              if (mode === 'disconnect') {
                response.destroy();
                resolve(type);
              } else {
                response.resume();
                response.once('end', () => resolve(type));
                response.once('error', reject);
              }
            });
            client.once('error', reject);
          });
          expect(contentType).toBe('text/event-stream');
          await serverClosed;
          const metrics = await registry.metrics();
          const labels = `surface="public",method="GET",route="${route}",status="${mode === 'complete' ? '200' : 'aborted'}"`;
          expect(metrics).toContain(
            `registry_service_http_stream_duration_seconds_count{${labels}} 1`,
          );
          expect(metrics).not.toContain(`registry_service_http_duration_seconds_count{${labels}}`);
          expect(metrics).not.toContain(`registry_service_http_payload_bytes_count{${labels}}`);
        } finally {
          await app.close();
        }
      });
    }
  }
});

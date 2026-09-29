// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import Fastify, { type FastifyInstance } from 'fastify';
import { registry } from './metrics.js';
import { transcriptStoreMetricsRegistry } from '@orca/transcript-store';

const SERVICE_NAME = 'harness-server';

export interface ReadinessState {
  ready: boolean;
  reasons: string[];
}

export function buildHealthzServer(
  readiness: () => ReadinessState = () => ({ ready: true, reasons: [] }),
): FastifyInstance {
  const app = Fastify({ logger: false });
  app.get('/healthz', async () => ({ status: 'ok', service: SERVICE_NAME }));
  app.get('/readyz', async (_request, reply) => {
    const state = readiness();
    if (!state.ready) {
      return reply
        .code(503)
        .send({ status: 'not_ready', service: SERVICE_NAME, reasons: state.reasons });
    }
    return { status: 'ready', service: SERVICE_NAME };
  });
  app.get('/metrics', async (_req, reply) => {
    reply.header('content-type', registry.contentType);
    const a = await registry.metrics();
    const b = await transcriptStoreMetricsRegistry.metrics();
    return `${a}\n${b}`;
  });
  return app;
}

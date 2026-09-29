// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import Fastify, { type FastifyInstance } from 'fastify';

const SERVICE_NAME = 'registry-service-ts';

export function buildHealthzServer(): FastifyInstance {
  const app = Fastify({ logger: false });
  app.get('/healthz', async () => ({ status: 'ok', service: SERVICE_NAME }));
  app.get('/readyz', async () => ({ status: 'ready', service: SERVICE_NAME }));
  return app;
}

// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createServer, type Server } from 'node:http';

/** Process liveness is separate from dependency readiness; never return dependency errors. */
export function createExporterHealthServer(options: {
  isLive: () => boolean;
  checkReady: () => Promise<boolean>;
}): Server {
  let pendingReadiness: Promise<boolean> | undefined;
  return createServer((request, response) => {
    const reply = (status: number): void => {
      response.writeHead(status, { 'content-type': 'text/plain', 'cache-control': 'no-store' });
      response.end(status === 200 ? 'ok\n' : 'unavailable\n');
    };
    if (request.method !== 'GET' || !['/healthz', '/readyz'].includes(request.url ?? '')) {
      reply(404);
    } else if (!options.isLive()) {
      reply(503);
    } else if (request.url === '/healthz') {
      reply(200);
    } else {
      // Coalesce overlapping probes instead of queueing duplicate dependency checks.
      pendingReadiness ??= Promise.resolve()
        .then(options.checkReady)
        .catch(() => false)
        .finally(() => {
          pendingReadiness = undefined;
        });
      void pendingReadiness.then((ready) => reply(ready && options.isLive() ? 200 : 503));
    }
  });
}

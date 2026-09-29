// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import assert from 'node:assert/strict';
import { PulsarTranscriptStore } from '../../dist/index.js';

// Run outside Vitest with --expose-gc: losing an in-use native client can
// hang or crash the process, which must fail this test without killing the
// other integration suites. Exercise the built library used by the services.
assert.equal(typeof globalThis.gc, 'function');
const gcTimer = setInterval(() => globalThis.gc(), 5);
try {
  for (let round = 0; round < 3; round++) {
    const suffix = `${Date.now()}_${round}`;
    const store = new PulsarTranscriptStore({
      serviceUrl: process.env.PULSAR_SERVICE_URL,
      tenant: process.env.PULSAR_TENANT ?? 'public',
      namespace: process.env.PULSAR_NAMESPACE ?? 'default',
      topicPrefix: `client_lifecycle_${suffix}`,
      readTimeoutMs: 500,
    });
    try {
      // No warm-up append: every operation must race the same cold client.
      await Promise.all(
        Array.from({ length: 16 }, async (_, i) => {
          const workspaceId = `ws_${suffix}`;
          const sessionId = `ses_${i}`;
          const event = {
            id: `evt_${suffix}_${i}`,
            workspaceId,
            sessionId,
            subpath: '',
            seq: 0,
            producedAt: new Date().toISOString(),
            producedBy: 'client',
            kind: 'user.message',
            payload: new Uint8Array([i]),
            idempotencyKey: '',
          };
          await store.append(workspaceId, sessionId, [event]);
          const seen = [];
          for await (const received of store.read(workspaceId, sessionId, {
            fromCursor: '',
            maxEvents: 0,
            subpath: '*',
          })) {
            seen.push(received);
          }
          assert.deepEqual(
            seen.map(({ id }) => id),
            [event.id],
          );
          assert.deepEqual(seen[0].payload, event.payload);
        }),
      );
    } finally {
      await store.close();
    }
  }
} finally {
  clearInterval(gcTimer);
}
console.log('Verified 48 concurrent session appends and reads');

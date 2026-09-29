// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import assert from 'node:assert/strict';
import Pulsar from 'pulsar-client';

// A broken native binding can crash during AsyncWorker destruction, after the
// read promise has rejected. Keep this probe outside the Vitest process.
assert.equal(typeof globalThis.gc, 'function');
const client = new Pulsar.Client({ serviceUrl: process.env.PULSAR_SERVICE_URL });
try {
  for (let round = 0; round < 5; round++) {
    const topic = `persistent://${process.env.PULSAR_TENANT ?? 'public'}/${process.env.PULSAR_NAMESPACE ?? 'default'}/reader_errors_${process.pid}_${Date.now()}_${round}`;
    const reader = await client.createReader({
      topic,
      startMessageId: Pulsar.MessageId.earliest(),
    });
    const producer = await client.createProducer({ topic, batchingEnabled: false });
    try {
      for (let attempt = 0; attempt < 20; attempt++) {
        await assert.rejects(reader.readNext(1), /TimeOut|Timeout/i);
        globalThis.gc();
      }
      const data = Buffer.from(`after-timeout-${round}`);
      await producer.send({ data });
      assert.deepEqual((await reader.readNext(5_000)).getData(), data);
    } finally {
      await reader.close();
      await producer.close();
    }
    // Exercise both timed and blocking native C API error paths.
    await assert.rejects(reader.readNext(1), /AlreadyClosed/i);
    await assert.rejects(reader.readNext(), /AlreadyClosed/i);
    globalThis.gc();
  }
} finally {
  await client.close();
}
console.log('Verified native Reader timeout, recovery and closed-reader errors');

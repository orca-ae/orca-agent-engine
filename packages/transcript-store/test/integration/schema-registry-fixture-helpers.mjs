// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import assert from 'node:assert/strict';

export async function createReadyEmptyTopic(admin, topic) {
  await admin.createTopics({
    topics: [{ topic, numPartitions: 1, replicationFactor: 1 }],
    // KafkaJS's internal leader wait misses some transient metadata errors.
    waitForLeaders: false,
    timeout: 5000,
  });
  const deadline = Date.now() + 10000;
  const transientTypes = new Set([
    'UNKNOWN_TOPIC_OR_PARTITION',
    'LEADER_NOT_AVAILABLE',
    'NOT_LEADER_FOR_PARTITION',
  ]);
  let lastError;
  let timer;
  const readiness = async () => {
    while (Date.now() < deadline) {
      try {
        const offsets = await admin.fetchTopicOffsets(topic);
        assert.deepEqual(
          offsets,
          [{ partition: 0, offset: '0', high: '0', low: '0' }],
          'fixture topic must be empty, with readable partition-0 offsets',
        );
        return offsets;
      } catch (error) {
        if (!transientTypes.has(error.type)) throw error;
        lastError = error;
        const remaining = deadline - Date.now();
        if (remaining <= 0) throw error;
        await new Promise((resolve) => setTimeout(resolve, Math.min(200, remaining)));
      }
    }
    throw lastError ?? new Error('Topic offset readiness exceeded 10s: ' + topic);
  };
  try {
    return await Promise.race([
      readiness(),
      new Promise((_, reject) => {
        timer = setTimeout(
          () => reject(lastError ?? new Error('Topic offset readiness exceeded 10s: ' + topic)),
          10000,
        );
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

export function assertDecodedEvent(decoded, expected) {
  assert.deepEqual({ ...decoded, payload: Buffer.from(decoded.payload) }, expected);
}

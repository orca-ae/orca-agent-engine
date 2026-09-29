// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { KafkaJSNumberOfRetriesExceeded, type Admin } from 'kafkajs';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { waitForKafkaTopics } from '../support/kafka.js';

const ready = [{ partition: 0, offset: '0', high: '0', low: '0' }];

describe('Kafka fixture topic readiness', () => {
  afterEach(() => vi.useRealTimers());

  it('waits through metadata propagation and leader election for every newly created topic', async () => {
    vi.useFakeTimers();
    const unknownTopic = Object.assign(new Error('topic metadata has not propagated'), {
      type: 'UNKNOWN_TOPIC_OR_PARTITION',
    });
    const admin = {
      fetchTopicOffsets: vi
        .fn<Admin['fetchTopicOffsets']>()
        .mockRejectedValueOnce(unknownTopic)
        .mockRejectedValueOnce(
          new KafkaJSNumberOfRetriesExceeded(unknownTopic, { retryCount: 2, retryTime: 100 }),
        )
        .mockRejectedValueOnce(
          Object.assign(new Error('no leader'), { type: 'LEADER_NOT_AVAILABLE' }),
        )
        .mockResolvedValueOnce([])
        .mockResolvedValueOnce(ready)
        .mockRejectedValueOnce(
          Object.assign(new Error('stale leader'), { type: 'NOT_LEADER_FOR_PARTITION' }),
        )
        .mockResolvedValue(ready),
    };
    const result = waitForKafkaTopics(admin, ['source', 'checkpoint', 'delivery']);
    await vi.runAllTimersAsync();
    await result;
    expect(admin.fetchTopicOffsets.mock.calls.map(([topic]) => topic)).toEqual([
      'source',
      'source',
      'source',
      'source',
      'source',
      'checkpoint',
      'checkpoint',
      'delivery',
    ]);
  });

  it('fails immediately on authorization errors', async () => {
    const error = Object.assign(new Error('not authorized'), {
      type: 'TOPIC_AUTHORIZATION_FAILED',
    });
    const admin = {
      fetchTopicOffsets: vi.fn<Admin['fetchTopicOffsets']>().mockRejectedValue(error),
    };
    await expect(waitForKafkaTopics(admin, ['source'])).rejects.toBe(error);
    expect(admin.fetchTopicOffsets).toHaveBeenCalledTimes(1);
  });

  it('bounds metadata retries and preserves the last broker error', async () => {
    vi.useFakeTimers();
    const error = Object.assign(new Error('unknown topic'), { type: 'UNKNOWN_TOPIC_OR_PARTITION' });
    const admin = {
      fetchTopicOffsets: vi.fn<Admin['fetchTopicOffsets']>().mockRejectedValue(error),
    };
    const result = expect(waitForKafkaTopics(admin, ['source'])).rejects.toMatchObject({
      message: 'Timed out waiting for Kafka topic source',
      cause: error,
    });
    await vi.advanceTimersByTimeAsync(30_000);
    await result;
  });
});

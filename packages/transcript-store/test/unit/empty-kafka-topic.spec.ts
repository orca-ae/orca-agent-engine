// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { Admin } from 'kafkajs';
import { afterEach, expect, it, vi } from 'vitest';
import { createEmptyTopic } from '../support/empty-kafka-topic.js';

const topic = 'orca.ws_empty.sessions.ses_empty.events';

function makeAdmin() {
  return {
    connect: vi.fn(async () => undefined),
    createTopics: vi.fn<Admin['createTopics']>().mockResolvedValue(true),
    fetchTopicOffsets: vi
      .fn<Admin['fetchTopicOffsets']>()
      .mockResolvedValue([{ partition: 0, offset: '0', high: '0', low: '0' }]),
    disconnect: vi.fn(async () => undefined),
  };
}

afterEach(() => vi.useRealTimers());

it('waits for readable offsets without KafkaJS premature leader-wait failure', async () => {
  vi.useFakeTimers();
  const admin = makeAdmin();
  const staleMetadata = Object.assign(new Error('This server does not host this topic-partition'), {
    type: 'UNKNOWN_TOPIC_OR_PARTITION',
  });
  // KafkaJS can successfully create the topic, then fail its internal metadata
  // wait before our offset-readiness loop gets a chance to handle propagation.
  admin.createTopics.mockImplementation(async ({ waitForLeaders }) => {
    if (waitForLeaders) throw staleMetadata;
    return true;
  });
  admin.fetchTopicOffsets.mockRejectedValueOnce(staleMetadata);
  const result = expect(createEmptyTopic(admin, topic)).resolves.toBeUndefined();
  await Promise.all([result, vi.runAllTimersAsync()]);
  expect(admin.createTopics).toHaveBeenCalledTimes(1);
  expect(admin.fetchTopicOffsets).toHaveBeenCalledTimes(2);
  expect(admin.fetchTopicOffsets).toHaveBeenLastCalledWith(topic);
  expect(admin.disconnect).toHaveBeenCalledOnce();
});

it('preserves topic creation failures and disconnects', async () => {
  const admin = makeAdmin();
  const error = new Error('Topic authorization failed');
  admin.createTopics.mockRejectedValue(error);
  await expect(createEmptyTopic(admin, topic)).rejects.toBe(error);
  expect(admin.fetchTopicOffsets).not.toHaveBeenCalled();
  expect(admin.disconnect).toHaveBeenCalledOnce();
});

it('bounds readiness polling and disconnects when metadata never becomes readable', async () => {
  vi.useFakeTimers();
  const admin = makeAdmin();
  const error = new Error('This server does not host this topic-partition');
  admin.fetchTopicOffsets.mockRejectedValue(error);
  const startedAt = Date.now();
  const result = expect(createEmptyTopic(admin, topic)).rejects.toBe(error);
  await Promise.all([result, vi.runAllTimersAsync()]);
  expect(Date.now() - startedAt).toBeGreaterThanOrEqual(10_000);
  expect(Date.now() - startedAt).toBeLessThanOrEqual(10_200);
  expect(admin.disconnect).toHaveBeenCalledOnce();
});

// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { Admin } from 'kafkajs';
import { createReadyKafkaTopic } from '../helpers/kafka-topic.js';

const topic = 'orca.ws_fixture.sessions.ses_fixture.events';
const metadata = (leader = 1) => ({
  topics: [
    {
      name: topic,
      partitions: [{ partitionErrorCode: 0, partitionId: 0, leader, replicas: [1], isr: [1] }],
    },
  ],
});
const protocolError = (code: number) => Object.assign(new Error('Kafka metadata error'), { code });
const admin = () => ({
  createTopics: vi.fn<Admin['createTopics']>().mockImplementation(async (options) => {
    // KafkaJS's built-in wait retries leader-not-available, but not unknown-topic.
    if (options.waitForLeaders) throw protocolError(3);
    return true;
  }),
  fetchTopicMetadata: vi.fn<Admin['fetchTopicMetadata']>().mockResolvedValue(metadata()),
});

describe('Kafka integration topic readiness', () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  it('waits through unknown-topic, missing metadata and an unassigned leader', async () => {
    const client = admin();
    client.fetchTopicMetadata
      .mockRejectedValueOnce(protocolError(3))
      .mockRejectedValueOnce(protocolError(5))
      .mockResolvedValueOnce({ topics: [] })
      .mockResolvedValueOnce(metadata(-1));
    const done = createReadyKafkaTopic(client, topic).then(
      () => null,
      (error: unknown) => error,
    );
    await vi.runAllTimersAsync();
    expect(await done).toBeNull();
    expect(client.createTopics).toHaveBeenCalledWith({
      topics: [{ topic, numPartitions: 1 }],
      waitForLeaders: false,
      timeout: 5000,
    });
    expect(client.fetchTopicMetadata).toHaveBeenCalledTimes(5);
    expect(client.fetchTopicMetadata).toHaveBeenLastCalledWith({ topics: [topic] });
  });

  it('checks readiness even when the topic already existed', async () => {
    const client = admin();
    client.createTopics.mockResolvedValue(false);
    await createReadyKafkaTopic(client, topic);
    expect(client.fetchTopicMetadata).toHaveBeenCalledWith({ topics: [topic] });
  });

  it.each([3, 5])(
    'times out persistent metadata error %s instead of accepting an unready topic',
    async (code) => {
      const client = admin();
      client.fetchTopicMetadata.mockRejectedValue(protocolError(code));
      const done = createReadyKafkaTopic(client, topic, 200).then(
        () => null,
        (error: unknown) => error,
      );
      await vi.runAllTimersAsync();
      expect(await done).toMatchObject({ message: expect.stringContaining('Timed out') });
      expect(client.fetchTopicMetadata.mock.calls.length).toBeGreaterThan(1);
      expect(client.fetchTopicMetadata.mock.calls.length).toBeLessThanOrEqual(5);
    },
  );

  it('does not turn authorization failures into readiness retries', async () => {
    const client = admin();
    client.createTopics.mockResolvedValue(true);
    const denied = protocolError(29);
    client.fetchTopicMetadata.mockRejectedValue(denied);
    await expect(createReadyKafkaTopic(client, topic)).rejects.toBe(denied);
    expect(client.fetchTopicMetadata).toHaveBeenCalledTimes(1);
  });

  it.each([3, 5])('waits through partition metadata error %s', async (code) => {
    const client = admin();
    const pending = metadata();
    pending.topics[0]!.partitions[0]!.partitionErrorCode = code;
    client.fetchTopicMetadata.mockResolvedValueOnce(pending);
    const done = createReadyKafkaTopic(client, topic);
    await vi.runAllTimersAsync();
    await done;
    expect(client.fetchTopicMetadata).toHaveBeenCalledTimes(2);
  });

  it('surfaces non-readiness partition errors immediately', async () => {
    const client = admin();
    const denied = metadata();
    denied.topics[0]!.partitions[0]!.partitionErrorCode = 29;
    client.fetchTopicMetadata.mockResolvedValue(denied);
    await expect(createReadyKafkaTopic(client, topic)).rejects.toThrow('metadata error 29');
    expect(client.fetchTopicMetadata).toHaveBeenCalledTimes(1);
  });
});

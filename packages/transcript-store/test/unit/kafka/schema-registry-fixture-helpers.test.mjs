// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { afterEach, expect, it, vi } from 'vitest';
import {
  assertDecodedEvent,
  createReadyEmptyTopic,
} from '../../integration/schema-registry-fixture-helpers.mjs';

const topic = 'genericKafkaSR-empty';
const emptyOffsets = [{ partition: 0, offset: '0', high: '0', low: '0' }];
const metadataError = (type) => Object.assign(new Error(type), { type });
const makeAdmin = () => ({
  createTopics: vi.fn().mockResolvedValue(true),
  fetchTopicOffsets: vi.fn().mockResolvedValue(emptyOffsets),
});

afterEach(() => vi.useRealTimers());

it('observes decoded bytes instead of substituting expected bytes', () => {
  const expected = { id: 'evt_1', seq: 1, payload: Buffer.from([0, 255]) };
  expect(() =>
    assertDecodedEvent({ ...expected, payload: new Uint8Array([1, 255]) }, expected),
  ).toThrow();
  expect(() =>
    assertDecodedEvent({ ...expected, payload: new Uint8Array([0, 255]) }, expected),
  ).not.toThrow();
  expect(() => assertDecodedEvent({ ...expected, seq: 0 }, expected)).toThrow();
});

it.each(['UNKNOWN_TOPIC_OR_PARTITION', 'LEADER_NOT_AVAILABLE', 'NOT_LEADER_FOR_PARTITION'])(
  'waits for real empty offsets after transient %s without seeding records',
  async (type) => {
    vi.useFakeTimers();
    const admin = makeAdmin();
    const error = metadataError(type);
    admin.createTopics.mockImplementation(async ({ waitForLeaders }) => {
      if (waitForLeaders) throw error;
      return true;
    });
    admin.fetchTopicOffsets.mockRejectedValueOnce(error);
    const result = expect(createReadyEmptyTopic(admin, topic)).resolves.toEqual(emptyOffsets);
    await Promise.all([result, vi.runAllTimersAsync()]);
    expect(admin.createTopics).toHaveBeenCalledOnce();
    expect(admin.createTopics).toHaveBeenCalledWith({
      topics: [{ topic, numPartitions: 1, replicationFactor: 1 }],
      waitForLeaders: false,
      timeout: 5000,
    });
    expect(admin.fetchTopicOffsets).toHaveBeenCalledTimes(2);
    expect(admin.fetchTopicOffsets).toHaveBeenLastCalledWith(topic);
  },
);

it.each(['TOPIC_AUTHORIZATION_FAILED', 'INVALID_TOPIC_EXCEPTION', 'unclassified'])(
  'fails immediately for permanent or unclassified readiness error %s',
  async (type) => {
    const admin = makeAdmin();
    const error = metadataError(type);
    admin.fetchTopicOffsets.mockRejectedValue(error);
    await expect(createReadyEmptyTopic(admin, topic)).rejects.toBe(error);
    expect(admin.fetchTopicOffsets).toHaveBeenCalledOnce();
  },
);

it('preserves create failures without querying offsets', async () => {
  const admin = makeAdmin();
  const error = metadataError('TOPIC_AUTHORIZATION_FAILED');
  admin.createTopics.mockRejectedValue(error);
  await expect(createReadyEmptyTopic(admin, topic)).rejects.toBe(error);
  expect(admin.fetchTopicOffsets).not.toHaveBeenCalled();
});

it('stops retrying persistent metadata errors at the deadline', async () => {
  vi.useFakeTimers();
  const admin = makeAdmin();
  const error = metadataError('UNKNOWN_TOPIC_OR_PARTITION');
  admin.fetchTopicOffsets.mockRejectedValue(error);
  const startedAt = Date.now();
  const result = expect(createReadyEmptyTopic(admin, topic)).rejects.toBe(error);
  await Promise.all([result, vi.runAllTimersAsync()]);
  expect(Date.now() - startedAt).toBe(10000);
  expect(admin.fetchTopicOffsets.mock.calls.length).toBeGreaterThan(1);
});

it('rejects nonempty offsets rather than treating a seeded topic as the fixture', async () => {
  const admin = makeAdmin();
  admin.fetchTopicOffsets.mockResolvedValue([{ partition: 0, offset: '1', high: '1', low: '0' }]);
  await expect(createReadyEmptyTopic(admin, topic)).rejects.toThrow('empty');
});

it('bounds an offset request that never settles', async () => {
  vi.useFakeTimers();
  const admin = makeAdmin();
  admin.fetchTopicOffsets.mockReturnValue(new Promise(() => {}));
  const startedAt = Date.now();
  const result = expect(createReadyEmptyTopic(admin, topic)).rejects.toThrow('exceeded 10s');
  await Promise.all([result, vi.runAllTimersAsync()]);
  expect(Date.now() - startedAt).toBe(10000);
  expect(admin.fetchTopicOffsets).toHaveBeenCalledOnce();
});

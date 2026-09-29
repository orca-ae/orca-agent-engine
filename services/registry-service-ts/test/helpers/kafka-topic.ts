// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { Admin } from 'kafkajs';

/** Avoid KafkaJS's leader-only wait: a newly created topic can also be unknown briefly. */
export async function createReadyKafkaTopic(
  admin: Pick<Admin, 'createTopics' | 'fetchTopicMetadata'>,
  topic: string,
  timeoutMs = 5000,
): Promise<void> {
  await admin.createTopics({
    topics: [{ topic, numPartitions: 1 }],
    waitForLeaders: false,
    timeout: timeoutMs,
  });
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    try {
      const metadata = await admin.fetchTopicMetadata({ topics: [topic] });
      const partitions = metadata.topics.find((entry) => entry.name === topic)?.partitions;
      const partition = partitions?.[0];
      if (partitions?.length === 1 && partition?.partitionId === 0) {
        if (![0, 3, 5].includes(partition.partitionErrorCode)) {
          throw new Error(`Kafka fixture partition metadata error ${partition.partitionErrorCode}`);
        }
        if (partition.partitionErrorCode === 0 && partition.leader >= 0) return;
      }
    } catch (error) {
      const code =
        typeof error === 'object' && error !== null
          ? (error as { code?: unknown }).code
          : undefined;
      // Only missing-topic and leader-election metadata is a readiness condition.
      if (code !== 3 && code !== 5) throw error;
    }
    if (Date.now() >= deadline) throw new Error(`Timed out waiting for Kafka topic ${topic}`);
    await new Promise((resolve) => setTimeout(resolve, Math.min(50, deadline - Date.now())));
  }
}

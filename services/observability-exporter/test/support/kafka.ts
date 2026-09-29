// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { KafkaJSNumberOfRetriesExceeded, type Admin } from 'kafkajs';

/** Wait for newly created fixture topics to be served by the broker. */
export async function waitForKafkaTopics(
  admin: Pick<Admin, 'fetchTopicOffsets'>,
  topics: readonly string[],
): Promise<void> {
  const deadline = Date.now() + 30_000;
  for (const topic of topics) {
    for (;;) {
      try {
        const offsets = await admin.fetchTopicOffsets(topic);
        if (offsets.length > 0) break;
      } catch (error) {
        const cause = error instanceof KafkaJSNumberOfRetriesExceeded ? error.cause : error;
        // KafkaJS createTopics(waitForLeaders: true) retries only
        // LEADER_NOT_AVAILABLE. KRaft can also return UNKNOWN_TOPIC_OR_PARTITION
        // before a just-created topic reaches the broker's metadata cache.
        if (
          typeof cause !== 'object' ||
          cause === null ||
          !('type' in cause) ||
          ![
            'UNKNOWN_TOPIC_OR_PARTITION',
            'LEADER_NOT_AVAILABLE',
            'NOT_LEADER_FOR_PARTITION',
          ].includes(String(cause.type))
        ) {
          throw error;
        }
        if (Date.now() >= deadline) {
          throw new Error(`Timed out waiting for Kafka topic ${topic}`, { cause: error });
        }
      }
      if (Date.now() >= deadline) throw new Error(`Timed out waiting for Kafka topic ${topic}`);
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
  }
}

// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { Admin } from 'kafkajs';

/** Create an empty topic and wait until a consumer can fetch its head offset. */
export async function createEmptyTopic(
  admin: Pick<Admin, 'connect' | 'createTopics' | 'fetchTopicOffsets' | 'disconnect'>,
  topic: string,
): Promise<void> {
  await admin.connect();
  try {
    await admin.createTopics({
      topics: [{ topic, numPartitions: 1 }],
      // KafkaJS's leader wait only retries LEADER_NOT_AVAILABLE, not transient
      // UNKNOWN_TOPIC_OR_PARTITION. Let the offset-readiness loop handle both.
      waitForLeaders: false,
      timeout: 5000,
    });
    // Topic creation is not a readiness signal. Wait for the same offset fetch
    // that the tail uses to seek its head, without seeding an event into the topic.
    const deadline = Date.now() + 10_000;
    for (;;) {
      try {
        await admin.fetchTopicOffsets(topic);
        return;
      } catch (exc) {
        if (Date.now() > deadline) throw exc;
        await new Promise((resolve) => setTimeout(resolve, 200));
      }
    }
  } finally {
    await admin.disconnect();
  }
}

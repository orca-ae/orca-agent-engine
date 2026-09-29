// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

const ID_PATTERN = /^[A-Za-z0-9_-]+$/;

/**
 * Shape of a valid (non-empty) topic prefix: one or more dot-terminated
 * segments, e.g. `public.default.`. The trailing dot is mandatory so the
 * prefix concatenates cleanly with the bare `orca.{ws}...` topic name.
 *
 * On a Kafka-on-Pulsar (KoP) endpoint, dotted Kafka topic names map to
 * `<tenant>.<namespace>.<local-topic>` — a `public.default.` prefix routes
 * the session topics into the default Pulsar tenant/namespace.
 */
export const TOPIC_PREFIX_REGEX = /^([A-Za-z0-9_-]+\.)+$/;

/** Throws when a non-empty `topicPrefix` does not match {@link TOPIC_PREFIX_REGEX}. */
export function validateTopicPrefix(topicPrefix: string): void {
  if (topicPrefix === '') return;
  if (!TOPIC_PREFIX_REGEX.test(topicPrefix)) {
    throw new Error(
      `invalid topic prefix: ${topicPrefix} (expected dot-terminated segments, e.g. "public.default.")`,
    );
  }
}

export function sessionTopicName(
  workspaceId: string,
  sessionId: string,
  topicPrefix = '',
  encoding: 'raw' | 'avro' = 'raw',
): string {
  validateTopicPrefix(topicPrefix);
  if (!ID_PATTERN.test(workspaceId)) {
    throw new Error(`invalid workspace_id: ${workspaceId}`);
  }
  if (!ID_PATTERN.test(sessionId)) {
    throw new Error(`invalid session_id: ${sessionId}`);
  }
  return `${topicPrefix}orca.${workspaceId}.sessions.${sessionId}.events${encoding === 'avro' ? '-avro' : ''}`;
}

export function parseCursor(cursor: string): bigint | null {
  if (cursor === '') return null;
  if (!/^\d+$/.test(cursor)) {
    throw new Error(`invalid cursor: ${cursor}`);
  }
  return BigInt(cursor);
}

export function formatCursor(offset: bigint): string {
  return offset.toString();
}

/**
 * Match `orca.{workspace_id}.sessions.{session_id}.events` topics.
 * Mirrors the `ID_PATTERN` constraint applied at construction.
 */
export const SESSION_TOPIC_REGEX = /^orca\.([A-Za-z0-9_-]+)\.sessions\.([A-Za-z0-9_-]+)\.events$/;

/** Alias for callers that prefer the noun form. */
export const SESSION_TOPIC_PATTERN = SESSION_TOPIC_REGEX;

const AVRO_SESSION_TOPIC_REGEX =
  /^orca\.([A-Za-z0-9_-]+)\.sessions\.([A-Za-z0-9_-]+)\.events-avro$/;

/** Select exactly one topic set; wire decoding compatibility does not widen routing. */
export function sessionTopicPattern(encoding: 'raw' | 'avro' = 'raw'): RegExp {
  return encoding === 'avro' ? AVRO_SESSION_TOPIC_REGEX : SESSION_TOPIC_REGEX;
}

export function parseSessionTopic(
  topic: string,
  encoding: 'raw' | 'avro' = 'raw',
): { workspaceId: string; sessionId: string } | null {
  const m = sessionTopicPattern(encoding).exec(topic);
  return m ? { workspaceId: m[1]!, sessionId: m[2]! } : null;
}

export interface SessionTopicMatch {
  workspaceId: string;
  sessionId: string;
  /**
   * The full (prefixed) topic name to actually produce to / subscribe to:
   * `topicPrefix + bareName`.
   */
  canonicalTopic: string;
}

/**
 * Match a topic name against the session-topic shape, tolerating both bare
 * and prefixed forms.
 *
 * A Kafka-on-Pulsar (KoP) endpoint lists topics in the default
 * tenant/namespace by their BARE local name (the `public.default.` part is
 * stripped from `admin.listTopics()` output), while produce/fetch require the
 * full prefixed name. Plain Kafka clusters list the full name. This helper
 * strips `topicPrefix` when present, matches the remainder against
 * {@link sessionTopicPattern} for the selected encoding, and returns the canonical (prefixed) name so
 * discovery works against either listing style.
 */
export function matchSessionTopic(
  topic: string,
  topicPrefix = '',
  encoding: 'raw' | 'avro' = 'raw',
): SessionTopicMatch | null {
  validateTopicPrefix(topicPrefix);
  const bare =
    topicPrefix !== '' && topic.startsWith(topicPrefix) ? topic.slice(topicPrefix.length) : topic;
  const m = sessionTopicPattern(encoding).exec(bare);
  if (!m) return null;
  return { workspaceId: m[1]!, sessionId: m[2]!, canonicalTopic: `${topicPrefix}${bare}` };
}

// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * Hand-rolled Event interface that mirrors the proto `Event` message in
 * `services/proto/transcript_store.proto`.  The library does not depend on the
 * proto-generated types, and the proto does not own the Kafka wire format:
 * `@orca/transcript-store`'s `kafka/serialize.ts` (raw) and `kafka/codec.ts`
 * (raw or Avro) do. In-process producers/consumers exchange this TS shape
 * directly.
 *
 * Note: `seq` is a `number` (not `bigint`). Kafka sources it from the message
 * offset via `Number(msg.offset)`; Postgres and Pulsar supply their own
 * backend cursor.
 */
export interface Event {
  id: string;
  workspaceId: string;
  sessionId: string;
  /** "" for the parent agent; "subagents/<id>" for a subagent trace. */
  subpath: string;
  /** Backend cursor: Kafka offset, Postgres `seq`, or a Pulsar message-ID-derived value. */
  seq: number;
  /** RFC3339 timestamp. */
  producedAt: string;
  /** "client" | "harness" | "transcript-store" */
  producedBy: string;
  /** Harness-defined; opaque to the store. */
  kind: string;
  payload: Uint8Array;
  idempotencyKey: string;
  /** Optional Registry-derived client user attribution. */
  userId?: string;
}

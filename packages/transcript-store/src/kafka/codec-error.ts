// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

export type KafkaTranscriptCodecErrorCode =
  | 'configuration'
  | 'registry_auth'
  | 'registry_not_found'
  | 'registry_unavailable'
  | 'registry_response'
  | 'schema'
  | 'record'
  | 'aborted'
  | 'closed'
  | 'capacity';

/** Deliberately excludes transport causes, URLs, credentials and response bodies. */
export class KafkaTranscriptCodecError extends Error {
  constructor(
    public readonly code: KafkaTranscriptCodecErrorCode,
    public readonly retryable = false,
  ) {
    super('Kafka transcript codec: ' + code);
    this.name = 'KafkaTranscriptCodecError';
  }
}

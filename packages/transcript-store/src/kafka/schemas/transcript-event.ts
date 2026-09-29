// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { Schema } from 'avsc';

/** Bundled in the JS output; no working-directory or asset-copy dependency. */
export const transcriptEventSchema: Schema = {
  type: 'record',
  name: 'TranscriptEvent',
  namespace: 'orca.transcript',
  fields: [
    { name: 'id', type: 'string' },
    { name: 'workspace_id', type: 'string' },
    { name: 'session_id', type: 'string' },
    { name: 'subpath', type: 'string', default: '' },
    { name: 'produced_at', type: 'string' },
    { name: 'produced_by', type: 'string' },
    { name: 'kind', type: 'string' },
    { name: 'payload', type: 'bytes' },
    { name: 'idempotency_key', type: 'string', default: '' },
    { name: 'user_id', type: ['null', 'string'], default: null },
  ],
};

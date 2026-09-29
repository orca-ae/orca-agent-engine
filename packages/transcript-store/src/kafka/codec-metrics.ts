// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { Counter, Histogram } from 'prom-client';
import { registry } from '../metrics.js';

export const schemaCache = new Counter({
  name: 'transcript_store_schema_cache_total',
  help: 'Schema resolver cache lookups',
  labelNames: ['result'] as const,
  registers: [registry],
});
export const schemaOperation = new Histogram({
  name: 'transcript_store_schema_operation_seconds',
  help: 'Registry operation latency including retries',
  labelNames: ['operation', 'result'] as const,
  registers: [registry],
  buckets: [0.01, 0.05, 0.1, 0.5, 1, 5, 15],
});
export const decodeFailures = new Counter({
  name: 'transcript_store_decode_failures_total',
  help: 'Transcript decoding failures by safe classification',
  labelNames: ['code'] as const,
  registers: [registry],
});

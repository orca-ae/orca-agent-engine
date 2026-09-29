// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { Counter, Registry, collectDefaultMetrics } from 'prom-client';

export const registry = new Registry();
collectDefaultMetrics({ register: registry, prefix: 'file_store_' });

export const createTotal = new Counter({
  name: 'file_store_create_total',
  help: 'FileStore.create calls',
  labelNames: ['status'] as const, // 'ok' | 'dedup' | 'error'
  registers: [registry],
});

export const dedupHits = new Counter({
  name: 'file_store_dedup_hits_total',
  help: 'CreateFile calls that found an existing (workspace, sha256) row',
  registers: [registry],
});

export const blobBytesTotal = new Counter({
  name: 'file_store_blob_bytes_total',
  help: 'Bytes flowing through the BlobStore by op',
  labelNames: ['op'] as const, // 'put' | 'open'
  registers: [registry],
});

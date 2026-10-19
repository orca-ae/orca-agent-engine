// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { ProjectedSpan } from './types.js';

export function isCanonicalSubpath(value: unknown): value is string {
  return (
    typeof value === 'string' &&
    (value === '' ||
      /^subagents\/[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/u.test(value))
  );
}

const FIELDS = ['parent_subpath', 'session_thread_id', 'parent_thread_id'] as const;
export const THREAD_METADATA_KEYS = FIELDS.map((field) => `orca.thread.${field}`);

function validField(field: string, value: unknown): value is string {
  return field === 'parent_subpath'
    ? isCanonicalSubpath(value)
    : typeof value === 'string' && /^[A-Za-z0-9_-]{1,128}$/u.test(value);
}

/** Only identity/lineage crosses the metadata boundary, never native messages. */
export function readThreadMetadata(payload: Record<string, unknown>): ProjectedSpan['metadata'] {
  return Object.fromEntries(
    FIELDS.flatMap((field) =>
      validField(field, payload[field]) ? [[`orca.thread.${field}`, payload[field] as string]] : [],
    ),
  );
}

export function parseThreadMetadata(value: Record<string, unknown>): ProjectedSpan['metadata'] {
  const metadata: ProjectedSpan['metadata'] = {};
  for (const field of FIELDS) {
    const key = `orca.thread.${field}`;
    if (!Object.hasOwn(value, key)) continue;
    if (!validField(field, value[key])) throw new Error('invalid thread metadata');
    metadata[key] = value[key] as string;
  }
  return metadata;
}

// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { MAX_READ_LIMIT_BYTES, type ReadPage, type ReadPageInput } from './read-page.js';
import type { SandboxHandle } from './sandbox-runtime.js';

export const DEFAULT_AGENT_READ_LIMIT_BYTES = 4096;
export const MAX_AGENT_READ_LIMIT_BYTES = 8192;
export const MAX_AGENT_READ_RESULT_BYTES = 16_384;
export const AGENT_READ_LIMIT_DESCRIPTION =
  'Optional requested maximum UTF-8 bytes (1-100000). Defaults to 4096; internally capped at ' +
  '8192 and reduced further to fit the serialized result budget. Metadata reports the effective limit.';

/** Budget the complete surface-specific envelope, never a sliced formatted page. */
export async function readAgentPage<T>(
  files: Pick<SandboxHandle['files'], 'readUtf8Page'>,
  path: string,
  input: ReadPageInput,
  format: (page: ReadPage) => T,
): Promise<T> {
  const requested = input.limit ?? DEFAULT_AGENT_READ_LIMIT_BYTES;
  // Validate before capping so invalid legacy arguments do not become valid reads.
  if (!Number.isSafeInteger(requested) || requested < 1 || requested > MAX_READ_LIMIT_BYTES) {
    throw new Error('limit must be an integer between 1 and 100000 bytes');
  }
  let limit = Math.min(requested, MAX_AGENT_READ_LIMIT_BYTES);
  for (;;) {
    // Each attempt is independently authorized by the runtime. A retry returns
    // only its own page/metadata, not a mixture of different file observations.
    const page = await files.readUtf8Page(path, {
      ...(input.offset !== undefined ? { offset: input.offset } : {}),
      limit,
    });
    const result = format(page);
    if (Buffer.byteLength(JSON.stringify(result), 'utf8') <= MAX_AGENT_READ_RESULT_BYTES) {
      return result;
    }
    if (limit === 1) throw new Error('page cannot fit the agent Read result budget');
    limit = Math.max(1, Math.floor(limit / 2));
  }
}

/** Keep ordinary diagnostics, but replace oversized errors rather than slicing them. */
export function agentReadError(error: unknown): string {
  const message = 'read failed: ' + (error instanceof Error ? error.message : 'unavailable');
  return Buffer.byteLength(JSON.stringify(message), 'utf8') <= 4096
    ? message
    : 'read failed: runtime error details exceed the agent Read result budget';
}

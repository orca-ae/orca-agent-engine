// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { readFile } from 'node:fs/promises';
import type { InternalServiceTokenProvider } from './registry-client.js';

/** Build a rotating file-backed or fixed internal Registry token source. */
export function buildInternalServiceTokenProvider(options: {
  token?: string;
  tokenFile?: string;
}): InternalServiceTokenProvider {
  if ((options.token === undefined ? 0 : 1) + (options.tokenFile === undefined ? 0 : 1) !== 1) {
    throw new Error(
      'observability-exporter requires exactly one of INTERNAL_SERVICE_TOKEN or INTERNAL_SERVICE_TOKEN_FILE',
    );
  }
  if (options.token !== undefined) {
    const token = validateToken(options.token);
    return async () => token;
  }
  const tokenFile = options.tokenFile!;
  return async () => validateToken(await readFile(tokenFile, 'utf8'));
}

function validateToken(raw: string): string {
  const token = raw.trim();
  if (token.length < 32 || token.length > 16 * 1024 || /\s/u.test(token)) {
    throw new Error('observability-exporter internal service token is invalid');
  }
  return token;
}

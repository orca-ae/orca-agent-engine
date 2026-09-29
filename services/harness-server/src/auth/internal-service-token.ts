// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { readFile } from 'node:fs/promises';

export type InternalServiceTokenProvider = () => Promise<string>;

export function buildInternalServiceTokenProvider(options: {
  token?: string;
  tokenFile?: string;
}): InternalServiceTokenProvider {
  if ((options.token ? 1 : 0) + (options.tokenFile ? 1 : 0) !== 1) {
    throw new Error(
      'Harness requires exactly one of INTERNAL_SERVICE_TOKEN or INTERNAL_SERVICE_TOKEN_FILE',
    );
  }
  if (options.token) {
    const token = validateToken(options.token);
    return async () => token;
  }
  const path = options.tokenFile!;
  return async () => validateToken(await readFile(path, 'utf8'));
}

function validateToken(raw: string): string {
  const token = raw.trim();
  if (!/^\S{32,}$/.test(token)) {
    throw new Error('internal service token must contain at least 32 non-whitespace characters');
  }
  return token;
}

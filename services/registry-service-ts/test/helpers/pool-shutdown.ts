// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { Pool } from 'pg';

/** Register before connecting; pool.end() can resolve before its clients emit end. */
export function trackPoolShutdown(pool: Pool): () => Promise<void> {
  const clientClosures: Promise<void>[] = [];
  pool.on('connect', (client) => {
    clientClosures.push(new Promise<void>((resolve) => client.once('end', resolve)));
  });
  return async () => {
    await pool.end();
    await Promise.all(clientClosures);
  };
}

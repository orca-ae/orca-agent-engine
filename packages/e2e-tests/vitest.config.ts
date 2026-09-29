// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { defineConfig } from 'vitest/config';

/**
 * Vitest config for `@orca/e2e-tests`.
 *
 * Drives the local stack started by `make stack-up`. There are no mocks; every
 * spec is a true black-box call against the running services. The longer
 * timeout reflects two slow-but-correct paths:
 *
 *  1. SSE handshakes — opening a stream + waiting for the heartbeat or the
 *     first transcript event takes the full heartbeat interval (~5s default
 *     on the dev stack) plus connection setup overhead.
 *  2. Sandbox provisioning (Layer B) — first cold-start of an `srt`
 *     sandbox can take 30-60s while the runtime image is being pulled.
 *
 * `singleFork` keeps the test workers in a single process so the API-key seed
 * helper (which inserts a row on first call and reuses it on every subsequent
 * call) shares state across spec files.
 */
export default defineConfig({
  test: {
    pool: 'forks',
    poolOptions: {
      forks: {
        singleFork: true,
      },
    },
    testTimeout: 60_000,
    hookTimeout: 120_000,
    include: ['test/**/*.spec.ts'],
    // Auto-loads `services/dev/.env` into the test process so locally-run
    // `pnpm e2e:agent` sees the same `ORCA_E2E_PLAINTEXT_KEY` /
    // `DATABASE_URL` etc. that the harness was started with — without the
    // user having to remember `set -a; source services/dev/.env; set +a`.
    // CI sets these envs at the job level so it ignores the loader; locally
    // the loader is what makes the quickstart work end-to-end.
    setupFiles: ['./test/_setup.ts'],
  },
});

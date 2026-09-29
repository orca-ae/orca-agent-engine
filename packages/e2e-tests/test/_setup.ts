// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * Vitest `setupFiles` shim — populates `process.env` from
 * `services/dev/.env` for any key the caller hasn't already set.
 *
 * Why this exists: the local-stack quickstart copies `.env.example` to
 * `services/dev/.env` and `make stack-up` sources it for the harness, but the
 * test process invoked via `pnpm e2e:agent` runs in a different shell and
 * sees none of those variables unless the user remembers
 * `set -a; source services/dev/.env; set +a` first. A deterministic
 * `ORCA_E2E_PLAINTEXT_KEY` lets a CI worker that reuses Postgres reseed the
 * same API-client key row; the Harness does not consume this credential.
 *
 * CI (`.github/workflows/e2e-stack.yml`) injects the same envs at the
 * job level so this loader is a no-op there — every key is already set
 * and the "do not override" rule below preserves the CI value.
 */
import { readFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const envPath = resolve(here, '../../../services/dev/.env');

try {
  const raw = readFileSync(envPath, 'utf8');
  for (const line of raw.split('\n')) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#')) continue;
    const eq = trimmed.indexOf('=');
    if (eq <= 0) continue;
    const key = trimmed.slice(0, eq).trim();
    if (key in process.env) continue; // do not clobber an already-set value
    let value = trimmed.slice(eq + 1).trim();
    if (
      (value.startsWith('"') && value.endsWith('"')) ||
      (value.startsWith("'") && value.endsWith("'"))
    ) {
      value = value.slice(1, -1);
    }
    process.env[key] = value;
  }
} catch (e) {
  // Missing .env is fine in CI (envs land via the job spec) and in any
  // workflow that explicitly exports the variables it needs. Surface other
  // I/O errors so a permission glitch doesn't masquerade as "no env".
  if ((e as NodeJS.ErrnoException).code !== 'ENOENT') {
    console.warn(`e2e-tests setup: failed to read ${envPath}: ${(e as Error).message}`);
  }
}

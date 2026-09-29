// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// E2BEnvironmentLauncher — live E2B integration test (gated).
//
// Proves `provision()` actually creates a real E2B sandbox when
// E2B_API_KEY + E2B_TEMPLATE_ID are configured — the one real-cloud
// assertion the rest of the launcher's test suite (cloud-environment-launcher
// .spec.ts, e2b-environment-launcher.spec.ts) deliberately avoids by fully
// faking the SandboxRuntime. Self-skips when creds are absent — mirrors
// harness-server's existing E2B-gated spec pattern
// (test/integration/e2b-sandbox.spec.ts: `describe.skipIf`, a console.warn on
// skip). No creds are required for `pnpm -F @orca/registry-service-ts test`
// (the unit suite) — this file lives under test/integration and is excluded
// from it.
//
// Deliberately narrow: only provision()/terminate() are exercised here, not
// startWorker(). Proving the FULL dial-back (exec environment-worker inside
// the box, have it actually connect to a real registry tunnel) needs a real
// Orca Environment image with environment-worker baked in, which is an infra
// dependency this port does not assume exists yet — the startWorker WIRING
// (the exact env vars an exec'd worker would see) is exhaustively covered by
// the fake-runtime unit suite instead.

import { describe, it, expect } from 'vitest';
import { E2BEnvironmentLauncher } from '../../src/environment/launcher/e2b-environment-launcher.js';

const apiKey = process.env['E2B_API_KEY'];
const templateId = process.env['E2B_TEMPLATE_ID'];
const skip = !apiKey || !templateId;

describe.skipIf(skip)('E2BEnvironmentLauncher (live)', () => {
  if (skip) {
    console.warn('e2b-environment-launcher-live: skipping — E2B_API_KEY/E2B_TEMPLATE_ID not set');
  }

  it('provision() creates a real E2B sandbox; terminate() destroys it', async () => {
    const launcher = new E2BEnvironmentLauncher({
      apiKey: apiKey!,
      templateId: templateId!,
      // Not exercised by this test (see module doc) — a syntactically valid
      // placeholder is enough since startWorker() is never called.
      workerLaunchCommand: ['true'],
      runnerLaunchCommand: ['true'],
    });

    const id = await launcher.provision('a2-e2b-live-test');
    try {
      expect(typeof id).toBe('string');
      expect(id.length).toBeGreaterThan(0);
      // startWorker() was never called — isRunning() reflects this launcher's
      // own bookkeeping (see cloud-environment-launcher.ts's doc), so a
      // freshly-provisioned, not-yet-started environment reads false.
      expect(await launcher.isRunning(id)).toBe(false);
    } finally {
      await launcher.terminate(id);
    }
    expect(await launcher.isRunning(id)).toBe(false);
  }, 120_000);
});

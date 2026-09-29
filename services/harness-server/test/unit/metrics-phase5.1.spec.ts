// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import {
  harnessFuseMountTotal,
  harnessOutputFilesIndexedTotal,
  harnessOutputIndexLagSeconds,
  harnessSandboxWriteDeniedTotal,
  harnessSandboxWritePolicySetupTotal,
  harnessStsMintTotal,
  registry,
} from '../../src/metrics.js';

/**
 * Smoke test for the FUSE / output-capture / STS / write-policy observability metrics.
 *
 * The goal is to fail loudly if a refactor accidentally drops a registration
 * (e.g. forgetting to pass `registers: [registry]`). We don't try to assert
 * full coverage of label permutations — the wiring sites have their own
 * unit tests for behavior; this test is purely about registration health.
 */
describe('Phase 5.1 metrics', () => {
  it('all four counters/histograms are reachable on the harness registry', () => {
    expect(registry.getSingleMetric('harness_fuse_mount_total')).toBe(harnessFuseMountTotal);
    expect(registry.getSingleMetric('harness_output_index_lag_seconds')).toBe(
      harnessOutputIndexLagSeconds,
    );
    expect(registry.getSingleMetric('harness_output_files_indexed_total')).toBe(
      harnessOutputFilesIndexedTotal,
    );
    expect(registry.getSingleMetric('harness_sts_mint_total')).toBe(harnessStsMintTotal);
    expect(registry.getSingleMetric('harness_sandbox_write_denied_total')).toBe(
      harnessSandboxWriteDeniedTotal,
    );
    expect(registry.getSingleMetric('harness_sandbox_write_policy_setup_total')).toBe(
      harnessSandboxWritePolicySetupTotal,
    );
  });

  it('harness_fuse_mount_total reports an incremented value back via the registry', async () => {
    // Snapshot the pre-existing value for the (tarball_prefetch, ok) cell —
    // other tests in this run may have already touched it.
    const before = await readCounterCell({
      name: 'harness_fuse_mount_total',
      labels: { strategy: 'tarball_prefetch', result: 'ok' },
    });

    harnessFuseMountTotal.inc({ strategy: 'tarball_prefetch', result: 'ok' });

    const after = await readCounterCell({
      name: 'harness_fuse_mount_total',
      labels: { strategy: 'tarball_prefetch', result: 'ok' },
    });

    expect(after - before).toBe(1);
  });
});

/**
 * Read a single counter label-cell value out of the registry by name + labels.
 * Returns 0 when no matching cell exists yet (prom-client doesn't materialize
 * counter cells until the first `.inc()` for that label combination).
 */
async function readCounterCell(opts: {
  name: string;
  labels: Record<string, string>;
}): Promise<number> {
  const json = await registry.getMetricsAsJSON();
  const metric = json.find((m) => m.name === opts.name);
  if (!metric) return 0;
  const values = (metric as { values: Array<{ labels: Record<string, string>; value: number }> })
    .values;
  const cell = values.find((v) =>
    Object.entries(opts.labels).every(([k, val]) => v.labels[k] === val),
  );
  return cell?.value ?? 0;
}

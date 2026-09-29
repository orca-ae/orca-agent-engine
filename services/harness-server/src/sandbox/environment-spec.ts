// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { EnvironmentSpec } from './sandbox-runtime.js';
import type { EnvironmentRecord } from '../clients/registry.js';
import { HARNESS_CATALOG, type HarnessType, type ResolvedHarness } from '@orca/harness-catalog';

/**
 * Map a registry Environment record (+ the resolved harness annotation) to the
 * sandbox `EnvironmentSpec`. For `mode: colocated` the sandbox image IS the
 * harness image: `environment.image ?? runtime override ?? catalog default`,
 * with the catalog entrypoint and harness HTTP port. For `separate` (or no
 * harness) only packages/networking/target are carried (today's behavior).
 *
 * `defaultImageOverrides` lets a deployment pin the in-sandbox image the
 * release actually published (chart `images.sandboxHarness.*` -> env ->
 * `ServiceConfig`) instead of the catalog's build-time default, without
 * touching every `Environment` row. An `Environment.image` still wins when
 * set — that per-environment pin is a deliberate operator/user choice.
 *
 * @throws Error when `harness.mode === 'colocated'` and no image resolves from
 *   any of the three sources. There is nothing to boot in that case; the
 *   dispatcher routes the failure through the session's `setup_failed` path.
 */
export function buildEnvironmentSpec(
  env: EnvironmentRecord | null | undefined,
  harness?: ResolvedHarness,
  defaultImageOverrides?: Partial<Record<HarnessType, string>>,
): EnvironmentSpec {
  const spec: EnvironmentSpec = {};
  if (env?.packages && Object.values(env.packages).some((items) => (items?.length ?? 0) > 0)) {
    spec.packages = env.packages;
  }
  if (env?.networking && Object.keys(env.networking).length > 0) spec.networking = env.networking;
  if (env?.target) spec.target = env.target;
  if (harness) {
    // OpenSandbox execd resolves ownership as account names, not numeric IDs.
    // Both reviewed images map these accounts to UID/GID 1000, which is the
    // nested agent boundary. Custom trusted images must preserve this contract.
    const uploadAccount = harness.mode === 'colocated' ? 'node' : 'ubuntu';
    spec.fileUploadOwnership = { owner: uploadAccount, group: uploadAccount };
  }

  if (harness?.mode === 'colocated') {
    const entry = HARNESS_CATALOG[harness.harness];
    // `!== null` rather than truthiness: the catalog now carries colocated
    // entries whose `entrypoint` is deliberately `null` (leave the image's own
    // CMD in charge) next to ones that set it, and `mock` carries neither an
    // image nor a port. Truthiness cannot tell "the catalog says none" from
    // "the catalog says nothing", and an empty-string image would have been
    // dropped silently instead of rejected.
    const image = env?.image ?? defaultImageOverrides?.[harness.harness] ?? entry.defaultImage;
    if (image === null || image === undefined || image.length === 0) {
      throw new Error(
        `harness '${harness.harness}' runs colocated but resolves to no sandbox image ` +
          `(Environment.image, the deployment default-image override, and the catalog default ` +
          `are all unset); harness-server cannot boot it`,
      );
    }
    spec.image = image;
    if (entry.entrypoint !== null) spec.entrypoint = entry.entrypoint;
    if (entry.port !== null) spec.exposePorts = [entry.port];
  }
  return spec;
}

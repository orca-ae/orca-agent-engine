// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { normalizeEnvironmentPackages } from '@orca/sandbox-runtime';
import type { Packages } from './sandbox-runtime.js';

export interface SandboxEnvironmentTrustInput {
  mode: 'separate' | 'colocated';
  image?: string | null;
  packages?: Packages;
  supportsEnvironmentPackages?: boolean;
}

/**
 * Until remote providers expose a sealed, provider-owned mount namespace,
 * Orca cannot safely run its write policy inside an arbitrary customer image:
 * an image daemon can race setup from outside the agent's bubblewrap namespace.
 * In-sandbox execution therefore uses only the operator-owned catalog image.
 *
 * Packages require provider-owned, isolated preparation. OpenSandbox runs
 * installers in a credential-free init container and exposes their output
 * only as an untrusted workload filesystem after that container terminates.
 */
export function assertSandboxEnvironmentTrust(input: SandboxEnvironmentTrustInput): void {
  if (input.mode === 'colocated' && input.image !== undefined && input.image !== null) {
    throw new Error(
      'custom in-sandbox images require a provider-sealed filesystem namespace and are not enabled',
    );
  }
  if (Object.keys(normalizeEnvironmentPackages(input.packages)).length === 0) return;
  if (input.supportsEnvironmentPackages === true) return;
  throw new Error('sandbox runtime does not support isolated Environment package preparation');
}

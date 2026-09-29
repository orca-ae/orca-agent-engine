// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { Packages } from '../sandbox-runtime.js';

export interface SandboxEnvironmentTrustInput {
  mode: 'separate' | 'colocated';
  image?: string | null;
  packages?: Packages;
}

/**
 * Until remote providers expose a sealed, provider-owned mount namespace,
 * Orca cannot safely run its write policy inside an arbitrary customer image:
 * an image daemon can race setup from outside the agent's bubblewrap namespace.
 * In-sandbox execution therefore uses only the operator-owned catalog image.
 *
 * Environment package installers are rejected for every managed sandbox mode.
 * Install hooks execute before resource and Skill roots are sealed and can
 * leave a background process with the same race capability.
 */
export function assertSandboxEnvironmentTrust(input: SandboxEnvironmentTrustInput): void {
  if (input.mode === 'colocated' && input.image !== undefined && input.image !== null) {
    throw new Error(
      'custom in-sandbox images require a provider-sealed filesystem namespace and are not enabled',
    );
  }
  if (!hasPackages(input.packages)) return;
  throw new Error(
    'managed sandbox execution does not allow environment package installers until filesystem sealing is available',
  );
}

function hasPackages(packages: Packages | undefined): boolean {
  return (
    packages !== undefined && Object.values(packages).some((items) => (items?.length ?? 0) > 0)
  );
}

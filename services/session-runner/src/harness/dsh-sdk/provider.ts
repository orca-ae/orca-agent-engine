// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { inspectDshInstallation } from '@orca/dsh-harness';
import type { AgentHarness } from '../agent-harness.js';
import { ProviderRegistry } from '../provider.js';
import { assertDshBootstrapHost } from './sandbox-launch.js';
import { validateDshBootstrapSnapshot } from './snapshot.js';

export const DSH_DORMANT_REASON =
  'DSH runner execution is disabled: credential, reporting and completion gates are unavailable';

/** Explicit bootstrap registration; intentionally absent from defaultRegisterProviders/hello. */
export function registerDormantDshSdkProvider(registry: ProviderRegistry): void {
  registry.register('dsh-sdk', (snapshot) => {
    validateDshBootstrapSnapshot(snapshot);
    const harness: AgentHarness = {
      async start() {
        validateDshBootstrapSnapshot(snapshot);
        assertDshBootstrapHost();
        inspectDshInstallation();
        throw new Error(DSH_DORMANT_REASON);
      },
      async submit() {
        throw new Error(DSH_DORMANT_REASON);
      },
      interrupt() {},
      async stop() {},
      async *events() {},
    };
    return harness;
  });
}

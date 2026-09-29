// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { ResolvedHarness } from './catalog.js';
import { resolveHarnessCapabilities } from './capabilities.js';

export type ExecutionOwner = 'registry' | 'harness-server';

/** Pinned harness capabilities decide ownership; worker availability never changes it. */
export function resolveExecutionOwner(target: string, selection: ResolvedHarness): ExecutionOwner {
  if (target !== 'cloud' && target !== 'self_hosted') {
    throw new Error(`unsupported environment target '${target}'`);
  }
  const capabilities = resolveHarnessCapabilities(selection);
  return target === 'self_hosted' ? 'registry' : capabilities.cloudExecutionOwner;
}

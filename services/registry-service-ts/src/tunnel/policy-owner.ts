// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0
import { randomUUID } from 'node:crypto';
import type { TransportRegistry } from '@orca/harness-tunnel';
const owners = new WeakMap<object, string>();
/** Stable for one live tunnel object; reconnect/owner restart always gets a new epoch identity. */
export function runnerPolicyOwner(registry: TransportRegistry, runnerId: string): string {
  const connection = registry.get(runnerId);
  if (!connection) throw new Error('policy runner is offline');
  let owner = owners.get(connection);
  if (!owner) {
    owner = randomUUID();
    owners.set(connection, owner);
  }
  return owner;
}

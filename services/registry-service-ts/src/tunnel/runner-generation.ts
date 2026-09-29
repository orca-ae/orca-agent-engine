// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { ConnectError, type TransportRegistry } from '@orca/harness-tunnel';

/** A retired bridge must never send requests to a replacement runner tunnel. */
export function pinRunnerGeneration(
  registry: TransportRegistry,
  runnerId: string,
): {
  registry: TransportRegistry;
  isCurrent(): boolean;
} {
  const generation = registry.get(runnerId);
  const isCurrent = () => generation !== undefined && registry.get(runnerId) === generation;
  return {
    isCurrent,
    registry: {
      get: (id) => (id === runnerId && isCurrent() ? generation : undefined),
      openRequest(id, requestId) {
        if (id !== runnerId || !isCurrent()) throw new ConnectError('runner generation retired');
        return registry.openRequest(id, requestId);
      },
      closeRequest: registry.closeRequest.bind(registry),
      requestIsOpen: registry.requestIsOpen.bind(registry),
      sendText: registry.sendText.bind(registry),
    },
  };
}

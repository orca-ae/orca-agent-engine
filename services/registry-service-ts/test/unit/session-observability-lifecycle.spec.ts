// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import {
  nextSessionObservabilityBindingLifecycle,
  SessionObservabilityBindingLifecycleError,
  type SessionObservabilityBindingLifecycleState,
} from '../../src/domain/session-observability-lifecycle.js';

const now = new Date('2035-08-18T03:00:00.000Z');

function state(
  overrides: Partial<SessionObservabilityBindingLifecycleState> = {},
): SessionObservabilityBindingLifecycleState {
  return {
    status: 'active',
    archivedAt: null,
    deletedAt: null,
    sessionRevocationEpoch: 4,
    ...overrides,
  };
}

describe('Session observability pin lifecycle', () => {
  it.each(['active', 'disabled'] as const)('archives a %s pin once', (status) => {
    expect(nextSessionObservabilityBindingLifecycle(state({ status }), 'archive', now)).toEqual({
      status: 'archived',
      archivedAt: now,
      deletedAt: null,
    });
  });

  it('keeps an archived pin unchanged on repeat archive', () => {
    const archivedAt = new Date('2035-08-18T02:00:00.000Z');

    expect(
      nextSessionObservabilityBindingLifecycle(
        state({ status: 'archived', archivedAt }),
        'archive',
        now,
      ),
    ).toBeNull();
  });

  it.each(['active', 'disabled'] as const)('deletes a %s pin directly', (status) => {
    expect(nextSessionObservabilityBindingLifecycle(state({ status }), 'delete', now)).toEqual({
      status: 'deleted',
      archivedAt: null,
      deletedAt: now,
    });
  });

  it('preserves an archive tombstone when deleting', () => {
    const archivedAt = new Date('2035-08-18T02:00:00.000Z');

    expect(
      nextSessionObservabilityBindingLifecycle(
        state({ status: 'archived', archivedAt }),
        'delete',
        now,
      ),
    ).toEqual({
      status: 'deleted',
      archivedAt,
      deletedAt: now,
    });
  });

  it.each([
    ['deleted pin', state({ status: 'deleted', deletedAt: now }), 'archive'],
    ['deleted pin', state({ status: 'deleted', deletedAt: now }), 'delete'],
    ['active pin with tombstone', state({ archivedAt: now }), 'archive'],
  ] as const)('%s rejects invalid %s lifecycle state', (_label, current, action) => {
    expect(() => nextSessionObservabilityBindingLifecycle(current, action, now)).toThrow(
      SessionObservabilityBindingLifecycleError,
    );
  });
});

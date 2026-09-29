// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { UNAUTHENTICATED_PATHS } from '../../src/auth/auth.js';

/**
 * The unauthenticated surface, pinned by literal.
 *
 * The behavioural tests in `discovery-routes.spec.ts` catch `/api` or `/apis`
 * being put back on the allowlist, because they assert the 401. Nothing catches
 * a *fourth* path arriving — `/version`, an `/openapi` route, a debug endpoint —
 * since a new entry opens a door no existing test knocks on. This assertion is
 * that missing test: it fails on any change to the set, which is the point.
 *
 * Widening it is a security decision. If this test fails, the fix is to make the
 * decision explicitly and record it, not to update the expectation.
 */
describe('the unauthenticated allowlist', () => {
  it('is exactly the two probes, plus test-only metrics', () => {
    expect([...UNAUTHENTICATED_PATHS].sort()).toEqual(['/healthz', '/metrics', '/readyz']);
  });

  it('does not include capability discovery', () => {
    // Discovery describes what this deployment serves. That is an answer about
    // the deployment, and a caller that cannot present a key has no claim on it
    // — the line Kubernetes draws between `system:public-info-viewer` (probes,
    // bound to `system:unauthenticated`) and `system:discovery` (`/api`,
    // `/apis`, bound to `system:authenticated` only).
    expect(UNAUTHENTICATED_PATHS.has('/api')).toBe(false);
    expect(UNAUTHENTICATED_PATHS.has('/apis')).toBe(false);
  });

  it('matches whole paths, so no future group inherits a probe entry', () => {
    // The hook tests membership, not prefixes: `/apis/<group>/<version>/*` is
    // authenticated because everything not in this set is, not because someone
    // remembers to add it.
    expect(UNAUTHENTICATED_PATHS.has('/apis/example.orca.dev/v1/things')).toBe(false);
    expect(UNAUTHENTICATED_PATHS.has('/healthz/')).toBe(false);
  });
});

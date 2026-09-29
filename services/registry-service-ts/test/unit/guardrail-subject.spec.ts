// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { authenticatedGuardrailSubject } from '../../src/auth/guardrail-subject.js';

/**
 * Nine lines with three branches, and the partition key for every
 * cross-session budget: `user_daily_cost_budget` accumulates spend under
 * whatever string this returns. A branch that fires in the wrong order does
 * not fail — it silently merges two principals into one budget, or splits one
 * principal across two, and the only symptom is a cap that stops at the wrong
 * number.
 *
 * Registry stamps the result at the trust boundary and the harness cannot
 * supply one, so this function is the whole of the decision.
 */
describe('authenticatedGuardrailSubject', () => {
  it('prefers the user over every other identity', () => {
    // A human acting through an api key is still that human: their daily spend
    // must accumulate across the keys they use, not once per key.
    expect(
      authenticatedGuardrailSubject({
        principal: 'svc_ci',
        apiKeyId: 'key_123',
        userId: 'usr_abc',
      }),
    ).toBe('user:usr_abc');
  });

  it('falls back to the api key when there is no user', () => {
    expect(authenticatedGuardrailSubject({ principal: 'svc_ci', apiKeyId: 'key_123' })).toBe(
      'api-key:key_123',
    );
  });

  it('falls back to the principal when there is neither', () => {
    expect(authenticatedGuardrailSubject({ principal: 'svc_ci' })).toBe('principal:svc_ci');
  });

  it('keeps the three namespaces from colliding', () => {
    // The prefixes are the only thing separating the identity spaces. Without
    // them an api key whose id happened to equal a user id would share that
    // user's budget.
    const shared = 'abc';
    const subjects = new Set([
      authenticatedGuardrailSubject({ principal: shared, userId: shared }),
      authenticatedGuardrailSubject({ principal: shared, apiKeyId: shared }),
      authenticatedGuardrailSubject({ principal: shared }),
    ]);
    expect(subjects.size).toBe(3);
  });

  it('treats an empty identity as absent rather than as a subject of its own', () => {
    // `''` is falsy, so it takes the next branch. Worth pinning: the
    // alternative — `user:` as a real subject — would pool every caller with a
    // blank user id into one shared budget.
    expect(
      authenticatedGuardrailSubject({ principal: 'svc_ci', userId: '', apiKeyId: 'key_1' }),
    ).toBe('api-key:key_1');
    expect(authenticatedGuardrailSubject({ principal: 'svc_ci', userId: '', apiKeyId: '' })).toBe(
      'principal:svc_ci',
    );
  });
});

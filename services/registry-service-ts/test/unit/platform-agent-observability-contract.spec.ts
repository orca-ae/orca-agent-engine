// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import {
  PlatformAgentObservabilityPolicySchema,
  PlatformAgentObservabilityPutHeadersSchema,
  PlatformAgentObservabilityPutRequestSchema,
} from '../../src/contracts/platform-agent-observability.contract.js';
import { platformAgentObservabilityPolicyEtag } from '../../src/domain/agent-observability-platform-policy.js';

const policy = {
  allowed_adapters: ['otlp_http', 'langfuse_sdk'],
  allowed_endpoint_classes: ['public', 'private'],
  max_capture_mode: 'raw_io',
};
const state = () =>
  PlatformAgentObservabilityPolicySchema.parse({
    ...policy,
    type: 'agent_observability_platform_policy',
    capture_restriction_epoch: 7,
    created_at: '2026-09-01T00:00:00.000Z',
    updated_at: '2026-09-17T00:00:00.000Z',
  });

describe('platform observability policy contract', () => {
  it.each(['metadata_only', 'redacted_io', 'raw_io'])(
    'accepts explicit %s and canonicalizes allowlists',
    (mode) => {
      expect(
        PlatformAgentObservabilityPutRequestSchema.parse({ ...policy, max_capture_mode: mode }),
      ).toEqual({
        allowed_adapters: ['langfuse_sdk', 'otlp_http'],
        allowed_endpoint_classes: ['private', 'public'],
        max_capture_mode: mode,
      });
    },
  );

  it.each([
    { ...policy, allowed_adapters: [] },
    { ...policy, allowed_adapters: ['unknown'] },
    { ...policy, allowed_endpoint_classes: [] },
    { ...policy, allowed_endpoint_classes: ['public', 'public'] },
    { ...policy, allowed_endpoint_classes: 'public' },
    { ...policy, max_capture_mode: 'redacted' },
    { ...policy, capture_restriction_epoch: 0 },
    { ...policy, updated_at: '2026-09-01T00:00:00.000Z' },
    { ...policy, credentials: { password: 'not-policy-data' } },
  ])('rejects malformed, ambiguous or server-owned policy fields: %j', (input) => {
    expect(PlatformAgentObservabilityPutRequestSchema.safeParse(input).success).toBe(false);
  });

  it('requires an exact strong ETag and a normalized bounded idempotency key', () => {
    expect(
      PlatformAgentObservabilityPutHeadersSchema.parse({
        'if-match': '"current"',
        'idempotency-key': '  operation-1  ',
      })['idempotency-key'],
    ).toBe('operation-1');
    for (const key of ['', ' ', 'x'.repeat(256), 'bad\u0000key']) {
      expect(
        PlatformAgentObservabilityPutHeadersSchema.safeParse({
          'if-match': '"current"',
          'idempotency-key': key,
        }).success,
      ).toBe(false);
    }
  });

  it('uses every authoritative response field in a stable strong ETag', () => {
    const current = state();
    const etag = platformAgentObservabilityPolicyEtag(current);
    expect(
      platformAgentObservabilityPolicyEtag({
        ...current,
        allowed_adapters: [...current.allowed_adapters].reverse(),
        allowed_endpoint_classes: [...current.allowed_endpoint_classes].reverse(),
      }),
    ).toBe(etag);
    for (const change of [
      { allowed_adapters: ['otlp_http'] as const },
      { allowed_endpoint_classes: ['public'] as const },
      { max_capture_mode: 'metadata_only' as const },
      { capture_restriction_epoch: 8 },
      { updated_at: '2026-09-17T00:00:00.001Z' },
      { created_at: '2026-09-01T00:00:00.001Z' },
    ]) {
      expect(
        platformAgentObservabilityPolicyEtag(
          PlatformAgentObservabilityPolicySchema.parse({ ...current, ...change }),
        ),
      ).not.toBe(etag);
    }
  });

  it('rejects unsafe epochs and regressed timestamps on authoritative state', () => {
    for (const epoch of [-1, 1.5, Number.MAX_SAFE_INTEGER + 1]) {
      expect(
        PlatformAgentObservabilityPolicySchema.safeParse({
          ...state(),
          capture_restriction_epoch: epoch,
        }).success,
      ).toBe(false);
    }
    expect(
      PlatformAgentObservabilityPolicySchema.safeParse({
        ...state(),
        updated_at: '2020-01-01T00:00:00.000Z',
      }).success,
    ).toBe(false);
  });
});

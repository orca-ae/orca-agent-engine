// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import {
  OrganizationAgentObservabilityDisableHeadersSchema,
  OrganizationAgentObservabilityDisableRequestSchema,
  OrganizationAgentObservabilityCredentialRotationHeadersSchema,
  OrganizationAgentObservabilityCredentialRotationRequestSchema,
  AgentObservabilityMutationCredentialsSchema,
  OrganizationAgentObservabilityPutRequestSchema,
  OrganizationAgentObservabilityPutHeadersSchema,
} from '../../src/contracts/agent-observability.contract.js';
import {
  OrganizationAgentObservabilityMutationRequestError,
  assertOrganizationAgentObservabilityDisablePrecondition,
  assertOrganizationAgentObservabilityMutationRequest,
  classifyOrganizationAgentObservabilityMutation,
  organizationAgentObservabilityCredentialRotationBody,
  organizationAgentObservabilityDisableBody,
  organizationAgentObservabilityMutationBody,
  parseOrganizationAgentObservabilityCredentialRotationRequest,
  parseOrganizationAgentObservabilityDisableRequest,
  parseOrganizationAgentObservabilityIdempotencyKey,
  parseOrganizationAgentObservabilityIfMatch,
  parseOrganizationAgentObservabilityPutRequest,
} from '../../src/domain/agent-observability-organization-mutation.js';
import { agentObservabilityMutationBodyHash } from '../../src/domain/agent-observability-mutations.js';
import {
  AGENT_OBSERVABILITY_CUSTOM_HEADER_MAX_COUNT,
  AGENT_OBSERVABILITY_CUSTOM_HEADER_NAME_MAX_LENGTH,
  AGENT_OBSERVABILITY_CUSTOM_HEADER_TOTAL_MAX_LENGTH,
  AGENT_OBSERVABILITY_CUSTOM_HEADER_VALUE_MAX_LENGTH,
  AGENT_OBSERVABILITY_IDEMPOTENCY_KEY_MAX_LENGTH,
  AGENT_OBSERVABILITY_SECRET_VALUE_MAX_LENGTH,
  isSingleStrongEntityTag,
  normalizeAgentObservabilityIdempotencyKey,
  normalizeAgentObservabilityOtlpHttpCredentialInput,
} from '../../src/domain/agent-observability-validation.js';

describe('organization agent observability PUT request', () => {
  it('canonicalizes accepted endpoint input before body hashing', () => {
    const request = parseOrganizationAgentObservabilityPutRequest({
      ...requestBody(),
      target: { ...requestBody().target, endpoint_url: 'HTTPS://COLLECTOR.EXAMPLE:443/v1/traces/' },
    });
    expect(request.target.endpoint).toBe('https://collector.example/v1/traces');
    expect(organizationAgentObservabilityMutationBody(request)).toMatchObject({
      target: { endpoint_url: 'https://collector.example/v1/traces' },
    });
    expect(
      agentObservabilityMutationBodyHash(organizationAgentObservabilityMutationBody(request)),
    ).toBe(
      agentObservabilityMutationBodyHash({
        ...requestBody(),
        target: {
          ...requestBody().target,
          endpoint_url: 'https://collector.example/v1/traces',
          external_project_id: null,
        },
        config: { ...requestBody().config, environment: null, release: null },
      }),
    );
  });

  it('freezes strict request fields and V1 adapter/protocol surface', () => {
    expect(
      OrganizationAgentObservabilityPutRequestSchema.safeParse({
        ...requestBody(),
        secret_ref: 'local:agent_observability/obssec_not_allowed',
      }).success,
    ).toBe(false);
    expect(
      OrganizationAgentObservabilityPutRequestSchema.safeParse({
        ...requestBody(),
        target: { ...requestBody().target, adapter_type: 'langfuse_sdk' },
      }).success,
    ).toBe(false);
    expect(
      OrganizationAgentObservabilityPutRequestSchema.safeParse({
        ...requestBody(),
        config: { ...requestBody().config, protocol: 'sdk' },
      }).success,
    ).toBe(false);
    expect(
      OrganizationAgentObservabilityPutRequestSchema.safeParse({
        ...requestBody(),
        config: { ...requestBody().config, sample_rate: 0.0003 },
      }).success,
    ).toBe(true);
    expect(
      OrganizationAgentObservabilityPutRequestSchema.safeParse({
        ...requestBody(),
        config: { ...requestBody().config, sample_rate: 0.00031 },
      }).success,
    ).toBe(false);
    expect(
      OrganizationAgentObservabilityPutRequestSchema.safeParse({
        ...requestBody(),
        config: { ...requestBody().config, sample_rate: 0.12345 },
      }).success,
    ).toBe(false);
  });

  it('classifies first, same-target, and target replacement credentials', () => {
    const first = parseOrganizationAgentObservabilityPutRequest(requestBody());
    const initial = classifyOrganizationAgentObservabilityMutation(null, first);
    expect(initial).toEqual({ type: 'initial' });
    expect(() => assertOrganizationAgentObservabilityMutationRequest(first, initial)).not.toThrow();

    const same = classifyOrganizationAgentObservabilityMutation(
      {
        id: 'aob_current',
        status: 'active',
        target: { ...first.target },
        configVersion: 1,
        credentialVersion: 1,
      },
      first,
    );
    expect(same).toEqual({ type: 'same_target', bindingId: 'aob_current' });
    expect(() => assertOrganizationAgentObservabilityMutationRequest(first, same)).toThrow(
      OrganizationAgentObservabilityMutationRequestError,
    );

    const policyOnly = parseOrganizationAgentObservabilityPutRequest({
      ...requestBody(),
      credentials: undefined,
    });
    expect(
      classifyOrganizationAgentObservabilityMutation(
        {
          id: 'aob_current',
          status: 'active',
          target: { ...policyOnly.target },
          configVersion: 1,
          credentialVersion: 1,
        },
        policyOnly,
      ),
    ).toEqual({ type: 'same_target', bindingId: 'aob_current' });

    const replacement = classifyOrganizationAgentObservabilityMutation(
      {
        id: 'aob_current',
        status: 'draining',
        target: { ...policyOnly.target },
        configVersion: 1,
        credentialVersion: 1,
      },
      first,
    );
    expect(replacement).toEqual({ type: 'replacement', previousActiveBindingId: null });
    expect(() =>
      assertOrganizationAgentObservabilityMutationRequest(first, replacement),
    ).not.toThrow();
  });

  it('requires Langfuse project identity but keeps OTLP credential modes independent', () => {
    const withoutProject = parseOrganizationAgentObservabilityPutRequest({
      ...requestBody(),
      target: { ...requestBody().target, external_project_id: null },
      config: { ...requestBody().config, semantic_profile: 'langfuse' },
    });
    expect(() =>
      assertOrganizationAgentObservabilityMutationRequest(withoutProject, { type: 'initial' }),
    ).toThrow(OrganizationAgentObservabilityMutationRequestError);

    const basic = parseOrganizationAgentObservabilityPutRequest({
      ...requestBody(),
      target: { ...requestBody().target, external_project_id: 'pk_project' },
      config: { ...requestBody().config, semantic_profile: 'langfuse' },
      credentials: { type: 'basic', username: 'pk_other', password: 'secret' },
    });
    expect(() =>
      assertOrganizationAgentObservabilityMutationRequest(basic, { type: 'initial' }),
    ).not.toThrow();

    const bearer = parseOrganizationAgentObservabilityPutRequest({
      ...requestBody(),
      target: { ...requestBody().target, external_project_id: 'pk_project' },
      config: { ...requestBody().config, semantic_profile: 'langfuse' },
      credentials: { type: 'bearer', token: 'secret' },
    });
    expect(() =>
      assertOrganizationAgentObservabilityMutationRequest(bearer, { type: 'initial' }),
    ).not.toThrow();

    const customHeaders = parseOrganizationAgentObservabilityPutRequest({
      ...requestBody(),
      target: { ...requestBody().target, external_project_id: 'pk_project' },
      config: { ...requestBody().config, semantic_profile: 'langfuse' },
      credentials: { type: 'custom_headers', headers: { 'x-api-key': 'secret' } },
    });
    expect(() =>
      assertOrganizationAgentObservabilityMutationRequest(customHeaders, { type: 'initial' }),
    ).not.toThrow();

    const policyOnly = parseOrganizationAgentObservabilityPutRequest({
      ...requestBody(),
      target: { ...requestBody().target, external_project_id: 'pk_project' },
      config: { ...requestBody().config, semantic_profile: 'langfuse' },
      credentials: undefined,
    });
    expect(() =>
      assertOrganizationAgentObservabilityMutationRequest(policyOnly, {
        type: 'same_target',
        bindingId: 'aob_current',
      }),
    ).not.toThrow();
  });

  it('requires bounded idempotency and one exact strong If-Match header', () => {
    expect(
      OrganizationAgentObservabilityPutHeadersSchema.safeParse({ 'idempotency-key': 'key' })
        .success,
    ).toBe(true);
    expect(
      OrganizationAgentObservabilityPutHeadersSchema.safeParse({
        'idempotency-key': 'key\nnext',
      }).success,
    ).toBe(false);
    expect(
      OrganizationAgentObservabilityPutHeadersSchema.safeParse({
        'idempotency-key': 'key',
        'if-match': 'W/"orca-aos-v1-current"',
      }).success,
    ).toBe(false);
    expect(parseOrganizationAgentObservabilityIdempotencyKey(' key ')).toBe('key');
    expect(() => parseOrganizationAgentObservabilityIdempotencyKey('   ')).toThrow(
      OrganizationAgentObservabilityMutationRequestError,
    );
    expect(() => parseOrganizationAgentObservabilityIdempotencyKey('key\nnext')).toThrow(
      OrganizationAgentObservabilityMutationRequestError,
    );
    expect(parseOrganizationAgentObservabilityIfMatch('"orca-aos-v1-current"')).toBe(
      '"orca-aos-v1-current"',
    );
    for (const value of ['W/"orca-aos-v1-current"', '*', '"one", "two"', 'orca-aos-v1-current']) {
      expect(() => parseOrganizationAgentObservabilityIfMatch(value)).toThrow(
        OrganizationAgentObservabilityMutationRequestError,
      );
    }
  });

  it('keeps credential, PUT, and rotation contract acceptance in parity', () => {
    const totalBoundaryHeaderValueLength =
      (AGENT_OBSERVABILITY_CUSTOM_HEADER_TOTAL_MAX_LENGTH - 4 * 'x-a'.length) / 4;
    const cases: Array<[string, unknown]> = [
      [
        'basic secret at shared boundary',
        {
          type: 'basic',
          username: 'project',
          password: 'p'.repeat(AGENT_OBSERVABILITY_SECRET_VALUE_MAX_LENGTH),
        },
      ],
      [
        'basic secret beyond shared boundary',
        {
          type: 'basic',
          username: 'project',
          password: 'p'.repeat(AGENT_OBSERVABILITY_SECRET_VALUE_MAX_LENGTH + 1),
        },
      ],
      [
        'basic secret control character',
        { type: 'basic', username: 'project\nnext', password: 'secret' },
      ],
      [
        'bearer secret at shared boundary',
        { type: 'bearer', token: 't'.repeat(AGENT_OBSERVABILITY_SECRET_VALUE_MAX_LENGTH) },
      ],
      [
        'bearer secret beyond shared boundary',
        {
          type: 'bearer',
          token: 't'.repeat(AGENT_OBSERVABILITY_SECRET_VALUE_MAX_LENGTH + 1),
        },
      ],
      ['bearer DEL control character', { type: 'bearer', token: 'secret\x7f' }],
      [
        'custom headers canonicalized',
        {
          type: 'custom_headers',
          headers: { 'X-Zeta': 'secret', 'x-Alpha': '' },
        },
      ],
      [
        'custom header name and value at boundaries',
        {
          type: 'custom_headers',
          headers: {
            [`x${'a'.repeat(AGENT_OBSERVABILITY_CUSTOM_HEADER_NAME_MAX_LENGTH - 1)}`]: 'v'.repeat(
              AGENT_OBSERVABILITY_CUSTOM_HEADER_VALUE_MAX_LENGTH,
            ),
          },
        },
      ],
      [
        'custom header name beyond boundary',
        {
          type: 'custom_headers',
          headers: {
            [`x${'a'.repeat(AGENT_OBSERVABILITY_CUSTOM_HEADER_NAME_MAX_LENGTH)}`]: 'secret',
          },
        },
      ],
      [
        'custom header value beyond boundary',
        {
          type: 'custom_headers',
          headers: {
            'x-api-key': 'v'.repeat(AGENT_OBSERVABILITY_CUSTOM_HEADER_VALUE_MAX_LENGTH + 1),
          },
        },
      ],
      [
        'custom header count at boundary',
        {
          type: 'custom_headers',
          headers: Object.fromEntries(
            Array.from({ length: AGENT_OBSERVABILITY_CUSTOM_HEADER_MAX_COUNT }, (_, index) => [
              `x-header-${index}`,
              'secret',
            ]),
          ),
        },
      ],
      [
        'custom header aggregate at boundary',
        {
          type: 'custom_headers',
          headers: Object.fromEntries(
            ['x-a', 'x-b', 'x-c', 'x-d'].map((name) => [
              name,
              'v'.repeat(totalBoundaryHeaderValueLength),
            ]),
          ),
        },
      ],
      [
        'custom header aggregate beyond boundary',
        {
          type: 'custom_headers',
          headers: {
            'x-a': 'v'.repeat(totalBoundaryHeaderValueLength + 1),
            'x-b': 'v'.repeat(totalBoundaryHeaderValueLength),
            'x-c': 'v'.repeat(totalBoundaryHeaderValueLength),
            'x-d': 'v'.repeat(totalBoundaryHeaderValueLength),
          },
        },
      ],
      ['empty custom headers', { type: 'custom_headers', headers: {} }],
      [
        'custom header injection',
        { type: 'custom_headers', headers: { 'x-api\r\nkey': 'secret' } },
      ],
      ['forbidden custom header', { type: 'custom_headers', headers: { Authorization: 'secret' } }],
      [
        'server-owned content type header',
        { type: 'custom_headers', headers: { 'content-type': 'application/json' } },
      ],
      [
        'case-variant server-owned content encoding header',
        { type: 'custom_headers', headers: { 'Content-Encoding': 'gzip' } },
      ],
      [
        'duplicate custom header names',
        { type: 'custom_headers', headers: { 'X-Api-Key': 'one', 'x-api-key': 'two' } },
      ],
      [
        'custom header value control character',
        { type: 'custom_headers', headers: { 'x-api-key': 'a\tb' } },
      ],
      [
        'too many custom headers',
        {
          type: 'custom_headers',
          headers: Object.fromEntries(
            Array.from({ length: AGENT_OBSERVABILITY_CUSTOM_HEADER_MAX_COUNT + 1 }, (_, index) => [
              `x-header-${index}`,
              'secret',
            ]),
          ),
        },
      ],
    ];

    for (const [, credentials] of cases) {
      const normalized = normalizeAgentObservabilityOtlpHttpCredentialInput(credentials);
      expect(AgentObservabilityMutationCredentialsSchema.safeParse(credentials).success).toBe(
        normalized !== null,
      );
      expect(
        OrganizationAgentObservabilityCredentialRotationRequestSchema.safeParse({ credentials })
          .success,
      ).toBe(normalized !== null);
      expect(
        OrganizationAgentObservabilityPutRequestSchema.safeParse({
          ...requestBody(),
          credentials,
        }).success,
      ).toBe(normalized !== null);

      if (normalized !== null) {
        expect(
          parseOrganizationAgentObservabilityPutRequest({ ...requestBody(), credentials })
            .credentials,
        ).toEqual(normalized);
        expect(
          parseOrganizationAgentObservabilityCredentialRotationRequest({ credentials }).credentials,
        ).toEqual(normalized);
      } else {
        expect(() =>
          parseOrganizationAgentObservabilityPutRequest({ ...requestBody(), credentials }),
        ).toThrow(OrganizationAgentObservabilityMutationRequestError);
        expect(() =>
          parseOrganizationAgentObservabilityCredentialRotationRequest({ credentials }),
        ).toThrow(OrganizationAgentObservabilityMutationRequestError);
      }
    }
  });

  it('keeps header contract acceptance and parser normalization in parity', () => {
    for (const value of [
      'key',
      ' key ',
      '\nkey\n',
      'k'.repeat(AGENT_OBSERVABILITY_IDEMPOTENCY_KEY_MAX_LENGTH),
      '   ',
      'key\nnext',
      'k'.repeat(AGENT_OBSERVABILITY_IDEMPOTENCY_KEY_MAX_LENGTH + 1),
      null,
    ]) {
      const normalized = normalizeAgentObservabilityIdempotencyKey(value);
      expect(
        OrganizationAgentObservabilityPutHeadersSchema.safeParse({ 'idempotency-key': value })
          .success,
      ).toBe(normalized !== null);
      if (normalized !== null) {
        expect(parseOrganizationAgentObservabilityIdempotencyKey(value)).toBe(normalized);
      } else {
        expect(() => parseOrganizationAgentObservabilityIdempotencyKey(value)).toThrow(
          OrganizationAgentObservabilityMutationRequestError,
        );
      }
    }

    for (const value of [
      undefined,
      '"orca-aos-v1-current"',
      '""',
      'W/"orca-aos-v1-current"',
      '*',
      '"one", "two"',
      '"contains space"',
      '"contains\ttab"',
      1,
    ]) {
      const headers: Record<string, unknown> = { 'idempotency-key': 'key' };
      if (value !== undefined) headers['if-match'] = value;
      const accepted = value === undefined || isSingleStrongEntityTag(value);
      expect(OrganizationAgentObservabilityPutHeadersSchema.safeParse(headers).success).toBe(
        accepted,
      );
      if (accepted) {
        expect(parseOrganizationAgentObservabilityIfMatch(value)).toBe(value ?? null);
      } else {
        expect(() => parseOrganizationAgentObservabilityIfMatch(value)).toThrow(
          OrganizationAgentObservabilityMutationRequestError,
        );
      }
    }
  });

  it('accepts only credentials for strict OTLP credential rotation', () => {
    const rotation = {
      credentials: { type: 'custom_headers' as const, headers: { 'X-Api-Key': 'secret' } },
    };
    expect(
      OrganizationAgentObservabilityCredentialRotationRequestSchema.safeParse(rotation).success,
    ).toBe(true);
    for (const forbidden of [
      { ...rotation, target: requestBody().target },
      { ...rotation, config: requestBody().config },
      { ...rotation, secret_ref: 'local:agent_observability/obssec_not_allowed' },
      { ...rotation, adapter_type: 'langfuse_sdk' },
    ]) {
      expect(
        OrganizationAgentObservabilityCredentialRotationRequestSchema.safeParse(forbidden).success,
      ).toBe(false);
      expect(() => parseOrganizationAgentObservabilityCredentialRotationRequest(forbidden)).toThrow(
        OrganizationAgentObservabilityMutationRequestError,
      );
    }

    const normalized = parseOrganizationAgentObservabilityCredentialRotationRequest(rotation);
    expect(normalized.credentials).toEqual({
      type: 'custom_headers',
      headers: { 'x-api-key': 'secret' },
    });
    expect(organizationAgentObservabilityCredentialRotationBody(normalized)).toEqual({
      credentials: normalized.credentials,
    });
  });

  it('requires one exact strong If-Match for credential rotation headers', () => {
    expect(
      OrganizationAgentObservabilityCredentialRotationHeadersSchema.safeParse({
        'idempotency-key': 'rotate-key',
      }).success,
    ).toBe(false);
    expect(
      OrganizationAgentObservabilityCredentialRotationHeadersSchema.safeParse({
        'idempotency-key': 'rotate-key',
        'if-match': 'W/"orca-aos-v1-current"',
      }).success,
    ).toBe(false);
    expect(
      OrganizationAgentObservabilityCredentialRotationHeadersSchema.safeParse({
        'idempotency-key': 'rotate-key',
        'if-match': '"orca-aos-v1-current"',
      }).success,
    ).toBe(true);
  });

  it('accepts only an empty emergency-disable body and optional strong If-Match', () => {
    expect(OrganizationAgentObservabilityDisableRequestSchema.safeParse({}).success).toBe(true);
    for (const body of [
      { binding_id: 'aob_caller_must_not_select' },
      { target: requestBody().target },
      { archive: true },
    ]) {
      expect(OrganizationAgentObservabilityDisableRequestSchema.safeParse(body).success).toBe(
        false,
      );
      expect(() => parseOrganizationAgentObservabilityDisableRequest(body)).toThrow(
        OrganizationAgentObservabilityMutationRequestError,
      );
    }
    const request = parseOrganizationAgentObservabilityDisableRequest({});
    expect(organizationAgentObservabilityDisableBody(request)).toEqual({});
    expect(
      OrganizationAgentObservabilityDisableHeadersSchema.safeParse({
        'idempotency-key': 'disable-key',
      }).success,
    ).toBe(true);
    expect(
      OrganizationAgentObservabilityDisableHeadersSchema.safeParse({
        'idempotency-key': 'disable-key',
        'if-match': 'W/"not-strong"',
      }).success,
    ).toBe(false);
    expect(
      assertOrganizationAgentObservabilityDisablePrecondition({
        currentEtag: '"current"',
        ifMatch: null,
      }),
    ).toBe('ok');
    expect(
      assertOrganizationAgentObservabilityDisablePrecondition({
        currentEtag: '"current"',
        ifMatch: '"stale"',
      }),
    ).toBe('stale');
  });
});

function requestBody() {
  return {
    target: {
      adapter_type: 'otlp_http' as const,
      endpoint_kind: 'traces_endpoint' as const,
      endpoint_class: 'public' as const,
      endpoint_url: 'https://collector.example/v1/traces',
    },
    config: {
      semantic_profile: 'otel_genai' as const,
      protocol: 'http/protobuf' as const,
      compression: 'gzip' as const,
      timeout_ms: 5000,
      capture_mode: 'metadata_only' as const,
      sample_rate: 1,
    },
    capture_ceiling: 'metadata_only' as const,
    credentials: { type: 'basic' as const, username: 'project', password: 'secret' },
  };
}

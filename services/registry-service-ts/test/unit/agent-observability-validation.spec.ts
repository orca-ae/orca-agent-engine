// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import {
  AgentObservabilityBindingViewSchema,
  AgentObservabilityEndpointUrlSchema,
} from '../../src/contracts/agent-observability.contract.js';
import {
  AGENT_OBSERVABILITY_ENDPOINT_MAX_LENGTH,
  AGENT_OBSERVABILITY_TIMEOUT_MAX_MS,
  isAgentObservabilityCustomHeaderValue,
  isAgentObservabilityBasicUsername,
  isAgentObservabilityExternalProjectId,
  isAgentObservabilityKeyHint,
  isAgentObservabilitySecretValue,
  isAgentObservabilityTimeoutMs,
  isCanonicalAgentObservabilityEndpoint,
  normalizeAgentObservabilityCustomHeaders,
  normalizeAgentObservabilityEndpoint,
  normalizeAgentObservabilityOtlpHttpCredentialInput,
} from '../../src/domain/agent-observability-validation.js';

const canonicalEndpoint = 'https://collector.example/v1/traces';

describe('agent observability endpoint canonicalization', () => {
  it('accepts one canonical absolute HTTP(S) form without network I/O', () => {
    expect(normalizeAgentObservabilityEndpoint(canonicalEndpoint)).toBe(canonicalEndpoint);
    expect(isCanonicalAgentObservabilityEndpoint(canonicalEndpoint)).toBe(true);
    expect(normalizeAgentObservabilityEndpoint('https://collector.example/')).toBe(
      'https://collector.example',
    );
    expect(isCanonicalAgentObservabilityEndpoint('https://collector.example')).toBe(true);
  });

  it.each([
    'https://collector.example/v1/traces?',
    'https://collector.example/v1/traces#',
    'https://@collector.example/v1/traces',
    'https://:@collector.example/v1/traces',
    'ftp://collector.example/v1/traces',
    'https:///v1/traces',
  ])('rejects unsafe endpoint before URL normalization: %s', (value) => {
    expect(normalizeAgentObservabilityEndpoint(value)).toBeNull();
    expect(isCanonicalAgentObservabilityEndpoint(value)).toBe(false);
    expect(AgentObservabilityEndpointUrlSchema.safeParse(value).success).toBe(false);
  });

  it.each([
    'https://collector.example/v1/traces/',
    'https://collector.example:443/v1/traces',
    'HTTPS://COLLECTOR.EXAMPLE/v1/traces',
  ])('rejects normalized-but-noncanonical stored endpoint %s', (value) => {
    expect(normalizeAgentObservabilityEndpoint(value)).not.toBeNull();
    expect(isCanonicalAgentObservabilityEndpoint(value)).toBe(false);
    expect(AgentObservabilityEndpointUrlSchema.safeParse(value).success).toBe(false);
  });

  it('rejects endpoints over bounded length', () => {
    const overlong = `https://collector.example/${'a'.repeat(AGENT_OBSERVABILITY_ENDPOINT_MAX_LENGTH)}`;
    expect(isCanonicalAgentObservabilityEndpoint(overlong)).toBe(false);
    expect(AgentObservabilityEndpointUrlSchema.safeParse(overlong).success).toBe(false);
  });
});

describe('agent observability public config metadata bounds', () => {
  it('limits timeout to the shared 1..120000 range in pure and contract validation', () => {
    expect(isAgentObservabilityTimeoutMs(AGENT_OBSERVABILITY_TIMEOUT_MAX_MS)).toBe(true);
    expect(isAgentObservabilityTimeoutMs(AGENT_OBSERVABILITY_TIMEOUT_MAX_MS + 1)).toBe(false);
    expect(
      AgentObservabilityBindingViewSchema.safeParse({
        ...bindingView(),
        config: { ...bindingView().config, timeout_ms: AGENT_OBSERVABILITY_TIMEOUT_MAX_MS + 1 },
      }).success,
    ).toBe(false);
  });

  it('bounds and sanitizes key hint and external project metadata', () => {
    expect(isAgentObservabilityKeyHint('...abcd')).toBe(true);
    expect(isAgentObservabilityKeyHint(' ...abcd')).toBe(false);
    expect(isAgentObservabilityKeyHint('line\nbreak')).toBe(false);
    expect(isAgentObservabilityKeyHint('a'.repeat(129))).toBe(false);
    expect(isAgentObservabilityExternalProjectId('pk_project')).toBe(true);
    expect(isAgentObservabilityExternalProjectId(' pk_project')).toBe(false);
    expect(isAgentObservabilityExternalProjectId('pk_project\nnext')).toBe(false);
    expect(isAgentObservabilityExternalProjectId('a'.repeat(513))).toBe(false);
    expect(
      AgentObservabilityBindingViewSchema.safeParse({
        ...bindingView(),
        credential: { ...bindingView().credential, key_hint: ' ...abcd' },
      }).success,
    ).toBe(false);
    expect(
      AgentObservabilityBindingViewSchema.safeParse({
        ...bindingView(),
        target: { ...bindingView().target, external_project_id: ' pk_project' },
      }).success,
    ).toBe(false);
  });
});

describe('agent observability OTLP custom headers', () => {
  it.each([
    'content-type',
    'Content-Type',
    'CONTENT-TYPE',
    'content-encoding',
    'Content-Encoding',
    'CONTENT-ENCODING',
  ])('rejects server-owned %s headers case-insensitively', (name) => {
    expect(normalizeAgentObservabilityCustomHeaders({ [name]: 'server-owned' })).toBeNull();
  });
});

describe('agent observability credential Unicode', () => {
  it.each([
    ['lone high surrogate', '\ud800'],
    ['lone low surrogate', '\udc00'],
  ])('rejects %s through secret and custom-header normalization', (_label, surrogate) => {
    const value = `credential-${surrogate}`;

    expect(isAgentObservabilitySecretValue(value)).toBe(false);
    expect(isAgentObservabilityCustomHeaderValue(value)).toBe(false);
    expect(
      normalizeAgentObservabilityOtlpHttpCredentialInput({
        type: 'basic',
        username: 'valid-user',
        password: value,
      }),
    ).toBeNull();
    expect(normalizeAgentObservabilityCustomHeaders({ 'x-api-key': value })).toBeNull();
  });

  it('accepts well-formed supplementary-code-point pairs', () => {
    const value = 'credential-\ud83d\ude00';

    expect(isAgentObservabilitySecretValue(value)).toBe(true);
    expect(isAgentObservabilityCustomHeaderValue(value)).toBe(true);
    expect(
      normalizeAgentObservabilityOtlpHttpCredentialInput({
        type: 'basic',
        username: value,
        password: value,
      }),
    ).not.toBeNull();
    expect(normalizeAgentObservabilityCustomHeaders({ 'x-api-key': value })).not.toBeNull();
  });
});

describe('agent observability Basic credentials', () => {
  it('rejects usernames containing the Basic delimiter without restricting passwords', () => {
    expect(isAgentObservabilitySecretValue('user:name')).toBe(true);
    expect(isAgentObservabilityBasicUsername('user:name')).toBe(false);
    expect(
      normalizeAgentObservabilityOtlpHttpCredentialInput({
        type: 'basic',
        username: 'user:name',
        password: 'password:may:contain:colons',
      }),
    ).toBeNull();
    expect(
      normalizeAgentObservabilityOtlpHttpCredentialInput({
        type: 'basic',
        username: 'username',
        password: 'password:may:contain:colons',
      }),
    ).toEqual({
      type: 'basic',
      username: 'username',
      password: 'password:may:contain:colons',
    });
  });
});

function bindingView() {
  return {
    id: 'aob_validation',
    scope: 'organization' as const,
    organization_id: 'org_validation',
    workspace_id: null,
    target: {
      adapter_type: 'otlp_http' as const,
      external_project_id: 'pk_validation',
      endpoint_kind: 'traces_endpoint' as const,
      endpoint_class: 'public' as const,
      endpoint_url: canonicalEndpoint,
    },
    status: 'active' as const,
    config: {
      version: 1,
      semantic_profile: 'langfuse' as const,
      protocol: 'http/protobuf' as const,
      compression: 'none' as const,
      timeout_ms: 5000,
      environment: null,
      release: null,
      capture_mode: 'redacted_io' as const,
      sample_rate: 1,
      config_schema_version: 1,
    },
    credential: {
      configured: true as const,
      version: 1,
      key_hint: '...abcd',
      rotated_at: '2026-08-27T00:00:00.000Z',
    },
  };
}

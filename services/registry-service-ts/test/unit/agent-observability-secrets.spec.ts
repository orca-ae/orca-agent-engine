// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import {
  AGENT_OBSERVABILITY_CUSTOM_HEADER_MAX_COUNT,
  AGENT_OBSERVABILITY_CUSTOM_HEADER_NAME_MAX_LENGTH,
  AGENT_OBSERVABILITY_CUSTOM_HEADER_VALUE_MAX_LENGTH,
  AgentObservabilitySecretBundleError,
  decodeAgentObservabilitySecretBundle,
  encodeAgentObservabilitySecretBundle,
  isAgentObservabilitySecretReference,
  newAgentObservabilitySecretReference,
  normalizeAgentObservabilityOtlpHttpCredentials,
} from '../../src/domain/agent-observability-secrets.js';

const identity = { bindingId: 'aob_bundle_a', credentialVersion: 7 };

describe('agent observability credential bundles', () => {
  it('canonically encodes every union with one binding and credential generation', () => {
    const basic = encodeAgentObservabilitySecretBundle({
      ...identity,
      adapterType: 'otlp_http',
      auth: { type: 'basic', username: 'otlp-user', password: 'basic-password' },
    });
    const bearer = encodeAgentObservabilitySecretBundle({
      ...identity,
      adapterType: 'otlp_http',
      auth: { type: 'bearer', token: 'bearer-token' },
    });
    const headers = encodeAgentObservabilitySecretBundle({
      ...identity,
      adapterType: 'otlp_http',
      auth: { type: 'custom_headers', headers: { 'X-Second': 'two', 'x-first': 'one' } },
    });
    const langfuse = encodeAgentObservabilitySecretBundle({
      ...identity,
      adapterType: 'langfuse_sdk',
      publicKey: 'pk-lf-public',
      secretKey: 'sk-lf-secret',
    });

    expect(basic).toBe(
      '{"adapter_type":"otlp_http","auth":{"password":"basic-password","type":"basic","username":"otlp-user"},"binding_id":"aob_bundle_a","credential_version":7,"version":1}',
    );
    expect(JSON.parse(headers)).toEqual({
      adapter_type: 'otlp_http',
      auth: { headers: { 'x-first': 'one', 'x-second': 'two' }, type: 'custom_headers' },
      binding_id: 'aob_bundle_a',
      credential_version: 7,
      version: 1,
    });
    expect(decodeAgentObservabilitySecretBundle(basic, identity)).toEqual({
      ...identity,
      adapterType: 'otlp_http',
      auth: { type: 'basic', username: 'otlp-user', password: 'basic-password' },
    });
    expect(decodeAgentObservabilitySecretBundle(bearer, identity)).toEqual({
      ...identity,
      adapterType: 'otlp_http',
      auth: { type: 'bearer', token: 'bearer-token' },
    });
    expect(decodeAgentObservabilitySecretBundle(headers, identity)).toEqual({
      ...identity,
      adapterType: 'otlp_http',
      auth: { type: 'custom_headers', headers: { 'x-first': 'one', 'x-second': 'two' } },
    });
    expect(decodeAgentObservabilitySecretBundle(langfuse, identity)).toEqual({
      ...identity,
      adapterType: 'langfuse_sdk',
      publicKey: 'pk-lf-public',
      secretKey: 'sk-lf-secret',
    });
  });

  it('fails closed for a cross-binding or stale-generation bundle', () => {
    const encoded = encodeAgentObservabilitySecretBundle({
      ...identity,
      adapterType: 'otlp_http',
      auth: { type: 'bearer', token: 'opaque-token' },
    });
    expect(() =>
      decodeAgentObservabilitySecretBundle(encoded, { ...identity, bindingId: 'aob_bundle_b' }),
    ).toThrow(AgentObservabilitySecretBundleError);
    expect(() =>
      decodeAgentObservabilitySecretBundle(encoded, { ...identity, credentialVersion: 8 }),
    ).toThrow(AgentObservabilitySecretBundleError);
  });

  it('rejects alternate JSON, endpoint fields, and user-supplied Authorization', () => {
    const canonical = encodeAgentObservabilitySecretBundle({
      ...identity,
      adapterType: 'otlp_http',
      auth: { type: 'bearer', token: 'opaque-token' },
    });
    expect(() =>
      decodeAgentObservabilitySecretBundle(
        '{"version":1,"adapter_type":"otlp_http","binding_id":"aob_bundle_a","credential_version":7,"auth":{"type":"bearer","token":"opaque-token"}}',
        identity,
      ),
    ).toThrow(AgentObservabilitySecretBundleError);
    expect(canonical).not.toContain('endpoint');
    expect(() =>
      encodeAgentObservabilitySecretBundle({
        ...identity,
        adapterType: 'otlp_http',
        auth: { type: 'basic', username: 'user:name', password: 'password' },
      }),
    ).toThrow(AgentObservabilitySecretBundleError);
    expect(() =>
      encodeAgentObservabilitySecretBundle({
        ...identity,
        adapterType: 'otlp_http',
        auth: {
          type: 'custom_headers',
          headers: { Authorization: 'Bearer caller-must-not-control-this' },
        },
      }),
    ).toThrow(AgentObservabilitySecretBundleError);
    expect(() =>
      encodeAgentObservabilitySecretBundle({
        ...identity,
        adapterType: 'otlp_http',
        auth: {
          type: 'custom_headers',
          headers: { [`x${'a'.repeat(AGENT_OBSERVABILITY_CUSTOM_HEADER_NAME_MAX_LENGTH)}`]: 'v' },
        },
      }),
    ).toThrow(AgentObservabilitySecretBundleError);
    expect(() =>
      encodeAgentObservabilitySecretBundle({
        ...identity,
        adapterType: 'otlp_http',
        auth: {
          type: 'custom_headers',
          headers: { 'x-safe': 'a'.repeat(AGENT_OBSERVABILITY_CUSTOM_HEADER_VALUE_MAX_LENGTH + 1) },
        },
      }),
    ).toThrow(AgentObservabilitySecretBundleError);
  });

  it.each([
    'Host',
    'Content-Length',
    'Connection',
    'traceparent',
    'tracestate',
    'baggage',
    'content-type',
    'Content-Type',
    'CONTENT-TYPE',
    'content-encoding',
    'Content-Encoding',
    'CONTENT-ENCODING',
  ])('rejects forbidden %s headers', (name) => {
    expect(() =>
      encodeAgentObservabilitySecretBundle({
        ...identity,
        adapterType: 'otlp_http',
        auth: { type: 'custom_headers', headers: { [name]: 'blocked' } },
      }),
    ).toThrow(AgentObservabilitySecretBundleError);
  });

  it('rejects header injection, duplicate case variants, and oversized maps', () => {
    for (const headers of [
      {},
      { 'x-safe': 'line\r\ninjected: true' },
      { 'X-Token': 'one', 'x-token': 'two' },
      Object.fromEntries(
        Array.from({ length: AGENT_OBSERVABILITY_CUSTOM_HEADER_MAX_COUNT + 1 }, (_, index) => [
          `x-${index}`,
          'value',
        ]),
      ),
    ]) {
      expect(() =>
        encodeAgentObservabilitySecretBundle({
          ...identity,
          adapterType: 'otlp_http',
          auth: { type: 'custom_headers', headers },
        }),
      ).toThrow(AgentObservabilitySecretBundleError);
    }
  });

  it('normalizes accepted custom headers before a mutation body can be hashed', () => {
    expect(
      normalizeAgentObservabilityOtlpHttpCredentials({
        type: 'custom_headers',
        headers: { 'X-Second': 'two', 'x-First': 'one' },
      }),
    ).toEqual({ type: 'custom_headers', headers: { 'x-first': 'one', 'x-second': 'two' } });
  });

  it('mints opaque references with no credential bytes', () => {
    const first = newAgentObservabilitySecretReference();
    const second = newAgentObservabilitySecretReference();
    expect(first).not.toBe(second);
    expect(isAgentObservabilitySecretReference(first)).toBe(true);
    expect(first).toMatch(/^local:agent_observability\/obssec_/);
    expect(first).not.toContain('basic-password');
    expect(first).not.toContain('bearer-token');
  });
});

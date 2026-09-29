// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it, vi } from 'vitest';
import {
  RegistryObservabilityClient,
  RegistryResolverHttpError,
  RegistryResolverResponseError,
  RegistryResolverScopeError,
} from '../../src/registry-client.js';
import { basicRegistrySecret, enabledRegistryContext } from '../support/registry.js';

const token = 'x'.repeat(32);
const requestScope = { workspaceId: 'ws_registry', sessionId: 'ses_registry' };

describe('RegistryObservabilityClient', () => {
  it('pins only trusted bounded attribution, omitting null or unsafe labels', async () => {
    const response = enabledRegistryContext();
    const config = (response.binding as { config: Record<string, unknown> }).config;
    config.environment = 'production';
    config.release = 'v1.2.3+abc';
    const client = registryClient(vi.fn(async () => jsonResponse(response)));
    await expect(client.resolveContext(requestScope)).resolves.toMatchObject({
      deliveryContext: {
        agentId: 'agt_registry',
        agentVersion: 1,
        harness: 'claude_code',
        harnessMode: 'colocated',
        environment: 'production',
        release: 'v1.2.3+abc',
      },
    });
    for (const unsafe of [
      null,
      '',
      'x'.repeat(129),
      'two words',
      'token=canary',
      'user@example.com',
      '\nprod',
    ]) {
      response.harness = unsafe;
      response.harness_mode = unsafe;
      config.environment = unsafe;
      config.release = unsafe;
      const result = await client.resolveContext(requestScope);
      expect(result.status).toBe('enabled');
      if (result.status !== 'enabled') throw new Error('expected enabled');
      for (const key of ['harness', 'harnessMode', 'environment', 'release']) {
        expect(result.deliveryContext).not.toHaveProperty(key);
      }
    }
    config.environment = 'x'.repeat(128);
    await expect(client.resolveContext(requestScope)).resolves.toMatchObject({
      deliveryContext: { environment: 'x'.repeat(128) },
    });
  });

  it.each([0, -1, 1.5, Number.MAX_SAFE_INTEGER + 1])(
    'rejects invalid agent version %s',
    async (version) => {
      const response = enabledRegistryContext();
      response.agent = { id: 'agt_registry', version };
      await expect(
        registryClient(async () => jsonResponse(response)).resolveContext(requestScope),
      ).rejects.toBeInstanceOf(RegistryResolverResponseError);
    },
  );

  it('omits oversized agent IDs without fabricating another identity', async () => {
    const response = enabledRegistryContext();
    const client = registryClient(async () => jsonResponse(response));
    for (const id of ['agt_' + 'x'.repeat(125)]) {
      response.agent = { id, version: 7 };
      const result = await client.resolveContext(requestScope);
      expect(result.status).toBe('enabled');
      if (result.status !== 'enabled') throw new Error('expected enabled');
      expect(result.deliveryContext).not.toHaveProperty('agentId');
      expect(result.deliveryContext.agentVersion).toBe(7);
      expect(JSON.stringify(result)).not.toContain(id);
    }
  });

  it('accepts raw capture only under a raw pin and every current ceiling', async () => {
    const response = enabledRegistryContext();
    response.capture = {
      pinned_mode: 'raw_io',
      effective_mode: 'raw_io',
      current_ceilings: {
        platform: 'raw_io',
        organization: 'raw_io',
        workspace: 'raw_io',
      },
    };
    const client = registryClient(vi.fn(async () => jsonResponse(response)));
    await expect(client.resolveContext(requestScope)).resolves.toMatchObject({
      deliveryContext: { captureMode: 'raw_io' },
    });
    (response.capture as { pinned_mode: string }).pinned_mode = 'metadata_only';
    await expect(client.resolveContext(requestScope)).rejects.toBeInstanceOf(
      RegistryResolverResponseError,
    );
    (response.capture as { pinned_mode: string }).pinned_mode = 'raw_io';
    (response.capture as { current_ceilings: { workspace: string } }).current_ceilings.workspace =
      'metadata_only';
    await expect(client.resolveContext(requestScope)).rejects.toBeInstanceOf(
      RegistryResolverResponseError,
    );
  });

  it.each(['pinned_mode', 'effective_mode', 'platform', 'organization', 'workspace'])(
    'never treats legacy redacted_io %s authority as raw authorization',
    async (field) => {
      const response = enabledRegistryContext();
      const capture = {
        pinned_mode: 'raw_io',
        effective_mode: 'raw_io',
        current_ceilings: { platform: 'raw_io', organization: 'raw_io', workspace: 'raw_io' },
      };
      if (field === 'pinned_mode' || field === 'effective_mode') capture[field] = 'redacted_io';
      else capture.current_ceilings[field as keyof typeof capture.current_ceilings] = 'redacted_io';
      response.capture = capture;
      if (field === 'effective_mode') {
        await expect(
          registryClient(async () => jsonResponse(response)).resolveContext(requestScope),
        ).resolves.toMatchObject({ deliveryContext: { captureMode: 'redacted_io' } });
        return;
      }
      await expect(
        registryClient(async () => jsonResponse(response)).resolveContext(requestScope),
      ).rejects.toBeInstanceOf(RegistryResolverResponseError);
    },
  );

  it('preserves legacy redacted_io secret authority without upgrading it to raw', async () => {
    const response = basicRegistrySecret();
    response.effective_capture_mode = 'redacted_io';
    await expect(
      registryClient(async () => jsonResponse(response)).resolveSecret(requestScope),
    ).resolves.toMatchObject({ effectiveCaptureMode: 'redacted_io' });
  });

  it('uses canonical Session scope and parses enabled non-secret context', async () => {
    const requestSignal = new AbortController().signal;
    const fetchImpl = vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
      expect(String(input)).toBe(
        'https://registry.internal/internal/v1/workspaces/ws_registry/sessions/ses_registry/agent-observability/context/resolve',
      );
      expect(init?.method).toBe('POST');
      expect(init?.body).toBe('{}');
      const headers = new Headers(init?.headers);
      expect(headers.get('authorization')).toBe(`Bearer ${token}`);
      expect(init?.signal).toBe(requestSignal);
      return jsonResponse(enabledRegistryContext());
    });
    const client = registryClient(fetchImpl);

    await expect(
      client.resolveContext({ ...requestScope, signal: requestSignal }),
    ).resolves.toMatchObject({
      status: 'enabled',
      deliveryContext: {
        organizationId: 'org_registry',
        bindingId: 'aob_registry',
        bindingVersion: 2,
        endpointUrl: 'https://collector.example/api/public/otel/v1/traces',
        captureMode: 'metadata_only',
      },
    });
    expect(fetchImpl).toHaveBeenCalledOnce();
  });

  it('keeps Registry error bodies out of typed errors', async () => {
    const client = registryClient(
      async () => new Response('secret Registry failure body', { status: 503 }),
    );

    const error = await client.resolveContext(requestScope).catch((failure: unknown) => failure);
    expect(error).toBeInstanceOf(RegistryResolverHttpError);
    expect(error).toMatchObject({ resolver: 'context', status: 503 });
    expect((error as Error).message).not.toContain('secret Registry failure body');
  });

  it('maps transport failure to retryable status-only failure', async () => {
    const client = registryClient(async () => {
      throw new TypeError('transport detail must not escape');
    });

    const error = await client.resolveContext(requestScope).catch((failure: unknown) => failure);
    expect(error).toMatchObject({ resolver: 'context', status: 503 });
    expect((error as Error).message).not.toContain('transport detail');
  });

  it('cancels an oversized Registry response without retaining its body', async () => {
    const bodyMarker = 'oversized-registry-secret-marker';
    const oversized = Buffer.concat([Buffer.from(bodyMarker), Buffer.alloc(128 * 1024)]);
    const cancel = vi.fn();
    const body = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(oversized);
      },
      cancel,
    });
    const client = registryClient(
      async () =>
        new Response(body, { status: 200, headers: { 'content-type': 'application/json' } }),
    );

    const error = await client.resolveSecret(requestScope).catch((failure: unknown) => failure);
    expect(error).toBeInstanceOf(RegistryResolverResponseError);
    expect(error).toMatchObject({ resolver: 'secret' });
    expect(cancel).toHaveBeenCalledOnce();
    expect(JSON.stringify(error)).not.toContain(bodyMarker);
    expect((error as Error).message).not.toContain(bodyMarker);
  });

  it('rejects malformed UTF-8 in a Basic credential without exposing credential bytes', async () => {
    const secretMarker = 'malformed-basic-secret-must-not-escape';
    const invalidMarker = '__INVALID_UTF8__';
    const response = basicRegistrySecret();
    const bundle = response.bundle as {
      auth: { password: string };
    };
    bundle.auth.password = `${secretMarker}${invalidMarker}`;
    const encoded = Buffer.from(JSON.stringify(response));
    const markerOffset = encoded.indexOf(invalidMarker);
    if (markerOffset < 0) throw new Error('malformed UTF-8 fixture marker is missing');
    const malformed = Buffer.concat([
      encoded.subarray(0, markerOffset),
      Buffer.from([0xc3, 0x28]),
      encoded.subarray(markerOffset + invalidMarker.length),
    ]);
    const client = registryClient(
      async () =>
        new Response(malformed, {
          status: 200,
          headers: { 'content-type': 'application/json' },
        }),
    );

    const error = await client.resolveSecret(requestScope).catch((failure: unknown) => failure);
    expect(error).toBeInstanceOf(RegistryResolverResponseError);
    expect(error).toMatchObject({ resolver: 'secret' });
    expect(JSON.stringify(error)).not.toContain(secretMarker);
    expect((error as Error).message).not.toContain(secretMarker);
  });

  it('fails closed on response scope and exposes only supported basic material', async () => {
    const responses = [
      jsonResponse({ ...enabledRegistryContext(), workspace_id: 'ws_other' }),
      jsonResponse(basicRegistrySecret()),
      jsonResponse({
        ...basicRegistrySecret(),
        credential_version: 4,
        bundle: {
          adapter_type: 'otlp_http',
          auth: { type: 'bearer', token: 'not-exposed-to-caller' },
        },
      }),
      jsonResponse({
        ...basicRegistrySecret(),
        credential_version: 5,
        bundle: {
          adapter_type: 'langfuse_sdk',
          public_key: 'not-exposed-public-key',
          secret_key: 'not-exposed-secret-key',
        },
      }),
      jsonResponse({
        ...basicRegistrySecret(),
        credential_version: 6,
        bundle: {
          adapter_type: 'otlp_http',
          auth: { type: 'custom_headers', headers: { 'x-tenant-key': 'not-exposed-value' } },
        },
      }),
    ];
    const client = registryClient(async () => responses.shift()!);

    await expect(client.resolveContext(requestScope)).rejects.toBeInstanceOf(
      RegistryResolverResponseError,
    );
    await expect(client.resolveSecret(requestScope)).resolves.toEqual({
      bindingId: 'aob_registry',
      bindingVersion: 2,
      effectiveCaptureMode: 'metadata_only',
      auth: { type: 'basic', username: 'pk-registry', password: 'sk-registry' },
    });
    await expect(client.resolveSecret(requestScope)).resolves.toEqual({
      bindingId: 'aob_registry',
      bindingVersion: 2,
      effectiveCaptureMode: 'metadata_only',
      auth: { type: 'unsupported' },
    });
    await expect(client.resolveSecret(requestScope)).resolves.toEqual({
      bindingId: 'aob_registry',
      bindingVersion: 2,
      effectiveCaptureMode: 'metadata_only',
      auth: { type: 'unsupported' },
    });
    await expect(client.resolveSecret(requestScope)).resolves.toEqual({
      bindingId: 'aob_registry',
      bindingVersion: 2,
      effectiveCaptureMode: 'metadata_only',
      auth: { type: 'unsupported' },
    });
  });

  it('requires exact context schema, scope, and status/reason combinations', async () => {
    const invalidVersion = { ...enabledRegistryContext(), schema_version: 2 };
    const invalidBindingScope = enabledRegistryContext();
    (invalidBindingScope.binding as { scope: string }).scope = 'tenant';
    const invalidEnabledReason = { ...enabledRegistryContext(), reason: 'binding_disabled' };
    const invalidDisabledReason = {
      ...enabledRegistryContext(),
      status: 'disabled',
      reason: 'workspace_disabled',
    };
    const invalidSuppressionReason = {
      ...enabledRegistryContext(),
      status: 'suppressed',
      reason: 'future_reason',
    };
    const validSuppressed = {
      ...enabledRegistryContext(),
      status: 'suppressed',
      reason: 'binding_disabled',
    };
    const responses = [
      invalidVersion,
      invalidBindingScope,
      invalidEnabledReason,
      invalidDisabledReason,
      invalidSuppressionReason,
      validSuppressed,
    ].map(jsonResponse);
    const client = registryClient(async () => responses.shift()!);

    for (let index = 0; index < 5; index += 1) {
      await expect(client.resolveContext(requestScope)).rejects.toBeInstanceOf(
        RegistryResolverResponseError,
      );
    }
    await expect(client.resolveContext(requestScope)).resolves.toEqual({ status: 'suppressed' });
  });

  it('uses current effective capture mode rather than binding requested mode', async () => {
    const effective = enabledRegistryContext();
    (effective.binding as { config: { capture_mode: string } }).config.capture_mode = 'raw_io';
    const client = registryClient(async () => jsonResponse(effective));

    await expect(client.resolveContext(requestScope)).resolves.toMatchObject({
      status: 'enabled',
      deliveryContext: { captureMode: 'metadata_only' },
    });
  });

  it('strictly parses secret authority without retaining rejected secret-shaped fields', async () => {
    const secretMarker = 'must-never-enter-error';
    const invalidResponses = [
      { ...basicRegistrySecret(), schema_version: 2 },
      { ...basicRegistrySecret(), authorization_id: 'invalid' },
      { ...basicRegistrySecret(), binding_version: 0 },
      { ...basicRegistrySecret(), credential_version: 0 },
      { ...basicRegistrySecret(), effective_capture_mode: 'full_content' },
      { ...basicRegistrySecret(), secret_ref: secretMarker },
      {
        ...basicRegistrySecret(),
        bundle: {
          adapter_type: 'otlp_http',
          auth: { type: 'basic', username: 'valid-user', password: '\ud800' },
        },
      },
      {
        ...basicRegistrySecret(),
        bundle: {
          adapter_type: 'otlp_http',
          auth: { type: 'basic', username: 'user:name', password: 'valid-password' },
        },
      },
      {
        ...basicRegistrySecret(),
        bundle: {
          adapter_type: 'otlp_http',
          auth: { type: 'custom_headers', headers: { 'x-api-key': '\udc00' } },
        },
      },
      {
        binding_id: 'aob_registry',
        credential_version: 3,
        credential: {
          adapter_type: 'otlp_http',
          auth: { type: 'basic', username: 'pk-registry', password: secretMarker },
        },
      },
    ];
    const client = registryClient(async () => jsonResponse(invalidResponses.shift()!));

    for (let index = 0; index < 10; index += 1) {
      const error = await client.resolveSecret(requestScope).catch((failure: unknown) => failure);
      expect(error).toBeInstanceOf(RegistryResolverResponseError);
      expect(JSON.stringify(error)).not.toContain(secretMarker);
      expect((error as Error).message).not.toContain(secretMarker);
    }
  });

  it('matches Registry workspace/session selector bounds before any request', async () => {
    expect(
      () =>
        new RegistryObservabilityClient({
          internalBaseUrl: 'https://registry.internal/internal',
          tokenProvider: async () => token,
        }),
    ).toThrow('internal base URL');
    const maxSessionId = `ses_${'s'.repeat(124)}`;
    expect(maxSessionId).toHaveLength(128);
    const fetchImpl = vi.fn(async () =>
      jsonResponse(enabledRegistryContext('ws_registry', maxSessionId)),
    );
    const client = registryClient(fetchImpl);

    await expect(
      client.resolveContext({ workspaceId: 'bad/path', sessionId: 'ses_registry' }),
    ).rejects.toBeInstanceOf(RegistryResolverScopeError);
    await expect(
      client.resolveContext({ workspaceId: 'w'.repeat(129), sessionId: 'ses_registry' }),
    ).rejects.toBeInstanceOf(RegistryResolverScopeError);
    await expect(
      client.resolveContext({ workspaceId: 'ws_registry', sessionId: `${maxSessionId}s` }),
    ).rejects.toBeInstanceOf(RegistryResolverScopeError);
    await expect(
      client.resolveContext({ workspaceId: 'ws_registry', sessionId: maxSessionId }),
    ).resolves.toMatchObject({ status: 'enabled' });
    expect(fetchImpl).toHaveBeenCalledOnce();
  });
});

function registryClient(fetchImpl: typeof fetch): RegistryObservabilityClient {
  return new RegistryObservabilityClient({
    internalBaseUrl: 'https://registry.internal',
    tokenProvider: async () => token,
    fetchImpl,
  });
}

function jsonResponse(value: unknown): Response {
  return new Response(JSON.stringify(value), {
    status: 200,
    headers: { 'content-type': 'application/json' },
  });
}

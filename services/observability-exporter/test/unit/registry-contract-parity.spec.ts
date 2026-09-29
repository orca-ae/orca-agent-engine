// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it, vi } from 'vitest';
import {
  AgentObservabilitySessionContextSuppressionReasonSchema,
  internalContract,
} from '../../../registry-service-ts/src/contracts/internal.contract.js';
import { AGENT_OBSERVABILITY_SECRET_VALUE_MAX_LENGTH } from '../../../registry-service-ts/src/domain/agent-observability-validation.js';
import {
  MAX_REGISTRY_RESPONSE_BYTES,
  RegistryObservabilityClient,
  RegistryResolverResponseError,
} from '../../src/registry-client.js';
import { basicRegistrySecret, enabledRegistryContext } from '../support/registry.js';

const workspaceId = 'ws_registry';
const sessionId = 'ses_registry';
const routePrefix = `https://registry.internal/internal/v1/workspaces/${workspaceId}/sessions/${sessionId}`;

describe('Registry observability resolver parity', () => {
  it('accepts raw_io in the actual Registry schemas and exporter without aliasing redacted_io', async () => {
    const response = enabledRegistryContext();
    (response.binding as { config: { capture_mode: string } }).config.capture_mode = 'raw_io';
    response.capture = {
      pinned_mode: 'raw_io',
      effective_mode: 'raw_io',
      current_ceilings: { platform: 'raw_io', organization: 'raw_io', workspace: 'raw_io' },
    };
    const context =
      internalContract.resolveAgentObservabilityContext.responses[200].parse(response);
    const rawSecret = { ...basicRegistrySecret(), effective_capture_mode: 'raw_io' };
    const secret = internalContract.resolveAgentObservabilitySecret.responses[200].parse(rawSecret);
    const client = new RegistryObservabilityClient({
      internalBaseUrl: 'https://registry.internal',
      tokenProvider: async () => 'x'.repeat(32),
      fetchImpl: async (input) =>
        jsonResponse(String(input).includes('/secret/') ? secret : context),
    });
    await expect(client.resolveContext({ workspaceId, sessionId })).resolves.toMatchObject({
      deliveryContext: { captureMode: 'raw_io' },
    });
    await expect(client.resolveSecret({ workspaceId, sessionId })).resolves.toMatchObject({
      effectiveCaptureMode: 'raw_io',
    });
    const legacy = { ...rawSecret, effective_capture_mode: 'redacted_io' };
    expect(
      internalContract.resolveAgentObservabilitySecret.responses[200].safeParse(legacy).success,
    ).toBe(true);
  });

  it('consumes current internalContract paths and exact response schemas', async () => {
    const contextRoute = internalContract.resolveAgentObservabilityContext;
    const secretRoute = internalContract.resolveAgentObservabilitySecret;
    const context = contextRoute.responses[200].parse(enabledRegistryContext());
    const secret = secretRoute.responses[200].parse(basicRegistrySecret());
    const fetchImpl = vi.fn(async (input: string | URL | Request) => {
      const url = String(input);
      if (url === `${routePrefix}/agent-observability/context/resolve`) {
        return jsonResponse(context);
      }
      if (url === `${routePrefix}/agent-observability/secret/resolve`) {
        return jsonResponse(secret);
      }
      return new Response('', { status: 404 });
    });
    const client = new RegistryObservabilityClient({
      internalBaseUrl: 'https://registry.internal',
      tokenProvider: async () => 'x'.repeat(32),
      fetchImpl,
    });

    expect(contextRoute.path).toBe(
      '/internal/v1/workspaces/:workspaceId/sessions/:sessionId/agent-observability/context/resolve',
    );
    expect(secretRoute.path).toBe(
      '/internal/v1/workspaces/:workspaceId/sessions/:sessionId/agent-observability/secret/resolve',
    );
    await expect(client.resolveContext({ workspaceId, sessionId })).resolves.toMatchObject({
      status: 'enabled',
      deliveryContext: { bindingId: 'aob_registry', bindingVersion: 2 },
    });
    await expect(client.resolveSecret({ workspaceId, sessionId })).resolves.toMatchObject({
      bindingId: 'aob_registry',
      bindingVersion: 2,
      effectiveCaptureMode: 'metadata_only',
      auth: { type: 'basic' },
    });
    expect(fetchImpl).toHaveBeenCalledTimes(2);
  });

  it('keeps obsolete routes and pre-merge envelopes outside both contracts', async () => {
    const obsoleteContextPath =
      '/internal/v1/workspaces/:workspaceId/sessions/:sessionId/observability-context/resolve';
    const obsoleteSecretPath =
      '/internal/v1/workspaces/:workspaceId/sessions/:sessionId/observability-secret/resolve';
    const paths = Object.values(internalContract).map((route) => route.path);
    const obsoleteContext = {
      schema_version: 1,
      status: 'enabled',
      reason: null,
      organization_id: 'org_registry',
      workspace_id: workspaceId,
      session_id: sessionId,
      capture: { effective_mode: 'metadata_only' },
      binding: { id: 'aob_registry', version: 2 },
    };
    const obsoleteSecret = {
      binding_id: 'aob_registry',
      credential_version: 3,
      credential: {
        adapter_type: 'otlp_http',
        auth: { type: 'basic', username: 'pk-registry', password: 'obsolete-secret-marker' },
      },
    };

    expect(paths).not.toContain(obsoleteContextPath);
    expect(paths).not.toContain(obsoleteSecretPath);
    expect(
      internalContract.resolveAgentObservabilityContext.responses[200].safeParse(obsoleteContext)
        .success,
    ).toBe(false);
    expect(
      internalContract.resolveAgentObservabilitySecret.responses[200].safeParse(obsoleteSecret)
        .success,
    ).toBe(false);

    const responses = [jsonResponse(obsoleteContext), jsonResponse(obsoleteSecret)];
    const client = new RegistryObservabilityClient({
      internalBaseUrl: 'https://registry.internal',
      tokenProvider: async () => 'x'.repeat(32),
      fetchImpl: async () => responses.shift()!,
    });
    await expect(client.resolveContext({ workspaceId, sessionId })).rejects.toBeInstanceOf(
      RegistryResolverResponseError,
    );
    const error = await client
      .resolveSecret({ workspaceId, sessionId })
      .catch((failure: unknown) => failure);
    expect(error).toBeInstanceOf(RegistryResolverResponseError);
    expect(JSON.stringify(error)).not.toContain('obsolete-secret-marker');
  });

  it('tracks every Registry status/reason and known provider bundle variant', async () => {
    const contextSchema = internalContract.resolveAgentObservabilityContext.responses[200];
    const secretSchema = internalContract.resolveAgentObservabilitySecret.responses[200];
    const contexts = AgentObservabilitySessionContextSuppressionReasonSchema.options.map((reason) =>
      contextSchema.parse({
        ...enabledRegistryContext(),
        status: 'suppressed',
        reason,
      }),
    );
    contexts.push(
      contextSchema.parse({
        ...enabledRegistryContext(),
        status: 'disabled',
        reason: 'session_pin_disabled',
      }),
    );
    const secrets = [
      {
        ...basicRegistrySecret(),
        bundle: {
          adapter_type: 'otlp_http',
          auth: { type: 'bearer', token: 'unsupported-bearer-secret' },
        },
      },
      {
        ...basicRegistrySecret(),
        bundle: {
          adapter_type: 'otlp_http',
          auth: { type: 'custom_headers', headers: { 'x-api-key': 'unsupported-header-secret' } },
        },
      },
      {
        ...basicRegistrySecret(),
        bundle: {
          adapter_type: 'langfuse_sdk',
          public_key: 'unsupported-public-key',
          secret_key: 'unsupported-sdk-secret',
        },
      },
    ].map((secret) => secretSchema.parse(secret));
    const responses = [...contexts, ...secrets].map(jsonResponse);
    const client = new RegistryObservabilityClient({
      internalBaseUrl: 'https://registry.internal',
      tokenProvider: async () => 'x'.repeat(32),
      fetchImpl: async () => responses.shift()!,
    });

    for (const context of contexts) {
      await expect(client.resolveContext({ workspaceId, sessionId })).resolves.toEqual({
        status: context.status,
      });
    }
    for (const _secret of secrets) {
      const parsed = await client.resolveSecret({ workspaceId, sessionId });
      expect(parsed.auth).toEqual({ type: 'unsupported' });
      expect(JSON.stringify(parsed)).not.toContain('unsupported-');
    }
  });

  it('fits the maximum legal three-byte-BMP Basic response inside the client cap', async () => {
    const maximumSecret = '\u0800'.repeat(AGENT_OBSERVABILITY_SECRET_VALUE_MAX_LENGTH);
    const response = basicRegistrySecret();
    response.bundle = {
      adapter_type: 'otlp_http',
      auth: { type: 'basic', username: maximumSecret, password: maximumSecret },
    };
    const parsed = internalContract.resolveAgentObservabilitySecret.responses[200].parse(response);
    const serialized = JSON.stringify(parsed);
    const serializedBytes = Buffer.byteLength(serialized, 'utf8');

    expect(serializedBytes).toBeGreaterThan(AGENT_OBSERVABILITY_SECRET_VALUE_MAX_LENGTH * 2 * 3);
    expect(serializedBytes).toBeLessThanOrEqual(MAX_REGISTRY_RESPONSE_BYTES);

    const client = new RegistryObservabilityClient({
      internalBaseUrl: 'https://registry.internal',
      tokenProvider: async () => 'x'.repeat(32),
      fetchImpl: async () => jsonResponse(parsed),
    });
    const resolved = await client.resolveSecret({ workspaceId, sessionId });
    expect(resolved.auth.type).toBe('basic');
    if (resolved.auth.type !== 'basic') throw new Error('expected Basic Registry credential');
    expect(resolved.auth.username.length).toBe(AGENT_OBSERVABILITY_SECRET_VALUE_MAX_LENGTH);
    expect(resolved.auth.password.length).toBe(AGENT_OBSERVABILITY_SECRET_VALUE_MAX_LENGTH);
  });
});

function jsonResponse(value: unknown): Response {
  return new Response(JSON.stringify(value), {
    status: 200,
    headers: { 'content-type': 'application/json' },
  });
}

// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { vaultsContract } from '../../src/contracts/vaults.contract.js';

const createBody = vaultsContract.createCredential.body;
const validateContract = vaultsContract.mcpOauthValidateCredential;
const validationResponse = validateContract.responses[200];

describe('VaultCredential provider variant', () => {
  it.each([
    ['anthropic', 'api_key'],
    ['openai', 'bearer'],
    ['openai_compatible', 'bearer'],
    ['azure_openai', 'api_key'],
    ['azure_openai', 'bearer'],
    ['vertex', 'gcp-service-account'],
    ['bedrock', 'aws-sig-v4'],
  ])('parses %s credentials using %s', (provider, scheme) => {
    expect(
      createBody.safeParse({
        auth: {
          type: 'provider',
          provider,
          scheme,
          logical_id: `llm:${provider}`,
          secret_value: 'write-only-secret',
        },
      }).success,
    ).toBe(true);
  });

  it('rejects an incompatible scheme and unsafe logical IDs', () => {
    expect(
      createBody.safeParse({
        auth: {
          type: 'provider',
          provider: 'anthropic',
          scheme: 'bearer',
          logical_id: 'llm:anthropic',
          secret_value: 'secret',
        },
      }).success,
    ).toBe(false);

    for (const logicalId of [
      '',
      'vcrd_concrete_id',
      'llm/anthropic',
      'llm%2Fanthropic',
      'llm anthropic',
      'llm:anthropic\n',
    ]) {
      expect(
        createBody.safeParse({
          auth: {
            type: 'provider',
            provider: 'anthropic',
            scheme: 'api_key',
            logical_id: logicalId,
            secret_value: 'secret',
          },
        }).success,
        logicalId,
      ).toBe(false);
    }
  });
});

describe('VaultCredentialCreate environment_variable variant', () => {
  it('parses an env-var credential with limited networking', () => {
    const parsed = createBody.safeParse({
      auth: {
        type: 'environment_variable',
        secret_name: 'OPENAI_API_KEY',
        secret_value: 'sk-secret',
        networking: { type: 'limited', allowed_hosts: ['api.openai.com'] },
      },
    });
    expect(parsed.success).toBe(true);
  });

  it('parses an env-var credential with unrestricted networking', () => {
    const parsed = createBody.safeParse({
      auth: {
        type: 'environment_variable',
        secret_name: 'TOKEN',
        secret_value: 'v',
        networking: { type: 'unrestricted' },
      },
    });
    expect(parsed.success).toBe(true);
  });

  it('accepts an empty limited host set and caps the list at 16 entries', () => {
    const parsed = createBody.safeParse({
      auth: {
        type: 'environment_variable',
        secret_name: 'TOKEN',
        secret_value: 'v',
        networking: { type: 'limited', allowed_hosts: [] },
      },
    });
    expect(parsed.success).toBe(true);
    expect(
      createBody.safeParse({
        auth: {
          type: 'environment_variable',
          secret_name: 'TOKEN',
          secret_value: 'v',
          networking: {
            type: 'limited',
            allowed_hosts: Array.from({ length: 17 }, (_, index) => `${index}.example.com`),
          },
        },
      }).success,
    ).toBe(false);
  });

  it('rejects an env-var credential missing secret_value', () => {
    const parsed = createBody.safeParse({
      auth: {
        type: 'environment_variable',
        secret_name: 'TOKEN',
        networking: { type: 'unrestricted' },
      },
    });
    expect(parsed.success).toBe(false);
  });

  it('rejects an unknown networking type', () => {
    const parsed = createBody.safeParse({
      auth: {
        type: 'environment_variable',
        secret_name: 'TOKEN',
        secret_value: 'v',
        networking: { type: 'open' },
      },
    });
    expect(parsed.success).toBe(false);
  });

  it('parses the official injection-location and OAuth refresh fields', () => {
    expect(
      createBody.safeParse({
        auth: {
          type: 'environment_variable',
          secret_name: 'TOKEN',
          secret_value: 'v',
          networking: { type: 'unrestricted' },
          injection_location: { header: true, body: false },
        },
      }).success,
    ).toBe(true);
    expect(
      createBody.safeParse({
        auth: {
          type: 'mcp_oauth',
          access_token: 'access',
          mcp_server_url: 'https://mcp.example.com',
          expires_at: '2099-12-31T23:59:59Z',
          refresh: {
            refresh_token: 'refresh',
            token_endpoint: 'https://auth.example.com/token',
            client_id: 'client',
            token_endpoint_auth: {
              type: 'client_secret_post',
              client_secret: 'secret',
            },
            resource: 'https://mcp.example.com',
            scope: 'read write',
          },
        },
      }).success,
    ).toBe(true);
  });

  it('rejects an environment-variable credential that enables no injection location', () => {
    expect(
      createBody.safeParse({
        auth: {
          type: 'environment_variable',
          secret_name: 'TOKEN',
          secret_value: 'v',
          networking: { type: 'unrestricted' },
          injection_location: { header: false, body: false },
        },
      }).success,
    ).toBe(false);
  });
});

describe('mcp_oauth_validate contract', () => {
  // Documented example from the Anthropic vaults beta doc (managed-agents-2026-04-01),
  // with the doc's `...` id placeholders expanded to charset-valid identifiers.
  const documentedInvalidExample = {
    type: 'vault_credential_validation',
    credential_id: 'vcrd_01ABC123',
    vault_id: 'vlt_01XYZ456',
    validated_at: '2026-04-29T17:12:00Z',
    has_refresh_token: false,
    status: 'invalid',
    mcp_probe: {
      method: 'initialize',
      http_response: {
        status_code: 401,
        content_type: 'application/json',
        body: '{"error":"invalid_token"}',
        body_truncated: false,
      },
    },
    refresh: { status: 'no_refresh_token', http_response: null },
  };

  it('parses the documented invalid-credential example', () => {
    const parsed = validationResponse.safeParse(documentedInvalidExample);
    expect(parsed.success).toBe(true);
  });

  it('parses a valid-status variant with a redacted refresh response', () => {
    const parsed = validationResponse.safeParse({
      ...documentedInvalidExample,
      has_refresh_token: true,
      status: 'valid',
      mcp_probe: {
        method: 'initialize',
        http_response: {
          status_code: 200,
          content_type: 'application/json',
          body: '{"jsonrpc":"2.0","id":1,"result":{}}',
          body_truncated: false,
        },
      },
      refresh: {
        status: 'succeeded',
        http_response: {
          status_code: 200,
          content_type: 'application/json',
          body: '[redacted]',
          body_truncated: false,
        },
      },
    });
    expect(parsed.success).toBe(true);
  });

  it('parses an unknown-status variant with a null probe response', () => {
    const parsed = validationResponse.safeParse({
      ...documentedInvalidExample,
      status: 'unknown',
      mcp_probe: { method: 'initialize', http_response: null },
    });
    expect(parsed.success).toBe(true);
  });

  it('requires content_type to be a string', () => {
    const parsed = validationResponse.safeParse({
      ...documentedInvalidExample,
      has_refresh_token: true,
      status: 'unknown',
      mcp_probe: { method: 'initialize', http_response: null },
      refresh: {
        status: 'connect_error',
        http_response: { status_code: 503, content_type: null, body: '', body_truncated: false },
      },
    });
    expect(parsed.success).toBe(false);
  });

  it('rejects the retired refresh-status values (success/unknown)', () => {
    expect(
      validationResponse.safeParse({
        ...documentedInvalidExample,
        refresh: { status: 'success', http_response: null },
      }).success,
    ).toBe(false);
    expect(
      validationResponse.safeParse({
        ...documentedInvalidExample,
        refresh: { status: 'unknown', http_response: null },
      }).success,
    ).toBe(false);
  });

  it('rejects an out-of-range top-level status', () => {
    const parsed = validationResponse.safeParse({
      ...documentedInvalidExample,
      status: 'broken',
    });
    expect(parsed.success).toBe(false);
  });

  it('accepts an empty request body and rejects extra keys', () => {
    expect(validateContract.body.safeParse({}).success).toBe(true);
    expect(validateContract.body.safeParse({ extra: true }).success).toBe(false);
  });
});

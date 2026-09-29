// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect } from 'vitest';
import { buildLlmGatewayEnv } from '../../src/harness/in-sandbox/llm-env.js';

describe('buildLlmGatewayEnv', () => {
  it('always includes LITELLM_API_BASE and LITELLM_API_KEY', () => {
    const env = buildLlmGatewayEnv({
      baseUrl: 'http://gw.local/v1/llm',
      token: 'test-jwt-token',
      sessionId: 'ses_test',
    });
    expect(env['LITELLM_API_BASE']).toBe('http://gw.local/v1/llm');
    expect(env['LITELLM_API_KEY']).toBe('test-jwt-token');
    expect(env['ANTHROPIC_CUSTOM_HEADERS']).toBe('X-Orca-Session-Id: ses_test');
    expect(env['LITELLM_DEFAULT_MODEL']).toBeUndefined();
  });

  it('includes LITELLM_DEFAULT_MODEL only when model is provided', () => {
    const env = buildLlmGatewayEnv({
      baseUrl: 'http://gw.local/v1/llm',
      token: 'test-jwt-token',
      sessionId: 'ses_test',
      model: 'claude-3-5-sonnet-20241022',
    });
    expect(env['LITELLM_API_BASE']).toBe('http://gw.local/v1/llm');
    expect(env['LITELLM_API_KEY']).toBe('test-jwt-token');
    expect(env['ANTHROPIC_CUSTOM_HEADERS']).toBe('X-Orca-Session-Id: ses_test');
    expect(env['LITELLM_DEFAULT_MODEL']).toBe('claude-3-5-sonnet-20241022');
  });

  it('does not include LITELLM_DEFAULT_MODEL when model is empty string', () => {
    const env = buildLlmGatewayEnv({
      baseUrl: 'http://gw.local/v1/llm',
      token: 'test-jwt-token',
      sessionId: 'ses_test',
      model: '',
    });
    expect(env['LITELLM_DEFAULT_MODEL']).toBeUndefined();
  });

  it('returns exactly three keys when model is absent', () => {
    const env = buildLlmGatewayEnv({
      baseUrl: 'http://gw.local/v1/llm',
      token: 'tok',
      sessionId: 'ses_test',
    });
    expect(Object.keys(env)).toHaveLength(3);
  });

  it('returns exactly four keys when model is present', () => {
    const env = buildLlmGatewayEnv({
      baseUrl: 'http://gw.local/v1/llm',
      token: 'tok',
      sessionId: 'ses_test',
      model: 'claude-3-opus-20240229',
    });
    expect(Object.keys(env)).toHaveLength(4);
  });
});

// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/** Env the in-sandbox harness's provider reads to route the model through the
 *  gateway: LITELLM_API_BASE (gateway LLM URL) + LITELLM_API_KEY (session JWT),
 *  plus the session identity required by managed-agent LLM requests.
 *  No provider keys ever enter the sandbox. */
export interface LlmGatewayEnvInput {
  baseUrl: string;
  token: string;
  sessionId: string;
  model?: string;
}

export function buildLlmGatewayEnv(input: LlmGatewayEnvInput): Record<string, string> {
  const env: Record<string, string> = {
    LITELLM_API_BASE: input.baseUrl,
    LITELLM_API_KEY: input.token,
    ANTHROPIC_CUSTOM_HEADERS: `X-Orca-Session-Id: ${input.sessionId}`,
  };
  if (input.model) env.LITELLM_DEFAULT_MODEL = input.model;
  return env;
}

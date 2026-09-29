// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import {
  RUNNER_PARENT_PID_ENV_VAR,
  RUNNER_REGISTRY_URL_ENV_VAR,
  RUNNER_TUNNEL_BINDING_TOKEN_ENV_VAR,
  RUNNER_WORKSPACE_ENV_VAR,
} from '@orca/harness-tunnel';
import {
  DEFAULT_RUNNER_IDLE_TIMEOUT_S,
  DEFAULT_RUNNER_MODEL,
  RUNNER_ALLOWED_MODELS_ENV_VAR,
  RUNNER_ANTHROPIC_API_KEY_ENV_VAR,
  RUNNER_IDLE_TIMEOUT_S_ENV_VAR,
  RUNNER_MODEL_DEFAULT_ENV_VAR,
  RUNNER_SESSION_ID_ENV_VAR,
  RUNNER_WORKSPACE_ID_ENV_VAR,
  loadConfig,
} from '../../src/config.js';

function baseEnv(): NodeJS.ProcessEnv {
  return {
    [RUNNER_TUNNEL_BINDING_TOKEN_ENV_VAR]: 'binding-token-fixture',
    [RUNNER_REGISTRY_URL_ENV_VAR]: 'ws://registry.internal:8081/runner',
    [RUNNER_WORKSPACE_ENV_VAR]: '/var/run/orca/ws-abc',
  };
}

describe('loadConfig', () => {
  it('reads the required runner-wiring vars', () => {
    const config = loadConfig(baseEnv());
    expect(config.bindingToken).toBe('binding-token-fixture');
    expect(config.registryRunnerUrl).toBe('ws://registry.internal:8081/runner');
    expect(config.workspace).toBe('/var/run/orca/ws-abc');
    expect(config.sessionId).toBeUndefined();
    expect(config.parentPid).toBeUndefined();
    // The owner pod stamps the workspace scope on persist, so the runner defaults
    // the workspace id to empty when the worker did not seed it.
    expect(config.workspaceId).toBe('');
  });

  it('reads the optional session id when pinned', () => {
    const config = loadConfig({ ...baseEnv(), [RUNNER_SESSION_ID_ENV_VAR]: 'ses_xyz' });
    expect(config.sessionId).toBe('ses_xyz');
  });

  it('reads the optional workspace id when pinned', () => {
    const config = loadConfig({ ...baseEnv(), [RUNNER_WORKSPACE_ID_ENV_VAR]: 'ws_abc' });
    expect(config.workspaceId).toBe('ws_abc');
  });

  it('parses the optional parent pid', () => {
    const config = loadConfig({ ...baseEnv(), [RUNNER_PARENT_PID_ENV_VAR]: '4242' });
    expect(config.parentPid).toBe(4242);
  });

  it.each([
    RUNNER_TUNNEL_BINDING_TOKEN_ENV_VAR,
    RUNNER_REGISTRY_URL_ENV_VAR,
    RUNNER_WORKSPACE_ENV_VAR,
  ])('throws when %s is missing', (missing) => {
    const env = baseEnv();
    delete env[missing];
    expect(() => loadConfig(env)).toThrow(new RegExp(`${missing} is required`));
  });

  it('throws when a required var is blank', () => {
    expect(() => loadConfig({ ...baseEnv(), [RUNNER_WORKSPACE_ENV_VAR]: '   ' })).toThrow(
      /is required/,
    );
  });

  it('rejects a non-positive parent pid', () => {
    expect(() => loadConfig({ ...baseEnv(), [RUNNER_PARENT_PID_ENV_VAR]: '0' })).toThrow(
      /positive integer/,
    );
  });

  it('defaults the idle timeout to 1 hour', () => {
    expect(loadConfig(baseEnv()).idleTimeoutS).toBe(DEFAULT_RUNNER_IDLE_TIMEOUT_S);
  });

  it('reads an explicit idle timeout', () => {
    expect(loadConfig({ ...baseEnv(), [RUNNER_IDLE_TIMEOUT_S_ENV_VAR]: '120' }).idleTimeoutS).toBe(
      120,
    );
  });

  it('allows 0 to disable the idle watchdog', () => {
    expect(loadConfig({ ...baseEnv(), [RUNNER_IDLE_TIMEOUT_S_ENV_VAR]: '0' }).idleTimeoutS).toBe(0);
  });

  it('rejects a negative / non-numeric idle timeout (fails loud, no silent default)', () => {
    expect(() => loadConfig({ ...baseEnv(), [RUNNER_IDLE_TIMEOUT_S_ENV_VAR]: '-1' })).toThrow(
      /non-negative number/,
    );
    expect(() => loadConfig({ ...baseEnv(), [RUNNER_IDLE_TIMEOUT_S_ENV_VAR]: 'soon' })).toThrow(
      /non-negative number/,
    );
  });

  it('defaults the claude provider wiring (model) so it is always registrable WITHOUT any Kafka env', () => {
    // The self-hosted runner has no transcript backend: it builds + boots with NO
    // KAFKA_* env. The provider config carries only the model default + optional LLM
    // knobs — no broker fields at all.
    const { provider } = loadConfig(baseEnv());
    expect(provider.modelDefault).toBe(DEFAULT_RUNNER_MODEL);
    expect(provider.fallbackApiKey).toBeUndefined();
    // No allow-list by default: a well-formed per-turn override is admitted on format
    // alone (the gateway is the model-policy enforcement point).
    expect(provider.allowedModels).toBeUndefined();
    // The provider config no longer carries any transcript-backend wiring.
    expect(provider).not.toHaveProperty('kafkaBrokers');
    expect(provider).not.toHaveProperty('kafkaClientId');
    expect(provider).not.toHaveProperty('kafkaTopicPrefix');
  });

  it('ignores KAFKA_* env entirely (no transcript backend on a self-hosted runner)', () => {
    // Even if a co-located KAFKA_BROKERS leaks into the env, the runner config does not
    // pick it up — the in-memory tunnel-fed store is the history substrate.
    const { provider } = loadConfig({
      ...baseEnv(),
      KAFKA_BROKERS: 'broker:9092',
      KAFKA_CLIENT_ID: 'leaked',
      KAFKA_TOPIC_PREFIX: 'leaked.',
    });
    expect(provider).not.toHaveProperty('kafkaBrokers');
    expect(provider.modelDefault).toBe(DEFAULT_RUNNER_MODEL);
  });

  it('leaves the per-turn model allow-list unset when the var is blank', () => {
    const { provider } = loadConfig({ ...baseEnv(), [RUNNER_ALLOWED_MODELS_ENV_VAR]: '  ,  ' });
    expect(provider.allowedModels).toBeUndefined();
  });

  it('parses the per-turn model allow-list (trimmed, de-duplicated, blanks dropped, order preserved)', () => {
    const { provider } = loadConfig({
      ...baseEnv(),
      [RUNNER_ALLOWED_MODELS_ENV_VAR]: 'claude-opus-4 , claude-sonnet-4 ,, claude-opus-4 ',
    });
    expect(provider.allowedModels).toEqual(['claude-opus-4', 'claude-sonnet-4']);
  });

  it('reads explicit provider wiring (model default + fallback LLM key)', () => {
    const { provider } = loadConfig({
      ...baseEnv(),
      [RUNNER_MODEL_DEFAULT_ENV_VAR]: 'claude-opus-4',
      [RUNNER_ANTHROPIC_API_KEY_ENV_VAR]: 'sk-fallback',
    });
    expect(provider.modelDefault).toBe('claude-opus-4');
    expect(provider.fallbackApiKey).toBe('sk-fallback');
  });
});

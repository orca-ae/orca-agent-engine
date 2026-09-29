// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { hostname } from 'node:os';
import {
  ENVIRONMENT_ID_ENV_VAR,
  ENVIRONMENT_KEY_ENV_VAR,
  ENVIRONMENT_WORKER_ID_ENV_VAR,
  ENVIRONMENT_WORKER_NAME_ENV_VAR,
  ORCA_ENVIRONMENT_TOKEN_ENV_VAR,
  REGISTRY_RUNNER_URL_ENV_VAR,
  REGISTRY_TUNNEL_BASE_URL_ENV_VAR,
  RUNNER_LAUNCH_COMMAND_ENV_VAR,
  WORKSPACE_DIR_ENV_VAR,
  loadConfig,
} from '../../src/config.js';

function fullEnv(): NodeJS.ProcessEnv {
  return {
    [ENVIRONMENT_ID_ENV_VAR]: 'env-123',
    [ENVIRONMENT_KEY_ENV_VAR]: 'secret-key',
    [REGISTRY_TUNNEL_BASE_URL_ENV_VAR]: 'wss://registry.example.com',
    [WORKSPACE_DIR_ENV_VAR]: '/var/lib/orca/workspaces',
    [RUNNER_LAUNCH_COMMAND_ENV_VAR]: 'node /opt/runner/main.js',
  };
}

describe('loadConfig', () => {
  it('parses a complete environment', () => {
    const config = loadConfig(fullEnv());
    expect(config.environmentId).toBe('env-123');
    expect(config.environmentKey).toBe('secret-key');
    expect(config.registryTunnelBaseUrl).toBe('wss://registry.example.com');
    expect(config.workspaceDir).toBe('/var/lib/orca/workspaces');
    expect(config.runnerLaunchCommand).toEqual(['node', '/opt/runner/main.js']);
    // Self-hosted (existing) path: no managed token / worker id are set.
    expect(config.environmentToken).toBeUndefined();
    expect(config.workerId).toBeUndefined();
  });

  it('defaults the registry runner URL to the tunnel base and name to the hostname', () => {
    const config = loadConfig(fullEnv());
    // One registry base serves both tunnels unless an explicit runner URL is set.
    expect(config.registryRunnerUrl).toBe('wss://registry.example.com');
    expect(config.name).toBe(hostname());
  });

  it('honors an explicit registry runner URL and worker name', () => {
    const env = fullEnv();
    env[REGISTRY_RUNNER_URL_ENV_VAR] = 'wss://runners.example.com';
    env[ENVIRONMENT_WORKER_NAME_ENV_VAR] = 'edge-box-7';
    const config = loadConfig(env);
    expect(config.registryRunnerUrl).toBe('wss://runners.example.com');
    expect(config.name).toBe('edge-box-7');
  });

  it('throws when a required variable is missing', () => {
    const env = fullEnv();
    delete env[ENVIRONMENT_ID_ENV_VAR];
    expect(() => loadConfig(env)).toThrow(/ENVIRONMENT_ID/);
  });

  it('throws when a required variable is blank', () => {
    const env = fullEnv();
    env[REGISTRY_TUNNEL_BASE_URL_ENV_VAR] = '   ';
    expect(() => loadConfig(env)).toThrow(/REGISTRY_TUNNEL_BASE_URL/);
  });

  it('throws when the runner launch command is empty', () => {
    const env = fullEnv();
    env[RUNNER_LAUNCH_COMMAND_ENV_VAR] = '   ';
    expect(() => loadConfig(env)).toThrow(/RUNNER_LAUNCH_COMMAND/);
  });
});

// ── Managed-auth fork: ORCA_ENVIRONMENT_TOKEN ───────────────
//
// A registry-launched worker (a server-managed sandbox with no operator to
// provision an Env Key ahead of time) is instead injected with a per-launch
// Environment Token plus its server-assigned identity. When that token is
// set, the Env Key is no longer required and the worker id/name are read
// from the injected identity env vars (`ENVIRONMENT_WORKER_ID` /
// `ENVIRONMENT_WORKER_NAME` — the exact vars `local-environment-launcher.ts`
// already forwards for `WorkerIdentity`) rather than falling back to the OS
// hostname. Absent the token, every existing (self-hosted) behavior is
// unchanged — proven above.

function managedEnv(): NodeJS.ProcessEnv {
  const env = fullEnv();
  delete env[ENVIRONMENT_KEY_ENV_VAR];
  env[ORCA_ENVIRONMENT_TOKEN_ENV_VAR] = 'et-managed-launch-token';
  env[ENVIRONMENT_WORKER_ID_ENV_VAR] = 'worker-abc123';
  env[ENVIRONMENT_WORKER_NAME_ENV_VAR] = 'sandbox-abc123';
  return env;
}

describe('loadConfig — managed auth (ORCA_ENVIRONMENT_TOKEN)', () => {
  it('parses a managed environment with no Env Key set', () => {
    const config = loadConfig(managedEnv());
    expect(config.environmentToken).toBe('et-managed-launch-token');
    expect(config.environmentKey).toBeUndefined();
    expect(config.workerId).toBe('worker-abc123');
    // Injected identity, NOT the OS hostname default.
    expect(config.name).toBe('sandbox-abc123');
    expect(config.name).not.toBe(hostname());
  });

  it('tolerates an Env Key also being set (the token still takes the managed path)', () => {
    const env = managedEnv();
    env[ENVIRONMENT_KEY_ENV_VAR] = 'secret-key-too';
    const config = loadConfig(env);
    expect(config.environmentToken).toBe('et-managed-launch-token');
    expect(config.environmentKey).toBe('secret-key-too');
  });

  it('throws when managed but ENVIRONMENT_WORKER_ID is missing (injected identity is required)', () => {
    const env = managedEnv();
    delete env[ENVIRONMENT_WORKER_ID_ENV_VAR];
    expect(() => loadConfig(env)).toThrow(/ENVIRONMENT_WORKER_ID/);
  });

  it('throws when managed but ENVIRONMENT_WORKER_NAME is missing (no hostname fallback)', () => {
    const env = managedEnv();
    delete env[ENVIRONMENT_WORKER_NAME_ENV_VAR];
    expect(() => loadConfig(env)).toThrow(/ENVIRONMENT_WORKER_NAME/);
  });

  it('does not require ENVIRONMENT_KEY when managed', () => {
    const env = managedEnv();
    expect(env[ENVIRONMENT_KEY_ENV_VAR]).toBeUndefined();
    expect(() => loadConfig(env)).not.toThrow();
  });
});

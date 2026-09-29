// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Runner-env construction cases.
//
// A spawned session runner must inherit only the allowlisted subset of the
// worker's environment (process essentials + locale + TLS trust stores), never
// the operator's unrelated secrets, plus the runner-wiring vars the runner reads
// to dial back the registry runner tunnel and bind its identity. The runner's
// own tunnel binding token is a control-plane secret seeded ONLY through the
// wiring var; an inherited copy under any other name must be stripped.

import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';
import {
  RUNNER_PARENT_PID_ENV_VAR,
  RUNNER_REGISTRY_URL_ENV_VAR as HOISTED_RUNNER_REGISTRY_URL_ENV_VAR,
  RUNNER_TUNNEL_BINDING_TOKEN_ENV_VAR,
  RUNNER_WORKSPACE_ENV_VAR,
} from '@orca/harness-tunnel';
import {
  RUNNER_ENV_PASSTHROUGH_ENV_VAR,
  RUNNER_REGISTRY_URL_ENV_VAR,
  buildRunnerEnv,
} from '../../src/runner-env.js';

describe('buildRunnerEnv', () => {
  it('allowlists worker env, strips non-essential secrets, and layers runner wiring', () => {
    const base: Record<string, string> = {
      PATH: '/usr/bin:/bin',
      HOME: '/home/alice',
      LANG: 'en_US.UTF-8',
      LC_CTYPE: 'UTF-8',
      SSL_CERT_FILE: '/etc/ssl/cert.pem',
      AWS_SECRET_ACCESS_KEY: 'aws-secret',
      SOME_RANDOM_VAR: 'x',
    };

    const env = buildRunnerEnv(base, {
      registryRunnerUrl: 'wss://registry.example.com',
      runnerId: 'runner_token_abc',
      bindingToken: 'tok',
      workspace: '/ws',
      parentPid: 42,
    });

    // Process essentials + locale + TLS trust stores pass through.
    expect(env.PATH).toBe('/usr/bin:/bin');
    expect(env.HOME).toBe('/home/alice');
    expect(env.LANG).toBe('en_US.UTF-8');
    expect(env.LC_CTYPE).toBe('UTF-8');
    expect(env.SSL_CERT_FILE).toBe('/etc/ssl/cert.pem');
    // Non-allowlisted vars are dropped (allowlist, not denylist) — the point.
    expect(env.AWS_SECRET_ACCESS_KEY).toBeUndefined();
    expect(env.SOME_RANDOM_VAR).toBeUndefined();
    // Runner wiring is layered on.
    expect(env[RUNNER_REGISTRY_URL_ENV_VAR]).toBe('wss://registry.example.com');
    expect(env[RUNNER_TUNNEL_BINDING_TOKEN_ENV_VAR]).toBe('tok');
    expect(env[RUNNER_WORKSPACE_ENV_VAR]).toBe('/ws');
    expect(env[RUNNER_PARENT_PID_ENV_VAR]).toBe('42');
  });

  it('forwards operator-named passthrough extras without opening the allowlist', () => {
    const base: Record<string, string> = {
      PATH: '/usr/bin',
      HOME: '/root',
      [RUNNER_ENV_PASSTHROUGH_ENV_VAR]: 'MY_GATEWAY_TOKEN, MY_GATEWAY_URL',
      MY_GATEWAY_TOKEN: 'tok-123',
      MY_GATEWAY_URL: 'https://llm.internal.example.com',
      UNLISTED_SECRET: 'nope',
    };

    const env = buildRunnerEnv(base, {
      registryRunnerUrl: 'wss://registry',
      runnerId: 'runner_token_abc',
      bindingToken: 'tok',
      workspace: '/ws',
      parentPid: 42,
    });

    // Named extras forward (whitespace around commas tolerated).
    expect(env.MY_GATEWAY_TOKEN).toBe('tok-123');
    expect(env.MY_GATEWAY_URL).toBe('https://llm.internal.example.com');
    // Anything unnamed stays behind the allowlist.
    expect(env.UNLISTED_SECRET).toBeUndefined();
  });

  it('strips an inherited binding-token secret carried under its own env name', () => {
    // The worker process holds the runner's binding token only transiently per
    // launch; it must never leak via the base environment. An inherited copy
    // under ORCA_RUNNER_TUNNEL_BINDING_TOKEN is stripped, then re-seeded with
    // THIS launch's token — not whatever leaked in.
    const base: Record<string, string> = {
      PATH: '/usr/bin',
      [RUNNER_TUNNEL_BINDING_TOKEN_ENV_VAR]: 'leaked-stale-token',
    };

    const env = buildRunnerEnv(base, {
      registryRunnerUrl: 'wss://registry',
      runnerId: 'runner_token_abc',
      bindingToken: 'fresh-token',
      workspace: '/ws',
      parentPid: 7,
    });

    expect(env[RUNNER_TUNNEL_BINDING_TOKEN_ENV_VAR]).toBe('fresh-token');
  });

  it('does not invent unset wiring inputs and copies the source env (no mutation)', () => {
    const base: Record<string, string> = { PATH: '/usr/bin' };
    const env = buildRunnerEnv(base, {
      registryRunnerUrl: 'wss://registry',
      runnerId: 'runner_token_abc',
      bindingToken: 'tok',
      workspace: '/ws',
      parentPid: 1,
    });
    // The source mapping is not mutated by the build.
    expect(base[RUNNER_REGISTRY_URL_ENV_VAR]).toBeUndefined();
    expect(Object.keys(base)).toEqual(['PATH']);
    // The result is an independent object.
    expect(env).not.toBe(base);
  });

  // The runner's own operator knobs. A self-hosted operator runs only
  // `oeadm worker`, so the worker's environment is the ONLY one they control: a
  // knob that does not cross this boundary cannot be set at all. For
  // ANTHROPIC_ALLOWED_MODELS that silence was a security control failing OPEN —
  // the runner's unset case admits any well-formed model id on format alone.
  it('forwards the session-runner operator knobs an operator can only set on the worker', () => {
    const base: Record<string, string> = {
      PATH: '/usr/bin',
      ANTHROPIC_ALLOWED_MODELS: 'claude-sonnet-4-5,claude-opus-4-1',
      ANTHROPIC_MODEL_DEFAULT: 'claude-sonnet-4-5',
      ORCA_RUNNER_IDLE_TIMEOUT_S: '900',
    };

    const env = buildRunnerEnv(base, {
      registryRunnerUrl: 'wss://registry',
      runnerId: 'runner_token_abc',
      bindingToken: 'tok',
      workspace: '/ws',
      parentPid: 42,
    });

    expect(env.ANTHROPIC_ALLOWED_MODELS).toBe('claude-sonnet-4-5,claude-opus-4-1');
    expect(env.ANTHROPIC_MODEL_DEFAULT).toBe('claude-sonnet-4-5');
    expect(env.ORCA_RUNNER_IDLE_TIMEOUT_S).toBe('900');
  });

  // The allowlist separates SECRETS from CONFIGURATION, not worker vars from
  // runner vars. A credential stays out by default and forwards only when the
  // operator names it — the deliberate opt-in the passthrough exists for.
  it('keeps ANTHROPIC_API_KEY out by default and forwards it only when named', () => {
    const base: Record<string, string> = { PATH: '/usr/bin', ANTHROPIC_API_KEY: 'sk-operator' };

    const wiring = {
      registryRunnerUrl: 'wss://registry',
      runnerId: 'runner_token_abc',
      bindingToken: 'tok',
      workspace: '/ws',
      parentPid: 42,
    };

    expect(buildRunnerEnv(base, wiring).ANTHROPIC_API_KEY).toBeUndefined();

    const named = buildRunnerEnv(
      { ...base, [RUNNER_ENV_PASSTHROUGH_ENV_VAR]: 'ANTHROPIC_API_KEY' },
      wiring,
    );
    expect(named.ANTHROPIC_API_KEY).toBe('sk-operator');
  });
});

/**
 * The runner-wiring family is single-sourced in `@orca/harness-tunnel`.
 *
 * A value comparison cannot prove this: a locally re-declared
 * `'ORCA_RUNNER_REGISTRY_URL'` is `===` the hoisted one right up until one side
 * changes, which is the drift worth catching and the exact moment the assertion
 * stops holding. So this reads the module's own SOURCE, the same mechanism
 * `session-runner`'s registry pin uses — a hand-copied literal is a textual fact
 * about the file, and only a textual assertion can see it.
 */
describe('runner-wiring names are imported, not re-declared', () => {
  const source = readFileSync(
    fileURLToPath(new URL('../../src/runner-env.ts', import.meta.url)),
    'utf8',
  );
  // Comments legitimately name the variable while explaining the rule.
  const code = source.replace(/\/\*[\s\S]*?\*\//g, '').replace(/^\s*\/\/.*$/gm, '');

  it('imports RUNNER_REGISTRY_URL_ENV_VAR from @orca/harness-tunnel', () => {
    const importBlock = /import\s*\{([\s\S]*?)\}\s*from\s*'@orca\/harness-tunnel'/.exec(code);
    expect(importBlock, 'runner-env.ts must import from @orca/harness-tunnel').not.toBeNull();
    expect(importBlock![1]).toContain('RUNNER_REGISTRY_URL_ENV_VAR');
  });

  it('declares no local copy of the runner-wiring literals', () => {
    for (const literal of [
      HOISTED_RUNNER_REGISTRY_URL_ENV_VAR,
      RUNNER_TUNNEL_BINDING_TOKEN_ENV_VAR,
      RUNNER_WORKSPACE_ENV_VAR,
      RUNNER_PARENT_PID_ENV_VAR,
    ]) {
      expect(code, `runner-env.ts re-declares the hoisted literal ${literal}`).not.toContain(
        `'${literal}'`,
      );
    }
  });

  it('re-exports the one hoisted value', () => {
    expect(RUNNER_REGISTRY_URL_ENV_VAR).toBe(HOISTED_RUNNER_REGISTRY_URL_ENV_VAR);
  });
});

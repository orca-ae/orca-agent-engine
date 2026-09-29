// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// launcherFactory — selects an EnvironmentLauncher backend by name, reading
// provider config from the registry's own process env (mirrors how
// registry-service-ts/src/config.ts reads every other provider setting).
// 'local' (A1), 'e2b' + 'opensandbox' (A2).

import { describe, expect, it } from 'vitest';
import {
  E2B_API_KEY_ENV_VAR,
  E2B_ENVIRONMENT_RUNNER_COMMAND_ENV_VAR,
  E2B_ENVIRONMENT_TEMPLATE_ID_ENV_VAR,
  E2B_ENVIRONMENT_WORKER_COMMAND_ENV_VAR,
  LOCAL_ENVIRONMENT_BASE_DIR_ENV_VAR,
  LOCAL_ENVIRONMENT_RUNNER_COMMAND_ENV_VAR,
  LOCAL_ENVIRONMENT_WORKER_COMMAND_ENV_VAR,
  OPEN_SANDBOX_DOMAIN_ENV_VAR,
  OPEN_SANDBOX_ENVIRONMENT_IMAGE_ENV_VAR,
  OPEN_SANDBOX_ENVIRONMENT_RUNNER_COMMAND_ENV_VAR,
  OPEN_SANDBOX_ENVIRONMENT_WORKER_COMMAND_ENV_VAR,
  e2bOptionsFromEnv,
  launcherFactory,
} from '../../src/environment/launcher/launcher-factory.js';
import { LocalEnvironmentLauncher } from '../../src/environment/launcher/local-environment-launcher.js';
import { E2BEnvironmentLauncher } from '../../src/environment/launcher/e2b-environment-launcher.js';
import { OpenSandboxEnvironmentLauncher } from '../../src/environment/launcher/opensandbox-environment-launcher.js';

function validLocalEnv(): NodeJS.ProcessEnv {
  return {
    [LOCAL_ENVIRONMENT_BASE_DIR_ENV_VAR]: '/var/lib/orca/local-environments',
    [LOCAL_ENVIRONMENT_WORKER_COMMAND_ENV_VAR]: 'node /opt/orca/environment-worker/dist/main.js',
    [LOCAL_ENVIRONMENT_RUNNER_COMMAND_ENV_VAR]: 'node /opt/orca/session-runner/dist/main.js',
  };
}

function validE2BEnv(): NodeJS.ProcessEnv {
  return {
    [E2B_API_KEY_ENV_VAR]: 'e2b_test_key',
    [E2B_ENVIRONMENT_WORKER_COMMAND_ENV_VAR]: 'node /opt/orca/environment-worker/dist/main.js',
    [E2B_ENVIRONMENT_RUNNER_COMMAND_ENV_VAR]: 'node /opt/orca/session-runner/dist/main.js',
  };
}

function validOpenSandboxEnv(): NodeJS.ProcessEnv {
  return {
    [OPEN_SANDBOX_DOMAIN_ENV_VAR]: 'opensandbox.internal:8080',
    [OPEN_SANDBOX_ENVIRONMENT_IMAGE_ENV_VAR]: 'ghcr.io/orca-ae/orca-environment:latest',
    [OPEN_SANDBOX_ENVIRONMENT_WORKER_COMMAND_ENV_VAR]:
      'node /opt/orca/environment-worker/dist/main.js',
    [OPEN_SANDBOX_ENVIRONMENT_RUNNER_COMMAND_ENV_VAR]: 'node /opt/orca/session-runner/dist/main.js',
  };
}

describe('launcherFactory — local', () => {
  it('resolves "local" to a LocalEnvironmentLauncher', () => {
    const launcher = launcherFactory('local', validLocalEnv());
    expect(launcher).toBeInstanceOf(LocalEnvironmentLauncher);
  });

  it('throws a clear error for an unknown backend name', () => {
    expect(() => launcherFactory('bogus-backend', validLocalEnv())).toThrow(/bogus-backend/);
  });

  it('throws a clear error when the local worker command is missing', () => {
    const env = validLocalEnv();
    delete env[LOCAL_ENVIRONMENT_WORKER_COMMAND_ENV_VAR];
    expect(() => launcherFactory('local', env)).toThrow(
      new RegExp(LOCAL_ENVIRONMENT_WORKER_COMMAND_ENV_VAR),
    );
  });

  it('throws a clear error when the local runner command is missing', () => {
    const env = validLocalEnv();
    delete env[LOCAL_ENVIRONMENT_RUNNER_COMMAND_ENV_VAR];
    expect(() => launcherFactory('local', env)).toThrow(
      new RegExp(LOCAL_ENVIRONMENT_RUNNER_COMMAND_ENV_VAR),
    );
  });

  it('defaults the base dir when unset', () => {
    const env = validLocalEnv();
    delete env[LOCAL_ENVIRONMENT_BASE_DIR_ENV_VAR];
    expect(() => launcherFactory('local', env)).not.toThrow();
  });
});

describe('launcherFactory — e2b', () => {
  it('resolves "e2b" to an E2BEnvironmentLauncher', () => {
    const launcher = launcherFactory('e2b', validE2BEnv());
    expect(launcher).toBeInstanceOf(E2BEnvironmentLauncher);
  });

  it('throws a clear error when E2B_API_KEY is missing', () => {
    const env = validE2BEnv();
    delete env[E2B_API_KEY_ENV_VAR];
    expect(() => launcherFactory('e2b', env)).toThrow(new RegExp(E2B_API_KEY_ENV_VAR));
  });

  it('throws a clear error when the e2b worker command is missing', () => {
    const env = validE2BEnv();
    delete env[E2B_ENVIRONMENT_WORKER_COMMAND_ENV_VAR];
    expect(() => launcherFactory('e2b', env)).toThrow(
      new RegExp(E2B_ENVIRONMENT_WORKER_COMMAND_ENV_VAR),
    );
  });

  it('throws a clear error when the e2b runner command is missing', () => {
    const env = validE2BEnv();
    delete env[E2B_ENVIRONMENT_RUNNER_COMMAND_ENV_VAR];
    expect(() => launcherFactory('e2b', env)).toThrow(
      new RegExp(E2B_ENVIRONMENT_RUNNER_COMMAND_ENV_VAR),
    );
  });

  it("reads the boot template from E2B_ENVIRONMENT_TEMPLATE_ID, not harness-server's E2B_TEMPLATE_ID", () => {
    // Important-4 fix: the registry launcher's boot image must NOT share
    // harness-server's session-sandbox template var — the two need different
    // images. templateId is optional, so the constructed launcher can't be
    // introspected for this (the SDK client swallows it); assert against the
    // exported pure mapper instead.
    const env = validE2BEnv();
    env[E2B_ENVIRONMENT_TEMPLATE_ID_ENV_VAR] = 'orca-environment-template';
    env['E2B_TEMPLATE_ID'] = 'harness-server-template-should-be-ignored';
    expect(e2bOptionsFromEnv(env).templateId).toBe('orca-environment-template');
  });

  it('does not fall back to the old E2B_TEMPLATE_ID name when the new one is unset', () => {
    const env = validE2BEnv();
    env['E2B_TEMPLATE_ID'] = 'harness-server-template-should-be-ignored';
    expect(e2bOptionsFromEnv(env).templateId).toBeUndefined();
  });
});

describe('launcherFactory — opensandbox', () => {
  it('resolves "opensandbox" to an OpenSandboxEnvironmentLauncher', () => {
    const launcher = launcherFactory('opensandbox', validOpenSandboxEnv());
    expect(launcher).toBeInstanceOf(OpenSandboxEnvironmentLauncher);
  });

  it('throws a clear error when OPEN_SANDBOX_DOMAIN is missing', () => {
    const env = validOpenSandboxEnv();
    delete env[OPEN_SANDBOX_DOMAIN_ENV_VAR];
    expect(() => launcherFactory('opensandbox', env)).toThrow(
      new RegExp(OPEN_SANDBOX_DOMAIN_ENV_VAR),
    );
  });

  it('throws a clear error when OPEN_SANDBOX_ENVIRONMENT_IMAGE is missing', () => {
    const env = validOpenSandboxEnv();
    delete env[OPEN_SANDBOX_ENVIRONMENT_IMAGE_ENV_VAR];
    expect(() => launcherFactory('opensandbox', env)).toThrow(
      new RegExp(OPEN_SANDBOX_ENVIRONMENT_IMAGE_ENV_VAR),
    );
  });

  it("does not fall back to harness-server's OPEN_SANDBOX_IMAGE name when OPEN_SANDBOX_ENVIRONMENT_IMAGE is unset", () => {
    // Important-4 fix: the two must be independent — setting only the OLD
    // (harness-server) name must NOT satisfy this factory's requirement.
    const env = validOpenSandboxEnv();
    delete env[OPEN_SANDBOX_ENVIRONMENT_IMAGE_ENV_VAR];
    env['OPEN_SANDBOX_IMAGE'] = 'harness-server-image-should-be-ignored';
    expect(() => launcherFactory('opensandbox', env)).toThrow(
      new RegExp(OPEN_SANDBOX_ENVIRONMENT_IMAGE_ENV_VAR),
    );
  });

  it('throws a clear error when the opensandbox worker command is missing', () => {
    const env = validOpenSandboxEnv();
    delete env[OPEN_SANDBOX_ENVIRONMENT_WORKER_COMMAND_ENV_VAR];
    expect(() => launcherFactory('opensandbox', env)).toThrow(
      new RegExp(OPEN_SANDBOX_ENVIRONMENT_WORKER_COMMAND_ENV_VAR),
    );
  });

  it('throws a clear error when the opensandbox runner command is missing', () => {
    const env = validOpenSandboxEnv();
    delete env[OPEN_SANDBOX_ENVIRONMENT_RUNNER_COMMAND_ENV_VAR];
    expect(() => launcherFactory('opensandbox', env)).toThrow(
      new RegExp(OPEN_SANDBOX_ENVIRONMENT_RUNNER_COMMAND_ENV_VAR),
    );
  });
});

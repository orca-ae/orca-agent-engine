// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { execFileSync } from 'node:child_process';
import { describe, expect, it, vi } from 'vitest';
import {
  buildSandboxNetworkEnvironment,
  buildSessionEgressEnvironment,
  parseSessionEgressGrant,
  parseSandboxNetworkRequest,
  parseSandboxNetworkContext,
  prepareSessionEgress,
  SESSION_EGRESS_BOOTSTRAP_PATH,
  SANDBOX_NETWORK_CONTEXT_PATH,
  SANDBOX_NETWORK_TRUST_PATH,
  type SandboxNetworkContext,
  type SessionEgressGrant,
} from '../../src/sandbox-egress.js';
import { buildSandboxEgressGitCloneCommand } from '../../src/sandbox-egress-git.js';
import { InMemorySandboxRuntime } from '../../src/in-memory/runtime.js';
import type { SandboxHandle } from '../../src/sandbox-runtime.js';

const context: SandboxNetworkContext = {
  binding_id: 'network',
  generation: 'network-generation',
  proxy_url: 'https://proxy.example:8443',
  proxy_capability: 'base-only',
  ca_bundle_path: SANDBOX_NETWORK_TRUST_PATH,
  internal_bypass: ['10.0.0.2'],
};
const grant: SessionEgressGrant = {
  network_binding_id: 'network',
  binding_id: 'session-binding',
  generation: 'session-generation',
  proxy_capability: 'a/%:@?#',
  placeholders: { API_KEY: 'api-marker' },
  git: { marker: 'git-marker', username: 'x-access-token' },
};

describe('Sandbox network and Session egress', () => {
  it('requires strict separate registration, context and grant shapes', () => {
    expect(parseSandboxNetworkRequest({ binding_id: 'network', generation: 'one' })).toEqual({
      binding_id: 'network',
      generation: 'one',
    });
    expect(parseSandboxNetworkContext(context)).toEqual(context);
    expect(parseSessionEgressGrant(grant)).toEqual(grant);
    expect(() => parseSessionEgressGrant({ ...grant, proxy_url: context.proxy_url })).toThrow(
      'unexpected',
    );
    expect(() => parseSessionEgressGrant(undefined)).toThrow('invalid');
    expect(() => parseSandboxNetworkContext({ ...context, ca_bundle_path: '/tmp/evil' })).toThrow(
      'invalid',
    );
    expect(() => parseSandboxNetworkRequest({ ...context })).toThrow('unexpected');
  });

  it('projects a Session capability without changing the Sandbox base context', () => {
    const base = buildSandboxNetworkEnvironment(context);
    const env = buildSessionEgressEnvironment(context, grant);
    expect(new URL(base.HTTPS_PROXY!).password).toBe('base-only');
    expect(decodeURIComponent(new URL(env.HTTPS_PROXY!).password)).toBe(grant.proxy_capability);
    expect(env.GH_TOKEN).toBe('git-marker');
    expect(env.API_KEY).toBe('api-marker');
    expect(base.GH_TOKEN).toBeUndefined();
    expect(base.API_KEY).toBeUndefined();
    expect(context.proxy_capability).toBe('base-only');
    expect(env.GIT_PROXY_SSL_CAINFO).toBe(SANDBOX_NETWORK_TRUST_PATH);
    expect(env.GIT_SSL_CAINFO).toBe(SANDBOX_NETWORK_TRUST_PATH);
    expect(() =>
      buildSessionEgressEnvironment(context, { ...grant, network_binding_id: 'other' }),
    ).toThrow('parent');
  });

  it('stages only the private Session script and leaves shared network bytes untouched', async () => {
    const writes = new Map<string, Buffer>();
    const configureSessionEgress = vi.fn();
    const handle = {
      configureSessionEgress,
      files: {
        read: vi.fn(async (path: string) => {
          expect(path).toBe(SANDBOX_NETWORK_CONTEXT_PATH);
          return Buffer.from(JSON.stringify(context));
        }),
        write: async (path: string, value: Buffer) => {
          writes.set(path, value);
        },
        delete: vi.fn(),
        chmod: vi.fn(),
      },
    } as unknown as SandboxHandle;
    await prepareSessionEgress(
      handle,
      { ...grant, placeholders: { API_KEY: "marker'quote" } },
      true,
    );
    expect([...writes.keys()]).toEqual([SESSION_EGRESS_BOOTSTRAP_PATH]);
    expect(configureSessionEgress.mock.calls[0]![0].GH_TOKEN).toBe('git-marker');
    const output = execFileSync(
      'sh',
      ['-c', writes.get(SESSION_EGRESS_BOOTSTRAP_PATH)!.toString() + 'printf "%s" "$API_KEY"'],
      { encoding: 'utf8' },
    );
    expect(output).toBe("marker'quote");
  });

  it('rejects unsupported acquisition and parent mismatches before grant application', async () => {
    await expect(
      new InMemorySandboxRuntime().acquire({
        sandboxNetwork: { binding_id: 'network', generation: 'one' },
      }),
    ).rejects.toThrow('managed OpenSandbox');
  });

  it('uses real Git credential fill with the local Session marker helper', () => {
    const env = buildSessionEgressEnvironment(context, grant);
    const result = execFileSync('git', ['credential', 'fill'], {
      env: { PATH: process.env.PATH, ...env },
      input: 'protocol=https\nhost=github.com\npath=owner/repo.git\n\n',
      encoding: 'utf8',
    });
    expect(result).toContain('password=git-marker');
    expect(result).not.toContain(grant.proxy_capability);
  });

  it.each([
    'ALL_PROXY',
    'https_proxy',
    'NO_PROXY',
    'GIT_CONFIG_COUNT',
    'GH_TOKEN',
    'ORCA_GIT_CREDS_TOKEN',
    'NPM_CONFIG_CAFILE',
    'CARGO_HTTP_CAINFO',
    'PIP_CERT',
    'REQUESTS_CA_BUNDLE',
  ])('rejects conflicting client environment %s', (name) => {
    expect(() => buildSessionEgressEnvironment(context, grant, { [name]: 'unsafe' })).toThrow(
      'conflicts',
    );
  });
  it.each(['.svc', '*', '10.0.0.0/8', 'proxy.example:80', '::1', 'a'.repeat(64) + '.example'])(
    'rejects ambiguous bypass %s',
    (host) => {
      expect(() => buildSandboxNetworkEnvironment({ ...context, internal_bypass: [host] })).toThrow(
        'canonical DNS',
      );
    },
  );
  it.each([
    'http://proxy.example',
    'https://user:secret@proxy.example',
    'https://proxy.example/path',
  ])('rejects insecure proxy %s', (proxy_url) => {
    expect(() => buildSandboxNetworkEnvironment({ ...context, proxy_url })).toThrow('HTTPS proxy');
  });
  it('rejects newline injection and marker collisions', () => {
    expect(() => parseSessionEgressGrant({ ...grant, proxy_capability: 'bad\nvalue' })).toThrow();
    expect(() =>
      parseSessionEgressGrant({ ...grant, placeholders: { GH_TOKEN: 'marker' } }),
    ).toThrow();
  });
});

describe('sandbox egress clone command', () => {
  it('keeps the canonical remote and pins branches without credential argv', () => {
    const command = buildSandboxEgressGitCloneCommand({
      url: 'https://github.com/owner/repo.git',
      mountPath: '/mnt/repo',
      checkout: { type: 'branch', value: 'release' },
    });
    execFileSync('bash', ['-n', '-c', command]);
    expect(command).toContain("'--branch' 'release'");
    expect(command).toContain("'--depth=1'");
    expect(command).toContain('http.followRedirects=false');
    expect(command).not.toContain('orca-marker');
  });

  it('clones history for an arbitrary commit before detached checkout', () => {
    const command = buildSandboxEgressGitCloneCommand({
      url: 'https://github.com/owner/repo.git',
      mountPath: '/mnt/repo',
      checkout: { type: 'commit', value: 'a'.repeat(40) },
    });
    expect(command).not.toContain('--depth=1');
    expect(command).toContain('checkout --detach');
    expect(command).toContain('mv -T');
  });

  it.each(['http://github.com/owner/repo.git', 'https://user:pat@github.com/owner/repo.git'])(
    'rejects insecure repository URL',
    (url) => {
      expect(() => buildSandboxEgressGitCloneCommand({ url, mountPath: '/mnt/repo' })).toThrow();
    },
  );
});

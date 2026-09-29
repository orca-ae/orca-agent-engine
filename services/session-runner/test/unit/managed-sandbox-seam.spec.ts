// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import * as childProcess from 'node:child_process';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { LocalSandboxRuntime } from '@orca/sandbox-runtime';
import { createManagedToolSandboxRuntime } from '../../src/sandbox/seam.js';

vi.mock('node:child_process', async (importOriginal) => {
  const actual = await importOriginal<typeof import('node:child_process')>();
  return { ...actual, execFileSync: vi.fn(actual.execFileSync) };
});

describe('managed model-tool runtime factory', () => {
  const platform = Object.getOwnPropertyDescriptor(process, 'platform')!;
  afterEach(() => {
    Object.defineProperty(process, 'platform', platform);
    vi.restoreAllMocks();
    vi.unstubAllEnvs();
  });

  it('fails closed on unsupported hosts instead of returning the in-memory runtime', () => {
    Object.defineProperty(process, 'platform', { ...platform, value: 'darwin' });
    expect(() =>
      createManagedToolSandboxRuntime({ harnessWorkDir: '/tools', networkAllowedDomains: [] }),
    ).toThrow(/fallback is disabled/);
  });

  it.each(['srt', 'bwrap'])('fails closed when %s is unavailable', (missing) => {
    Object.defineProperty(process, 'platform', { ...platform, value: 'linux' });
    vi.spyOn(childProcess, 'execFileSync').mockImplementation((file) => {
      if (file === missing) throw new Error(`${missing} unavailable`);
      return Buffer.from('version');
    });
    expect(() =>
      createManagedToolSandboxRuntime({ harnessWorkDir: '/tools', networkAllowedDomains: [] }),
    ).toThrow(`${missing} unavailable`);
  });

  it('validates the exact supplied hosts and ignores ambient egress settings', () => {
    Object.defineProperty(process, 'platform', { ...platform, value: 'linux' });
    const probe = vi.spyOn(childProcess, 'execFileSync').mockReturnValue(Buffer.from('version'));
    vi.stubEnv('AI_GATEWAY_URL', 'https://ambient-gateway');
    vi.stubEnv('S3_ENDPOINT', 'https://ambient-store');
    const runtime = createManagedToolSandboxRuntime({
      harnessWorkDir: '/tools',
      networkAllowedDomains: ['TOOLS.EXAMPLE', 'tools.example'],
    });
    expect(runtime).toBeInstanceOf(LocalSandboxRuntime);
    expect(probe.mock.calls.map(([command]) => command)).toEqual(['srt', 'bwrap']);
    const config = (runtime as LocalSandboxRuntime).buildSandboxConfig({
      root: '/tools/session',
      tmp: '/tools/session/tmp',
    });
    expect(config.network).toEqual({
      allowedDomains: ['tools.example'],
      deniedDomains: [],
      allowLocalBinding: false,
    });
    expect(() =>
      createManagedToolSandboxRuntime({
        harnessWorkDir: '/tools',
        networkAllowedDomains: ['https://bad.example/path'],
      }),
    ).toThrow(/domain is invalid/);
    expect(probe).toHaveBeenCalledTimes(2);
  });
});

// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { afterEach, expect, it, vi } from 'vitest';
import { dshProfileConfigSha256, type DshProfileFiles } from '@orca/harness-catalog';
import { OpenSandboxRuntime } from '../../src/sandbox/opensandbox/runtime.js';

afterEach(() => vi.restoreAllMocks());

it.each([false, true])(
  'keeps DSH-only preparation separate from package write policy; failed probe=%s',
  async (fail) => {
    const files: DshProfileFiles = {
      'cordis.patch.yml': '[]',
      'package.json': '{}',
      'pnpm-lock.yaml': '',
      'pnpm-workspace.yaml': '',
    };
    const profile = {
      version: 1 as const,
      source_file_id: 'file_test',
      source_sha256: 'a'.repeat(64),
      config_sha256: dshProfileConfigSha256(files),
      files,
    };
    const calls: Array<{ url: string; method: string; body?: string }> = [];
    vi.spyOn(globalThis, 'fetch').mockImplementation(async (input, init) => {
      const url = String(input);
      const method = init?.method ?? 'GET';
      const body = init?.body?.toString();
      calls.push({ url, method, ...(body ? { body } : {}) });
      if (url.endsWith('/sandboxes') && method === 'POST')
        return Response.json({ id: 'sb_profile' });
      if (url.endsWith('/sandboxes/sb_profile') && method === 'GET')
        return Response.json({ status: { state: 'Allocated' } });
      if (url.endsWith('/sandboxes/sb_profile') && method === 'DELETE')
        return new Response(null, { status: 204 });
      if (url.endsWith('/endpoints/44772')) return Response.json({ endpoint: 'execd.test:44772' });
      if (url.endsWith('/ping')) return new Response('');
      if (url.endsWith('/command')) {
        const payload =
          fail && body?.includes('.orca-profile-revision')
            ? { type: 'error', error: { evalue: '1' } }
            : { type: 'execution_complete' };
        return new Response('data: ' + JSON.stringify(payload) + '\n\n', {
          headers: { 'content-type': 'text/event-stream' },
        });
      }
      throw new Error('Unexpected provider request: ' + url);
    });
    const runtime = new OpenSandboxRuntime({
      domain: 'opensandbox.test',
      protocol: 'http',
      image: 'test-image',
      timeoutSeconds: 60,
      useServerProxy: false,
      requestTimeoutSeconds: 5,
    });
    expect(runtime.capabilities.supportsDshProfiles).toBe(true);
    if (fail) {
      await expect(runtime.acquire({ dshProfile: profile })).rejects.toThrow(
        'profile preparation probe',
      );
      expect(calls.filter((call) => call.method === 'DELETE')).toHaveLength(1);
    } else {
      const handle = await runtime.acquire({ dshProfile: profile });
      await expect(
        handle.prepareWritePolicy!({ writablePaths: [], readonlyPaths: [] }),
      ).resolves.toBeUndefined();
      expect(
        calls.some((call) => call.method === 'GET' && call.url.endsWith('/sandboxes/sb_profile')),
      ).toBe(true);
      const create = calls.find(
        (call) => call.url.endsWith('/sandboxes') && call.method === 'POST',
      )!;
      expect(JSON.parse(create.body!).timeout).toBe(960);
      expect(
        calls
          .filter((call) => call.url.endsWith('/command'))
          .map((call) => call.body)
          .join(''),
      ).not.toContain('orca_package_boundary_ok');
      await handle.destroy();
    }
  },
);

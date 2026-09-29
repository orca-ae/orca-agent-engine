// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// OpenSandboxRuntime — relocation coverage (moved from harness-server's
// src/sandbox/opensandbox/runtime.ts to @orca/cloud-sandbox by A2, then
// hardened to match main's post-extraction write-policy/FUSE/ranged-read
// work).
//
// `acquire()` delegates body construction to the exported pure function
// `buildOpenSandboxAcquireBody`, which these tests import and assert on
// directly — the real production code path, no HTTP calls, no subclassing.
// A regression in the body shape (env/image/exposePorts/resourceLimits)
// fails here, exactly as it did at the old harness-server location. The
// `OpenSandboxRuntime.acquire hardening` block below drives `acquire()`
// itself against a mocked `fetch`, mirroring
// services/harness-server/test/unit/sandbox-harness-env.spec.ts.

import { buildSandboxWritePolicy } from '@orca/sandbox-runtime';
import { afterEach, describe, expect, it, vi } from 'vitest';
import {
  OpenSandboxRuntime,
  buildOpenSandboxAcquireBody,
  buildOpenSandboxEndpointUrl,
} from '../../src/opensandbox/runtime.js';
import type { EnvironmentSpec } from '@orca/sandbox-runtime';

const BASE_OPTS = {
  image: 'ghcr.io/orca-ae/sandbox-harness-claude-code:latest',
  timeoutSeconds: 60,
};

afterEach(() => {
  vi.restoreAllMocks();
});

describe('OpenSandboxRuntime', () => {
  it('advertises FUSE by default and no local-memory fallback', () => {
    const rt = new OpenSandboxRuntime({
      domain: 'localhost:18080',
      protocol: 'http',
      image: 'opensandbox/code-interpreter:v1.0.2',
      timeoutSeconds: 1800,
      useServerProxy: true,
      requestTimeoutSeconds: 30,
    });
    expect(rt.capabilities.supportsFuse).toBe(true);
    expect(rt.capabilities.supportsLocalMemory).toBeUndefined();
    expect(rt.capabilities.supportsWritePolicy).toBe(true);
  });

  it('disables FUSE when configured', () => {
    const rt = new OpenSandboxRuntime({
      domain: 'localhost:18080',
      protocol: 'http',
      image: 'opensandbox/code-interpreter:v1.0.2',
      timeoutSeconds: 1800,
      useServerProxy: true,
      requestTimeoutSeconds: 30,
      enableFuse: false,
    });
    expect(rt.capabilities.supportsFuse).toBe(false);
  });
});

describe('buildOpenSandboxAcquireBody', () => {
  it('merges harnessEnv into the container env map', () => {
    const spec: EnvironmentSpec = {
      harnessEnv: {
        LITELLM_API_BASE: 'http://gw.local/v1/llm',
        LITELLM_API_KEY: 'jwt_llm_test',
        LITELLM_DEFAULT_MODEL: 'claude-3-5-sonnet-20241022',
      },
    };
    const body = buildOpenSandboxAcquireBody(spec, BASE_OPTS);
    const env = body['env'] as Record<string, string>;
    expect(env['LITELLM_API_BASE']).toBe('http://gw.local/v1/llm');
    expect(env['LITELLM_API_KEY']).toBe('jwt_llm_test');
    expect(env['LITELLM_DEFAULT_MODEL']).toBe('claude-3-5-sonnet-20241022');
  });

  it('does not pass packages through the container env', () => {
    const spec: EnvironmentSpec = {
      packages: { apt: ['git', 'curl'], npm: ['typescript'] },
      harnessEnv: {
        LITELLM_API_BASE: 'http://gw.local/v1/llm',
        LITELLM_API_KEY: 'tok',
      },
    };
    const body = buildOpenSandboxAcquireBody(spec, BASE_OPTS);
    const env = body['env'] as Record<string, string>;
    expect(env['ORCA_ENV_PACKAGES_JSON']).toBeUndefined();
    expect(env['LITELLM_API_BASE']).toBe('http://gw.local/v1/llm');
    expect(env['LITELLM_API_KEY']).toBe('tok');
  });

  it('uses env.image override over constructor image', () => {
    const spec: EnvironmentSpec = {
      image: 'ghcr.io/orca-ae/sandbox-harness-custom:v1',
    };
    const body = buildOpenSandboxAcquireBody(spec, BASE_OPTS);
    expect((body['image'] as Record<string, string>)['uri']).toBe(
      'ghcr.io/orca-ae/sandbox-harness-custom:v1',
    );
  });

  it('uses an in-sandbox entrypoint override with its per-acquire image', () => {
    const body = buildOpenSandboxAcquireBody(
      {
        image: 'ghcr.io/orca-ae/sandbox-harness-custom:v1',
        entrypoint: ['node', 'dist/index.js'],
      },
      {
        ...BASE_OPTS,
        entrypoint: ['/opt/code-interpreter/code-interpreter.sh'],
      },
    );

    expect(body['entrypoint']).toEqual(['node', 'dist/index.js']);
  });

  it('uses the constructor entrypoint for the default separate image', () => {
    const body = buildOpenSandboxAcquireBody({}, { ...BASE_OPTS, entrypoint: ['separate.sh'] });
    expect(body['entrypoint']).toEqual(['separate.sh']);
  });

  it('falls back to constructor image when env.image is absent', () => {
    const body = buildOpenSandboxAcquireBody({}, BASE_OPTS);
    expect((body['image'] as Record<string, string>)['uri']).toBe(
      'ghcr.io/orca-ae/sandbox-harness-claude-code:latest',
    );
  });

  it('passes exposePorts when spec includes them', () => {
    const spec: EnvironmentSpec = { exposePorts: [4096] };
    const body = buildOpenSandboxAcquireBody(spec, BASE_OPTS);
    expect(body['exposePorts']).toEqual([4096]);
  });

  it('omits exposePorts from body when spec has none', () => {
    const body = buildOpenSandboxAcquireBody({}, BASE_OPTS);
    expect(body['exposePorts']).toBeUndefined();
  });

  it('treats undefined harnessEnv as empty (no extra keys in env)', () => {
    const body = buildOpenSandboxAcquireBody({}, BASE_OPTS);
    const env = body['env'] as Record<string, string>;
    expect(Object.keys(env)).toHaveLength(0);
  });

  it('includes the full real body shape (resourceLimits, metadata, etc.)', () => {
    const body = buildOpenSandboxAcquireBody({}, BASE_OPTS);
    expect(body['resourceLimits']).toBeDefined();
    expect(body['secureAccess']).toBe(false);
    expect(body['metadata']).toEqual({ owner: 'orca-managed-agents' });
    expect(body['extensions']).toEqual({ 'bootstrap.execd.isolation': 'enable' });
    expect(body['timeout']).toBe(60);
  });

  it('requests the custom server FUSE device extension only when FUSE is enabled', () => {
    expect(
      buildOpenSandboxAcquireBody({}, { ...BASE_OPTS, enableFuse: true })['extensions'],
    ).toEqual({
      'bootstrap.execd.isolation': 'enable',
      'orca.fuse.device': 'enable',
    });
    expect(
      buildOpenSandboxAcquireBody({}, { ...BASE_OPTS, enableFuse: false })['extensions'],
    ).toEqual({
      'bootstrap.execd.isolation': 'enable',
    });
  });

  it('uses caller-supplied resource limits when provided', () => {
    const body = buildOpenSandboxAcquireBody(
      {},
      {
        ...BASE_OPTS,
        resourceLimits: { cpu: '250m', memory: '1Gi' },
      },
    );
    expect(body['resourceLimits']).toEqual({ cpu: '250m', memory: '1Gi' });
  });
});

describe('buildOpenSandboxEndpointUrl', () => {
  it('normalizes endpoint output using the runtime protocol', () => {
    expect(buildOpenSandboxEndpointUrl('sandbox.local:4096', 'http')).toBe(
      'http://sandbox.local:4096',
    );
  });

  it('leaves an already-schemed endpoint untouched', () => {
    expect(buildOpenSandboxEndpointUrl('https://sandbox.local:4096', 'http')).toBe(
      'https://sandbox.local:4096',
    );
  });
});

describe('OpenSandboxRuntime.acquire hardening', () => {
  it('rejects runtime package installation before creating an isolated workload', async () => {
    const calls = mockOpenSandboxFetch([]);
    const runtime = newTestRuntime({ enableFuse: false });

    await expect(runtime.acquire({ packages: { pip: ['requests'] } })).rejects.toThrow(
      /does not allow runtime package installation before the agent isolation boundary/,
    );
    expect(calls).toEqual([]);
  });

  it('probes FUSE prerequisites before returning an enabled runtime handle', async () => {
    const calls = mockOpenSandboxFetch([commandStream({ exitCode: 0 })]);
    const runtime = newTestRuntime({ enableFuse: true });

    await runtime.acquire({ requiresFuse: true });

    const commandBody = JSON.parse(calls.at(-1)?.body ?? '{}') as Record<string, unknown>;
    expect(commandBody['command']).toContain('id -u');
    expect(commandBody['command']).toContain('command -v s3fs');
    expect(commandBody['command']).toContain('command -v fusermount3');
    expect(commandBody['command']).toContain('test -c /dev/fuse');
    expect(commandBody['command']).toContain('mount -t tmpfs');
  });

  it('defaults each acquisition to no FUSE privilege when no S3 mount is requested', async () => {
    const calls = mockOpenSandboxFetch([]);
    const runtime = newTestRuntime({ enableFuse: true });

    await runtime.acquire({});

    const createBody = JSON.parse(calls[0]!.body ?? '{}') as Record<string, unknown>;
    expect(createBody['extensions']).toEqual({ 'bootstrap.execd.isolation': 'enable' });
    expect(calls.some((call) => call.url.endsWith('/command'))).toBe(false);
  });

  it('fails closed and destroys sandbox when FUSE prerequisite probe fails', async () => {
    const calls = mockOpenSandboxFetch([commandStream({ stderr: 'missing s3fs', exitCode: 1 })]);
    const runtime = newTestRuntime({ enableFuse: true });

    await expect(runtime.acquire({ requiresFuse: true })).rejects.toThrow(
      /FUSE prerequisite probe failed.*s3fs.*fusermount3.*\/dev\/fuse.*missing s3fs/,
    );
    expect(calls.map((call) => `${call.method} ${call.url}`)).toContain(
      'DELETE http://opensandbox.test/v1/sandboxes/sb_test',
    );
  });

  it('runs privileged commands directly as root, strips sudo, and forwards envs', async () => {
    const calls = mockOpenSandboxFetch([
      commandStream({ exitCode: 0 }),
      commandStream({ stdout: 'mounted', exitCode: 0 }),
    ]);
    const runtime = newTestRuntime({ enableFuse: true });
    const handle = await runtime.acquire({ requiresFuse: true });

    const result = await handle.runPrivileged!(' sudo s3fs bucket /mnt/data', {
      envs: { AWS_ACCESS_KEY_ID: 'key' },
    });

    expect(result).toMatchObject({ stdout: 'mounted', exit_code: 0 });
    const commandBody = JSON.parse(calls.at(-1)?.body ?? '{}') as Record<string, unknown>;
    expect(commandBody).toMatchObject({
      command: 's3fs bucket /mnt/data',
      envs: { AWS_ACCESS_KEY_ID: 'key' },
    });
  });

  it('applies in-sandbox ownership metadata to uploaded files and new parent directories', async () => {
    let uploadMetadata: unknown;
    mockOpenSandboxFetch([], async (url, init) => {
      if (url !== 'http://execd.test:44772/files/upload') return;
      const form = init?.body as FormData;
      const metadata = form.get('metadata');
      expect(metadata).toBeInstanceOf(Blob);
      uploadMetadata = JSON.parse(await (metadata as Blob).text());
    });
    const runtime = newTestRuntime();

    const handle = await runtime.acquire({
      fileUploadOwnership: { owner: 'node', group: 'node' },
    });
    await handle.files.write('/workspace/repo/README.md', Buffer.from('owned by node'));

    expect(uploadMetadata).toEqual({
      path: '/workspace/repo/README.md',
      owner: 'node',
      group: 'node',
    });
  });

  it('accepts empty package lists without running an install command', async () => {
    const calls = mockOpenSandboxFetch([]);
    const runtime = newTestRuntime();

    await runtime.acquire({ packages: { pip: [], npm: [] } });

    expect(calls.some((call) => call.url.endsWith('/command'))).toBe(false);
  });

  it('routes bounded reads through one fd-helper command without downloading the file', async () => {
    const payload = JSON.stringify({
      data_base64: Buffer.from('hello', 'utf8').toString('base64'),
      total_bytes: 5,
    });
    const calls = mockOpenSandboxFetch([commandStream({ stdout: payload, exitCode: 0 })]);
    const handle = await newTestRuntime().acquire({});

    const page = await handle.files.readUtf8Page(
      '/mnt/inputs/document.txt',
      {},
      { readableRoots: ['/mnt/inputs'] },
    );

    expect(page.content).toBe('hello');
    const commandCalls = calls.filter((call) => call.url.endsWith('/command'));
    expect(commandCalls).toHaveLength(1);
    const commandBody = JSON.parse(commandCalls[0]!.body ?? '{}') as Record<string, unknown>;
    expect(commandBody['command']).toEqual(expect.stringContaining('/proc/self/fd/'));
    expect(commandBody['command']).toEqual(expect.stringContaining('/mnt/inputs/document.txt'));
    expect(commandBody['timeout']).toBe(10_000);
    expect(commandBody['envs']).toEqual({
      BASH_ENV: '',
      ENV: '',
      NODE_OPTIONS: '',
      PATH: '/usr/local/bin:/usr/bin',
    });
    expect(calls.some((call) => call.url.includes('/files/download'))).toBe(false);
  });

  it('drops the outer root identity for the policy probe and every shell tool', async () => {
    const calls = mockOpenSandboxFetch([]);
    const handle = await newTestRuntime().acquire({});
    const policy = buildSandboxWritePolicy([]);
    await handle.prepareWritePolicy!(policy);
    await handle.runWithWritePolicy!({ tool: 'bash', args: { command: 'id' } }, policy);
    await handle.runWithWritePolicy!({ tool: 'glob', args: { pattern: '*' } }, policy);
    await handle.runWithWritePolicy!({ tool: 'grep', args: { pattern: 'hello' } }, policy);

    const commands = calls
      .filter((call) => call.url.endsWith('/command'))
      .map((call) => JSON.parse(call.body!).command as string);
    // The read and alias probes stay privileged; the execution probe uses
    // exactly the same identity and isolation as Bash, Glob and Grep.
    expect(commands).toHaveLength(6);
    for (const command of commands.slice(2)) {
      expect(command).toMatch(
        /^setpriv --reuid 1000 --regid 1000 --clear-groups --bounding-set=-all --no-new-privs -- 'bwrap'/,
      );
      expect(command).toContain("'--unshare-user'");
      expect(command).toContain("'--cap-drop' 'ALL'");
      expect(command).toContain("'--ro-bind' '/' '/'");
    }
  });

  it('fails policy setup when the ranged-read prerequisite probe fails', async () => {
    const calls = mockOpenSandboxFetch([
      commandStream({ stderr: '/bin/sh: node: not found', exitCode: 127 }),
    ]);
    const handle = await newTestRuntime().acquire({});

    await expect(handle.prepareWritePolicy!(buildSandboxWritePolicy([]))).rejects.toThrow(
      /ranged-read prerequisite probe failed.*node: not found/,
    );

    const commandCalls = calls.filter((call) => call.url.endsWith('/command'));
    expect(commandCalls).toHaveLength(1);
    const commandBody = JSON.parse(commandCalls[0]!.body ?? '{}') as Record<string, unknown>;
    expect(commandBody['command']).toEqual(expect.stringContaining('/proc/self/fd/'));
    expect(commandBody['timeout']).toBe(10_000);
  });

  it('routes filesystem-root preflight through one timed sandbox command', async () => {
    const calls = mockOpenSandboxFetch([commandStream({ exitCode: 0 })]);
    const handle = await newTestRuntime().acquire({});

    await handle.prepareFilesystemRoots!(['/mnt/inputs', '/workspace/skills']);

    const commandCalls = calls.filter((call) => call.url.endsWith('/command'));
    expect(commandCalls).toHaveLength(1);
    const commandBody = JSON.parse(commandCalls[0]!.body ?? '{}') as Record<string, unknown>;
    expect(commandBody['command']).toEqual(expect.stringContaining('/proc/self/mountinfo'));
    expect(commandBody['command']).toEqual(expect.stringContaining('/mnt/inputs'));
    expect(commandBody['command']).toEqual(expect.stringContaining('/workspace/skills'));
    expect(commandBody['timeout']).toBe(10_000);
  });

  it('rejects an error terminal even when execd reports evalue zero', async () => {
    mockOpenSandboxFetch([
      new Response(sse({ type: 'error', error: { evalue: '0' } }), {
        status: 200,
        headers: { 'content-type': 'text/event-stream' },
      }),
    ]);
    const handle = await newTestRuntime().acquire({});

    await expect(handle.prepareFilesystemRoots!(['/mnt/inputs'])).rejects.toThrow(
      /filesystem-root preflight failed/,
    );
  });

  it('rejects an oversized command stream before buffering unbounded output', async () => {
    mockOpenSandboxFetch([
      new Response(sse({ type: 'stdout', text: 'x'.repeat(1024 * 1024) }), {
        status: 200,
        headers: { 'content-type': 'text/event-stream' },
      }),
    ]);
    const handle = await newTestRuntime().acquire({});

    await expect(handle.run({ tool: 'bash', args: { command: 'yes' } })).rejects.toThrow(
      /command stream exceeded 1048576-byte limit/,
    );
  });
});

describe('OpenSandboxRuntime error-path hardening', () => {
  it('marks a severed command stream so it is distinguishable from a real exit 1', async () => {
    // Stream ends with stdout only — no execution_complete, no error event
    // (proxy dropped the connection mid-command). The synthesized exit 1
    // must carry a marker; a bare 1 is byte-for-byte
    // identical to the command legitimately failing.
    const truncated = new Response(sse({ type: 'stdout', text: 'partial output' }), {
      status: 200,
      headers: { 'content-type': 'text/event-stream' },
    });
    mockOpenSandboxFetch([truncated]);
    const handle = await newTestRuntime().acquire({});

    const result = await handle.run({ tool: 'bash', args: { command: 'echo hi' } });
    expect(result.exit_code).toBe(1);
    expect(result.stderr).toContain('[orca: command stream truncated before completion]');
    expect(result.stdout).toContain('partial output');
  });

  it('surfaces a bad glob/grep root as an error result instead of empty success', async () => {
    // Pin the `cd … || exit 2` semantics for this adapter too.
    mockOpenSandboxFetch([
      commandStream({ stderr: 'cd: /no/such/dir: No such file or directory', exitCode: 2 }),
    ]);
    const handle = await newTestRuntime().acquire({});

    const glob = await handle.run({ tool: 'glob', args: { pattern: '*', root: '/no/such/dir' } });
    expect(glob.exit_code).toBe(2);
    expect(glob.stderr).toContain('No such file or directory');
    expect(glob.output).toEqual([]);
  });

  it('an error event with an unparseable value is a fault, not a glob no-match', async () => {
    // evalue is an exception VALUE (a message string is realistic). The
    // synthesized fallback must not be exit 1 — the one code glob/grep treat
    // as clean no-match — or an execd-signalled fault reads as an empty
    // success.
    mockOpenSandboxFetch([
      new Response(
        sse({ type: 'stdout', text: 'partial.py' }) +
          sse({ type: 'error', error: { evalue: 'RuntimeError: worker died' } }),
        { status: 200, headers: { 'content-type': 'text/event-stream' } },
      ),
    ]);
    const handle = await newTestRuntime().acquire({});
    const glob = await handle.run({ tool: 'glob', args: { pattern: '*.py' } });
    expect(glob.exit_code).toBe(2);
    // Partial matches ride along with the fault.
    expect(glob.output).toEqual(['partial.py']);
  });

  it('destroy() stays retryable after a failed remote delete', async () => {
    // Latching `destroyed` before the DELETE succeeds makes a
    // retry silently no-op on a still-running (billing) sandbox.
    mockOpenSandboxFetch([]);
    const handle = await newTestRuntime().acquire({});

    let deletes = 0;
    vi.spyOn(globalThis, 'fetch').mockImplementation(async (input, init) => {
      const url = String(input);
      const method = init?.method ?? 'GET';
      if (method === 'DELETE' && url === 'http://opensandbox.test/v1/sandboxes/sb_test') {
        deletes += 1;
        return deletes === 1
          ? new Response('boom', { status: 500 })
          : new Response(null, { status: 204 });
      }
      return new Response(`unexpected ${method} ${url}`, { status: 500 });
    });

    await expect(handle.destroy()).rejects.toThrow();
    // The retry must actually re-issue the DELETE, not short-circuit.
    await handle.destroy();
    expect(deletes).toBe(2);
    // Now latched: further calls are the documented idempotent no-op.
    await handle.destroy();
    expect(deletes).toBe(2);
  });
});

function newTestRuntime(
  overrides: Partial<ConstructorParameters<typeof OpenSandboxRuntime>[0]> = {},
): OpenSandboxRuntime {
  return new OpenSandboxRuntime({
    domain: 'opensandbox.test',
    protocol: 'http',
    image: 'ghcr.io/orca-ae/sandbox:latest',
    timeoutSeconds: 60,
    useServerProxy: false,
    requestTimeoutSeconds: 1,
    enableFuse: false,
    ...overrides,
  });
}

interface FetchCall {
  url: string;
  method: string;
  body?: string | undefined;
}

function mockOpenSandboxFetch(
  commandResponses: Array<Response | Error>,
  inspect?: (url: string, init?: RequestInit) => Promise<void> | void,
): FetchCall[] {
  const calls: FetchCall[] = [];
  vi.spyOn(globalThis, 'fetch').mockImplementation(async (input, init) => {
    const url = String(input);
    const method = init?.method ?? 'GET';
    await inspect?.(url, init);
    calls.push({ url, method, body: init?.body?.toString() });

    if (method === 'POST' && url === 'http://opensandbox.test/v1/sandboxes') {
      return jsonResponse({ id: 'sb_test' });
    }
    if (method === 'DELETE' && url === 'http://opensandbox.test/v1/sandboxes/sb_test') {
      return new Response('', { status: 204 });
    }
    if (method === 'GET' && url.endsWith('/sandboxes/sb_test/endpoints/44772')) {
      return jsonResponse({ endpoint: 'execd.test:44772' });
    }
    if (method === 'GET' && url === 'http://execd.test:44772/ping') {
      return new Response('', { status: 200 });
    }
    if (method === 'POST' && url === 'http://execd.test:44772/command') {
      const next = commandResponses.shift();
      if (next instanceof Error) throw next;
      return next ?? commandStream({ exitCode: 0 });
    }
    if (method === 'POST' && url === 'http://execd.test:44772/files/upload') {
      return new Response('', { status: 200 });
    }
    return new Response(`unexpected ${method} ${url}`, { status: 500 });
  });
  return calls;
}

function jsonResponse(body: unknown): Response {
  return new Response(JSON.stringify(body), {
    status: 200,
    headers: { 'content-type': 'application/json' },
  });
}

function commandStream(opts: { stdout?: string; stderr?: string; exitCode: number }): Response {
  const events: string[] = [];
  if (opts.stdout) events.push(sse({ type: 'stdout', text: opts.stdout }));
  if (opts.stderr) events.push(sse({ type: 'stderr', text: opts.stderr }));
  if (opts.exitCode === 0) {
    events.push(sse({ type: 'execution_complete' }));
  } else {
    events.push(sse({ type: 'error', error: { evalue: String(opts.exitCode) } }));
  }
  return new Response(events.join(''), {
    status: 200,
    headers: { 'content-type': 'text/event-stream' },
  });
}

function sse(payload: unknown): string {
  return `data: ${JSON.stringify(payload)}\n\n`;
}

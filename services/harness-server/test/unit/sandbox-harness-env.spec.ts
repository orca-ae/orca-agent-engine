// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * Unit tests: the OpenSandbox runtime passes
 * `EnvironmentSpec.harnessEnv` + per-acquire image/exposePorts into the
 * sandbox create request body.
 *
 * These exercise the REAL production code path: `OpenSandboxRuntime.acquire`
 * delegates body construction to the exported pure function
 * `buildOpenSandboxAcquireBody`, which the tests below import and assert on
 * directly. No HTTP calls, no subclassing — a regression in the body shape
 * (env/image/exposePorts/resourceLimits) fails here.
 *
 * E2B note: E2BSandboxRuntime.acquire uses a dynamic import of
 * @e2b/code-interpreter which provisions a real sandbox. That change is
 * verified by compilation + type-check; a cloud-gated integration test would
 * be the appropriate place for the runtime assertion (e2b-sandbox.spec.ts
 * style). No test requiring real E2B creds is added to the default unit suite.
 */

import { afterEach, describe, expect, it, vi } from 'vitest';
import {
  buildE2BEndpointUrl,
  buildE2BPrerequisiteProbeCommand,
  buildE2BPrivilegedCommand,
} from '../../src/sandbox/e2b/runtime.js';
import {
  buildOpenSandboxAcquireBody,
  buildOpenSandboxEndpointUrl,
  OpenSandboxRuntime,
} from '../../src/sandbox/opensandbox/runtime.js';
import type { EnvironmentSpec } from '../../src/sandbox/sandbox-runtime.js';

const BASE_OPTS = {
  image: 'ghcr.io/orca-ae/sandbox-harness-claude-code:latest',
  timeoutSeconds: 60,
};

afterEach(() => {
  vi.restoreAllMocks();
});

describe('buildOpenSandboxAcquireBody (Task 2)', () => {
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
    expect(body['extensions']).toEqual({
      'bootstrap.execd.isolation': 'enable',
      'orca.fuse.device': 'enable',
    });
    expect(body['timeout']).toBe(60);
  });

  it('always requests gVisor in-sandbox FUSE', () => {
    expect(buildOpenSandboxAcquireBody({}, BASE_OPTS)['extensions']).toEqual({
      'bootstrap.execd.isolation': 'enable',
      'orca.fuse.device': 'enable',
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

describe('OpenSandboxRuntime.acquire hardening', () => {
  it('rejects runtime package installation before creating an isolated workload', async () => {
    const calls = mockOpenSandboxFetch([]);
    const runtime = newTestRuntime();

    await expect(runtime.acquire({ packages: { pip: ['requests'] } })).rejects.toThrow(
      /does not allow runtime package installation before the agent isolation boundary/,
    );
    expect(calls).toEqual([]);
  });

  it('probes gVisor FUSE prerequisites before returning a runtime handle', async () => {
    const calls = mockOpenSandboxFetch([], undefined, { recordFuseProbe: true });
    const runtime = newTestRuntime();

    await runtime.acquire({});

    const commandBody = JSON.parse(calls.at(-1)?.body ?? '{}') as Record<string, unknown>;
    expect(commandBody['command']).toContain('id -u');
    expect(commandBody['command']).toContain('command -v s3fs');
    expect(commandBody['command']).toContain('command -v fusermount3');
    expect(commandBody['command']).toContain('test -c /dev/fuse');
    expect(commandBody['command']).toContain('exec 9<>/dev/fuse');
    expect(commandBody['command']).toContain('mount -t tmpfs');
  });

  it('requests gVisor FUSE even when no S3 mount is requested', async () => {
    const calls = mockOpenSandboxFetch([]);
    const runtime = newTestRuntime();

    await runtime.acquire({});

    const createBody = JSON.parse(calls[0]!.body ?? '{}') as Record<string, unknown>;
    expect(createBody['extensions']).toEqual({
      'bootstrap.execd.isolation': 'enable',
      'orca.fuse.device': 'enable',
    });
    expect(calls.some((call) => call.url.endsWith('/command'))).toBe(false);
  });

  it('fails closed and destroys sandbox when FUSE prerequisite probe fails', async () => {
    const calls = mockOpenSandboxFetch([], undefined, {
      recordFuseProbe: true,
      fuseProbeResponse: commandStream({ stderr: 'missing s3fs', exitCode: 1 }),
    });
    const runtime = newTestRuntime();

    await expect(runtime.acquire({})).rejects.toThrow(
      /FUSE prerequisite probe failed.*s3fs.*fusermount3.*\/dev\/fuse.*missing s3fs/,
    );
    expect(calls.map((call) => `${call.method} ${call.url}`)).toContain(
      'DELETE http://opensandbox.test/v1/sandboxes/sb_test',
    );
  });

  it('runs privileged commands directly as root, strips sudo, and forwards envs', async () => {
    const calls = mockOpenSandboxFetch([commandStream({ stdout: 'mounted', exitCode: 0 })]);
    const runtime = newTestRuntime();
    const handle = await runtime.acquire({});

    const result = await handle.runPrivileged(' sudo s3fs bucket /mnt/data', {
      envs: { AWS_ACCESS_KEY_ID: 'key' },
    });

    expect(result).toMatchObject({ stdout: 'mounted', exit_code: 0 });
    const commandBody = JSON.parse(calls.at(-1)?.body ?? '{}') as Record<string, unknown>;
    expect(commandBody).toMatchObject({
      command: 's3fs bucket /mnt/data',
      envs: { AWS_ACCESS_KEY_ID: 'key' },
    });
  });

  it('rejects pause and resume without lifecycle requests', async () => {
    const calls = mockOpenSandboxFetch([]);
    const handle = await newTestRuntime().acquire({});

    await expect(handle.pause()).rejects.toThrow(/pause is disabled for gVisor in-sandbox FUSE/);
    await expect(handle.resume()).rejects.toThrow(/resume is disabled for gVisor in-sandbox FUSE/);

    expect(calls.some((call) => call.url.endsWith('/pause'))).toBe(false);
    expect(calls.some((call) => call.url.endsWith('/resume'))).toBe(false);
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
    const policy = {
      writablePaths: [{ path: '/mnt/session/outputs', kind: 'session_output' as const }],
      readonlyPaths: [],
    };
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

    await expect(
      handle.prepareWritePolicy!({ writablePaths: [], readonlyPaths: [] }),
    ).rejects.toThrow(/ranged-read prerequisite probe failed.*node: not found/);

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
    expect(commandBody['envs']).toEqual({
      BASH_ENV: '',
      ENV: '',
      NODE_OPTIONS: '',
      PATH: '/usr/local/bin:/usr/bin',
    });
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

  it('bounds a command stream that never reaches a terminal event', async () => {
    let cancelled = false;
    const hangingStream = new ReadableStream<Uint8Array>({
      cancel() {
        cancelled = true;
      },
    });
    mockOpenSandboxFetch([
      new Response(hangingStream, {
        status: 200,
        headers: { 'content-type': 'text/event-stream' },
      }),
    ]);
    const handle = await newTestRuntime({ requestTimeoutSeconds: 0.02 }).acquire({});

    await expect(handle.run({ tool: 'bash', args: { command: 'true' } })).rejects.toThrow(
      /command stream timed out/,
    );
    expect(cancelled).toBe(true);
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

  it('keeps the command stream budget above an explicit command timeout', async () => {
    let streamController: ReadableStreamDefaultController<Uint8Array> | undefined;
    mockOpenSandboxFetch([
      new Response(
        new ReadableStream<Uint8Array>({
          start(controller) {
            streamController = controller;
          },
        }),
        {
          status: 200,
          headers: { 'content-type': 'text/event-stream' },
        },
      ),
    ]);
    const handle = await newTestRuntime({ requestTimeoutSeconds: 0.02 }).acquire({});
    const running = handle.run({
      tool: 'bash',
      args: { command: 'sleep 0.05', timeout_ms: 1_000 },
    });

    await new Promise((resolve) => setTimeout(resolve, 40));
    streamController!.enqueue(new TextEncoder().encode(sse({ type: 'execution_complete' })));
    streamController!.close();

    await expect(running).resolves.toMatchObject({ exit_code: 0 });
  });
});

describe('sandbox endpoint URL helpers', () => {
  it('preserves only sudoers-approved S3 transport envs for E2B root helpers', () => {
    expect(
      buildE2BPrivilegedCommand(' sudo /usr/local/bin/orca-s3fs-mount bucket /mnt/memory/x opts', [
        'BASH_ENV',
        'ENV',
        'PATH',
        'ORCA_S3_ACCESS_KEY_ID',
        'ORCA_S3_SECRET_ACCESS_KEY',
      ]),
    ).toBe(
      'sudo --preserve-env=ORCA_S3_ACCESS_KEY_ID,ORCA_S3_SECRET_ACCESS_KEY /usr/local/bin/orca-s3fs-mount bucket /mnt/memory/x opts',
    );
    expect(() => buildE2BPrivilegedCommand('true', ['BASH_ENV', 'LD_PRELOAD'])).toThrow(
      /unsupported E2B privileged environment: LD_PRELOAD/,
    );
  });

  it('fails E2B acquisition on broad sudo, arbitrary root shell, or missing FUSE prerequisites', () => {
    const probe = buildE2BPrerequisiteProbeCommand();
    expect(probe).toContain('test -c /dev/fuse');
    expect(probe).toContain('NOPASSWD:[[:space:]]*ALL|NOPASSWD:SETENV');
    expect(probe).toContain('sudo -n /bin/sh -c id');
    expect(probe).toContain('/usr/local/bin/orca-s3fs-mount');
    expect(probe).toContain('/mnt/custom');
    expect(probe).toContain('credlib=/tmp/forbidden.so');
    expect(probe).toContain('test "$helper_rc" = 64');
    expect(probe).toContain('test "$custom_mount_rc" = 64');
  });

  it('normalizes E2B getHost output into an HTTPS URL', () => {
    expect(buildE2BEndpointUrl('abc-4096.e2b.dev')).toBe('https://abc-4096.e2b.dev');
    expect(buildE2BEndpointUrl('https://abc-4096.e2b.dev')).toBe('https://abc-4096.e2b.dev');
  });

  it('normalizes OpenSandbox endpoint output using the runtime protocol', () => {
    expect(buildOpenSandboxEndpointUrl('sandbox.local:4096', 'http')).toBe(
      'http://sandbox.local:4096',
    );
    expect(buildOpenSandboxEndpointUrl('https://sandbox.local:4096', 'http')).toBe(
      'https://sandbox.local:4096',
    );
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
    ...overrides,
  });
}

interface FetchCall {
  url: string;
  method: string;
  body?: string;
}

function mockOpenSandboxFetch(
  commandResponses: Array<Response | Error>,
  inspect?: (url: string, init?: RequestInit) => Promise<void> | void,
  options: { recordFuseProbe?: boolean; fuseProbeResponse?: Response } = {},
): FetchCall[] {
  const calls: FetchCall[] = [];
  vi.spyOn(globalThis, 'fetch').mockImplementation(async (input, init) => {
    const url = String(input);
    const method = init?.method ?? 'GET';
    await inspect?.(url, init);
    const body = init?.body?.toString();
    const isFuseProbe =
      method === 'POST' &&
      url === 'http://execd.test:44772/command' &&
      body?.includes('exec 9<>/dev/fuse') === true;
    if (!isFuseProbe || options.recordFuseProbe) calls.push({ url, method, body });

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
      if (isFuseProbe) {
        return options.fuseProbeResponse ?? commandStream({ exitCode: 0 });
      }
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

// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createServer, type IncomingMessage, type ServerResponse } from 'node:http';
import type { AddressInfo } from 'node:net';
import { Readable } from 'node:stream';
import { BinaryReader, BinaryWriter } from '@bufbuild/protobuf/wire';
import { afterEach, describe, expect, it } from 'vitest';
import { AgentEnvRuntime, buildAgentEnvAcquireBody } from '../../src/sandbox/agentenv/runtime.js';
import { buildSandboxWritePolicy } from '../../src/sandbox/write-policy.js';

const CONNECT_DATA_FLAG = 0x00;
const CONNECT_END_FLAG = 0x02;

describe('AgentEnvRuntime', () => {
  const closeCallbacks: Array<() => Promise<void>> = [];

  afterEach(async () => {
    await Promise.all(closeCallbacks.splice(0).map(async (close) => await close()));
  });

  it('maps environment settings to a secure cold sandbox request', () => {
    expect(
      buildAgentEnvAcquireBody(
        {
          image: 'registry.example/orca:session',
          harnessEnv: { SESSION_TOKEN: 'token' },
          networking: { allowOut: ['example.com'] },
        },
        {
          image: 'registry.example/orca:default',
          timeoutSeconds: 900,
          cpuCount: 2,
          memoryMB: 2048,
          diskSizeMB: 65536,
        },
      ),
    ).toEqual({
      image: 'registry.example/orca:session',
      timeout: 900,
      autoPause: false,
      secure: true,
      metadata: { owner: 'orca-managed-agents' },
      envVars: { SESSION_TOKEN: 'token' },
      network: { allowOut: ['example.com'] },
      cpuCount: 2,
      memoryMB: 2048,
      diskSizeMB: 65536,
    });
  });

  it('runs commands and files through envd and maps lifecycle operations', async () => {
    const requests: Array<{ method: string; path: string; headers: IncomingMessage['headers'] }> =
      [];
    let processCalls = 0;
    let processFailureAfter: number | undefined;
    const processCommands: string[] = [];
    let createBody: Record<string, unknown> | undefined;

    const baseUrl = await startServer(async (request, response) => {
      const url = new URL(request.url ?? '/', 'http://agentenv.test');
      requests.push({
        method: request.method ?? 'GET',
        path: url.pathname,
        headers: request.headers,
      });

      if (request.method === 'POST' && url.pathname === '/sandboxes-cold') {
        createBody = JSON.parse((await readRequest(request)).toString('utf8')) as Record<
          string,
          unknown
        >;
        sendJson(response, 201, {
          sandboxID: 'sandbox-1',
          envdAccessToken: 'envd-token-1',
        });
        return;
      }
      if (request.method === 'POST' && url.pathname === '/process.Process/Start') {
        processCalls += 1;
        expect(request.headers['x-agentenv-sandbox-id']).toBe('sandbox-1');
        expect(request.headers['x-agentenv-target-port']).toBe('49983');
        expect(request.headers['x-access-token']).toMatch(/^envd-token-/);
        expect(request.headers['x-api-key']).toBeUndefined();
        const requestBody = await readRequest(request);
        expect(requestBody[0]).toBe(CONNECT_DATA_FLAG);
        processCommands.push(decodeStartCommand(requestBody));
        if (processFailureAfter !== undefined) {
          if (processFailureAfter === 0) {
            processFailureAfter = undefined;
            sendProcessResponse(response, '', 'forced process failure', 1);
            return;
          }
          processFailureAfter -= 1;
        }
        sendProcessResponse(response, processCalls >= 3 ? 'command output\n' : '', '', 0);
        return;
      }
      if (request.method === 'POST' && url.pathname === '/files') {
        expect(request.headers['x-agentenv-sandbox-id']).toBe('sandbox-1');
        expect(url.searchParams.get('path')).toBeTruthy();
        expect(url.searchParams.get('username')).toBe('ubuntu');
        await readRequest(request);
        sendJson(response, 200, []);
        return;
      }
      if (request.method === 'GET' && url.pathname === '/files') {
        expect(url.searchParams.get('path')).toBe('/mnt/session/outputs/result.txt');
        expect(url.searchParams.get('username')).toBe('ubuntu');
        response.writeHead(200, { 'content-type': 'application/octet-stream' });
        response.end('stored bytes');
        return;
      }
      if (request.method === 'POST' && url.pathname === '/sandboxes/sandbox-1/pause') {
        response.writeHead(204).end();
        return;
      }
      if (request.method === 'POST' && url.pathname === '/sandboxes/sandbox-1/connect') {
        sendJson(response, 201, {
          sandboxID: 'sandbox-1',
          envdAccessToken: 'envd-token-2',
        });
        return;
      }
      if (request.method === 'DELETE' && url.pathname === '/sandboxes/sandbox-1') {
        response.writeHead(204).end();
        return;
      }
      sendJson(response, 404, { code: 404, message: 'not found' });
    });
    closeCallbacks.push(baseUrl.close);

    const runtime = new AgentEnvRuntime({
      baseUrl: `${baseUrl.url}///`,
      apiKey: 'api-key',
      image: 'registry.example/orca-agentenv:test',
      timeoutSeconds: 600,
      requestTimeoutSeconds: 2,
      cpuCount: 1,
      memoryMB: 512,
    });
    const sandbox = await runtime.acquire({
      harnessEnv: { SAFE_ENV: 'value' },
    });

    expect(runtime.capabilities).toEqual({
      supportsFuse: false,
      supportsLocalMemory: true,
      supportsWritePolicy: true,
    });
    expect(createBody).toMatchObject({
      image: 'registry.example/orca-agentenv:test',
      timeout: 600,
      autoPause: false,
      secure: true,
      cpuCount: 1,
      memoryMB: 512,
      envVars: { SAFE_ENV: 'value' },
    });
    expect(requests[0]?.headers['x-api-key']).toBe('api-key');

    await expect(sandbox.run({ tool: 'bash', args: { command: 'printf test' } })).resolves.toEqual({
      stdout: 'command output\n',
      stderr: '',
      exit_code: 0,
    });
    await expect(
      sandbox.run({ tool: 'bash', args: { command: "printf 'bounded'", timeout_ms: 250 } }),
    ).resolves.toMatchObject({ exit_code: 0 });
    expect(processCommands).toContain('printf test');
    expect(processCommands).toContain(
      "timeout --signal=TERM --kill-after=1s 0.250s /bin/bash -lc 'printf '\\''bounded'\\'''",
    );

    await expect(
      sandbox.run({ tool: 'glob', args: { pattern: '*.txt', root: '/tmp' } }),
    ).resolves.toEqual({ output: ['command output'] });
    await expect(
      sandbox.run({ tool: 'grep', args: { pattern: 'needle', root: '/tmp' } }),
    ).resolves.toEqual({ output: 'command output\n' });
    await expect(sandbox.run({ tool: 'glob', args: { pattern: '*.txt' } })).resolves.toEqual({
      output: ['command output'],
    });
    await expect(sandbox.run({ tool: 'grep', args: { pattern: 'needle' } })).resolves.toEqual({
      output: 'command output\n',
    });
    await expect(sandbox.run({ tool: 'unknown', args: {} })).resolves.toEqual({
      exit_code: 127,
      stderr: 'unknown tool: unknown',
    });

    await sandbox.prepareFilesystemRoots?.(['/mnt/inputs/input.txt']);
    const policy = buildSandboxWritePolicy([]);
    await sandbox.prepareWritePolicy?.(policy);
    await expect(
      sandbox.runWithWritePolicy?.(
        { tool: 'bash', args: { command: 'printf isolated', timeout_ms: 500 } },
        policy,
      ),
    ).resolves.toMatchObject({ exit_code: 0 });
    await expect(
      sandbox.runWithWritePolicy?.(
        { tool: 'glob', args: { pattern: '*.txt', root: '/mnt/session/outputs' } },
        policy,
      ),
    ).resolves.toEqual({ output: ['command output'] });
    await expect(
      sandbox.runWithWritePolicy?.(
        { tool: 'grep', args: { pattern: 'needle', root: '/mnt/session/outputs' } },
        policy,
      ),
    ).resolves.toEqual({ output: 'command output\n' });
    await expect(
      sandbox.runWithWritePolicy?.({ tool: 'glob', args: { pattern: '*.txt' } }, policy),
    ).resolves.toEqual({ output: ['command output'] });
    await expect(
      sandbox.runWithWritePolicy?.({ tool: 'grep', args: { pattern: 'needle' } }, policy),
    ).resolves.toEqual({ output: 'command output\n' });
    await expect(
      sandbox.runWithWritePolicy?.({ tool: 'unknown', args: {} }, policy),
    ).resolves.toEqual({ exit_code: 127, stderr: 'unknown tool: unknown' });
    await expect(
      sandbox.canonicalizePathForPolicy?.('/mnt/session/outputs/result.txt'),
    ).resolves.toBe('command output');
    await expect(
      sandbox.runPrivileged('sudo printf trusted', { envs: { TRUSTED_ENV: 'value' } }),
    ).resolves.toMatchObject({ exit_code: 0 });
    expect(processCommands).toContain('printf trusted');

    await sandbox.files.write('/mnt/session/outputs/result.txt', Buffer.from('payload'));
    expect(processCommands).toContain("chmod 0777 -- '/mnt/session/outputs'");
    await sandbox.files.write('/tmp/stream.txt', Readable.from([Buffer.from('stream payload')]));
    await expect(sandbox.files.read('/mnt/session/outputs/result.txt')).resolves.toEqual(
      Buffer.from('stored bytes'),
    );
    await expect(sandbox.files.list('/tmp')).resolves.toEqual(['command output']);
    await sandbox.files.chmod('/mnt/session/outputs/result.txt', 0o640);
    await expect(sandbox.files.chmod('/tmp/invalid', 0o1000)).rejects.toThrow(/invalid file mode/);
    await sandbox.files.chmodMany?.('/mnt/session/outputs', [
      { path: '/mnt/session/outputs/result.txt', mode: 0o444 },
    ]);
    await sandbox.files.chmodMany?.('/mnt/session/outputs', []);
    await sandbox.files.delete('/tmp/stream.txt');

    processFailureAfter = 0;
    await expect(sandbox.prepareFilesystemRoots?.(['/mnt/inputs/failure.txt'])).rejects.toThrow(
      /filesystem-root preflight.*forced process failure/,
    );
    processFailureAfter = 0;
    await expect(sandbox.prepareWritePolicy?.(policy)).rejects.toThrow(
      /ranged-read prerequisite.*forced process failure/,
    );
    processFailureAfter = 1;
    await expect(sandbox.prepareWritePolicy?.(policy)).rejects.toThrow(
      /filesystem-alias.*forced process failure/,
    );
    processFailureAfter = 2;
    await expect(sandbox.prepareWritePolicy?.(policy)).rejects.toThrow(
      /write-policy sandbox.*forced process failure/,
    );
    processFailureAfter = 0;
    await expect(
      sandbox.canonicalizePathForPolicy?.('/mnt/session/outputs/failure.txt'),
    ).rejects.toThrow(/realpath failed.*forced process failure/);
    processFailureAfter = 0;
    await expect(
      sandbox.files.readUtf8Page?.(
        '/mnt/session/outputs/result.txt',
        {},
        { readableRoots: ['/mnt/session/outputs'] },
      ),
    ).rejects.toThrow(/ranged read failed.*forced process failure/);
    processFailureAfter = 0;
    await expect(sandbox.files.list('/tmp/failure')).rejects.toThrow(/ls .*forced process failure/);
    processFailureAfter = 0;
    await expect(sandbox.files.chmod('/tmp/failure', 0o640)).rejects.toThrow(
      /chmod .*forced process failure/,
    );
    processFailureAfter = 0;
    await expect(
      sandbox.files.write('/mnt/session/outputs/failure.txt', Buffer.from('payload')),
    ).rejects.toThrow(/chmod managed parent.*forced process failure/);
    processFailureAfter = 0;
    await expect(
      sandbox.files.chmodMany?.('/mnt/session/outputs', [
        { path: '/mnt/session/outputs/result.txt', mode: 0o444 },
      ]),
    ).rejects.toThrow(/batch chmod failed.*forced process failure/);
    processFailureAfter = 0;
    await expect(sandbox.files.delete('/tmp/failure')).rejects.toThrow(
      /delete .*forced process failure/,
    );

    await sandbox.pause();
    await sandbox.resume();
    await expect(sandbox.endpoint?.(8080)).resolves.toEqual({
      url: baseUrl.url,
      headers: {
        'x-agentenv-sandbox-id': 'sandbox-1',
        'x-agentenv-target-port': '8080',
        'X-Access-Token': 'envd-token-2',
      },
    });

    await sandbox.destroy();
    await sandbox.destroy();
    await sandbox.pause();
    await sandbox.resume();
    await expect(sandbox.run({ tool: 'bash', args: { command: 'true' } })).rejects.toThrow(
      /destroyed/,
    );
    await expect(sandbox.runPrivileged('true')).rejects.toThrow(/destroyed/);
    expect(requests.filter((request) => request.method === 'DELETE')).toHaveLength(1);
  });

  it('cleans up a secure sandbox response that omits the envd token', async () => {
    let deletes = 0;
    const baseUrl = await startServer(async (request, response) => {
      const url = new URL(request.url ?? '/', 'http://agentenv.test');
      if (request.method === 'POST' && url.pathname === '/sandboxes-cold') {
        await readRequest(request);
        sendJson(response, 201, { sandboxID: 'sandbox-without-token' });
        return;
      }
      if (request.method === 'DELETE' && url.pathname === '/sandboxes/sandbox-without-token') {
        deletes += 1;
        response.writeHead(204).end();
        return;
      }
      sendJson(response, 404, { code: 404, message: 'not found' });
    });
    closeCallbacks.push(baseUrl.close);

    const runtime = new AgentEnvRuntime({
      baseUrl: baseUrl.url,
      apiKey: 'api-key',
      image: 'registry.example/orca-agentenv:test',
      timeoutSeconds: 600,
      requestTimeoutSeconds: 2,
    });

    await expect(runtime.acquire({})).rejects.toThrow(/did not include envdAccessToken/);
    expect(deletes).toBe(1);
  });

  it('reports lifecycle failures and cleans up a sandbox with missing prerequisites', async () => {
    let createCalls = 0;
    let processCalls = 0;
    let deletes = 0;
    const baseUrl = await startServer(async (request, response) => {
      const url = new URL(request.url ?? '/', 'http://agentenv.test');
      if (request.method === 'POST' && url.pathname === '/sandboxes-cold') {
        createCalls += 1;
        await readRequest(request);
        if (createCalls === 1) {
          sendJson(response, 201, { envdAccessToken: 'token-without-id' });
          return;
        }
        if (createCalls === 2) {
          sendJson(response, 503, { code: 503, message: 'capacity unavailable' });
          return;
        }
        sendJson(response, 201, {
          sandboxID: 'sandbox-missing-prerequisites',
          envdAccessToken: 'envd-token',
        });
        return;
      }
      if (request.method === 'POST' && url.pathname === '/process.Process/Start') {
        processCalls += 1;
        await readRequest(request);
        if (processCalls === 1) {
          sendProcessResponse(response, '', '', 0);
        } else {
          sendProcessResponse(response, '', 'x'.repeat(600), 1);
        }
        return;
      }
      if (
        request.method === 'DELETE' &&
        url.pathname === '/sandboxes/sandbox-missing-prerequisites'
      ) {
        deletes += 1;
        response.writeHead(204).end();
        return;
      }
      if (request.method === 'POST' && url.pathname === '/sandboxes/pause-failure/pause') {
        response.writeHead(500, { 'content-type': 'text/plain' }).end('pause failed');
        return;
      }
      if (request.method === 'DELETE' && url.pathname === '/sandboxes/delete-failure') {
        response.writeHead(500).end();
        return;
      }
      if (request.method === 'DELETE' && url.pathname === '/sandboxes/already-deleted') {
        response.writeHead(404).end();
        return;
      }
      sendJson(response, 404, { code: 404, message: 'not found' });
    });
    closeCallbacks.push(baseUrl.close);

    const runtime = new AgentEnvRuntime({
      baseUrl: baseUrl.url,
      apiKey: 'api-key',
      image: 'registry.example/orca-agentenv:test',
      timeoutSeconds: 600,
      requestTimeoutSeconds: 2,
    });

    await expect(runtime.acquire({})).rejects.toThrow(/response did not include sandboxID/);
    await expect(runtime.acquire({})).rejects.toThrow(/status=503.*capacity unavailable/);
    await expect(runtime.acquire({})).rejects.toThrow(
      /prerequisite probe failed.*xxxxxxxx.*stdout=<empty>/,
    );
    expect(deletes).toBe(1);
    await expect(runtime.pauseSandbox('pause-failure')).rejects.toThrow(/status=500.*pause failed/);
    await expect(runtime.deleteSandbox('delete-failure')).rejects.toThrow(/status=500/);
    await expect(runtime.deleteSandbox('already-deleted')).resolves.toBeUndefined();
  });

  it('decodes stdout, stderr, pty, unknown, and end-error process events', async () => {
    let processCalls = 0;
    const baseUrl = await startServer(async (request, response) => {
      const url = new URL(request.url ?? '/', 'http://agentenv.test');
      if (request.method === 'POST' && url.pathname === '/sandboxes-cold') {
        await readRequest(request);
        sendJson(response, 201, { sandboxID: 'sandbox-events', envdAccessToken: 'token' });
        return;
      }
      if (request.method === 'POST' && url.pathname === '/process.Process/Start') {
        processCalls += 1;
        await readRequest(request);
        if (processCalls <= 2) {
          sendProcessResponse(response, '', '', 0);
        } else {
          sendProcessVariantResponse(response);
        }
        return;
      }
      if (request.method === 'DELETE' && url.pathname === '/sandboxes/sandbox-events') {
        response.writeHead(204).end();
        return;
      }
      sendJson(response, 404, { code: 404, message: 'not found' });
    });
    closeCallbacks.push(baseUrl.close);

    const runtime = new AgentEnvRuntime({
      baseUrl: baseUrl.url,
      apiKey: 'api-key',
      image: 'registry.example/orca-agentenv:test',
      timeoutSeconds: 600,
      requestTimeoutSeconds: 2,
    });
    const sandbox = await runtime.acquire({});

    await expect(sandbox.run({ tool: 'bash', args: { command: 'events' } })).resolves.toEqual({
      stdout: 'stdout',
      stderr: 'stderrptyend failure',
      exit_code: 7,
    });
    await sandbox.destroy();
  });

  it('rejects malformed envd command streams', async () => {
    const scenarios: Array<{
      name: string;
      expected: RegExp;
      send: (response: ServerResponse) => void;
    }> = [
      {
        name: 'HTTP failure',
        expected: /status=502.*envd unavailable/,
        send: (response) =>
          response.writeHead(502, { 'content-type': 'text/plain' }).end('envd unavailable'),
      },
      {
        name: 'unexpected flag',
        expected: /unexpected Connect flag 0x1/,
        send: (response) => sendConnectBytes(response, connectEnvelope(0x01, Buffer.alloc(0))),
      },
      {
        name: 'truncated envelope',
        expected: /truncated Connect envelope/,
        send: (response) => sendConnectBytes(response, Buffer.from([0, 0, 0, 0, 5, 1])),
      },
      {
        name: 'missing exit event',
        expected: /without a process exit event/,
        send: (response) =>
          sendConnectBytes(response, connectEnvelope(CONNECT_END_FLAG, Buffer.alloc(0))),
      },
      {
        name: 'Connect error',
        expected: /internal: stream failed/,
        send: (response) =>
          sendConnectBytes(
            response,
            connectEnvelope(
              CONNECT_END_FLAG,
              Buffer.from(
                JSON.stringify({ error: { code: 'internal', message: 'stream failed' } }),
              ),
            ),
          ),
      },
      {
        name: 'Connect error without details',
        expected: /unknown/,
        send: (response) =>
          sendConnectBytes(
            response,
            connectEnvelope(CONNECT_END_FLAG, Buffer.from(JSON.stringify({ error: {} }))),
          ),
      },
      {
        name: 'invalid Connect trailer',
        expected: /invalid Connect end envelope/,
        send: (response) =>
          sendConnectBytes(response, connectEnvelope(CONNECT_END_FLAG, Buffer.from('not-json'))),
      },
      {
        name: 'oversized stream',
        expected: /exceeded 1048576-byte limit/,
        send: (response) => sendConnectBytes(response, Buffer.alloc(1024 * 1024 + 1)),
      },
    ];

    for (const scenario of scenarios) {
      let processCalls = 0;
      const sandboxId = `sandbox-${scenario.name.replaceAll(' ', '-').toLowerCase()}`;
      const baseUrl = await startServer(async (request, response) => {
        const url = new URL(request.url ?? '/', 'http://agentenv.test');
        if (request.method === 'POST' && url.pathname === '/sandboxes-cold') {
          await readRequest(request);
          sendJson(response, 201, { sandboxID: sandboxId, envdAccessToken: 'token' });
          return;
        }
        if (request.method === 'POST' && url.pathname === '/process.Process/Start') {
          processCalls += 1;
          await readRequest(request);
          if (processCalls <= 2) sendProcessResponse(response, '', '', 0);
          else scenario.send(response);
          return;
        }
        if (request.method === 'DELETE' && url.pathname === `/sandboxes/${sandboxId}`) {
          response.writeHead(204).end();
          return;
        }
        sendJson(response, 404, { code: 404, message: 'not found' });
      });
      closeCallbacks.push(baseUrl.close);

      const runtime = new AgentEnvRuntime({
        baseUrl: baseUrl.url,
        apiKey: 'api-key',
        image: 'registry.example/orca-agentenv:test',
        timeoutSeconds: 600,
        requestTimeoutSeconds: 2,
      });
      const sandbox = await runtime.acquire({});
      await expect(sandbox.run({ tool: 'bash', args: { command: scenario.name } })).rejects.toThrow(
        scenario.expected,
      );
      await sandbox.destroy();
    }
  });

  it('rejects unsupported mutable image setup before creating a sandbox', async () => {
    const runtime = new AgentEnvRuntime({
      baseUrl: 'http://127.0.0.1:1',
      apiKey: 'api-key',
      image: 'registry.example/orca-agentenv:test',
      timeoutSeconds: 600,
      requestTimeoutSeconds: 1,
    });

    await expect(runtime.acquire({ packages: { npm: ['typescript'] } })).rejects.toThrow(
      /prebuilt image/,
    );
    await expect(runtime.acquire({ entrypoint: ['sleep', 'infinity'] })).rejects.toThrow(
      /entrypoint override/,
    );
    await expect(runtime.acquire({ exposePorts: [8080] })).rejects.toThrow(/exposed ports/);
  });
});

async function startServer(
  handler: (request: IncomingMessage, response: ServerResponse) => Promise<void>,
): Promise<{ url: string; close: () => Promise<void> }> {
  const server = createServer((request, response) => {
    void handler(request, response).catch((error: unknown) => {
      response.writeHead(500, { 'content-type': 'text/plain' });
      response.end(error instanceof Error ? error.message : String(error));
    });
  });
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const address = server.address() as AddressInfo;
  return {
    url: `http://127.0.0.1:${address.port}`,
    close: async () => {
      await new Promise<void>((resolve, reject) => {
        server.close((error) => (error ? reject(error) : resolve()));
      });
    },
  };
}

async function readRequest(request: IncomingMessage): Promise<Buffer> {
  const chunks: Buffer[] = [];
  for await (const chunk of request) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk as Uint8Array));
  }
  return Buffer.concat(chunks);
}

function sendJson(response: ServerResponse, status: number, body: unknown): void {
  response.writeHead(status, { 'content-type': 'application/json' });
  response.end(JSON.stringify(body));
}

function sendProcessResponse(
  response: ServerResponse,
  stdout: string,
  stderr: string,
  exitCode: number,
): void {
  const envelopes: Buffer[] = [];
  if (stdout) envelopes.push(connectEnvelope(CONNECT_DATA_FLAG, startDataEvent(1, stdout)));
  if (stderr) envelopes.push(connectEnvelope(CONNECT_DATA_FLAG, startDataEvent(2, stderr)));
  envelopes.push(connectEnvelope(CONNECT_DATA_FLAG, startEndEvent(exitCode)));
  envelopes.push(connectEnvelope(CONNECT_END_FLAG, Buffer.from('{}')));
  response.writeHead(200, { 'content-type': 'application/connect+proto' });
  response.end(Buffer.concat(envelopes));
}

function sendProcessVariantResponse(response: ServerResponse): void {
  const unknownResponse = new BinaryWriter().uint32(18).bytes(Buffer.from('unknown')).finish();
  const unknownProcess = new BinaryWriter();
  unknownProcess.uint32(10).bytes(Buffer.from('unknown'));
  const responseWithUnknownProcess = new BinaryWriter();
  responseWithUnknownProcess.uint32(10).bytes(unknownProcess.finish());
  const envelopes = [
    connectEnvelope(CONNECT_DATA_FLAG, startDataEvent(1, 'stdout')),
    connectEnvelope(CONNECT_DATA_FLAG, startDataEvent(2, 'stderr')),
    connectEnvelope(CONNECT_DATA_FLAG, startDataEvent(3, 'pty')),
    connectEnvelope(CONNECT_DATA_FLAG, unknownResponse),
    connectEnvelope(CONNECT_DATA_FLAG, responseWithUnknownProcess.finish()),
    connectEnvelope(CONNECT_DATA_FLAG, startEndEvent(7, 'end failure')),
    connectEnvelope(CONNECT_END_FLAG, Buffer.from('{}')),
  ];
  sendConnectBytes(response, Buffer.concat(envelopes));
}

function sendConnectBytes(response: ServerResponse, bytes: Uint8Array): void {
  response.writeHead(200, { 'content-type': 'application/connect+proto' });
  response.end(bytes);
}

function startDataEvent(field: 1 | 2 | 3, value: string): Uint8Array {
  const data = new BinaryWriter();
  data.uint32(field === 1 ? 10 : field === 2 ? 18 : 26).bytes(Buffer.from(value));
  const process = new BinaryWriter();
  process.uint32(18).bytes(data.finish());
  const response = new BinaryWriter();
  response.uint32(10).bytes(process.finish());
  return response.finish();
}

function startEndEvent(exitCode: number, error?: string): Uint8Array {
  const end = new BinaryWriter();
  end.uint32(8).sint32(exitCode);
  end.uint32(16).bool(true);
  if (error !== undefined) end.uint32(34).string(error);
  const process = new BinaryWriter();
  process.uint32(26).bytes(end.finish());
  const response = new BinaryWriter();
  response.uint32(10).bytes(process.finish());
  return response.finish();
}

function connectEnvelope(flag: number, payload: Uint8Array): Buffer {
  const envelope = Buffer.allocUnsafe(5 + payload.byteLength);
  envelope[0] = flag;
  envelope.writeUInt32BE(payload.byteLength, 1);
  Buffer.from(payload).copy(envelope, 5);
  return envelope;
}

function decodeStartCommand(envelope: Buffer): string {
  const payloadLength = envelope.readUInt32BE(1);
  const request = new BinaryReader(envelope.subarray(5, 5 + payloadLength));
  let processPayload: Uint8Array | undefined;
  while (request.pos < request.len) {
    const tag = request.uint32();
    if (tag >>> 3 === 1 && (tag & 7) === 2) {
      processPayload = request.bytes();
      break;
    }
    request.skip(tag & 7);
  }
  if (!processPayload) throw new Error('missing process config');

  const process = new BinaryReader(processPayload);
  const args: string[] = [];
  while (process.pos < process.len) {
    const tag = process.uint32();
    if (tag >>> 3 === 2 && (tag & 7) === 2) {
      args.push(process.string());
      continue;
    }
    process.skip(tag & 7);
  }
  if (args.length < 2) throw new Error('missing bash command argument');
  return args[1]!;
}

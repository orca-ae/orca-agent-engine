// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { spawn, spawnSync } from 'node:child_process';
import { createServer, type IncomingMessage, type ServerResponse, type Server } from 'node:http';
import { resolve as resolvePath } from 'node:path';
import { fileURLToPath } from 'node:url';
import { dirname } from 'node:path';
import type { AddressInfo } from 'node:net';

/**
 * Unit test for the in-sandbox `orca-git-creds` bash helper.
 *
 * The helper is a credential-protocol bridge: git pipes `key=value\n` records
 * to it on stdin, and the helper translates them into a `POST` against
 * `ORCA_GIT_CREDS_URL` with the JWT in the `Authorization: Bearer …` header.
 * The registry resolves the bound vault and returns `{ username, password }`
 * which the helper echoes back in git's expected `username=…\npassword=…\n`
 * shape.
 *
 * This test boots an in-process node:http fixture as the registry mock,
 * spawns the bash script via `child_process.spawn`, and asserts:
 *   - happy path 200 → username + password echoed on stdout, request body
 *     carries protocol/host/path verbatim, JWT shows up as `Bearer …`.
 *   - missing `ORCA_GIT_CREDS_URL` → silent exit 0 (helper is opt-in).
 *   - 404 → silent exit 0 (no creds; git falls through to anonymous).
 *   - 401 → silent exit 0 (helper does not propagate auth errors to git).
 *   - `store` and `erase` actions → silent no-ops; no HTTP traffic.
 *
 * Skipped on hosts that don't have `bash`, `curl`, or `jq` available — CI
 * runners (ubuntu-latest) have all three; macOS dev machines need a brew
 * install of jq if the test is run locally.
 */

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const SCRIPT_PATH = resolvePath(__dirname, '../../sandbox-templates/orca-default/orca-git-creds');

interface CapturedRequest {
  authHeader: string | undefined;
  body: { protocol?: string; host?: string; path?: string };
}

interface MockServer {
  url: string;
  proxyUrl: string;
  captures: CapturedRequest[];
  setResponse: (status: number, body: object | string | null) => void;
  resetCaptures: () => void;
  close: () => Promise<void>;
}

async function startMockServer(): Promise<MockServer> {
  const captures: CapturedRequest[] = [];
  let response: { status: number; body: object | string | null } = {
    status: 200,
    body: { username: 'x-access-token', password: 'fake-pat' },
  };
  const handler = (req: IncomingMessage, res: ServerResponse): void => {
    let raw = '';
    req.on('data', (c) => (raw += c));
    req.on('end', () => {
      let body: { protocol?: string; host?: string; path?: string } = {};
      try {
        body = raw ? (JSON.parse(raw) as typeof body) : {};
      } catch {
        body = {};
      }
      captures.push({
        authHeader:
          typeof req.headers['authorization'] === 'string'
            ? req.headers['authorization']
            : undefined,
        body,
      });
      res.statusCode = response.status;
      if (response.body === null) {
        res.end();
      } else if (typeof response.body === 'string') {
        res.end(response.body);
      } else {
        res.setHeader('content-type', 'application/json');
        res.end(JSON.stringify(response.body));
      }
    });
  };
  const server: Server = createServer(handler);
  await new Promise<void>((r) => server.listen(0, '127.0.0.1', () => r()));
  const port = (server.address() as AddressInfo).port;
  const proxyUrl = `http://127.0.0.1:${port}`;
  const url = `${proxyUrl}/v1/git-creds`;
  return {
    url,
    proxyUrl,
    captures,
    setResponse: (status, body) => {
      response = { status, body };
    },
    resetCaptures: () => {
      captures.length = 0;
    },
    close: () =>
      new Promise<void>((r) => {
        server.close(() => r());
      }),
  };
}

interface ScriptResult {
  stdout: string;
  stderr: string;
  exit_code: number;
}

async function runHelper(input: {
  stdin: string;
  env: Record<string, string | undefined>;
  action?: string;
}): Promise<ScriptResult> {
  const action = input.action ?? 'get';
  // Filter out undefined env entries so we don't pass `undefined` strings to
  // child_process. PATH is forwarded so curl + jq are resolvable.
  const env: Record<string, string> = {};
  if (process.env['PATH']) env['PATH'] = process.env['PATH'];
  for (const [k, v] of Object.entries(input.env)) {
    if (typeof v === 'string') env[k] = v;
  }
  const proc = spawn('bash', [SCRIPT_PATH, action], {
    env: env as NodeJS.ProcessEnv,
    stdio: ['pipe', 'pipe', 'pipe'],
  });
  proc.stdin.write(input.stdin);
  proc.stdin.end();
  let stdout = '';
  let stderr = '';
  proc.stdout.on('data', (c) => (stdout += c));
  proc.stderr.on('data', (c) => (stderr += c));
  const exit_code = await new Promise<number>((r) => {
    proc.on('exit', (code) => r(code ?? -1));
  });
  return { stdout, stderr, exit_code };
}

async function runGitCredentialFill(input: {
  url: string;
  env: Record<string, string>;
}): Promise<ScriptResult> {
  const home = process.env['HOME'] ?? process.cwd();
  const proc = spawn(
    'git',
    [
      '-c',
      'credential.helper=',
      '-c',
      `credential.helper=${SCRIPT_PATH}`,
      '-c',
      'credential.useHttpPath=true',
      'credential',
      'fill',
    ],
    {
      env: {
        PATH: process.env['PATH'],
        HOME: home,
        XDG_CONFIG_HOME: process.env['XDG_CONFIG_HOME'] ?? resolvePath(home, '.config'),
        ...input.env,
      } as NodeJS.ProcessEnv,
      stdio: ['pipe', 'pipe', 'pipe'],
    },
  );
  proc.stdin.end(`url=${input.url}\n\n`);
  let stdout = '';
  let stderr = '';
  proc.stdout.on('data', (chunk) => (stdout += chunk));
  proc.stderr.on('data', (chunk) => (stderr += chunk));
  const exit_code = await new Promise<number>((resolve) => {
    proc.on('exit', (code) => resolve(code ?? -1));
  });
  return { stdout, stderr, exit_code };
}

/**
 * Returns true iff `name` is on PATH. Uses `spawnSync('bash', ['-c', 'command
 * -v NAME'])` so we don't shell out via exec; the input is hard-coded to
 * literal tool names (no user-supplied data).
 */
function hasTool(name: string): boolean {
  const r = spawnSync('bash', ['-c', `command -v ${name}`], { stdio: 'ignore' });
  return r.status === 0;
}

const HAS_DEPS = hasTool('bash') && hasTool('curl') && hasTool('git') && hasTool('jq');

describe.skipIf(!HAS_DEPS)('orca-git-creds (bash helper)', () => {
  let mock: MockServer;

  beforeAll(async () => {
    mock = await startMockServer();
  });

  afterAll(async () => {
    await mock.close();
  });

  it('parses git stdin, posts to ORCA_GIT_CREDS_URL with Bearer auth, echoes username+password', async () => {
    mock.setResponse(200, { username: 'x-access-token', password: 'real-pat' });
    mock.resetCaptures();
    const stdin =
      'protocol=https\nhost=github.com\npath=/org/repo.git/info/refs?service=git-upload-pack\n\n';
    const r = await runHelper({
      stdin,
      env: {
        ORCA_GIT_CREDS_URL: mock.url,
        ORCA_GIT_CREDS_TOKEN: 'jwt-fake',
      },
    });
    expect(r.exit_code).toBe(0);
    expect(r.stdout).toContain('username=x-access-token');
    expect(r.stdout).toContain('password=real-pat');
    expect(mock.captures).toHaveLength(1);
    expect(mock.captures[0]!.authHeader).toBe('Bearer jwt-fake');
    expect(mock.captures[0]!.body).toEqual({
      protocol: 'https',
      host: 'github.com',
      path: '/org/repo.git/info/refs?service=git-upload-pack',
    });
  });

  it('receives the repository path when Git useHttpPath is enabled', async () => {
    mock.setResponse(200, { username: 'x-access-token', password: 'real-pat' });
    mock.resetCaptures();

    const result = await runGitCredentialFill({
      url: 'https://github.com/orca-ae/orca-agent-engine.git',
      env: {
        ORCA_GIT_CREDS_URL: mock.url,
        ORCA_GIT_CREDS_TOKEN: 'jwt-fake',
      },
    });

    expect(result).toMatchObject({ exit_code: 0, stderr: '' });
    expect(result.stdout).toContain('username=x-access-token');
    expect(mock.captures).toHaveLength(1);
    expect(mock.captures[0]!.body).toEqual({
      protocol: 'https',
      host: 'github.com',
      path: 'orca-ae/orca-agent-engine.git',
    });
  });

  it('forces cluster-local callbacks through the nested sandbox HTTP proxy', async () => {
    mock.setResponse(200, { username: 'x-access-token', password: 'real-pat' });
    mock.resetCaptures();

    const result = await runHelper({
      stdin: 'protocol=https\nhost=github.com\npath=orca-ae/orca-agent-engine.git\n\n',
      env: {
        ORCA_GIT_CREDS_URL: 'http://registry-host.opensandbox.svc.cluster.local:8080/v1/git-creds',
        ORCA_GIT_CREDS_TOKEN: 'jwt-fake',
        SANDBOX_RUNTIME: '1',
        http_proxy: mock.proxyUrl,
        NO_PROXY: '.local',
        no_proxy: '.local',
      },
    });

    expect(result).toMatchObject({ exit_code: 0, stderr: '' });
    expect(result.stdout).toContain('username=x-access-token');
    expect(result.stdout).toContain('password=real-pat');
    expect(mock.captures).toHaveLength(1);
    expect(mock.captures[0]!.authHeader).toBe('Bearer jwt-fake');
  });

  it('exits 0 silently when ORCA_GIT_CREDS_URL is missing (helper is opt-in)', async () => {
    mock.resetCaptures();
    const r = await runHelper({
      stdin: 'protocol=https\nhost=github.com\n\n',
      env: { ORCA_GIT_CREDS_TOKEN: 'jwt-fake' },
    });
    expect(r.exit_code).toBe(0);
    expect(r.stdout).toBe('');
    // No HTTP fired (URL was unset).
    expect(mock.captures).toHaveLength(0);
  });

  it('exits 0 silently when ORCA_GIT_CREDS_TOKEN is missing', async () => {
    mock.resetCaptures();
    const r = await runHelper({
      stdin: 'protocol=https\nhost=github.com\n\n',
      env: { ORCA_GIT_CREDS_URL: mock.url },
    });
    expect(r.exit_code).toBe(0);
    expect(r.stdout).toBe('');
    expect(mock.captures).toHaveLength(0);
  });

  it('exits 0 silently on 404 from the registry (helper falls through to no-creds)', async () => {
    mock.setResponse(404, { error: 'not found' });
    mock.resetCaptures();
    const r = await runHelper({
      stdin: 'protocol=https\nhost=github.com\npath=/missing/repo\n\n',
      env: {
        ORCA_GIT_CREDS_URL: mock.url,
        ORCA_GIT_CREDS_TOKEN: 'jwt-fake',
      },
    });
    expect(r.exit_code).toBe(0);
    expect(r.stdout).toBe('');
    expect(mock.captures).toHaveLength(1);
  });

  it('exits 0 silently on 401 (helper does not propagate auth errors to git)', async () => {
    mock.setResponse(401, { error: 'unauthorized' });
    mock.resetCaptures();
    const r = await runHelper({
      stdin: 'protocol=https\nhost=github.com\n\n',
      env: {
        ORCA_GIT_CREDS_URL: mock.url,
        ORCA_GIT_CREDS_TOKEN: 'jwt-fake',
      },
    });
    expect(r.exit_code).toBe(0);
    expect(r.stdout).toBe('');
    expect(mock.captures).toHaveLength(1);
  });

  it('exits 0 silently when the response is missing username/password fields', async () => {
    mock.setResponse(200, { error: 'no creds available' });
    mock.resetCaptures();
    const r = await runHelper({
      stdin: 'protocol=https\nhost=github.com\n\n',
      env: {
        ORCA_GIT_CREDS_URL: mock.url,
        ORCA_GIT_CREDS_TOKEN: 'jwt-fake',
      },
    });
    expect(r.exit_code).toBe(0);
    expect(r.stdout).toBe('');
    expect(mock.captures).toHaveLength(1);
  });

  it('handles store and erase operations as silent no-ops (only get is implemented)', async () => {
    mock.setResponse(200, { username: 'x-access-token', password: 'should-not-fire' });
    mock.resetCaptures();
    const stdin = 'protocol=https\nhost=github.com\nusername=x-access-token\npassword=secret\n\n';
    for (const op of ['store', 'erase']) {
      const r = await runHelper({
        stdin,
        env: {
          ORCA_GIT_CREDS_URL: mock.url,
          ORCA_GIT_CREDS_TOKEN: 'jwt-fake',
        },
        action: op,
      });
      expect(r.exit_code).toBe(0);
      expect(r.stdout).toBe('');
    }
    // store/erase must NOT have fired any HTTP traffic — the helper drains
    // stdin and exits silently.
    expect(mock.captures).toHaveLength(0);
  });
});

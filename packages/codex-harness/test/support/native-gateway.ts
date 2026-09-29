// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Optional cross-repository check: the caller supplies a locally built Gateway.
// Test credentials are generated here and never leave loopback HTTP.
import { createServer } from 'node:net';
import { generateKeyPairSync, sign } from 'node:crypto';
import { spawn } from 'node:child_process';
import { mkdtemp, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

export async function startNativeGateway(binary: string, upstream: string) {
  const root = await mkdtemp(join(tmpdir(), 'orca-native-gateway-'));
  const { privateKey, publicKey } = generateKeyPairSync('rsa', {
    modulusLength: 2048,
    publicKeyEncoding: { type: 'spki', format: 'pem' },
    privateKeyEncoding: { type: 'pkcs8', format: 'pem' },
  });
  const pem = join(root, 'public.pem');
  await writeFile(pem, publicKey);
  const probe = createServer();
  await new Promise<void>((resolve) => probe.listen(0, '127.0.0.1', resolve));
  const port = (probe.address() as { port: number }).port;
  await new Promise<void>((resolve) => probe.close(() => resolve()));
  const config = {
    server: { listen: `127.0.0.1:${port}`, admin_listen: '127.0.0.1:0' },
    identity: {
      scope_dims: ['workspace_id', 'session_id', 'llm_routes', 'llm_models'],
      validators: [
        {
          name: 'session',
          kind: 'jwt',
          issuers: ['orca-registry'],
          audiences: ['ai-gateway'],
          static_public_key_pem: pem,
          scope_from: {
            jwt_claims: {
              workspace_id: 'workspace_id',
              session_id: 'session_id',
              llm_routes: 'llm_routes',
              llm_models: 'llm_models',
            },
          },
        },
      ],
    },
    vaults: [{ name: 'openai-key', resolver: 'env', env_var: 'CODEX_TEST_UPSTREAM_KEY' }],
    destinations: {
      openai: { kind: 'openai', base_url: upstream, credentials: { vault: 'openai-key' } },
    },
    routes: [
      {
        name: 'llm-responses',
        match: { path: '/v1/responses' },
        strategy: { mode: 'fallback', targets: [{ destination: 'openai' }] },
      },
    ],
    plugins: {
      authorizers: [
        {
          name: 'scoped',
          kind: 'yaml_acl',
          required: true,
          rules: [
            {
              effect: 'allow',
              action: 'invoke',
              resource: { kind: 'model', provider: 'openai' },
              conditions: [{ resource_name_in_scope_list: { scope: 'llm_models' } }],
            },
            {
              effect: 'allow',
              action: 'invoke',
              resource: { kind: 'route' },
              conditions: [{ resource_name_in_scope_list: { scope: 'llm_routes' } }],
            },
          ],
        },
      ],
    },
  };
  const configPath = join(root, 'gateway.json');
  await writeFile(configPath, JSON.stringify(config));
  const child = spawn(binary, ['run', '--config', configPath], {
    env: { ...process.env, CODEX_TEST_UPSTREAM_KEY: 'test-upstream-key' },
    stdio: ['ignore', 'ignore', 'pipe'],
  });
  let stderr = '';
  child.stderr.on('data', (chunk) => {
    stderr = (stderr + String(chunk)).slice(-4096);
  });
  const exited = new Promise<void>((resolve) => {
    child.once('exit', () => resolve());
    child.once('error', () => resolve());
  });
  const close = async () => {
    child.kill();
    await exited;
    await rm(root, { recursive: true, force: true });
  };
  const url = `http://127.0.0.1:${port}`;
  try {
    const deadline = Date.now() + 10000;
    for (;;) {
      try {
        await fetch(url);
        break;
      } catch {
        /* wait for bind */
      }
      if (child.exitCode !== null || Date.now() > deadline)
        throw new Error(`Gateway did not start: ${stderr}`);
      await new Promise((resolve) => setTimeout(resolve, 20));
    }
  } catch (error) {
    await close();
    throw error;
  }
  const now = Math.floor(Date.now() / 1000);
  const encoded = [
    { alg: 'RS256', typ: 'JWT' },
    {
      iss: 'orca-registry',
      aud: 'ai-gateway',
      sub: 'ses_sdk',
      iat: now,
      exp: now + 300,
      workspace_id: 'ws_test',
      session_id: 'ses_sdk',
      llm_routes: ['llm-responses'],
      llm_models: ['gpt-5.4'],
    },
  ]
    .map((value) => Buffer.from(JSON.stringify(value)).toString('base64url'))
    .join('.');
  const apiKey = `${encoded}.${sign('RSA-SHA256', Buffer.from(encoded), privateKey).toString('base64url')}`;
  return { url: `${url}/v1`, apiKey, close };
}

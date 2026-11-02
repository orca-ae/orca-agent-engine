// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { execFileSync, spawnSync } from 'node:child_process';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { expect, it } from 'vitest';

const guard = fileURLToPath(
  new URL('../../../dsh-harness/profile/registry-origin.cjs', import.meta.url),
);

function run(script: string, args: string[] = []) {
  return spawnSync(
    process.execPath,
    ['--require', guard, '--input-type=module', '-e', script, ...args],
    {
      env: { ...process.env, NODE_OPTIONS: '' },
      encoding: 'utf8',
      timeout: 15_000,
    },
  );
}

it('rejects disallowed request/get argument forms before transport, including ESM named imports', () => {
  const result = run(
    [
      "import assert from 'node:assert/strict';",
      "import http, { request as httpRequest } from 'node:http';",
      "import https, { request as httpsRequest } from 'node:https';",
      'const denied = [',
      "  () => httpRequest('http://registry.npmjs.org/pkg'),",
      "  () => httpsRequest('https://example.invalid/pkg'),",
      "  () => https.get(new URL('https://registry.npmjs.org:444/pkg')),",
      "  () => https.request({hostname: 'example.invalid', path: '/pkg'}),",
      "  () => https.get('https://registry.npmjs.org/pkg', {hostname:'example.invalid'}),",
      "  () => https.request(new URL('https://registry.npmjs.org/pkg'), {port:444}),",
      "  () => https.request({host:'registry.npmjs.org', defaultPort:444}),",
      "  () => https.request({hostname:'registry.npmjs.org', socketPath:'/tmp/socket'}),",
      "  () => http.get({hostname:'registry.npmjs.org'}),",
      '];',
      "for (const request of denied) assert.throws(request, {code:'ORCA_DSH_REGISTRY_ORIGIN'});",
    ].join(String.fromCharCode(10)),
  );
  expect(result.status, result.stderr).toBe(0);
});

it('allows same-origin HTTPS redirects and rejects cross-origin hops with a policy error', () => {
  const fixture = mkdtempSync(join(tmpdir(), 'orca-installer-origin-'));
  try {
    execFileSync(
      'openssl',
      [
        'req',
        '-x509',
        '-newkey',
        'rsa:2048',
        '-nodes',
        '-keyout',
        join(fixture, 'key.pem'),
        '-out',
        join(fixture, 'cert.pem'),
        '-days',
        '1',
        '-subj',
        '/CN=registry.npmjs.org',
      ],
      { stdio: 'ignore' },
    );
    // A TLS fixture agent routes declared registry requests to loopback. Production has no such agent.
    const result = run(
      [
        "import assert from 'node:assert/strict';",
        "import https from 'node:https';",
        "import http from 'node:http';",
        "import tls from 'node:tls';",
        "import {readFileSync} from 'node:fs';",
        'const directory = process.argv[1]; let hits = 0; let forbiddenHits = 0;',
        'const server = https.createServer({',
        "  key:readFileSync(directory+'/key.pem'), cert:readFileSync(directory+'/cert.pem'),",
        '}, (request, response) => {',
        '  hits++;',
        "  if (request.url === '/blocked') { forbiddenHits++; response.end('forbidden'); return; }",
        '  const redirects = {',
        "    '/same':'/ok', '/cross':'https://example.invalid/blocked',",
        "    '/port':'https://registry.npmjs.org:444/blocked',",
        "    '/downgrade':'http://registry.npmjs.org/blocked',",
        "    '/relative':'//example.invalid/blocked',",
        '  };',
        '  if (redirects[request.url]) response.writeHead(302, {location:redirects[request.url]});',
        "  response.end('ok');",
        '});',
        "await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));",
        'class FixtureAgent extends https.Agent {',
        '  createConnection() { return tls.connect({',
        "    host:'127.0.0.1', port:server.address().port, rejectUnauthorized:false,",
        '  }); }',
        '}',
        'const agent = new FixtureAgent();',
        'async function fetch(url) {',
        '  const response = await new Promise((resolve,reject) => {',
        "    const client = new URL(url).protocol === 'https:' ? https : http;",
        "    client.get(url, {agent}, resolve).on('error', reject);",
        '  });',
        '  response.resume();',
        '  if (response.statusCode === 302) return fetch(new URL(response.headers.location, url));',
        '  return response.statusCode;',
        '}',
        'try {',
        "  assert.equal(await fetch('https://registry.npmjs.org/same'), 200);",
        "  for (const path of ['cross','port','downgrade','relative'])",
        "    await assert.rejects(fetch('https://registry.npmjs.org/'+path), {code:'ORCA_DSH_REGISTRY_ORIGIN'});",
        '  assert.equal(hits, 6); assert.equal(forbiddenHits, 0);',
        '  console.log(JSON.stringify({sameOriginRedirect:true, deniedRedirects:4, forbiddenHits}));',
        '} finally { agent.destroy(); await new Promise(resolve => server.close(resolve)); }',
      ].join(String.fromCharCode(10)),
      [fixture],
    );
    expect(result.status, result.stderr).toBe(0);
    expect(result.stdout).toContain('"forbiddenHits":0');
  } finally {
    rmSync(fixture, { recursive: true, force: true });
  }
});

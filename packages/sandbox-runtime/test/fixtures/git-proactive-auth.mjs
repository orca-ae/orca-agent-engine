// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Isolates initial Git authentication against a strict local 403 endpoint.
// This HTTP marker-only fixture does not qualify TLS-to-proxy or Cilium enforcement.
import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { createServer } from 'node:http';

const tags = process.argv.slice(2);
if (tags.length === 0)
  tags.push('orca-oip038-clients-debian:local', 'orca-oip038-clients-ubuntu:local');
const counts = { authorized: 0, denied: 0 };
const expected = 'Basic ' + Buffer.from('x-access-token:orca-marker').toString('base64');
const server = createServer((request, response) => {
  if (request.headers.authorization !== expected) {
    counts.denied++;
    response.writeHead(403).end();
    return;
  }
  counts.authorized++;
  const line = '# service=git-upload-pack\n';
  const packet = (Buffer.byteLength(line) + 4).toString(16).padStart(4, '0') + line;
  response.writeHead(200, { 'content-type': 'application/x-git-upload-pack-advertisement' });
  response.end(packet + '00000000');
});
await new Promise((resolve) => server.listen(0, '0.0.0.0', resolve));
try {
  const port = server.address().port;
  for (const tag of tags) {
    for (const proactive of [false, true]) {
      const before = { ...counts };
      const helper = '!f() { printf "username=x-access-token\npassword=orca-marker\n"; }; f';
      const args = [
        'run',
        '--rm',
        ...(process.platform === 'linux' ? ['--add-host=host.docker.internal:host-gateway'] : []),
        tag,
        'git',
        '-c',
        'credential.helper=' + helper,
        '-c',
        'http.proactiveAuth=' + (proactive ? 'basic' : 'none'),
        '-c',
        'credential.useHttpPath=true',
        'ls-remote',
        'http://host.docker.internal:' + port + '/owner/repo.git',
      ];
      const outcome = await new Promise((resolve) =>
        execFile('docker', args, { timeout: 20_000 }, (error) => resolve(error)),
      );
      if (proactive) {
        assert.equal(outcome, null);
        assert.equal(counts.denied, before.denied);
        assert.equal(counts.authorized, before.authorized + 1);
      } else {
        assert.ok(outcome);
        assert.equal(counts.denied, before.denied + 1);
        assert.equal(counts.authorized, before.authorized);
      }
    }
    console.log(
      tag + ': unauthenticated-first fails 403; proactive Basic succeeds on first request',
    );
  }
} finally {
  server.closeAllConnections();
  await new Promise((resolve) => server.close(resolve));
}

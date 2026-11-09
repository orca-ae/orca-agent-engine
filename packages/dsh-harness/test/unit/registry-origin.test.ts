// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { expect, it } from 'vitest';

it('validates logical registry URL origins independently of an HTTPS proxy agent default port', () => {
  const guard = fileURLToPath(new URL('../../profile/registry-origin.cjs', import.meta.url));
  const output = execFileSync(
    process.execPath,
    [
      '-e',
      [
        "const assert = require('node:assert/strict'); const https = require('node:https');",
        'let calls = 0; https.request = () => { calls++; };',
        'require(' + JSON.stringify(guard) + ');',
        "https.request('https://registry.npmjs.org/package', {agent:{defaultPort:80}});",
        "assert.throws(() => https.request('https://other.example/package', {agent:{defaultPort:80}}), /package sources/);",
        "assert.throws(() => https.request('https://registry.npmjs.org:8443/package', {agent:{defaultPort:80}}), /package sources/);",
        "assert.throws(() => https.request('http://registry.npmjs.org/package'), /package sources/);",
        "https.request({protocol:'https:',hostname:'registry.npmjs.org',port:'',agent:{defaultPort:80}});",
        "assert.throws(() => https.request({hostname:'registry.npmjs.org',port:80}), /package sources/);",
        "assert.throws(() => https.request('https://registry.npmjs.org/package', {hostname:'other.example'}), /package sources/);",
        "assert.throws(() => https.request('https://registry.npmjs.org/package', {createConnection(){}}), /package sources/);",
        "assert.equal(calls,2); process.stdout.write('pass');",
      ].join('\n'),
    ],
    { encoding: 'utf8' },
  );
  expect(output).toBe('pass');
});

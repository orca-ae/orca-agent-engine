// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Installer-only source policy for pinned pnpm's node-fetch HTTP client, not network isolation.
const http = require('node:http');
const https = require('node:https');
const { urlToHttpOptions } = require('node:url');
const { syncBuiltinESMExports } = require('node:module');

function checkOrigin(protocol, input, overrides) {
  const fromUrl = typeof input === 'string' || input instanceof URL;
  const options = fromUrl ? urlToHttpOptions(new URL(input)) : { ...input };
  if (fromUrl && overrides && typeof overrides === 'object') Object.assign(options, overrides);
  const port =
    options.port ||
    options.defaultPort ||
    options.agent?.defaultPort ||
    (protocol === 'https:' ? 443 : 80);
  if (
    (options.protocol ?? protocol) !== 'https:' ||
    String(options.hostname ?? options.host ?? 'localhost').toLowerCase() !==
      'registry.npmjs.org' ||
    String(port) !== '443' ||
    options.socketPath ||
    options.createConnection
  ) {
    const error = new Error('DSH installer requires https://registry.npmjs.org package sources');
    error.code = 'ORCA_DSH_REGISTRY_ORIGIN';
    throw error;
  }
}

for (const [client, protocol] of [
  [http, 'http:'],
  [https, 'https:'],
]) {
  for (const method of ['request', 'get']) {
    const original = client[method];
    client[method] = function (...args) {
      checkOrigin(protocol, args[0], args[1]);
      return Reflect.apply(original, this, args);
    };
  }
}
syncBuiltinESMExports();

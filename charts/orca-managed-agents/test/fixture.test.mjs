// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import assert from 'node:assert/strict';
import { once } from 'node:events';
import { after, before, test } from 'node:test';
import { server } from './kind/fixture-server.mjs';

let baseUrl;
before(async () => {
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  baseUrl = 'http://127.0.0.1:' + server.address().port;
});
after(async () => {
  server.closeAllConnections();
  await new Promise((resolve, reject) =>
    server.close((error) => (error ? reject(error) : resolve())),
  );
});

function rpc(body, headers = {}) {
  return fetch(baseUrl + '/mcp', {
    method: 'POST',
    headers: {
      authorization: 'Bearer kind-helm-e2e-secret',
      accept: 'application/json, text/event-stream',
      'content-type': 'application/json',
      ...headers,
    },
    body: JSON.stringify({ jsonrpc: '2.0', ...body }),
  });
}

async function summary() {
  return (await (await fetch(baseUrl + '/captured')).json()).mcp;
}

test('unsupported pre-initialize discovery falls back to a strictly session-bound tool round', async () => {
  const before = await summary();
  const discovery = await rpc({ id: 0, method: 'server/discover' });
  assert.equal(discovery.status, 200);
  assert.deepEqual(await discovery.json(), {
    jsonrpc: '2.0',
    id: 0,
    error: { code: -32601, message: 'Method not found' },
  });
  assert.equal(discovery.headers.get('mcp-session-id'), null);

  const initialized = await rpc({
    id: 1,
    method: 'initialize',
    params: {
      protocolVersion: '2025-06-18',
      capabilities: {},
      clientInfo: { name: 'fixture-test', version: '1.0.0' },
    },
  });
  assert.equal(initialized.status, 200);
  const sessionId = initialized.headers.get('mcp-session-id');
  assert.ok(sessionId);
  assert.equal((await initialized.json()).result.protocolVersion, '2025-06-18');
  const headers = { 'mcp-session-id': sessionId };
  const notification = await rpc({ method: 'notifications/initialized' }, headers);
  assert.equal(notification.status, 202);
  assert.equal(await notification.text(), '');
  const tools = await rpc({ id: 2, method: 'tools/list' }, headers);
  assert.equal(tools.status, 200);
  assert.equal((await tools.json()).result.tools[0].name, 'echo');
  const call = await rpc(
    {
      id: 3,
      method: 'tools/call',
      params: { name: 'echo', arguments: { message: 'KIND_HELM_DISCOVERY_TEST' } },
    },
    headers,
  );
  assert.equal(call.status, 200);
  assert.equal((await call.json()).result.content[0].text, 'MCP_OK KIND_HELM_DISCOVERY_TEST');

  const after = await summary();
  assert.equal(after.missingSessionIds, before.missingSessionIds);
  assert.equal(after.invalidSessionIds, before.invalidSessionIds);
  assert.equal(after.validSessionIds - before.validSessionIds, 3);
  assert.equal(after.unauthorized, before.unauthorized);
  assert.equal(after.invalidAccepts, before.invalidAccepts);
  assert.ok(after.methods.includes('server/discover'));
});

test('discovery still requires upstream authorization and Streamable HTTP Accept', async () => {
  const unauthorized = await rpc(
    { id: 0, method: 'server/discover' },
    { authorization: 'Bearer wrong' },
  );
  assert.equal(unauthorized.status, 401);
  assert.equal((await unauthorized.json()).error.code, -32001);
  const unacceptable = await rpc(
    { id: 0, method: 'server/discover' },
    { accept: 'application/json' },
  );
  assert.equal(unacceptable.status, 406);
  assert.equal((await unacceptable.json()).error.code, -32000);
});

test('session-bound requests still reject and count missing or invalid session IDs', async () => {
  const before = await summary();
  for (const method of ['notifications/initialized', 'tools/list', 'tools/call']) {
    const body = method.startsWith('notifications/') ? { method } : { id: 4, method };
    const missing = await rpc(body);
    assert.equal(missing.status, 400);
    assert.equal((await missing.json()).error.message, 'Missing MCP session ID');
    const invalid = await rpc(body, { 'mcp-session-id': 'wrong-session' });
    assert.equal(invalid.status, 404);
    assert.equal((await invalid.json()).error.message, 'Unknown MCP session ID');
  }
  const after = await summary();
  assert.equal(after.missingSessionIds - before.missingSessionIds, 3);
  assert.equal(after.invalidSessionIds - before.invalidSessionIds, 3);
});

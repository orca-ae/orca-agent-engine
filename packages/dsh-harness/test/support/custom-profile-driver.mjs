// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Runs in the test's disposable Linux container. Only model HTTP is scripted.
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { readFile, mkdir, writeFile } from 'node:fs/promises';
import { DshSdkWorker } from '/app/packages/dsh-harness/dist/index.js';

const profile = JSON.parse(await readFile('/fixture/descriptor.json', 'utf8'));
await mkdir('/tmp/work', { recursive: true });
const events = [];
const requests = [];
let scripts = [];
let allowed = true;
let checkpoint;
let worker;
let calls = 0;
let modelRequests = 0;
const server = createServer((req, res) => {
  const chunks = [];
  req.on('data', (chunk) => chunks.push(chunk));
  req.on('end', () => {
    const body = JSON.parse(Buffer.concat(chunks).toString());
    modelRequests++;
    requests.push(body);
    const block = scripts.shift();
    assert(block, 'unexpected model request');
    const tool = block.type === 'tool_use';
    res.writeHead(200, { 'content-type': 'text/event-stream' });
    const send = (frame) =>
      res.write('event: ' + frame.type + '\ndata: ' + JSON.stringify(frame) + '\n\n');
    send({
      type: 'message_start',
      message: {
        id: 'fixture',
        type: 'message',
        role: 'assistant',
        model: body.model,
        content: [],
        usage: { input_tokens: 4, output_tokens: 0 },
      },
    });
    send({
      type: 'content_block_start',
      index: 0,
      content_block: tool ? { ...block, input: {} } : block,
    });
    if (tool)
      send({
        type: 'content_block_delta',
        index: 0,
        delta: { type: 'input_json_delta', partial_json: JSON.stringify(block.input) },
      });
    send({ type: 'content_block_stop', index: 0 });
    send({
      type: 'message_delta',
      delta: { stop_reason: tool ? 'tool_use' : 'end_turn', stop_sequence: null },
      usage: { output_tokens: 2 },
    });
    send({ type: 'message_stop' });
    res.end();
  });
});
await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
const managedTool = {
  name: 'managed_fixture',
  description: 'Image-owned fixture schema',
  inputSchema: { type: 'object' },
};
const start = async (restored, tools = []) => {
  events.length = 0;
  worker = new DshSdkWorker((event) => {
    events.push(event);
    if (event.type === 'checkpoint') checkpoint = event.checkpoint;
    if (event.type === 'orchestration_request') {
      calls++;
      void worker.handle({ type: 'orchestration_result', id: event.id, allowed });
    }
  });
  await worker.handle({
    type: 'start',
    root: '/tmp/work',
    sessionId: 'ses_profile_fixture',
    model: 'deepseek-v4-flash',
    modelProvider: 'deepseek',
    system: 'PROFILE_FIXTURE_SYSTEM',
    apiKey: 'synthetic-not-a-provider-credential',
    dshGatewayUrl: 'http://127.0.0.1:' + server.address().port,
    tools,
    dshProfile: profile,
    dshManagedTools: [managedTool],
    archivedThreadSubpaths: [],
    ...(restored ? { checkpoint: restored } : {}),
  });
  const ready = events.find((event) => event.type === 'ready');
  assert(
    ready?.tools.some((tool) => tool.name === 'business_probe'),
    'initialize-only business registration absent',
  );
  assert.equal(ready.harness_profile.config_sha256, profile.config_sha256);
  assert.equal(requests.length, 0, 'initialization made a model request');
  return ready.harness_profile;
};
let seq = 0;
const turn = async (n) => {
  scripts = [
    { type: 'tool_use', id: 'call-' + ++seq, name: 'business_probe', input: { n } },
    { type: 'text', text: 'completed' },
  ];
  const from = events.length;
  await worker.handle({
    type: 'submit',
    text: 'Remember PROFILE_MEMORY and run the sample business tool.',
    sourceEventId: 'evt_profile_' + seq,
  });
  assert.equal(scripts.length, 0);
  assert(
    !events.slice(from).some((event) => event.type === 'failure'),
    JSON.stringify(events.slice(from)),
  );
  const canonical = events.slice(from).filter((event) => event.type === 'canonical_event');
  assert.equal(canonical.filter((event) => event.event.kind === 'agent.tool_use').length, 1);
  assert.equal(canonical.filter((event) => event.event.kind === 'agent.tool_result').length, 1);
};
const sideEffects = async () => {
  try {
    return (await readFile('/tmp/business-effects', 'utf8')).trim().split('\n').length;
  } catch (error) {
    if (error.code === 'ENOENT') return 0;
    throw error;
  }
};
try {
  if (process.env.ORCA_EXPECT_MANAGED_ALIAS) {
    await assert.rejects(start(), /aliases a managed module/);
    assert.equal(modelRequests, 0);
    console.log(JSON.stringify({ managedModuleAliasRejectedBeforeImports: true, modelRequests }));
  } else {
    const identity = await start();
    assert.equal(requests.length, 0);
    await turn(2);
    assert.equal(await sideEffects(), 1);
    assert.equal(calls, 1);
    const saved = checkpoint;
    assert(saved);
    allowed = false;
    await turn(3);
    assert.equal(await sideEffects(), 1, 'denied tool produced a side effect');
    await worker.close();
    requests.length = 0;
    assert.deepEqual(await start(saved, [managedTool]), identity, 'policy affected schema digest');
    const before = calls;
    await turn(4);
    assert.equal(calls, before + 1, 'plugin did not reach shared authorization');
    allowed = true;
    assert.equal(await sideEffects(), 1);
    assert(JSON.stringify(requests[0]).includes('PROFILE_MEMORY'), 'cold recovery lost context');
    await worker.close();
    requests.length = 0;
    const beforeDrift = { modelRequests, calls, effects: await sideEffects() };
    // Simulate a plugin deriving its schema from external state; keep the checkpoint intact.
    await writeFile('/tmp/orca-fixture-schema-type', 'string');
    const changed = await start();
    assert.notEqual(changed.tool_manifest_sha256, identity.tool_manifest_sha256);
    assert.deepEqual({ ...changed, tool_manifest_sha256: identity.tool_manifest_sha256 }, identity);
    await worker.close();
    await assert.rejects(start(saved), /checkpoint harness profile differs/);
    await worker.close();
    assert.equal(modelRequests, beforeDrift.modelRequests, 'schema drift reached the model');
    assert.equal(calls, beforeDrift.calls, 'schema drift reached tool authorization');
    assert.equal(await sideEffects(), beforeDrift.effects, 'schema drift executed a tool');
    requests.length = 0;
    await writeFile('/tmp/orca-fixture-schema-type', 'integer');
    await start(saved);
    scripts = [
      { type: 'tool_use', id: 'mutate', name: 'business_probe', input: { n: 7 } },
      { type: 'text', text: 'must not be requested' },
    ];
    await worker.handle({
      type: 'submit',
      text: 'Exercise a forbidden registry mutation.',
      sourceEventId: 'evt_mutate_profile',
    });
    assert(
      events.some((event) => event.type === 'failure'),
      'mid-turn mutation did not fail',
    );
    assert.equal(requests.length, 1, 'schema mutation reached another model request');
    assert.equal(scripts.length, 1);
    assert.equal(await sideEffects(), 2);
    console.log(
      JSON.stringify({
        bridge: 'real pinned DSH',
        runtime: {
          dsh: '0.2.1-alpha.2',
          node: process.version,
          platform: process.platform,
          arch: process.arch,
        },
        harness_profile: identity,
        initializedTool: true,
        toolExecuted: true,
        deniedWithoutSideEffects: true,
        canonicalCallResultPairs: true,
        coldRestore: true,
        policyIndependentManifest: true,
        schemaDriftRejected: true,
        midTurnMutationRejected: true,
        modelRequests,
        modelOrigin: 'loopback scripted HTTP only',
      }),
    );
  }
} finally {
  await worker?.close();
  server.closeAllConnections();
  await new Promise((resolve) => server.close(resolve));
}

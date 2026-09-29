// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/** Opt-in real-infrastructure fixture: bash services/dev/scripts/test-transcript-avro.sh. */
/* global fetch, AbortController, AbortSignal */
import assert from 'node:assert/strict';
import { randomBytes } from 'node:crypto';
import avro from 'avsc';
import { Kafka, logLevel } from 'kafkajs';
import {
  createKafkaTranscriptCodec,
  KafkaSessionEventSource,
  KafkaTranscriptStore,
  sessionTopicName,
} from '../../dist/index.js';
import { registryProxy } from './schema-registry-proxy.mjs';
import { assertDecodedEvent, createReadyEmptyTopic } from './schema-registry-fixture-helpers.mjs';

// Last-resort bound includes connect/disconnect and finally cleanup. A failed
// process returns control to the shell EXIT trap, which removes its containers.
// Unref permits normal exit, but keep this armed if a leaked socket stays alive.
setTimeout(() => {
  console.error('FAIL genericKafkaSR exceeded 180s including resource cleanup');
  process.exit(1);
}, 180000).unref();

const registry = 'http://127.0.0.1:18081';
const subject = 'orca.transcript.TranscriptEvent';
const kafka = new Kafka({
  brokers: ['127.0.0.1:19092'],
  clientId: 'transcript-avro-fixture',
  logLevel: logLevel.ERROR,
});
const admin = kafka.admin();
const codecs = [];
const stores = [];
const topics = [];
let proxy;
const run = Date.now().toString(36);
const workspaceId = 'ws_avro';
let seq = 0;
function event(sessionId, kind, payload, userId) {
  return {
    id: 'evt_' + run + '_' + ++seq,
    workspaceId,
    sessionId,
    subpath: '',
    seq: 0,
    producedAt: '2026-09-11T00:00:00.000Z',
    producedBy: 'client',
    kind,
    payload: Buffer.from(payload),
    idempotencyKey: '',
    ...(userId === undefined ? {} : { userId }),
  };
}
async function request(path, body) {
  const response = await fetch(registry + path, {
    ...(body === undefined ? {} : { method: 'POST', body: JSON.stringify(body) }),
    headers: { 'content-type': 'application/vnd.schemaregistry.v1+json' },
    signal: AbortSignal.timeout(10000),
  });
  assert.ok(response.ok, path + ': ' + response.status + ' ' + (await response.clone().text()));
  return response.json();
}
function writer(options = {}) {
  const codec = createKafkaTranscriptCodec({
    encoding: 'avro',
    schemaRegistry: { url: registry, ...options },
  });
  codecs.push(codec);
  const store = new KafkaTranscriptStore({ kafka, codec });
  stores.push(store);
  return { codec, store };
}
async function topicFor(sessionId, encoding = 'avro') {
  const topic = sessionTopicName(workspaceId, sessionId, '', encoding);
  assert.equal(
    topic,
    `orca.${workspaceId}.sessions.${sessionId}.events${encoding === 'avro' ? '-avro' : ''}`,
  );
  topics.push(topic);
  const offsets = await createReadyEmptyTopic(admin, topic);
  console.log(
    'PASS genericKafkaSR empty topic metadata offsets (no seed records)',
    topic,
    JSON.stringify(offsets),
  );
  return topic;
}
async function verifyReader(mode, sessionId, expected, schemaRegistry, encoding = 'avro') {
  // Each operation gets a genuinely cold decoder, independent of all writers.
  const decoder = createKafkaTranscriptCodec({ encoding, schemaRegistry });
  decoder.prepareWriter = async () => {
    assert.fail('read-only codec must never prepare/register a writer');
  };
  const reader = new KafkaTranscriptStore({ kafka, codec: decoder });
  codecs.push(decoder);
  stores.push(reader);
  const controller = new AbortController();
  let timer;
  const operation = async () => {
    const actual = [];
    const iterable =
      mode === 'read'
        ? reader.read(workspaceId, sessionId, {
            fromCursor: '',
            maxEvents: 0,
            subpath: '*',
            signal: controller.signal,
          })
        : reader.tail(workspaceId, sessionId, {
            fromCursor: '0',
            subpath: '*',
            signal: controller.signal,
          });
    try {
      for await (const value of iterable) {
        actual.push({ ...value, payload: Buffer.from(value.payload) });
        // Explicit-cursor tail is unbounded: early break must stop its consumer.
        if (mode === 'tail' && actual.length === expected.length) break;
      }
      assert.deepEqual(
        actual,
        expected.map((value, index) => ({
          ...value,
          seq: index,
          payload: Buffer.from(value.payload),
        })),
      );
      console.log(
        'PASS Orca ' +
          encoding +
          ' cold-cache ' +
          mode +
          ': exact IDs, sequential offsets, all metadata and bytes',
      );
    } finally {
      controller.abort();
      await reader.close();
      await decoder.close();
    }
  };
  try {
    await Promise.race([
      operation(),
      new Promise((_, reject) => {
        timer = setTimeout(() => {
          controller.abort();
          reject(new Error(encoding + ' ' + mode + ' exceeded 20s, including consumer cleanup'));
        }, 20000);
      }),
    ]);
  } finally {
    clearTimeout(timer);
    controller.abort();
  }
}

// Independent consumer: KafkaJS messages -> wire ID -> fetched schema -> standard avsc.
// No Orca decoder participates in these assertions.
async function consume(topic, count) {
  const consumer = kafka.consumer({
    groupId: 'genericKafkaSR-' + run + '-' + ++seq,
    retry: { retries: 3 },
  });
  let timer;
  const messages = [];
  try {
    await consumer.connect();
    await consumer.subscribe({ topic, fromBeginning: true });
    await new Promise((resolve, reject) => {
      timer = setTimeout(() => reject(new Error('Kafka consume exceeded 30s: ' + topic)), 30000);
      consumer.on(consumer.events.CRASH, ({ payload }) => reject(payload.error));
      consumer
        .run({
          autoCommit: false,
          eachMessage: async ({ message }) => {
            messages.push(message);
            if (messages.length === count) resolve();
          },
        })
        .catch(reject);
    });
    return messages;
  } finally {
    clearTimeout(timer);
    await consumer.disconnect();
  }
}
async function standardDecode(message) {
  assert.equal(message.value[0], 0, 'standard Confluent wire magic');
  const id = message.value.readUInt32BE(1);
  const schema = await request('/schemas/ids/' + id);
  const type = avro.Type.forSchema(JSON.parse(schema.schema));
  return { id, type, record: type.fromBuffer(message.value.subarray(5)) };
}
function assertHeaders(message, e, encoding) {
  const fields = {
    id: e.id,
    workspace_id: e.workspaceId,
    session_id: e.sessionId,
    subpath: e.subpath,
    produced_at: e.producedAt,
    produced_by: e.producedBy,
    kind: e.kind,
    idempotency_key: e.idempotencyKey,
    user_id: e.userId,
    ...(encoding === 'avro' ? { orca_transcript_encoding: 'avro-v1' } : {}),
  };
  assert.deepEqual(
    message.headers,
    Object.fromEntries(
      Object.entries(fields)
        .filter(([, value]) => value)
        .map(([key, value]) => [key, Buffer.from(value)]),
    ),
  );
}
async function verifyStandard(topic, events, schemaId) {
  const messages = await consume(topic, events.length);
  for (let index = 0; index < events.length; index++) {
    const e = events[index];
    const message = messages[index];
    assert.equal(message.offset, String(index));
    assert.equal(message.key.toString(), e.id);
    assertHeaders(message, e, 'avro');
    const { id, record } = await standardDecode(message);
    assert.equal(id, schemaId);
    assert.deepEqual(
      { ...record },
      {
        id: e.id,
        workspace_id: e.workspaceId,
        session_id: e.sessionId,
        subpath: e.subpath,
        produced_at: e.producedAt,
        produced_by: e.producedBy,
        kind: e.kind,
        payload: Buffer.from(e.payload),
        idempotency_key: e.idempotencyKey,
        user_id: e.userId ?? null,
      },
    );
    assert.equal(record.payload.toString('hex'), Buffer.from(e.payload).toString('hex'));
    console.log(
      'PASS independent Avro ID/offset/metadata/payload hex',
      e.id,
      message.offset,
      record.payload.toString('hex'),
    );
  }
  return messages;
}
async function verifyDiscovery(encoding, expected, schemaRegistry) {
  const codec = createKafkaTranscriptCodec({ encoding, schemaRegistry });
  codec.prepareWriter = async () => assert.fail('event source must never prepare a writer');
  codecs.push(codec);
  const subscriptions = [];
  let onBatchProcessed;
  const rawTopic = sessionTopicName(workspaceId, expected.sessionId);
  const source = new KafkaSessionEventSource({
    // Observe subscriptions without replacing any broker I/O. Both topic sets
    // are returned by the real Admin; the extra pattern admits either suffix.
    kafka: {
      admin: () => kafka.admin(),
      consumer: (options) => {
        const consumer = kafka.consumer(options);
        consumer.on(consumer.events.END_BATCH_PROCESS, () => onBatchProcessed?.());
        const subscribe = consumer.subscribe.bind(consumer);
        consumer.subscribe = async (options) => {
          subscriptions.push(options.topic);
          return subscribe(options);
        };
        return consumer;
      },
    },
    codec,
    groupId: 'fixture-discovery-' + run + '-' + encoding,
    topicPattern: new RegExp('^' + rawTopic.replaceAll('.', '\\.') + '(?:-avro)?$'),
    topicDiscoveryIntervalMs: 100,
  });
  let timer;
  const actual = [];
  try {
    await new Promise((resolve, reject) => {
      timer = setTimeout(() => reject(new Error('event source discovery exceeded 20s')), 20000);
      // Do not stop inside the handler: the source must acknowledge the event
      // before shutdown fences the consumer generation.
      onBatchProcessed = () => {
        if (actual.length > 0) resolve();
      };
      source
        .start(async (value) => {
          actual.push({ ...value, payload: Buffer.from(value.payload) });
        })
        .catch(reject);
    });
  } finally {
    clearTimeout(timer);
    await source.stop();
    await codec.close();
  }
  assert.deepEqual(subscriptions, [rawTopic + (encoding === 'avro' ? '-avro' : '')]);
  assert.deepEqual(actual, [{ ...expected, seq: 0, payload: Buffer.from(expected.payload) }]);
  console.log(
    'PASS real event-source ' +
      encoding +
      ' discovery: only selected topic subscribed; exact event bytes/metadata',
  );
}
function decoder(schemaRegistry) {
  const codec = createKafkaTranscriptCodec({ encoding: 'avro', schemaRegistry });
  codec.prepareWriter = async () =>
    assert.fail('read-only codec must never prepare/register a writer');
  codecs.push(codec);
  return codec;
}

try {
  await admin.connect();
  proxy = await registryProxy();
  const secureOptions = {
    url: 'https://127.0.0.1:18085',
    auth: proxy.auth,
    tls: { ca: proxy.ca },
    maxAttempts: 1,
  };
  const { codec, store } = writer(secureOptions);
  await codec.prepareWriter();
  const versions = await request('/subjects/' + subject + '/versions');
  assert.deepEqual(versions, [1], 'fresh isolated Registry required; clean retained fixture first');
  const initial = await request('/subjects/' + subject + '/versions/1');
  const sessionId = 'ses_' + run;
  const topic = await topicFor(sessionId);
  const events = [
    event(
      sessionId,
      'user.message',
      ' { "tool": {"name":"搜索","arguments":{"q":"中文"}}, "content": [{"type":"text","text":"你好，世界 🌊"}] } ',
      'user_中文',
    ),
    event(sessionId, 'opaque.binary', randomBytes(64)),
    event(sessionId, 'opaque.binary', Buffer.from([0, 255, 128, 1])),
    event(sessionId, 'session.archived', Buffer.alloc(0)),
  ];
  await store.append(workspaceId, sessionId, events);
  assert.deepEqual(await request('/subjects'), [subject], 'record-name subject, not topic-value');
  const messages = await verifyStandard(topic, events, initial.id);
  const { record: business } = await standardDecode(messages[0]);
  assert.deepEqual(JSON.parse(business.payload.toString('utf8')), {
    content: [{ type: 'text', text: '你好，世界 🌊' }],
    tool: { name: '搜索', arguments: { q: '中文' } },
  });
  await verifyReader('read', sessionId, events, secureOptions);
  await verifyReader('tail', sessionId, events, secureOptions);
  console.log('PASS genericKafkaSR automatic registration, standard Avro bytes and business JSON');

  const evolved = JSON.parse(initial.schema);
  evolved.fields.push({ name: 'fixture_optional', type: ['null', 'string'], default: null });
  const v2 = await request('/subjects/' + subject + '/versions', {
    schemaType: 'AVRO',
    schema: JSON.stringify(evolved),
  });
  assert.notEqual(v2.id, initial.id);
  const incompatible = JSON.parse(initial.schema);
  incompatible.fields.find((field) => field.name === 'payload').type = 'long';
  const rejectedSchema = await fetch(registry + '/subjects/' + subject + '/versions', {
    method: 'POST',
    headers: { 'content-type': 'application/vnd.schemaregistry.v1+json' },
    body: JSON.stringify({ schemaType: 'AVRO', schema: JSON.stringify(incompatible) }),
    signal: AbortSignal.timeout(10000),
  });
  assert.equal(rejectedSchema.status, 409);
  const exact = writer({ ...secureOptions, autoRegister: false });
  await exact.codec.prepareWriter();
  assert.equal((await exact.codec.encode(events[0])).value.readUInt32BE(1), initial.id);
  const evolvedType = avro.Type.forSchema(evolved);
  for (const message of await verifyStandard(topic, events, initial.id)) {
    const { type } = await standardDecode(message);
    const resolved = evolvedType.fromBuffer(
      message.value.subarray(5),
      evolvedType.createResolver(type),
    );
    assert.equal(resolved.fixture_optional, null);
  }
  await verifyReader('read', sessionId, events, secureOptions);
  console.log(
    'PASS schema evolution, incompatible schema rejection, earliest replay, exact v1 lookup',
  );

  const preregSubject = 'orca.transcript.fixture_preregistered';
  const preregId = await request('/subjects/' + preregSubject + '/versions', {
    schemaType: 'AVRO',
    schema: initial.schema,
  });
  const prereg = writer({ ...secureOptions, subject: preregSubject, autoRegister: false });
  await prereg.codec.prepareWriter();
  const preregSession = sessionId + '_pre';
  const preregTopic = await topicFor(preregSession);
  const preregEvents = [event(preregSession, 'user.message', '{"content":"预注册"}')];
  await prereg.store.append(workspaceId, preregSession, preregEvents);
  await verifyStandard(preregTopic, preregEvents, preregId.id);
  assert.deepEqual(await request('/subjects/' + preregSubject + '/versions'), [1]);
  assert.ok(
    proxy.requests.some(
      (r) => r.method === 'POST' && r.path === '/subjects/' + preregSubject && r.authenticated,
    ),
  );
  assert.ok(
    proxy.requests.some(
      (r) => r.method === 'POST' && r.path === '/subjects/' + subject + '/versions',
    ),
  );

  const deniedOptions = { ...secureOptions, auth: { ...proxy.auth, password: 'wrong' } };
  const denied = writer(deniedOptions);
  await assert.rejects(denied.codec.prepareWriter(), (e) => e.code === 'registry_auth');
  const route = { workspaceId, sessionId };
  await assert.rejects(
    decoder(deniedOptions).decode(messages[0], route),
    (e) => e.code === 'registry_auth',
  );
  const recovering = decoder(secureOptions);
  proxy.state.unavailable = true;
  await assert.rejects(
    recovering.decode(messages[0], route),
    (e) => e.code === 'registry_unavailable',
  );
  const recoveringWriter = writer({ ...secureOptions, autoRegister: false });
  await assert.rejects(
    recoveringWriter.codec.prepareWriter(),
    (e) => e.code === 'registry_unavailable',
  );
  proxy.state.unavailable = false;
  assert.deepEqual(
    Buffer.from((await recovering.decode(messages[0], route)).payload),
    events[0].payload,
  );
  await recoveringWriter.codec.prepareWriter();
  console.log(
    'PASS actual codec rejects 401/503 without raw fallback; failed reader/writer retry recovers',
  );

  // Valid outer Avro retains invalid inner JSON/UTF-8 bytes without interpreting them.
  const badSession = sessionId + '_badjson';
  const badTopic = await topicFor(badSession);
  const invalid = [
    event(badSession, 'user.message', '{invalid'),
    event(badSession, 'user.message', Buffer.from([255])),
  ];
  await store.append(workspaceId, badSession, invalid);
  for (const message of await verifyStandard(badTopic, invalid, initial.id)) {
    const { record } = await standardDecode(message);
    assert.throws(() => JSON.parse(record.payload.toString('utf8')), SyntaxError);
  }
  await verifyReader('read', badSession, invalid, secureOptions);

  const isolatedSession = sessionId + '_isolated';
  const rawTopic = await topicFor(isolatedSession, 'raw');
  const avroTopic = sessionTopicName(workspaceId, isolatedSession, '', 'avro');
  assert.equal(avroTopic, rawTopic + '-avro');
  assert.ok(!(await admin.listTopics()).includes(avroTopic));
  topics.push(avroTopic);
  const legacy = new KafkaTranscriptStore({ kafka });
  stores.push(legacy);
  const raw = event(isolatedSession, 'user.message', '{"content":"legacy is not Avro"}');
  raw.idempotencyKey = 'isolated-legacy-key';
  await legacy.append(workspaceId, isolatedSession, [raw]);
  const rawBefore = await consume(rawTopic, 1);
  assert.equal(rawBefore[0].offset, '0');
  assert.equal(rawBefore[0].key.toString(), raw.id);
  assert.deepEqual(rawBefore[0].value, raw.payload);
  assertHeaders(rawBefore[0], raw, 'raw');
  const rawOffsets = await admin.fetchTopicOffsets(rawTopic);
  assert.equal(rawOffsets[0].high, '1');
  await legacy.close();
  await verifyReader('read', isolatedSession, [raw], secureOptions, 'raw');
  assert.ok(!(await admin.listTopics()).includes(avroTopic));
  const retained = event(
    isolatedSession,
    'user.message',
    ' { "content": "Avro 中文 survives" } ',
    'user_isolated_中文',
  );
  retained.subpath = 'subagents/isolated';
  retained.producedBy = 'harness';
  retained.idempotencyKey = 'isolated-avro-key';
  // The append, not fixture precreation, must create the separate Avro topic.
  await store.append(workspaceId, isolatedSession, [retained]);
  await verifyStandard(avroTopic, [retained], initial.id);
  assert.equal((await admin.fetchTopicOffsets(avroTopic))[0].high, '1');
  assert.deepEqual(await admin.fetchTopicOffsets(rawTopic), rawOffsets);
  assert.deepEqual(await consume(rawTopic, 1), rawBefore, 'legacy wire records remain unchanged');
  await verifyReader('read', isolatedSession, [retained], { url: registry });
  const beforeTail = proxy.requests.length;
  await verifyReader('tail', isolatedSession, [retained], secureOptions);
  const tailRequests = proxy.requests.slice(beforeTail);
  assert.ok(
    tailRequests.some(
      (r) => r.method === 'GET' && r.path === '/schemas/ids/' + initial.id && r.authenticated,
    ),
  );
  assert.ok(tailRequests.every((r) => r.method === 'GET'));
  const beforeRawReopen = proxy.requests.length;
  await verifyReader('read', isolatedSession, [raw], secureOptions, 'raw');
  await verifyReader('tail', isolatedSession, [raw], secureOptions, 'raw');
  assert.equal(
    proxy.requests.length,
    beforeRawReopen,
    'raw mode with Registry URL must not fetch Avro schemas',
  );
  console.log(
    'PASS same-session mode change: raw .events unchanged; Avro .events-avro starts at offset 0; cold read/tail and raw reopen isolated',
  );

  await verifyDiscovery('raw', raw, secureOptions);
  await verifyDiscovery('avro', retained, secureOptions);

  const wrongSession = sessionId + '_wrongid';
  const wrongTopic = await topicFor(wrongSession);
  const wrongEvent = event(wrongSession, 'opaque.binary', Buffer.from([0, 255]));
  const wrongMessage = await codec.encode(wrongEvent);
  wrongMessage.value = Buffer.from(wrongMessage.value);
  wrongMessage.value.writeUInt32BE(2147483647, 1);
  const producer = kafka.producer();
  try {
    await producer.connect();
    await producer.send({ topic: wrongTopic, messages: [wrongMessage] });
  } finally {
    await producer.disconnect();
  }
  const afterWrong = event(wrongSession, 'user.message', '{"content":"after wrong ID"}');
  await store.append(workspaceId, wrongSession, [afterWrong]);
  const wrongMessages = await consume(wrongTopic, 2);
  const cold = decoder(secureOptions);
  const wrongRoute = { workspaceId, sessionId: wrongSession };
  await assert.rejects(
    cold.decode(wrongMessages[0], wrongRoute),
    (e) => e.code === 'registry_not_found',
  );
  const unknownResponse = await fetch(registry + '/schemas/ids/2147483647', {
    signal: AbortSignal.timeout(10000),
  });
  assert.equal(unknownResponse.status, 404);
  await unknownResponse.arrayBuffer();
  const decodedAfterWrong = await cold.decode(wrongMessages[1], wrongRoute);
  assertDecodedEvent(decodedAfterWrong, { ...afterWrong, seq: 1 });
  // A normal Orca iterator fails at the unknown ID: no silent skip or raw fallback.
  const poisonReader = new KafkaTranscriptStore({ kafka, codec: decoder(secureOptions) });
  stores.push(poisonReader);
  await assert.rejects(
    async () => {
      for await (const value of poisonReader.read(workspaceId, wrongSession, {
        fromCursor: '',
        maxEvents: 0,
        subpath: '*',
        signal: AbortSignal.timeout(20000),
      }))
        assert.fail('unknown ID must throw before yielding ' + value.id);
    },
    (e) => e.code === 'registry_not_found',
  );
  assert.ok(
    proxy.requests.every((r) => !r.path.endsWith('/latest') && r.method !== 'PUT'),
    'production codecs never request latest or mutate compatibility',
  );
  console.log('PASS unknown wire ID throws, valid subsequent message decodes independently');
  console.log('PASS genericKafkaSR: actual Apache Kafka + separate Schema Registry fixture');
} finally {
  await Promise.allSettled(stores.map((store) => store.close()));
  await Promise.allSettled(codecs.map((codec) => codec.close()));
  await proxy?.close();
  if (topics.length) await admin.deleteTopics({ topics }).catch(() => {});
  await admin.disconnect();
}

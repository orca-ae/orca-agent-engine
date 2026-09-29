// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, expectTypeOf } from 'vitest';
import { createServer } from 'node:http';
import { createServer as createHttpsServer } from 'node:https';
import { cert, key } from './tls-fixture.js';
import { registry as metrics } from '../../../src/metrics.js';
import avro from 'avsc';
import type { ServerResponse, IncomingMessage } from 'node:http';
import { transcriptEventSchema } from '../../../src/kafka/schemas/transcript-event.js';
import type { RecordBatchEntry } from 'kafkajs';
import { createKafkaTranscriptCodec, type KafkaTranscriptCodec } from '../../../src/kafka/codec.js';
import { eventToKafkaMessage } from '../../../src/kafka/serialize.js';
const event = {
  id: 'e',
  workspaceId: 'w',
  sessionId: 's',
  subpath: '',
  seq: 0,
  producedAt: '2026-09-11T00:00:00Z',
  producedBy: 'user',
  kind: 'user.message',
  payload: new Uint8Array([0, 255, 1]),
  idempotencyKey: '',
};
const route = { workspaceId: 'w', sessionId: 's' };
async function stub(handler: (req: IncomingMessage, res: ServerResponse) => void | Promise<void>) {
  const server = createServer(handler);
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  return {
    url: 'http://127.0.0.1:' + (server.address() as { port: number }).port,
    close: () =>
      new Promise<void>((resolve) => {
        server.closeAllConnections();
        server.close(() => resolve());
      }),
  };
}
function framed(id = 1, schema = transcriptEventSchema, extra: Record<string, unknown> = {}) {
  const prefix = Buffer.alloc(5);
  prefix.writeUInt32BE(id, 1);
  return message({
    ...eventToKafkaMessage(event),
    headers: {
      ...eventToKafkaMessage(event).headers,
      orca_transcript_encoding: Buffer.from('avro-v1'),
    },
    value: Buffer.concat([
      prefix,
      avro.Type.forSchema(schema).toBuffer({
        id: 'e',
        workspace_id: 'w',
        session_id: 's',
        subpath: '',
        produced_at: event.producedAt,
        produced_by: 'user',
        kind: 'user.message',
        payload: Buffer.from(event.payload),
        idempotency_key: '',
        user_id: null,
        ...extra,
      }),
    ]),
  });
}
function message(wire: ReturnType<typeof eventToKafkaMessage>): RecordBatchEntry {
  return {
    ...wire,
    key: Buffer.from(String(wire.key)),
    value: wire.value as Buffer,
    offset: '42',
    timestamp: '0',
    attributes: 0,
  } as RecordBatchEntry;
}
describe('Kafka transcript codec public seam', () => {
  it('exposes the selected encoding as readonly optional routing metadata', async () => {
    expectTypeOf<Pick<KafkaTranscriptCodec, 'encoding'>>().toEqualTypeOf<{
      readonly encoding?: 'raw' | 'avro';
    }>();
    for (const encoding of [undefined, 'raw', 'avro'] as const) {
      const codec = createKafkaTranscriptCodec({
        ...(encoding === undefined ? {} : { encoding }),
        schemaRegistry: { url: 'http://localhost:1' },
      });
      try {
        expect(codec.encoding).toBe(encoding ?? 'raw');
      } finally {
        await codec.close();
      }
    }
  });
  it.each(['.', '..'])(
    'rejects subject %s that URL normalization would route to another endpoint',
    (subject) => {
      expect(() =>
        createKafkaTranscriptCodec({
          schemaRegistry: { url: 'http://localhost:1/prefix', subject },
        }),
      ).toThrow();
    },
  );
  it('reports bounded schema/cache/decode classifications without identity labels', async () => {
    const server = await stub((_req, res) => {
      res.end(JSON.stringify({ schema: JSON.stringify(transcriptEventSchema) }));
    });
    const codec = createKafkaTranscriptCodec({ schemaRegistry: { url: server.url } });
    try {
      await codec.decode(framed(), route);
      await codec.decode(framed(), route);
      await expect(
        codec.decode({ ...framed(), key: Buffer.from('wrong') }, route),
      ).rejects.toMatchObject({ code: 'record' });
      const rendered = await metrics.metrics();
      expect(rendered).toContain('transcript_store_schema_cache_total{result="hit"}');
      expect(rendered).toContain('transcript_store_schema_cache_total{result="miss"}');
      expect(rendered).toContain(
        'transcript_store_schema_operation_seconds_count{operation="read",result="success"}',
      );
      expect(rendered).toContain('transcript_store_decode_failures_total{code="record"}');
    } finally {
      await codec.close();
      await server.close();
    }
  });
  it.each(['aborted', 'closed'] as const)(
    'counts early %s decode failures exactly once',
    async (code) => {
      const codec = createKafkaTranscriptCodec();
      const controller = new AbortController();
      const counter = metrics.getSingleMetric('transcript_store_decode_failures_total')!;
      const before =
        (await counter.get()).values.find((value) => value.labels.code === code)?.value ?? 0;
      try {
        if (code === 'closed') await codec.close();
        else controller.abort();
        await expect(
          codec.decode(message(eventToKafkaMessage(event)), route, controller.signal),
        ).rejects.toMatchObject({ code });
        expect(
          (await counter.get()).values.find((value) => value.labels.code === code)?.value,
        ).toBe(before + 1);
      } finally {
        await codec.close();
      }
    },
  );
  it('verifies custom CA, presents mTLS identity and sends independent Basic credentials over HTTPS', async () => {
    const authorizations: string[] = [];
    const server = createHttpsServer(
      { cert, key, ca: cert, requestCert: true, rejectUnauthorized: true },
      (req, res) => {
        authorizations.push(req.headers.authorization ?? '');
        res.end(JSON.stringify({ id: 1 }));
      },
    );
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
    const url = 'https://localhost:' + (server.address() as { port: number }).port;
    const trusted = createKafkaTranscriptCodec({
      encoding: 'avro',
      schemaRegistry: {
        url,
        auth: { username: 'public', password: 'raw-jwt' },
        tls: { ca: cert, cert, key },
      },
    });
    const untrusted = createKafkaTranscriptCodec({
      encoding: 'avro',
      schemaRegistry: { url, maxAttempts: 1 },
    });
    try {
      await expect(untrusted.prepareWriter()).rejects.toMatchObject({
        code: 'registry_unavailable',
      });
      await trusted.prepareWriter();
      expect(authorizations).toEqual(['Basic cHVibGljOnJhdy1qd3Q=']);
    } finally {
      await trusted.close();
      await untrusted.close();
      server.closeAllConnections();
      await new Promise<void>((resolve) => server.close(() => resolve()));
    }
  });
  it('resolves old defaulted fields and ignores new writer fields without losing payload bytes', async () => {
    const base = transcriptEventSchema as avro.schema.RecordType;
    const oldSchema = {
      ...base,
      fields: base.fields.filter(
        (field) => !['subpath', 'idempotency_key', 'user_id'].includes(field.name),
      ),
    };
    const newSchema = {
      ...base,
      fields: [...base.fields, { name: 'future', type: 'string' as const, default: '' }],
    };
    const server = await stub((req, res) => {
      res.end(
        JSON.stringify({ schema: JSON.stringify(req.url?.endsWith('/1') ? oldSchema : newSchema) }),
      );
    });
    const codec = createKafkaTranscriptCodec({ schemaRegistry: { url: server.url } });
    try {
      expect(await codec.decode(framed(1, oldSchema), route)).toEqual({ ...event, seq: 42 });
      expect(await codec.decode(framed(2, newSchema, { future: 'value' }), route)).toEqual({
        ...event,
        seq: 42,
      });
    } finally {
      await codec.close();
      await server.close();
    }
  });
  it('sanitizes synchronous TLS setup errors', async () => {
    const codec = createKafkaTranscriptCodec({
      encoding: 'avro',
      schemaRegistry: {
        url: 'https://localhost:1',
        tls: { cert: Buffer.from('SECRET'), key: Buffer.from('SECRET') },
        maxAttempts: 1,
      },
    });
    try {
      await expect(codec.prepareWriter()).rejects.toMatchObject({
        code: 'configuration',
        retryable: false,
        message: 'Kafka transcript codec: configuration',
      });
    } finally {
      await codec.close();
    }
  });
  it('honors Retry-After without exceeding the operation deadline', async () => {
    let calls = 0;
    const server = await stub((_req, res) => {
      calls++;
      res.writeHead(429, { 'retry-after': '5' });
      res.end('{}');
    });
    const codec = createKafkaTranscriptCodec({
      encoding: 'avro',
      schemaRegistry: { url: server.url, operationTimeoutMs: 350 },
    });
    try {
      await expect(codec.prepareWriter()).rejects.toMatchObject({
        code: 'registry_unavailable',
        retryable: true,
      });
      expect(calls).toBe(1);
    } finally {
      await codec.close();
      await server.close();
    }
  });
  it('rejects marked null, unknown/duplicate markers and missing Registry, but skips route poison first', async () => {
    const codec = createKafkaTranscriptCodec();
    const good = framed();
    await expect(codec.decode(good, route)).rejects.toMatchObject({
      code: 'configuration',
      retryable: false,
    });
    await expect(
      codec.decode(
        { ...good, headers: { ...good.headers, orca_transcript_encoding: 'future' } },
        route,
      ),
    ).rejects.toMatchObject({ code: 'record' });
    await expect(
      codec.decode(
        {
          ...good,
          headers: { ...good.headers, orca_transcript_encoding: [Buffer.from('avro-v1')] },
        },
        route,
      ),
    ).rejects.toMatchObject({ code: 'record' });
    expect(
      await codec.decode({ ...good, value: null }, { ...route, workspaceId: 'other' }),
    ).toBeNull();
    await codec.close();
  });

  it('checks every duplicate metadata field, key and malformed framing without raw fallback', async () => {
    let gets = 0;
    const server = await stub((_req, res) => {
      gets++;
      res.end(JSON.stringify({ schema: JSON.stringify(transcriptEventSchema) }));
    });
    const codec = createKafkaTranscriptCodec({ schemaRegistry: { url: server.url } });
    try {
      const good = framed();
      for (const value of [
        null,
        Buffer.alloc(0),
        Buffer.from([0, 0, 0, 0, 1]),
        Buffer.from([1, 0, 0, 0, 1, 0]),
      ]) {
        await expect(codec.decode({ ...good, value }, route)).rejects.toMatchObject({
          code: 'record',
        });
      }
      expect(gets).toBe(0);
      for (const key of [
        'id',
        'kind',
        'produced_by',
        'subpath',
        'produced_at',
        'idempotency_key',
        'user_id',
      ]) {
        await expect(
          codec.decode(
            { ...good, headers: { ...good.headers, [key]: Buffer.from('conflict') } },
            route,
          ),
        ).rejects.toMatchObject({ code: 'record' });
        await expect(
          codec.decode({ ...good, headers: { ...good.headers, [key]: [Buffer.from('x')] } }, route),
        ).rejects.toMatchObject({ code: 'record' });
      }
      await expect(
        codec.decode({ ...good, key: Buffer.from('other') }, route),
      ).rejects.toMatchObject({ code: 'record' });
      await expect(
        codec.decode(framed(1, transcriptEventSchema, { workspace_id: 'other' }), route),
      ).rejects.toMatchObject({ code: 'record' });
      await expect(
        codec.decode({ ...good, value: Buffer.concat([good.value!, Buffer.from([0])]) }, route),
      ).rejects.toMatchObject({ code: 'record' });
      expect(gets).toBe(1);
    } finally {
      await codec.close();
      await server.close();
    }
  });

  it('coalesces reader misses, aborts only one waiter, evicts LRU and closes outstanding I/O', async () => {
    let gets = 0;
    const pending: ServerResponse[] = [];
    const server = await stub((_req, res) => {
      gets++;
      pending.push(res);
    });
    const codec = createKafkaTranscriptCodec({
      schemaRegistry: { url: server.url, maxCacheEntries: 1, maxInflight: 1 },
    });
    const waitRequest = async (count: number) => {
      for (let i = 0; gets < count && i < 100; i++)
        await new Promise((resolve) => setTimeout(resolve, 2));
      expect(gets).toBe(count);
    };
    const respond = () =>
      pending.shift()!.end(JSON.stringify({ schema: JSON.stringify(transcriptEventSchema) }));
    try {
      const controller = new AbortController();
      const cancelled = codec.decode(framed(), route, controller.signal);
      const cancelledResult = expect(cancelled).rejects.toMatchObject({ code: 'aborted' });
      const survivor = codec.decode(framed(), route);
      await waitRequest(1);
      controller.abort();
      await cancelledResult;
      await expect(codec.decode(framed(2), route)).rejects.toMatchObject({
        code: 'capacity',
        retryable: true,
      });
      respond();
      expect((await survivor)?.id).toBe('e');
      await codec.decode(framed(), route);
      expect(gets).toBe(1);
      const second = codec.decode(framed(2), route);
      await waitRequest(2);
      respond();
      await second;
      const evicted = codec.decode(framed(), route);
      await waitRequest(3);
      respond();
      await evicted;
      const stopped = codec.decode(framed(3), route);
      const stoppedResult = expect(stopped).rejects.toMatchObject({ code: 'closed' });
      await waitRequest(4);
      await codec.close();
      await stoppedResult;
    } finally {
      await codec.close();
      await server.close();
    }
  });

  it.each([401, 403, 404, 409, 302, 429, 500])(
    'classifies HTTP %i safely and retries only transient status',
    async (status) => {
      let calls = 0;
      const server = await stub((_req, res) => {
        calls++;
        res.writeHead(status);
        res.end('SECRET JWT PAYLOAD');
      });
      const codec = createKafkaTranscriptCodec({
        encoding: 'avro',
        schemaRegistry: { url: server.url, maxAttempts: 2 },
      });
      try {
        const error = await codec.prepareWriter().catch((e) => e);
        expect(error.message).not.toContain('SECRET');
        expect(error.retryable).toBe(status === 429 || status === 500);
        expect(calls).toBe(status === 429 || status === 500 ? 2 : 1);
        expect(error.code).toBe(
          status === 401 || status === 403
            ? 'registry_auth'
            : status === 404
              ? 'registry_not_found'
              : status === 429 || status === 500
                ? 'registry_unavailable'
                : 'registry_response',
        );
      } finally {
        await codec.close();
        await server.close();
      }
    },
  );

  it('uses exact pre-registration lookup and retries a failed operation rather than caching failure', async () => {
    const paths: string[] = [];
    const server = await stub((req, res) => {
      paths.push(req.method + ' ' + req.url);
      res.writeHead(paths.length === 1 ? 404 : 200);
      res.end(JSON.stringify({ id: 1 }));
    });
    const codec = createKafkaTranscriptCodec({
      encoding: 'avro',
      schemaRegistry: { url: server.url + '/prefix/', subject: 'env/record', autoRegister: false },
    });
    try {
      await expect(codec.prepareWriter()).rejects.toMatchObject({ code: 'registry_not_found' });
      await codec.prepareWriter();
      expect(paths).toEqual([
        'POST /prefix/subjects/env%2Frecord',
        'POST /prefix/subjects/env%2Frecord',
      ]);
    } finally {
      await codec.close();
      await server.close();
    }
  });

  it.each([
    { schemaType: 'JSON', schema: '{}' },
    { schema: '"null"' },
    { schema: JSON.stringify({ ...(transcriptEventSchema as object), name: 'Wrong' }) },
    { schema: JSON.stringify(transcriptEventSchema), references: [{ name: 'external' }] },
    { schema: '{}' },
  ])('rejects unsupported writer schemas', async (response) => {
    const server = await stub((_req, res) => {
      res.end(JSON.stringify(response));
    });
    const codec = createKafkaTranscriptCodec({ schemaRegistry: { url: server.url } });
    try {
      await expect(codec.decode(framed(), route)).rejects.toMatchObject({ code: 'schema' });
    } finally {
      await codec.close();
      await server.close();
    }
  });

  it('bounds response bytes and total request time', async () => {
    const server = await stub((req, res) => {
      if (req.url?.includes('/subjects/')) res.end('x'.repeat(100));
    });
    const writer = createKafkaTranscriptCodec({
      encoding: 'avro',
      schemaRegistry: { url: server.url, maxResponseBytes: 20 },
    });
    const reader = createKafkaTranscriptCodec({
      schemaRegistry: {
        url: server.url,
        requestTimeoutMs: 10,
        operationTimeoutMs: 30,
        maxAttempts: 3,
      },
    });
    try {
      await expect(writer.prepareWriter()).rejects.toMatchObject({ code: 'registry_response' });
      const start = Date.now();
      await expect(reader.decode(framed(), route)).rejects.toMatchObject({
        code: 'registry_unavailable',
        retryable: true,
      });
      expect(Date.now() - start).toBeLessThan(500);
    } finally {
      await writer.close();
      await reader.close();
      await server.close();
    }
  });
  it('registers the exact schema once, frames standard Avro bytes, and reads by ID without registering', async () => {
    const requests: string[] = [];
    let schema: string = '';
    const server = createServer(async (req, res) => {
      requests.push(`${req.method} ${req.url}`);
      let body = '';
      for await (const chunk of req) body += chunk;
      if (req.method === 'POST') {
        schema = JSON.parse(body).schema;
        res.end(JSON.stringify({ id: 258 }));
      } else res.end(JSON.stringify({ schema, schemaType: 'AVRO' }));
    });
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
    const url = `http://127.0.0.1:${(server.address() as { port: number }).port}/prefix`;
    const writer = createKafkaTranscriptCodec({ encoding: 'avro', schemaRegistry: { url } });
    const reader = createKafkaTranscriptCodec({ schemaRegistry: { url } });
    try {
      await Promise.all([writer.prepareWriter(), writer.prepareWriter(), reader.prepareWriter()]);
      for (const payload of [new Uint8Array([0, 255, 1]), new Uint8Array(), Buffer.from('中文')]) {
        const wire = await writer.encode({ ...event, payload });
        expect((wire.value as Buffer).subarray(0, 5)).toEqual(Buffer.from([0, 0, 0, 1, 2]));
        expect(wire.headers?.orca_transcript_encoding).toEqual(Buffer.from('avro-v1'));
        const record = avro.Type.forSchema(JSON.parse(schema)).fromBuffer(
          (wire.value as Buffer).subarray(5),
        );
        expect(record.payload).toEqual(Buffer.from(payload));
        expect(record.seq).toBeUndefined();
        expect(record.user_id).toBeNull();
        expect(await reader.decode(message(wire), route)).toEqual({
          ...event,
          payload: new Uint8Array(payload),
          seq: 42,
        });
      }
      expect(requests).toEqual([
        'POST /prefix/subjects/orca.transcript.TranscriptEvent/versions',
        'GET /prefix/schemas/ids/258',
      ]);
    } finally {
      await writer.close();
      await reader.close();
      await new Promise<void>((resolve) => server.close(() => resolve()));
    }
  });
  it('keeps the default raw wire byte-identical and restores broker offset without sniffing magic', async () => {
    const codec = createKafkaTranscriptCodec();
    await codec.prepareWriter();
    const wire = await codec.encode(event);
    expect(wire).toEqual(eventToKafkaMessage(event));
    expect(await codec.decode(message(wire), route)).toEqual({ ...event, seq: 42 });
    await codec.close();
  });
});

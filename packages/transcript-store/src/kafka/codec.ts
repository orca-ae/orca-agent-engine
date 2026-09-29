// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { KafkaMessage, Message } from 'kafkajs';
import type { Event } from '../types.js';
import { eventToKafkaMessage, kafkaMessageToEventForRoute } from './serialize.js';
import avro from 'avsc';
import { KafkaTranscriptCodecError as CodecError } from './codec-error.js';
import { SchemaRegistryClient, type SchemaRegistryOptions } from './schema-registry.js';
import { transcriptEventSchema } from './schemas/transcript-event.js';
import { schemaCache, decodeFailures } from './codec-metrics.js';
export interface KafkaTranscriptCodecOptions {
  encoding?: 'raw' | 'avro';
  schemaRegistry?: SchemaRegistryOptions;
}
export interface KafkaTranscriptCodec {
  /** Topic routing mode; legacy injected codecs without metadata use raw topics. */
  readonly encoding?: 'raw' | 'avro';
  prepareWriter(): Promise<void>;
  encode(event: Event): Promise<Message>;
  decode(
    message: KafkaMessage,
    route: { workspaceId: string; sessionId: string },
    signal?: AbortSignal,
  ): Promise<Event | null>;
  close(): Promise<void>;
}
export function createKafkaTranscriptCodec(
  options: KafkaTranscriptCodecOptions = {},
): KafkaTranscriptCodec {
  if (options.encoding !== undefined && options.encoding !== 'raw' && options.encoding !== 'avro')
    throw new CodecError('configuration');
  if (options.encoding === 'avro' && !options.schemaRegistry) throw new CodecError('configuration');
  const registry = options.schemaRegistry
    ? new SchemaRegistryClient(options.schemaRegistry)
    : undefined;
  const schemaOptions = options.schemaRegistry;
  let closed = false;
  let writer: Promise<number> | undefined;
  let readerType: avro.Type | undefined;
  const type = () => (readerType ??= avro.Type.forSchema(transcriptEventSchema));
  const cache = new Map<number, { resolver: avro.Resolver; bytes: number }>();
  const inflight = new Map<number, Promise<avro.Resolver>>();
  let cacheBytes = 0;
  const check = (signal?: AbortSignal) => {
    if (closed) throw new CodecError('closed');
    if (signal?.aborted) throw new CodecError('aborted');
  };
  const prepare = (): Promise<number> => {
    check();
    if (!registry) return Promise.reject(new CodecError('configuration'));
    if (!writer) {
      const subject = encodeURIComponent(
        schemaOptions?.subject ?? 'orca.transcript.TranscriptEvent',
      );
      writer = registry
        .request(
          '/subjects/' + subject + (schemaOptions?.autoRegister === false ? '' : '/versions'),
          {
            schemaType: 'AVRO',
            schema: JSON.stringify(transcriptEventSchema),
          },
        )
        .then((result) => {
          if (
            !Number.isInteger(result.id) ||
            (result.id as number) < 1 ||
            (result.id as number) > 0xffffffff
          )
            throw new CodecError('registry_response');
          return result.id as number;
        })
        .catch((error) => {
          writer = undefined;
          throw error;
        });
    }
    return writer;
  };
  const resolverFor = (id: number): Promise<avro.Resolver> => {
    const cached = cache.get(id);
    if (cached) {
      schemaCache.inc({ result: 'hit' });
      cache.delete(id);
      cache.set(id, cached);
      return Promise.resolve(cached.resolver);
    }
    const pending = inflight.get(id);
    schemaCache.inc({ result: pending ? 'coalesced' : 'miss' });
    if (pending) return pending;
    if (!registry) return Promise.reject(new CodecError('configuration'));
    if (inflight.size >= (schemaOptions?.maxInflight ?? 16))
      return Promise.reject(new CodecError('capacity', true));
    const operation = registry
      .request('/schemas/ids/' + id)
      .then((result) => {
        if (
          (result.schemaType !== undefined && result.schemaType !== 'AVRO') ||
          typeof result.schema !== 'string' ||
          (result.references !== undefined &&
            (!Array.isArray(result.references) || result.references.length !== 0))
        )
          throw new CodecError('schema');
        let resolver: avro.Resolver;
        try {
          const schema = avro.Type.forSchema(JSON.parse(result.schema));
          if (schema.typeName !== 'record' || schema.name !== 'orca.transcript.TranscriptEvent')
            throw new Error();
          resolver = type().createResolver(schema);
        } catch {
          throw new CodecError('schema');
        }
        const bytes = Buffer.byteLength(result.schema);
        const maxBytes = schemaOptions?.maxCacheBytes ?? 8_388_608;
        if (bytes <= maxBytes && !closed) {
          while (
            cache.size &&
            (cache.size >= (schemaOptions?.maxCacheEntries ?? 256) || cacheBytes + bytes > maxBytes)
          ) {
            const oldest = cache.keys().next().value!;
            cacheBytes -= cache.get(oldest)!.bytes;
            cache.delete(oldest);
          }
          cache.set(id, { resolver, bytes });
          cacheBytes += bytes;
        }
        return resolver;
      })
      .finally(() => {
        inflight.delete(id);
      });
    inflight.set(id, operation);
    return operation;
  };
  return {
    encoding: options.encoding ?? 'raw',
    async prepareWriter() {
      check();
      if (options.encoding === 'avro') await prepare();
    },
    async encode(event) {
      check();
      const wire = eventToKafkaMessage(event);
      if (options.encoding !== 'avro') return wire;
      const id = await prepare();
      check();
      try {
        const record = {
          id: event.id,
          workspace_id: event.workspaceId,
          session_id: event.sessionId,
          subpath: event.subpath || '',
          produced_at: event.producedAt,
          produced_by: event.producedBy,
          kind: event.kind,
          payload: Buffer.from(event.payload),
          idempotency_key: event.idempotencyKey || '',
          user_id: event.userId || null,
        };
        const prefix = Buffer.alloc(5);
        prefix.writeUInt32BE(id, 1);
        return {
          ...wire,
          value: Buffer.concat([prefix, type().toBuffer(record)]),
          headers: { ...wire.headers, orca_transcript_encoding: Buffer.from('avro-v1') },
        };
      } catch {
        throw new CodecError('record');
      }
    },
    async decode(message, route, signal) {
      try {
        check(signal);
        // Route poison is intentionally skipped before marker validation or Registry I/O.
        const headerEvent = kafkaMessageToEventForRoute(message, route);
        if (!headerEvent) return null;
        const marker = message.headers?.orca_transcript_encoding;
        if (marker === undefined) return headerEvent;
        if (Array.isArray(marker) || marker === null || marker.toString() !== 'avro-v1')
          throw new CodecError('record');
        for (const name of [
          'id',
          'workspace_id',
          'session_id',
          'subpath',
          'produced_at',
          'produced_by',
          'kind',
          'idempotency_key',
          'user_id',
        ]) {
          if (Array.isArray(message.headers?.[name])) throw new CodecError('record');
        }
        if (!registry) throw new CodecError('configuration');
        const value = message.value;
        if (!value || value.length < 6 || value[0] !== 0 || value.readUInt32BE(1) === 0)
          throw new CodecError('record');
        const resolver = await abortable(resolverFor(value.readUInt32BE(1)), signal);
        check(signal);
        try {
          const record = type().fromBuffer(value.subarray(5), resolver) as Record<string, unknown>;
          if (!record || !Buffer.isBuffer(record.payload)) throw new Error();
          const pairs = {
            id: 'id',
            workspace_id: 'workspaceId',
            session_id: 'sessionId',
            subpath: 'subpath',
            produced_at: 'producedAt',
            produced_by: 'producedBy',
            kind: 'kind',
            idempotency_key: 'idempotencyKey',
            user_id: 'userId',
          } as const;
          for (const [field, property] of Object.entries(pairs)) {
            if ((record[field] ?? '') !== (headerEvent[property as keyof Event] ?? ''))
              throw new Error();
          }
          if (!message.key || !message.key.equals(Buffer.from(headerEvent.id))) throw new Error();
          return { ...headerEvent, payload: new Uint8Array(record.payload) };
        } catch {
          throw new CodecError('record');
        }
      } catch (error) {
        decodeFailures.inc({ code: error instanceof CodecError ? error.code : 'record' });
        throw error;
      }
    },
    async close() {
      closed = true;
      await registry?.close();
      cache.clear();
      cacheBytes = 0;
    },
  };
}

/** Cancellation belongs to the waiter, never to the shared cache fill. */
function abortable<T>(promise: Promise<T>, signal?: AbortSignal): Promise<T> {
  if (!signal) return promise;
  return new Promise((resolve, reject) => {
    const abort = () => reject(new CodecError('aborted'));
    signal.addEventListener('abort', abort, { once: true });
    if (signal.aborted) abort();
    promise.then(resolve, reject).finally(() => signal.removeEventListener('abort', abort));
  });
}

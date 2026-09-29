// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import multipart from '@fastify/multipart';
import Fastify, { type FastifyInstance } from 'fastify';
import { Readable } from 'node:stream';
import type { CreateFileInput, FileRecord, FileStore } from '@orca/file-store';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { registerFilesRoutes } from '../../src/api/files.routes.js';

class StubFileStore implements FileStore {
  readonly records = new Map<string, FileRecord>();
  lastCreate: CreateFileInput | null = null;
  createCalls = 0;

  async create(input: CreateFileInput): Promise<FileRecord> {
    this.createCalls += 1;
    this.lastCreate = input;
    for await (const _chunk of input.content) {
      // Drain the stream like the real store.
    }
    const now = new Date('2026-07-23T00:00:00.000Z');
    const record: FileRecord = {
      id: 'file_created',
      workspaceId: input.workspaceId,
      filename: input.filename,
      mimeType: input.mimeType,
      sizeBytes: 5,
      sha256: '0'.repeat(64),
      metadata: input.metadata ?? {},
      purpose: input.purpose ?? 'agent',
      scopeId: input.scopeId ?? null,
      downloadable: input.downloadable ?? input.purpose === 'agent_output',
      archivedAt: null,
      createdAt: now,
      updatedAt: now,
    };
    this.records.set(record.id, record);
    return record;
  }

  async get(_workspaceId: string, fileId: string): Promise<FileRecord | null> {
    return this.records.get(fileId) ?? null;
  }

  async list() {
    return { items: [...this.records.values()], nextCursor: null };
  }

  async open() {
    return { stream: Readable.from('hello'), sizeBytes: 5, sha256: '0'.repeat(64) };
  }

  async archive(): Promise<void> {}

  async delete(_workspaceId: string, fileId: string): Promise<void> {
    this.records.delete(fileId);
  }

  async close(): Promise<void> {}
}

describe('Files Claude wire routes', () => {
  let app: FastifyInstance;
  let store: StubFileStore;

  beforeEach(async () => {
    app = Fastify();
    await app.register(multipart);
    app.addHook('onRequest', async (request) => {
      request.auth = {
        workspaceId: 'ws_files_route_test',
        principal: 'test',
        scopes: [],
        authMethod: 'api-key',
      };
    });
    store = new StubFileStore();
    registerFilesRoutes(app, store, { maxFileBytes: 64 });
    await app.ready();
  });

  afterEach(async () => {
    await app.close();
  });

  it('ignores public purpose/scope selectors and returns canonical metadata', async () => {
    const boundary = 'orca-file-boundary';
    const payload = Buffer.from(
      [
        `--${boundary}\r\nContent-Disposition: form-data; name="purpose"\r\n\r\nagent_output\r\n`,
        `--${boundary}\r\nContent-Disposition: form-data; name="scope_id"\r\n\r\nses_forged\r\n`,
        `--${boundary}\r\nContent-Disposition: form-data; name="file"; filename="hello.txt"\r\nContent-Type: text/plain\r\n\r\nhello\r\n`,
        `--${boundary}--\r\n`,
      ].join(''),
    );

    const response = await app.inject({
      method: 'POST',
      url: '/v1/files',
      headers: { 'content-type': `multipart/form-data; boundary=${boundary}` },
      payload,
    });

    expect(response.statusCode).toBe(200);
    expect(store.lastCreate).toMatchObject({ purpose: 'agent', scopeId: null, metadata: {} });
    expect(response.json()).toEqual({
      id: 'file_created',
      created_at: '2026-07-23T00:00:00.000Z',
      filename: 'hello.txt',
      mime_type: 'text/plain',
      size_bytes: 5,
      type: 'file',
      downloadable: false,
      scope: null,
    });
  });

  it('ignores public selectors that appear after the streamed file part', async () => {
    const boundary = 'orca-file-after-boundary';
    const payload = Buffer.from(
      [
        `--${boundary}\r\nContent-Disposition: form-data; name="file"; filename="hello.txt"\r\nContent-Type: text/plain\r\n\r\nhello\r\n`,
        `--${boundary}\r\nContent-Disposition: form-data; name="purpose"\r\n\r\nagent_output\r\n`,
        `--${boundary}\r\nContent-Disposition: form-data; name="scope_id"\r\n\r\nses_forged\r\n`,
        `--${boundary}--\r\n`,
      ].join(''),
    );

    const response = await app.inject({
      method: 'POST',
      url: '/v1/files',
      headers: { 'content-type': `multipart/form-data; boundary=${boundary}` },
      payload,
    });

    expect(response.statusCode).toBe(200);
    expect(store.lastCreate).toMatchObject({ purpose: 'agent', scopeId: null, metadata: {} });
    expect(response.json()).toMatchObject({ type: 'file', downloadable: false, scope: null });
  });

  it('returns 413 when the streamed file exceeds the configured limit', async () => {
    const boundary = 'orca-file-large-boundary';
    const payload = Buffer.from(
      [
        `--${boundary}\r\nContent-Disposition: form-data; name="file"; filename="large.txt"\r\nContent-Type: text/plain\r\n\r\n`,
        'x'.repeat(65),
        `\r\n--${boundary}--\r\n`,
      ].join(''),
    );

    const response = await app.inject({
      method: 'POST',
      url: '/v1/files',
      headers: { 'content-type': `multipart/form-data; boundary=${boundary}` },
      payload,
    });

    expect(response.statusCode).toBe(413);
    expect(response.json()).toEqual({ error: 'file exceeds 64 byte limit' });
    expect(store.records.size).toBe(0);
  });

  it('accepts a streamed file exactly at the configured limit', async () => {
    const boundary = 'orca-file-exact-limit-boundary';
    const payload = Buffer.from(
      [
        `--${boundary}\r\nContent-Disposition: form-data; name="file"; filename="exact.txt"\r\nContent-Type: text/plain\r\n\r\n`,
        'x'.repeat(64),
        `\r\n--${boundary}--\r\n`,
      ].join(''),
    );

    const response = await app.inject({
      method: 'POST',
      url: '/v1/files',
      headers: { 'content-type': `multipart/form-data; boundary=${boundary}` },
      payload,
    });

    expect(response.statusCode).toBe(200);
    expect(store.records.size).toBe(1);
  });

  it('drains duplicate file parts without persisting them or hanging', async () => {
    const boundary = 'orca-file-duplicate-boundary';
    const payload = Buffer.from(
      [
        `--${boundary}\r\nContent-Disposition: form-data; name="file"; filename="first.txt"\r\nContent-Type: text/plain\r\n\r\nfirst\r\n`,
        `--${boundary}\r\nContent-Disposition: form-data; name="file"; filename="second.txt"\r\nContent-Type: text/plain\r\n\r\nsecond\r\n`,
        `--${boundary}--\r\n`,
      ].join(''),
    );

    const response = await app.inject({
      method: 'POST',
      url: '/v1/files',
      headers: { 'content-type': `multipart/form-data; boundary=${boundary}` },
      payload,
    });

    expect(response.statusCode).toBe(200);
    expect(store.createCalls).toBe(1);
    expect(store.lastCreate?.filename).toBe('first.txt');
  });

  it('ignores an oversized duplicate part after persisting the accepted file', async () => {
    const boundary = 'orca-file-oversized-duplicate-boundary';
    const payload = Buffer.from(
      [
        `--${boundary}\r\nContent-Disposition: form-data; name="file"; filename="first.txt"\r\nContent-Type: text/plain\r\n\r\nfirst\r\n`,
        `--${boundary}\r\nContent-Disposition: form-data; name="file"; filename="ignored.txt"\r\nContent-Type: text/plain\r\n\r\n`,
        'x'.repeat(65),
        `\r\n--${boundary}--\r\n`,
      ].join(''),
    );

    const response = await app.inject({
      method: 'POST',
      url: '/v1/files',
      headers: { 'content-type': `multipart/form-data; boundary=${boundary}` },
      payload,
    });

    expect(response.statusCode).toBe(200);
    expect(store.createCalls).toBe(1);
    expect(store.records.size).toBe(1);
    expect(store.lastCreate?.filename).toBe('first.txt');
  });

  it('returns the official delete tombstone', async () => {
    store.records.set('file_delete', {
      id: 'file_delete',
      workspaceId: 'ws_files_route_test',
      filename: 'delete.txt',
      mimeType: 'text/plain',
      sizeBytes: 1,
      sha256: '0'.repeat(64),
      metadata: {},
      purpose: 'agent',
      scopeId: null,
      downloadable: false,
      archivedAt: null,
      createdAt: new Date(),
      updatedAt: new Date(),
    });

    const response = await app.inject({ method: 'DELETE', url: '/v1/files/file_delete' });

    expect(response.statusCode).toBe(200);
    expect(response.json()).toEqual({ id: 'file_delete', type: 'file_deleted' });
  });
});

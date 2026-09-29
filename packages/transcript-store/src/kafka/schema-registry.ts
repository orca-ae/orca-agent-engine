// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import http from 'node:http';
import https from 'node:https';
import { setTimeout as delay } from 'node:timers/promises';
import { KafkaTranscriptCodecError as CodecError } from './codec-error.js';
import { schemaOperation } from './codec-metrics.js';

class RetryAfterError extends CodecError {
  constructor(readonly delayMs: number) {
    super('registry_unavailable', true);
  }
}

export interface SchemaRegistryOptions {
  url: string;
  subject?: string;
  autoRegister?: boolean;
  auth?: { username: string; password: string };
  tls?: { ca?: Buffer; cert?: Buffer; key?: Buffer };
  requestTimeoutMs?: number;
  operationTimeoutMs?: number;
  maxAttempts?: number;
  maxResponseBytes?: number;
  maxCacheEntries?: number;
  maxCacheBytes?: number;
  maxInflight?: number;
}

export function validateSchemaRegistryOptions(options: SchemaRegistryOptions): void {
  let url: URL;
  try {
    url = new URL(options.url);
  } catch {
    throw new CodecError('configuration');
  }
  if (
    !['http:', 'https:'].includes(url.protocol) ||
    url.username ||
    url.password ||
    options.url.includes('?') ||
    options.url.includes('#') ||
    ((options.auth || options.tls) && url.protocol !== 'https:') ||
    (options.auth &&
      (!options.auth.username || !options.auth.password || options.auth.username.includes(':'))) ||
    !!options.tls?.cert !== !!options.tls?.key ||
    (options.subject !== undefined &&
      (!options.subject.trim() || options.subject === '.' || options.subject === '..')) ||
    (options.autoRegister !== undefined && typeof options.autoRegister !== 'boolean')
  ) {
    throw new CodecError('configuration');
  }
  const bounds = {
    requestTimeoutMs: 300_000,
    operationTimeoutMs: 300_000,
    maxAttempts: 10,
    maxResponseBytes: 16_777_216,
    maxCacheEntries: 4096,
    maxCacheBytes: 67_108_864,
    maxInflight: 256,
  };
  for (const [key, max] of Object.entries(bounds)) {
    const value = options[key as keyof typeof bounds];
    if (value !== undefined && (!Number.isSafeInteger(value) || value < 1 || value > max)) {
      throw new CodecError('configuration');
    }
  }
}

export class SchemaRegistryClient {
  private readonly controller = new AbortController();
  private readonly operations = new Set<Promise<unknown>>();
  constructor(private readonly options: SchemaRegistryOptions) {
    validateSchemaRegistryOptions(options);
  }

  request(path: string, body?: unknown): Promise<Record<string, unknown>> {
    if (this.controller.signal.aborted) return Promise.reject(new CodecError('closed'));
    const stop = schemaOperation.startTimer({
      operation: body === undefined ? 'read' : path.endsWith('/versions') ? 'register' : 'lookup',
    });
    const operation = this.perform(path, body).then(
      (result) => {
        stop({ result: 'success' });
        return result;
      },
      (error) => {
        stop({ result: error instanceof CodecError ? error.code : 'registry_unavailable' });
        throw error;
      },
    );
    this.operations.add(operation);
    void operation.finally(() => this.operations.delete(operation)).catch(() => {});
    return operation;
  }

  async close(): Promise<void> {
    this.controller.abort();
    await Promise.allSettled([...this.operations]);
  }

  private async perform(path: string, body?: unknown): Promise<Record<string, unknown>> {
    const deadline = Date.now() + (this.options.operationTimeoutMs ?? 15_000);
    const attempts = this.options.maxAttempts ?? 3;
    for (let attempt = 0; attempt < attempts; attempt++) {
      if (this.controller.signal.aborted) throw new CodecError('closed');
      const remaining = deadline - Date.now();
      if (remaining <= 0) throw new CodecError('registry_unavailable', true);
      try {
        return await this.once(
          path,
          body,
          Math.min(remaining, this.options.requestTimeoutMs ?? 5000),
        );
      } catch (error) {
        if (this.controller.signal.aborted) throw new CodecError('closed');
        if (!(error instanceof CodecError) || !error.retryable || attempt + 1 === attempts)
          throw error;
        const remaining = deadline - Date.now();
        const requestedPause = Math.max(
          100 * 2 ** attempt + Math.random() * 100,
          error instanceof RetryAfterError ? error.delayMs : 0,
        );
        if (requestedPause >= remaining) throw new CodecError('registry_unavailable', true);
        try {
          await delay(requestedPause, undefined, { signal: this.controller.signal });
        } catch {
          throw new CodecError('closed');
        }
      }
    }
    throw new CodecError('registry_unavailable', true);
  }

  private once(path: string, body: unknown, timeout: number): Promise<Record<string, unknown>> {
    return new Promise((resolve, reject) => {
      const url = new URL(this.options.url);
      url.pathname = url.pathname.replace(/[/]$/, '') + path;
      const data = body === undefined ? undefined : JSON.stringify(body);
      const headers: Record<string, string> = { accept: 'application/vnd.schemaregistry.v1+json' };
      if (data !== undefined) {
        headers['content-type'] = 'application/vnd.schemaregistry.v1+json';
        headers['content-length'] = String(Buffer.byteLength(data));
      }
      if (this.options.auth)
        headers.authorization =
          'Basic ' +
          Buffer.from(this.options.auth.username + ':' + this.options.auth.password).toString(
            'base64',
          );
      let req: http.ClientRequest;
      try {
        req = (url.protocol === 'https:' ? https : http).request(url, {
          method: data === undefined ? 'GET' : 'POST',
          headers,
          ...this.options.tls,
          signal: this.controller.signal,
        });
      } catch {
        reject(new CodecError('configuration'));
        return;
      }
      const timer = setTimeout(
        () => req.destroy(new CodecError('registry_unavailable', true)),
        timeout,
      );
      let settled = false;
      const fail = (error: CodecError) => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        reject(error);
        req.destroy();
      };
      req.on('error', (error) =>
        fail(error instanceof CodecError ? error : new CodecError('registry_unavailable', true)),
      );
      req.on('response', (res) => {
        const status = res.statusCode ?? 0;
        if (status < 200 || status >= 300) {
          res.destroy();
          const retryAfter = res.headers['retry-after'];
          if ((status === 429 || status >= 500) && retryAfter) {
            const seconds = Number(retryAfter);
            const milliseconds = Number.isFinite(seconds)
              ? seconds * 1000
              : Date.parse(retryAfter) - Date.now();
            if (Number.isFinite(milliseconds) && milliseconds > 0) {
              fail(new RetryAfterError(milliseconds));
              return;
            }
          }
          fail(
            new CodecError(
              status === 401 || status === 403
                ? 'registry_auth'
                : status === 404
                  ? 'registry_not_found'
                  : status === 429 || status >= 500
                    ? 'registry_unavailable'
                    : 'registry_response',
              status === 429 || status >= 500,
            ),
          );
          return;
        }
        const chunks: Buffer[] = [];
        let size = 0;
        res.on('error', () => fail(new CodecError('registry_unavailable', true)));
        res.on('data', (chunk: Buffer) => {
          size += chunk.length;
          if (size > (this.options.maxResponseBytes ?? 1_048_576)) {
            fail(new CodecError('registry_response'));
            res.destroy();
          } else chunks.push(chunk);
        });
        res.on('end', () => {
          if (settled) return;
          try {
            const result: unknown = JSON.parse(Buffer.concat(chunks).toString('utf8'));
            if (!result || typeof result !== 'object' || Array.isArray(result)) throw new Error();
            settled = true;
            clearTimeout(timer);
            resolve(result as Record<string, unknown>);
          } catch {
            fail(new CodecError('registry_response'));
          }
        });
      });
      req.end(data);
    });
  }
}

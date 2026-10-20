// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import {
  assertSdkTerminalUsage,
  validatedProviderFetch,
  type SdkTerminalUsage,
} from '@orca/sdk-harness';

export function assertDshProtocol(api: string): void {
  if (
    ![
      'anthropic-messages',
      'openai-completions',
      'openai-responses',
      'google-generative-ai',
    ].includes(api)
  )
    throw new Error('DSH managed transport/usage is not integrated for protocol ' + api);
}

interface ManagedCredentials {
  origin: string;
  token: string;
  sessionId: string;
  provider?: string;
  api?: string;
  baseUrl?: string;
  model?: string;
}

const count = (value: unknown): number => {
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < 0)
    throw new Error('DSH Messages usage unavailable');
  return value;
};

/** Observes raw cumulative Messages counters; never substitutes normalized SDK zeroes. */
export class MessagesUsage {
  private start: Record<string, unknown> | undefined;
  private output: number | undefined;
  private stopped = false;
  event(value: Record<string, unknown>): void {
    if (this.stopped) throw new Error('Messages event after terminal');
    if (value.type === 'message_start') {
      if (this.start) throw new Error('Duplicate Messages start');
      this.start = (value.message as { usage?: Record<string, unknown> })?.usage;
      if (!this.start) throw new Error('Missing Messages start usage');
    }
    if (value.type === 'message_delta') {
      if (!this.start) throw new Error('Messages delta before start');
      const raw = value.usage as Record<string, unknown>;
      for (const field of [
        'input_tokens',
        'cache_read_input_tokens',
        'cache_creation_input_tokens',
      ]) {
        if (raw?.[field] === undefined) continue;
        const updated = count(raw[field]);
        if (updated < count(this.start[field] ?? 0))
          throw new Error('Non-cumulative Messages usage');
        this.start[field] = updated;
      }
      const next = count(raw?.output_tokens);
      if (this.output !== undefined && next < this.output)
        throw new Error('Non-cumulative Messages usage');
      this.output = next;
    }
    if (value.type === 'error') throw new Error('DSH Messages stream failed');
    if (value.type === 'message_stop') this.stopped = true;
  }
  finish(): SdkTerminalUsage {
    if (!this.stopped || !this.start || this.output === undefined)
      throw new Error('DSH Messages terminal usage unavailable');
    const cached = count(this.start.cache_read_input_tokens ?? 0);
    const usage = {
      input_tokens: count(this.start.input_tokens) + cached,
      output_tokens: this.output,
      cached_input_tokens: cached,
      cache_write_input_tokens: count(this.start.cache_creation_input_tokens ?? 0),
    };
    assertSdkTerminalUsage(usage);
    return usage;
  }
}

export function gatewayBase(origin: string | undefined): string {
  if (!origin) throw new Error('DSH requires a managed Gateway origin');
  const url = new URL(origin);
  if (
    !['http:', 'https:'].includes(url.protocol) ||
    url.username ||
    url.password ||
    url.pathname !== '/' ||
    url.search ||
    url.hash
  )
    throw new Error('Invalid DSH Gateway origin');
  return `${url.origin}/v1/proxy/deepseek`;
}

/** Installed only in the private DSH child; preserves bytes and the native serializer. */
export function installManagedFetch(
  credentials: () => ManagedCredentials,
  completed: (usage: SdkTerminalUsage, nativeSessionId: string) => void,
  started?: (nativeSessionId: string) => void,
): () => void {
  const original = globalThis.fetch;
  globalThis.fetch = async (input, init) => {
    const auth = credentials();
    if (auth.provider) {
      assertDshProtocol(auth.api!);
      const request = new Request(input, init);
      const upstream = new URL(auth.baseUrl!);
      const url = new URL(request.url);
      const prefix = upstream.pathname.replace(/[/]$/, '');
      const suffix = url.pathname.slice(prefix.length);
      const endpoint =
        auth.api === 'anthropic-messages'
          ? ['/v1/messages', '/messages']
          : auth.api === 'openai-responses'
            ? ['/responses']
            : auth.api === 'openai-completions'
              ? ['/chat/completions']
              : [];
      const google =
        auth.api === 'google-generative-ai' &&
        suffix.startsWith('/models/') &&
        suffix.endsWith(':streamGenerateContent');
      if (
        request.method !== 'POST' ||
        url.origin !== upstream.origin ||
        !url.pathname.startsWith(prefix + '/') ||
        (!endpoint.includes(suffix) && !google)
      )
        throw new Error('DSH unmanaged network request rejected');
      if (!auth.token) throw new Error('DSH scoped credentials unavailable');
      const body = await request.arrayBuffer();
      if (body.byteLength > 32 * 1024 * 1024) throw new Error('DSH request exceeds limit');
      try {
        const matches = google
          ? decodeURIComponent(suffix) === '/models/' + auth.model + ':streamGenerateContent'
          : JSON.parse(new TextDecoder().decode(body))?.model === auth.model;
        if (!matches) throw new Error('model mismatch');
      } catch {
        throw new Error('DSH request model is invalid or differs from pinned Agent');
      }
      const headers = new Headers(request.headers);
      const id = headers.get('x-deepseek-harness-session-id');
      if (!id) throw new Error('DSH native request identity unavailable');
      headers.delete('x-deepseek-harness-session-id');
      for (const name of [
        'authorization',
        'proxy-authorization',
        'x-api-key',
        'api-key',
        'x-goog-api-key',
        'cookie',
        'host',
      ])
        headers.delete(name);
      headers.set('Authorization', 'Bearer ' + auth.token);
      headers.set('X-Orca-Session-Id', auth.sessionId);
      for (const name of [...url.searchParams.keys()]) {
        if (!['alt', 'beta'].includes(name)) url.searchParams.delete(name);
      }
      const target = new URL(gatewayBase(auth.origin));
      target.pathname = '/v1/proxy/' + auth.provider + url.pathname;
      target.search = url.search;
      started?.(id);
      return validatedProviderFetch(
        auth.api!,
        async () => {
          const response = await original(target, {
            method: 'POST',
            headers,
            body,
            signal: request.signal,
            redirect: 'error',
          });
          if (
            !response.ok ||
            !response.body ||
            !response.headers.get('content-type')?.includes('text/event-stream')
          )
            throw new Error('DSH managed model request did not return a successful stream');
          return response;
        },
        (usage) => completed(usage, id),
      )(request);
    }
    if (typeof init?.body === 'string' && Buffer.byteLength(init.body) > 32 * 1024 * 1024)
      throw new Error('DSH Messages request exceeds limit');
    const request = new Request(input, init);
    if (request.url !== `${gatewayBase(auth.origin)}/v1/messages` || request.method !== 'POST')
      throw new Error('DSH unmanaged network request rejected');
    if (!auth.token) throw new Error('DSH scoped credentials unavailable');
    const body = await request.arrayBuffer();
    if (body.byteLength > 32 * 1024 * 1024) throw new Error('DSH Messages request exceeds limit');
    const headers = new Headers(request.headers);
    const nativeSessionId = headers.get('x-deepseek-harness-session-id');
    if (!nativeSessionId) throw new Error('DSH native request identity unavailable');
    headers.delete('x-deepseek-harness-session-id');
    started?.(nativeSessionId);
    headers.delete('x-api-key');
    headers.set('Authorization', `Bearer ${auth.token}`);
    headers.set('X-Orca-Session-Id', auth.sessionId);
    const response = await original(request.url, {
      method: 'POST',
      headers,
      body,
      signal: request.signal,
      redirect: 'error',
    });
    if (
      !response.ok ||
      !response.body ||
      !response.headers.get('content-type')?.includes('text/event-stream')
    )
      throw new Error('DSH Messages request did not return a successful stream');
    const observer = new MessagesUsage();
    const decoder = new TextDecoder();
    let pending = '';
    let eventBytes = 0;
    const stream = response.body.pipeThrough(
      new TransformStream<Uint8Array, Uint8Array>({
        transform(chunk, controller) {
          pending += decoder.decode(chunk, { stream: true });
          if (Buffer.byteLength(pending) > 16 * 1024 * 1024)
            throw new Error('DSH SSE event exceeds limit');
          let newline: number;
          while ((newline = pending.indexOf('\n')) !== -1) {
            const line = pending.slice(0, newline).replace(/\r$/, '');
            pending = pending.slice(newline + 1);
            eventBytes += Buffer.byteLength(line);
            if (eventBytes > 16 * 1024 * 1024) throw new Error('DSH SSE event exceeds limit');
            if (!line) eventBytes = 0;
            if (line.startsWith('data:')) {
              const event = JSON.parse(line.slice(5).trim()) as Record<string, unknown>;
              observer.event(event);
              // DSH ends its iterator at message_stop, before HTTP EOF.
              if (event.type === 'message_stop') completed(observer.finish(), nativeSessionId);
            }
          }
          controller.enqueue(chunk);
        },
        flush() {
          if (pending.trim()) throw new Error('Truncated DSH SSE frame');
          observer.finish();
        },
      }),
    );
    return new Response(stream, { status: response.status, headers: response.headers });
  };
  return () => {
    globalThis.fetch = original;
  };
}

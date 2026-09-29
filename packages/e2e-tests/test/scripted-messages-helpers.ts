// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createServer, type IncomingMessage, type Server } from 'node:http';

export interface MessagesRequest {
  model: string;
  system?: unknown;
  messages: Array<{ role: string; content: unknown }>;
  [key: string]: unknown;
}

export interface ScriptedReply {
  text?: string;
  tool?: { name: string; input: Record<string, unknown> };
  input?: number;
  output?: number;
  actualModel?: string;
}

export interface MessagesExchange {
  body: MessagesRequest;
  reply: ScriptedReply;
  messageId: string;
}

export async function readRequest(req: IncomingMessage): Promise<string> {
  const chunks: Buffer[] = [];
  for await (const chunk of req) chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  return Buffer.concat(chunks).toString('utf8');
}

export async function listen(server: Server): Promise<number> {
  await new Promise<void>((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', () => {
      server.off('error', reject);
      resolve();
    });
  });
  const address = server.address();
  if (!address || typeof address === 'string') throw new Error('Expected a loopback TCP listener');
  return address.port;
}

export async function closeServer(server: Server): Promise<void> {
  server.closeAllConnections();
  await new Promise<void>((resolve, reject) =>
    server.close((error) =>
      error && (error as NodeJS.ErrnoException).code !== 'ERR_SERVER_NOT_RUNNING'
        ? reject(error)
        : resolve(),
    ),
  );
}

/** Only the model HTTP boundary is scripted: the pinned SDK still parses SSE and executes MCP. */
export class ScriptedMessages {
  readonly exchanges: MessagesExchange[] = [];
  /** Full stack history retained for diagnostics across scenario resets. */
  readonly history: MessagesExchange[] = [];
  readonly unexpected: string[] = [];
  private scripts: Array<{
    model: string;
    systemIncludes?: string;
    replies: Array<ScriptedReply | ((body: MessagesRequest) => ScriptedReply)>;
  }> = [];
  readonly server = createServer((req, res) => {
    void (async () => {
      // Pinned Claude SDK probes its base URL before sending Messages.
      if (req.method === 'HEAD' && req.url === '/api/hello') {
        res.writeHead(404);
        res.end();
        return;
      }
      if (req.method !== 'POST' || req.url?.split('?')[0] !== '/v1/messages') {
        throw new Error(`Unexpected model endpoint: ${req.method} ${req.url}`);
      }
      const body = JSON.parse(await readRequest(req)) as MessagesRequest;
      const script = this.scripts.find(
        (entry) =>
          entry.model === body.model &&
          (!entry.systemIncludes ||
            JSON.stringify(body.system ?? '').includes(entry.systemIncludes)),
      );
      if (!script) throw new Error(`No model script for ${body.model}`);
      const last = body.messages.at(-1)?.content;
      const toolResult =
        Array.isArray(last) &&
        last.some((block: { type?: string }) => block.type === 'tool_result');
      // Explicit zero usage makes tool-result followups cost-neutral. Nothing can fall back to a provider.
      const next = toolResult
        ? { text: 'Tool result received.', input: 0, output: 0 }
        : script.replies.shift();
      if (!next)
        throw new Error(`Model script exhausted for ${body.model}: ${JSON.stringify(last)}`);
      const reply = typeof next === 'function' ? next(body) : next;
      const messageId = `msg_spend_${this.history.length + 1}`;
      const exchange = { body, reply, messageId };
      this.exchanges.push(exchange);
      this.history.push(exchange);
      res.writeHead(200, { 'content-type': 'text/event-stream' });
      const event = (type: string, data: Record<string, unknown>) =>
        res.write(`event: ${type}\ndata: ${JSON.stringify({ type, ...data })}\n\n`);
      event('message_start', {
        message: {
          id: messageId,
          type: 'message',
          role: 'assistant',
          model: reply.actualModel ?? body.model,
          content: [],
          stop_reason: null,
          stop_sequence: null,
          usage: {
            input_tokens: reply.input ?? 1000,
            output_tokens: 0,
            cache_creation_input_tokens: 0,
            cache_read_input_tokens: 0,
          },
        },
      });
      event('content_block_start', {
        index: 0,
        content_block: reply.tool
          ? {
              type: 'tool_use',
              id: `toolu_spend_${this.exchanges.length}`,
              name: reply.tool.name,
              input: {},
            }
          : { type: 'text', text: '' },
      });
      event('content_block_delta', {
        index: 0,
        delta: reply.tool
          ? { type: 'input_json_delta', partial_json: JSON.stringify(reply.tool.input) }
          : { type: 'text_delta', text: reply.text ?? 'Fixture complete.' },
      });
      event('content_block_stop', { index: 0 });
      // SDK final usage is cumulative; output arrives only here to catch early accounting.
      event('message_delta', {
        delta: { stop_reason: reply.tool ? 'tool_use' : 'end_turn', stop_sequence: null },
        usage: { output_tokens: reply.output ?? 500 },
      });
      event('message_stop', {});
      res.end();
    })().catch((error: unknown) => {
      const message = String(error);
      this.unexpected.push(message);
      if (!res.headersSent) res.writeHead(400, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ type: 'error', error: { type: 'invalid_request_error', message } }));
    });
  });

  script(
    model: string,
    replies: Array<ScriptedReply | ((body: MessagesRequest) => ScriptedReply)>,
    systemIncludes?: string,
  ): void {
    const previous = this.scripts.find(
      (entry) => entry.model === model && entry.systemIncludes === systemIncludes,
    );
    if (previous?.replies.length) throw new Error(`Unconsumed model script for ${model}`);
    this.scripts = this.scripts.filter((entry) => entry !== previous);
    this.scripts.push({
      model,
      replies: [...replies],
      ...(systemIncludes ? { systemIncludes } : {}),
    });
  }

  assertHealthy(): void {
    if (this.unexpected.length) throw new Error(this.unexpected.join('\n'));
  }

  /** Called after terminating a case's sessions, before reusing native model IDs. */
  reset(): void {
    this.assertHealthy();
    this.scripts = [];
    this.exchanges.length = 0;
  }
}

// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import modelCatalog from './model-catalog.json' with { type: 'json' };
import { assertCodexTerminalUsage } from './usage.js';
import type { Transport } from '@modelcontextprotocol/sdk/shared/transport.js';
import { Codex, type Thread, type ModelReasoningEffort } from '@openai/codex-sdk';
import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import { StreamableHTTPServerTransport } from '@modelcontextprotocol/sdk/server/streamableHttp.js';
import {
  CallToolRequestSchema,
  ListToolsRequestSchema,
  type CallToolResult,
} from '@modelcontextprotocol/sdk/types.js';
import { createServer } from 'node:http';
import { createHash, randomUUID } from 'node:crypto';
import { mkdtemp, mkdir, readFile, writeFile, readdir, lstat, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, dirname } from 'node:path';
import type { WorkerCommand, WorkerEvent, StartCommand, CodexCheckpoint } from './wire.js';

// Keep decoded bounds aligned with harness-catalog/src/harness-models.ts validation.
const MAX_CHECKPOINT_BYTES = 16 * 1024 * 1024;
const MAX_CHECKPOINT_FILES = 32;
const THREAD_ID = /^[a-zA-Z0-9-]{1,128}$/;
const ROLLOUT_PATH = /^sessions\/[0-9]{4}\/[0-9]{2}\/[0-9]{2}\/rollout-[A-Za-z0-9_.-]+\.jsonl$/;

export type CodexFactory = (options: ConstructorParameters<typeof Codex>[0]) => Codex;
export interface CodexTurnOptions {
  apiKey: string;
  baseUrl?: string;
  system?: string;
}

/** SDK subprocess runtime. All tools leave this process through the Orca relay. */
export class CodexSdkWorker {
  private thread: Thread | undefined;
  private input: StartCommand | undefined;
  private relay: { port: number; token: string } | undefined;
  private home = '';
  private abort: AbortController | undefined;
  private active: Promise<void> | undefined;
  private http: ReturnType<typeof createServer> | undefined;
  private transports = new Set<StreamableHTTPServerTransport>();
  private pending = new Map<string, (value: CallToolResult) => void>();
  private started = false;
  private fatal = false;
  private starting: Promise<void> | undefined;
  private closing: Promise<void> | undefined;
  constructor(
    private readonly emit: (event: WorkerEvent) => void,
    private readonly makeCodex: CodexFactory = (options) => new Codex(options),
  ) {}

  async handle(command: WorkerCommand): Promise<void> {
    if (command.type === 'tool_result') {
      this.pending.get(command.id)?.(command.result);
      this.pending.delete(command.id);
    } else if (command.type === 'interrupt') {
      this.abort?.abort();
      this.cancelTools();
    } else if (command.type === 'stop') {
      await this.close();
    } else if (command.type === 'start') {
      if (this.started || this.closing)
        throw new Error('Codex SDK worker already started or closed');
      this.started = true;
      this.starting = this.start(command);
      try {
        await this.starting;
        if (this.closing) throw new Error('Codex SDK worker closed during startup');
        this.emit({ type: 'ready' });
      } finally {
        this.starting = undefined;
      }
    } else {
      if (!this.thread || this.active || this.closing || this.fatal)
        throw new Error('Codex SDK is not ready for a turn');
      this.active = this.run(command.text).finally(() => {
        this.active = undefined;
      });
      await this.active;
    }
  }

  private async start(input: StartCommand): Promise<void> {
    this.home = await mkdtemp(join(tmpdir(), 'orca-codex-sdk-'));
    // Never load ambient account credentials or project-selected MCP servers.
    await mkdir(this.home, { recursive: true, mode: 0o700 });
    if (input.checkpoint) await restoreCheckpoint(this.home, input.checkpoint, input.system);
    const catalogPath = join(this.home, 'model-catalog.json');
    await writeFile(catalogPath, JSON.stringify(modelCatalog), { mode: 0o600 });
    const token = randomUUID();
    this.http = createServer((req, res) => {
      if (req.headers.authorization !== `Bearer ${token}` || req.method !== 'POST') {
        res.writeHead(403).end();
        return;
      }
      const transport = new StreamableHTTPServerTransport({ enableJsonResponse: true });
      const server = new Server(
        { name: 'orca', version: '1.0.0' },
        { capabilities: { tools: {} } },
      );
      server.setRequestHandler(ListToolsRequestSchema, async () => ({ tools: input.tools }));
      server.setRequestHandler(CallToolRequestSchema, async (request) => {
        if (!input.tools.some((tool) => tool.name === request.params.name))
          throw new Error('tool not enabled');
        if (!this.abort || this.abort.signal.aborted) throw new Error('turn interrupted');
        const id = randomUUID();
        return await new Promise<CallToolResult>((resolve) => {
          this.pending.set(id, resolve);
          this.emit({
            type: 'tool_call',
            id,
            name: request.params.name,
            arguments: request.params.arguments ?? {},
          });
        });
      });
      this.transports.add(transport);
      res.on('close', () => {
        this.transports.delete(transport);
        void server.close();
      });
      void server
        .connect(transport as Transport)
        .then(() => transport.handleRequest(req, res))
        .catch(() => {
          if (!res.headersSent) res.writeHead(500);
          res.end();
        });
    });
    await new Promise<void>((resolve, reject) => {
      this.http!.once('error', reject);
      this.http!.listen(0, '127.0.0.1', resolve);
    });
    const address = this.http.address();
    if (!address || typeof address === 'string') throw new Error('MCP relay has no address');
    this.relay = { port: address.port, token };
    this.thread = this.createThread(input, input.checkpoint?.threadId);
    this.input = input;
  }

  /** Refresh egress credentials between turns, preserving the native thread and its instructions. */
  refreshOptions(options: CodexTurnOptions): void {
    if (!this.input || !this.thread || this.active || this.closing || this.fatal)
      throw new Error('Codex SDK credentials can only be refreshed while idle');
    if (!options.apiKey) throw new Error('Codex SDK requires an API key');
    // exec resume retains the original native developer instructions even when
    // its CLI config changes. Refuse a change rather than silently ignoring it.
    if (this.thread.id && options.system !== undefined && options.system !== this.input.system)
      throw new Error('Codex SDK cannot change developer instructions on an existing thread');
    const input = { ...this.input, ...options };
    const thread = this.createThread(input, this.thread.id ?? undefined);
    this.input = input;
    this.thread = thread;
  }

  private createThread(input: StartCommand, threadId?: string): Thread {
    const relay = this.relay!;
    const env: Record<string, string> = {
      HOME: this.home,
      CODEX_HOME: this.home,
      PATH: process.env.PATH ?? '/usr/bin:/bin',
    };
    // Disable SDK-native execution surfaces. Orca's relay is the only executable
    // tool surface; approvals, sandbox policy and event identities stay in Orca.
    const codex = this.makeCodex({
      apiKey: input.apiKey,
      ...(input.baseUrl ? { baseUrl: input.baseUrl } : {}),
      env,
      config: {
        model_catalog_json: join(this.home, 'model-catalog.json'),
        model_provider: 'orca',
        developer_instructions: input.system,
        tools: { experimental_request_user_input: { enabled: false } },
        features: {
          goals: false,
          tool_search: false,
          tool_search_always_defer_mcp_tools: false,
          default_mode_request_user_input: false,
          collaboration_modes: false,
          shell_tool: false,
          unified_exec: false,
          apply_patch_freeform: false,
          view_image: false,
          multi_agent: false,
          multi_agent_mode: false,
          multi_agent_v2: false,
          apps: false,
          plugins: false,
          hooks: false,
          plugin_hooks: false,
          js_repl: false,
          code_mode: false,
          browser_use: false,
          computer_use: false,
          image_generation: false,
          memories: false,
          exec_permission_approvals: false,
          request_permissions_tool: false,
          remote_models: false,
          responses_websockets: false,
          responses_websockets_v2: false,
          skill_mcp_dependency_install: false,
          skip_host_skill_discovery: true,
        },
      },
      // Replace the table as a whole, so a project .codex/config.toml cannot
      // add an unmediated MCP server beside Orca's relay.
      configOverrides: [
        `mcp_servers={orca={default_tools_approval_mode="approve",required=true,tool_timeout_sec=600,url="http://127.0.0.1:${relay.port}/mcp",http_headers={Authorization="Bearer ${relay.token}"}}}`,
        `model_providers={orca={name="Orca OpenAI",base_url=${JSON.stringify(input.baseUrl ?? 'https://api.openai.com/v1')},env_key="CODEX_API_KEY",wire_api="responses",http_headers={"X-Orca-Session-Id"=${JSON.stringify(input.sessionId)}}}}`,
      ],
    });
    const options = {
      model: input.model,
      workingDirectory: input.root,
      skipGitRepoCheck: true,
      approvalPolicy: 'never' as const,
      sandboxMode: 'read-only' as const,
      webSearchMode: 'disabled' as const,
      ...(input.effort ? { modelReasoningEffort: input.effort as ModelReasoningEffort } : {}),
    };
    return threadId ? codex.resumeThread(threadId, options) : codex.startThread(options);
  }

  private async run(text: string): Promise<void> {
    this.abort = new AbortController();
    try {
      const { events } = await this.thread!.runStreamed(text, { signal: this.abort.signal });
      for await (const event of events) {
        if (event.type === 'turn.completed') {
          try {
            assertCodexTerminalUsage(event.usage);
          } catch (error) {
            // Neither normalized accounting nor resumable history may acknowledge
            // an SDK turn whose original counters were malformed.
            this.fatal = true;
            throw error;
          }
        }
        this.emit({ type: 'event', event });
      }
    } catch (error) {
      if (!this.abort.signal.aborted || this.fatal)
        this.emit({
          type: 'failure',
          message: error instanceof Error ? error.message : 'Codex SDK failed',
          ...(this.fatal ? { fatal: true } : {}),
        });
    } finally {
      this.cancelTools();
      try {
        if (!this.fatal && this.thread?.id)
          this.emit({
            type: 'checkpoint',
            checkpoint: await captureCheckpoint(this.home, this.thread.id, this.input!.system),
          });
      } catch {
        this.fatal = true;
        this.emit({
          type: 'failure',
          message: 'Codex SDK native history could not be checkpointed',
          fatal: true,
        });
      }
      this.abort = undefined;
      this.emit({ type: 'done' });
    }
  }

  private cancelTools(): void {
    for (const resolve of this.pending.values())
      resolve({ isError: true, content: [{ type: 'text', text: 'turn interrupted' }] });
    this.pending.clear();
  }
  close(): Promise<void> {
    this.closing ??= this.dispose();
    return this.closing;
  }

  private async dispose(): Promise<void> {
    // A caller may close while startup is allocating its private directory or
    // binding the relay. Join that allocation before releasing its resources.
    await this.starting?.catch(() => undefined);
    this.abort?.abort();
    this.cancelTools();
    await this.active;
    for (const transport of this.transports) await transport.close();
    this.http?.closeAllConnections();
    await new Promise<void>((resolve) =>
      this.http ? this.http.close(() => resolve()) : resolve(),
    );
    if (this.home) await rm(this.home, { recursive: true, force: true });
  }
}

export async function captureCheckpoint(
  home: string,
  threadId: string,
  system: string,
): Promise<CodexCheckpoint> {
  if (!THREAD_ID.test(threadId)) throw new Error('invalid Codex thread id');
  const files: Record<string, string> = {};
  let bytes = 0;
  async function walk(dir: string): Promise<void> {
    for (const entry of await readdir(join(home, dir), { withFileTypes: true })) {
      const path = `${dir}/${entry.name}`;
      if (entry.isDirectory()) await walk(path);
      else if (entry.isFile() && ROLLOUT_PATH.test(path) && entry.name.includes(threadId)) {
        const stat = await lstat(join(home, path));
        bytes += stat.size;
        if (bytes > MAX_CHECKPOINT_BYTES) throw new Error('Codex history exceeds checkpoint limit');
        if (Object.keys(files).length >= MAX_CHECKPOINT_FILES)
          throw new Error('Codex history exceeds checkpoint file limit');
        files[path] = (await readFile(join(home, path))).toString('base64');
      }
    }
  }
  await walk('sessions');
  if (!Object.keys(files).length) throw new Error('Codex native rollout is missing');
  return { version: 1, threadId, files, instructionsSha256: instructionsDigest(system) };
}

function decodeCheckpoint(checkpoint: CodexCheckpoint): Array<readonly [string, Buffer]> {
  if (checkpoint.version !== 1 || !THREAD_ID.test(checkpoint.threadId))
    throw new Error('invalid Codex checkpoint');
  const entries = Object.entries(checkpoint.files);
  if (
    !entries.length ||
    entries.length > MAX_CHECKPOINT_FILES ||
    entries.some(([path]) => !ROLLOUT_PATH.test(path) || !path.includes(checkpoint.threadId))
  )
    throw new Error('invalid Codex rollout path');
  let bytes = 0;
  const decoded = entries.map(([path, data]) => {
    if (typeof data === 'string' && data.length > Math.ceil(MAX_CHECKPOINT_BYTES / 3) * 4)
      throw new Error('Codex history exceeds checkpoint limit');
    if (typeof data !== 'string' || data.length % 4 !== 0 || !/^[A-Za-z0-9+/]*={0,2}$/.test(data))
      throw new Error('invalid Codex checkpoint encoding');
    const buffer = Buffer.from(data, 'base64');
    bytes += buffer.length;
    if (bytes > MAX_CHECKPOINT_BYTES) throw new Error('Codex history exceeds checkpoint limit');
    return [path, buffer] as const;
  });
  return decoded;
}

function instructionsDigest(system: string): string {
  return createHash('sha256').update(system, 'utf8').digest('hex');
}

/** Native resume keeps its original instructions; changed catalogs must not be silently ignored. */
export function assertCodexCheckpointInstructions(
  checkpoint: CodexCheckpoint,
  system: string,
): void {
  if (checkpoint.instructionsSha256 !== undefined) {
    if (!/^[a-f0-9]{64}$/.test(checkpoint.instructionsSha256))
      throw new Error('invalid Codex checkpoint instructions digest');
    if (checkpoint.instructionsSha256 === instructionsDigest(system)) return;
  } else {
    // Legacy 0.154.0 rollouts preserve the managed text as the first input_text
    // block of the initial developer message. Other blocks include SDK-generated
    // instructions with temporary HOME paths and are not part of Orca's prompt.
    // Only this evidenced, unambiguous layout can establish legacy compatibility.
    const files = decodeCheckpoint(checkpoint);
    if (files.length === 1 && system.length > 0) {
      try {
        const records = files[0]![1]
          .toString('utf8')
          .trim()
          .split('\n')
          .map((line) => JSON.parse(line));
        const first = records[0];
        const initialMessage = records.find((record) => record.type === 'response_item');
        if (
          first?.type === 'session_meta' &&
          first.payload?.id === checkpoint.threadId &&
          first.payload?.cli_version === '0.154.0' &&
          initialMessage?.payload?.type === 'message' &&
          initialMessage.payload.role === 'developer' &&
          initialMessage.payload.content?.[0]?.type === 'input_text' &&
          initialMessage.payload.content[0].text === system
        )
          return;
      } catch {
        // An incomplete or unfamiliar legacy record does not establish compatibility.
      }
    }
  }
  throw new Error(
    'Codex checkpoint developer instructions differ or cannot be verified; native history was preserved',
  );
}

/**
 * Transition a verified managed instruction catalog in the pinned native rollout.
 * The caller must authorize the previous instructions against the immutable Agent
 * pins. Conversation/tool records and the thread identity are preserved verbatim.
 */
export function transitionCodexCheckpointInstructions(
  checkpoint: CodexCheckpoint,
  system: string,
  acceptsPreviousSystem: (system: string) => boolean,
): CodexCheckpoint {
  try {
    assertCodexCheckpointInstructions(checkpoint, system);
    return checkpoint;
  } catch {
    // A mismatch is repairable only with an evidenced native layout and old digest.
  }
  const files = decodeCheckpoint(checkpoint);
  if (files.length !== 1) throw new Error('unsupported Codex instruction transition layout');
  const [path, bytes] = files[0]!;
  const lines = bytes.toString('utf8').trimEnd().split('\n');
  const records = lines.map((line) => JSON.parse(line));
  const meta = records[0];
  const initial = records.find((record) => record.type === 'response_item');
  if (
    meta?.type !== 'session_meta' ||
    meta.payload?.id !== checkpoint.threadId ||
    meta.payload?.cli_version !== '0.154.0' ||
    initial?.payload?.type !== 'message' ||
    initial.payload.role !== 'developer' ||
    initial.payload.content?.[0]?.type !== 'input_text' ||
    typeof initial.payload.content[0].text !== 'string'
  )
    throw new Error('unsupported Codex instruction transition layout');
  const previous =
    checkpoint.instructionsSha256 === instructionsDigest('')
      ? ''
      : (initial.payload.content[0].text as string);
  assertCodexCheckpointInstructions(checkpoint, previous);
  if (!acceptsPreviousSystem(previous))
    throw new Error('Codex checkpoint instruction transition is not a pinned Skill policy change');
  const replace = (message: typeof initial.payload): void => {
    if (previous === '') {
      if (system !== '') message.content.unshift({ type: 'input_text', text: system });
    } else {
      if (message.content?.[0]?.text !== previous)
        throw new Error('Codex compacted instructions cannot be verified');
      if (system === '') message.content.shift();
      else message.content[0].text = system;
    }
  };
  replace(initial.payload);
  const changed = records.map((record, index) => {
    if (record === initial) return JSON.stringify(record);
    if (record.type === 'compacted') {
      // Native compaction can retain a replacement conversation. Apply the
      // same verified catalog to its initial developer message as well.
      const history = record.payload?.replacement_history;
      if (!Array.isArray(history))
        throw new Error('unsupported Codex compacted instruction transition layout');
      const developer = history.find((item: { role?: string }) => item.role === 'developer');
      if (!developer) throw new Error('Codex compacted developer instructions are missing');
      replace(developer);
      return JSON.stringify(record);
    }
    return lines[index]!;
  });
  const transitioned: CodexCheckpoint = {
    ...checkpoint,
    instructionsSha256: instructionsDigest(system),
    files: { [path]: Buffer.from(changed.join('\n') + '\n').toString('base64') },
  };
  decodeCheckpoint(transitioned);
  return transitioned;
}

export async function restoreCheckpoint(
  home: string,
  checkpoint: CodexCheckpoint,
  system: string,
): Promise<void> {
  const decoded = decodeCheckpoint(checkpoint);
  assertCodexCheckpointInstructions(checkpoint, system);
  // The checkpoint is authoritative; remove stale partial turns before restoring.
  await rm(join(home, 'sessions'), { recursive: true, force: true });
  for (const [path, data] of decoded) {
    await mkdir(dirname(join(home, path)), { recursive: true, mode: 0o700 });
    await writeFile(join(home, path), data, { mode: 0o600 });
  }
}

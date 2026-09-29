// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { assertSdkTerminalUsage as assertCodexTerminalUsage } from '@orca/sdk-harness';
import {
  assertPiCheckpointInstructions,
  transitionPiCheckpointInstructions,
} from '@orca/pi-harness';
import { parseCustomTools } from '../../custom-tools.js';
import {
  isPinnedSkillsCatalog,
  RESOURCE_CHECKPOINT_EVENT,
  type SandboxHandle,
} from '../../sandbox/seam.js';
import { mkdir } from 'node:fs/promises';
import { join } from 'node:path';
import type { Transport } from '@modelcontextprotocol/sdk/shared/transport.js';
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { InMemoryTransport } from '@modelcontextprotocol/sdk/inMemory.js';
import { StreamableHTTPClientTransport } from '@modelcontextprotocol/sdk/client/streamableHttp.js';
import type { Tool, CallToolResult } from '@modelcontextprotocol/sdk/types.js';
import type { SdkEvent as ThreadEvent } from '@orca/sdk-harness';
import { existsSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { randomUUID } from 'node:crypto';
import { buildNativeCliBridgeServer } from '../../mcp/native-cli-bridge.js';
import { launchNativeCli, type NativeCliProcess } from '../../sandbox/native-cli-launcher.js';
import type {
  AgentHarness,
  AgentEvent,
  SessionStartInput,
  UserEvent,
  TerminationReason,
} from '../agent-harness.js';
import { terminalError } from '../agent-harness.js';
import type { WorkerCommand, WorkerEvent, CodexCheckpoint } from '@orca/codex-harness';
import {
  assertCodexCheckpointInstructions,
  transitionCodexCheckpointInstructions,
  customToolResultToMcp,
  CustomToolResultConversionError,
} from '@orca/codex-harness';

import { HARNESS_CHECKPOINT_EVENT } from '@orca/harness-tunnel';
export interface CodexSdkOptions {
  provider?: 'codex-sdk' | 'pi-sdk';
  sandbox?: SandboxHandle;
  apiKey: string;
  baseUrl?: string;
  piGatewayUrl?: string;
  checkpoint?: CodexCheckpoint;
  launch?: typeof launchNativeCli;
  workerCommand?: string;
  workerArgs?: string[];
  timeoutMs?: number;
}
interface ToolBinding {
  custom?: true;
  serverName?: string;
  remoteName?: string;
  tool: Tool;
  name: string;
  call(input: Record<string, unknown>): Promise<CallToolResult>;
}

/** The SDK owns its native thread. Orca owns tools, approval, persistence and events. */
export class CodexSdkHarness implements AgentHarness {
  readonly preserveSandboxOnRefresh = true;
  private input: SessionStartInput | undefined;
  private child: NativeCliProcess | undefined;
  private reader: Promise<void> | undefined;
  private clients: Client[] = [];
  private tools = new Map<string, ToolBinding>();
  private queue: AgentEvent[] = [];
  private waiters: Array<(event: IteratorResult<AgentEvent>) => void> = [];
  private closed = false;
  private pending: { resolve(): void; reject(error: Error): void } | undefined;
  private running = false;
  private interrupted = false;
  private failed = false;
  private fatal = false;
  private checkpoint: CodexCheckpoint | undefined;
  private toolCalls = new Set<Promise<void>>();
  private customToolIds = new Set<string>();
  private abort: AbortController | undefined;
  constructor(private readonly options: CodexSdkOptions) {
    this.checkpoint = options.checkpoint;
  }

  private get sdkName(): string {
    return this.options.provider === 'pi-sdk' ? 'Pi SDK' : 'Codex SDK';
  }
  private get harnessName(): string {
    return this.options.provider === 'pi-sdk' ? 'pi_sdk' : 'codex_sdk';
  }

  async start(input: SessionStartInput): Promise<void> {
    if (this.input) throw new Error(`${this.sdkName} already started`);
    const sandbox = input.sandbox ?? this.options.sandbox;
    if (!sandbox?.spawn)
      throw new Error(`${this.harnessName} requires a sandbox with streaming process support`);
    if (
      (this.options.provider !== 'pi-sdk' && input.agentSnapshot.model_provider !== 'openai') ||
      !input.agentSnapshot.model_provider ||
      !input.agentSnapshot.model_id
    )
      throw new Error(`${this.harnessName} requires a pinned supported model`);
    if (this.checkpoint) {
      const system = input.agentSnapshot.system ?? '';
      const catalog = input.managedSkillCatalog;
      if (catalog) {
        if (!isPinnedSkillsCatalog(system, catalog.baseSystem, catalog.skills))
          throw new Error('managed Skill instructions differ from the verified pins');
        this.checkpoint = (
          this.options.provider === 'pi-sdk'
            ? transitionPiCheckpointInstructions
            : transitionCodexCheckpointInstructions
        )(this.checkpoint, system, (previous) =>
          isPinnedSkillsCatalog(previous, catalog.baseSystem, catalog.skills),
        );
      }
      (this.options.provider === 'pi-sdk'
        ? assertPiCheckpointInstructions
        : assertCodexCheckpointInstructions)(this.checkpoint, system);
    }
    if (!this.options.apiKey)
      throw new Error(`${this.harnessName} requires gateway LLM credentials or OPENAI_API_KEY`);
    if (input.agentSnapshot.skills_plugin_dir || input.delegate)
      throw new Error(
        `${this.harnessName} does not support Claude Skills plugins or multiagent rosters`,
      );
    if ((input.toolSandbox === undefined) !== (input.runToolWithResources === undefined))
      throw new Error(`${this.harnessName} managed tools require a resource persistence barrier`);
    this.input = input;
    const root = (sandbox as SandboxHandle & { rootDir?(): string }).rootDir?.();
    if (!root) throw new Error(`${this.harnessName} requires a sandbox-visible root directory`);
    try {
      const server = buildNativeCliBridgeServer(input.toolSandbox ?? sandbox, {
        allowedLogicalNames: input.agentSnapshot.allowed_tool_names ?? [],
      });
      const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
      const local = new Client({ name: 'orca-codex-sdk', version: '1.0.0' });
      this.clients.push(local);
      await server.connect(serverTransport);
      await local.connect(clientTransport);
      if (local.getServerCapabilities()?.tools) await this.addTools(undefined, local);
      for (const { serverName } of input.remoteMcpToolsets ?? []) {
        const config = input.mcpServers?.[serverName];
        if (!config) throw new Error(`missing MCP configuration: ${serverName}`);
        const client = new Client({ name: 'orca-codex-sdk', version: '1.0.0' });
        this.clients.push(client);
        await client.connect(
          new StreamableHTTPClientTransport(new URL(config.url), {
            requestInit: { headers: config.headers },
          }) as Transport,
        );
        await this.addTools(serverName, client);
      }
      for (const tool of parseCustomTools(
        input.agentSnapshot.custom_tools ?? [],
        input.agentSnapshot.allowed_tool_names ?? [],
      )) {
        if (this.tools.has(tool.name)) throw new Error(`duplicate tool: ${tool.name}`);
        if (!input.awaitCustomToolResult)
          throw new Error('custom tool callback routing is unavailable');
        const { properties, required, ...schema } = tool.input_schema;
        this.tools.set(tool.name, {
          name: tool.name,
          custom: true,
          tool: {
            name: tool.name,
            description: tool.description,
            inputSchema: {
              ...schema,
              ...(properties ? { properties } : {}),
              ...(required ? { required } : {}),
            } as Tool['inputSchema'],
          },
          call: async () => {
            throw new Error('custom tools require a client callback');
          },
        });
      }
      const built = fileURLToPath(
        new URL(
          `./harness/${this.options.provider ?? 'codex-sdk'}/worker-entry.js`,
          import.meta.url,
        ),
      );
      const sibling = fileURLToPath(
        new URL(
          this.options.provider === 'pi-sdk' ? '../pi-sdk/worker-entry.js' : './worker-entry.js',
          import.meta.url,
        ),
      );
      const entry = existsSync(built) ? built : sibling;
      // The raw worker handle is separate from the policy-enforced model-tool root.
      // env -i is required even when spawn merges process.env into its shell.
      const workerRoot = join(root, '.orca-worker');
      const workerHome = join(workerRoot, 'home');
      const workerTmp = join(workerRoot, 'tmp');
      const workerCwd = join(workerRoot, 'cwd');
      await Promise.all(
        [workerHome, workerTmp, workerCwd].map((dir) =>
          mkdir(dir, { recursive: true, mode: 0o700 }),
        ),
      );
      const env = [
        `PATH=${process.env.PATH ?? '/usr/local/bin:/usr/bin:/bin'}`,
        `HOME=${workerHome}`,
        `TMPDIR=${workerTmp}`,
        ...['NODE_EXTRA_CA_CERTS', 'SSL_CERT_FILE', 'SSL_CERT_DIR'].flatMap((key) =>
          process.env[key] ? [`${key}=${process.env[key]}`] : [],
        ),
      ];
      this.child = (this.options.launch ?? launchNativeCli)(sandbox, {
        cmd: '/usr/bin/env',
        args: [
          '-i',
          ...env,
          this.options.workerCommand ?? process.execPath,
          ...(this.options.workerArgs ?? [entry]),
        ],
        cwd: '/.orca-worker/cwd',
        env: { BASH_ENV: '/dev/null', ENV: '/dev/null' },
      });
      const ready = this.waitForBoundary();
      this.reader = this.readEvents();
      this.send({
        type: 'start',
        root: workerCwd,
        sessionId: input.sessionId,
        model: input.agentSnapshot.model_id,
        modelProvider: input.agentSnapshot.model_provider,
        system: input.agentSnapshot.system ?? '',
        apiKey: this.options.apiKey,
        ...(this.options.baseUrl ? { baseUrl: this.options.baseUrl } : {}),
        ...(this.options.provider === 'pi-sdk' && this.options.piGatewayUrl
          ? { piGatewayUrl: this.options.piGatewayUrl }
          : {}),
        ...(input.agentSnapshot.model_effort ? { effort: input.agentSnapshot.model_effort } : {}),
        tools: [...this.tools.values()].map((binding) => binding.tool),
        ...(this.checkpoint ? { checkpoint: this.checkpoint } : {}),
      });
      await ready;
    } catch (error) {
      await this.stop('error');
      throw error;
    }
  }

  private async addTools(server: string | undefined, client: Client): Promise<void> {
    let cursor: string | undefined;
    do {
      const result = await client.listTools(cursor ? { cursor } : {});
      for (const tool of result.tools) {
        const name = server === undefined ? tool.name : `mcp__${server}__${tool.name}`;
        if (this.tools.has(name)) throw new Error(`duplicate tool: ${name}`);
        this.tools.set(name, {
          name,
          ...(server === undefined ? {} : { serverName: server, remoteName: tool.name }),
          tool: { ...tool, name },
          call: async (args) =>
            (await client.callTool({ name: tool.name, arguments: args })) as CallToolResult,
        });
      }
      cursor = result.nextCursor;
    } while (cursor);
  }

  async submit(event: UserEvent): Promise<void> {
    if (this.closed || !this.child || this.running || this.fatal)
      throw new Error(`${this.sdkName} is unavailable`);
    if (event.kind !== 'user.message') return;
    const payload = event.payload as { content?: unknown; text?: unknown };
    const content = payload?.content;
    const text =
      typeof content === 'string'
        ? content
        : Array.isArray(content)
          ? content
              .map((block: { type?: string; text?: string }) => {
                if (block.type !== 'text' || typeof block.text !== 'string')
                  throw new Error(`${this.harnessName} currently accepts text messages`);
                return block.text;
              })
              .join('\n')
          : typeof payload?.text === 'string'
            ? payload.text
            : '';
    if (!text) throw new Error(`${this.harnessName} requires a text message`);
    this.running = true;
    this.failed = false;
    this.interrupted = false;
    this.abort = new AbortController();
    this.emit({ kind: 'session.status_running', payload: {} });
    try {
      const done = this.waitForBoundary();
      this.send({ type: 'submit', text });
      await done;
    } catch (error) {
      this.fault(error instanceof Error ? error.message : `${this.sdkName} failed`);
      this.child?.kill();
      this.child = undefined;
    } finally {
      this.abort.abort();
      await Promise.allSettled(this.toolCalls);
      this.running = false;
      if (!this.failed)
        this.emit({ kind: 'session.status_idle', payload: { stop_reason: { type: 'end_turn' } } });
      this.emit({ kind: 'agent.turn_completed', payload: {} });
    }
  }

  interrupt(): void {
    this.interrupted = true;
    this.abort?.abort();
    this.send({ type: 'interrupt' });
  }
  async stop(_reason: TerminationReason): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    this.abort?.abort();
    this.child?.kill();
    this.pending?.reject(new Error(`${this.sdkName} stopped`));
    this.pending = undefined;
    await this.reader;
    await Promise.allSettled(this.clients.map((client) => client.close()));
    for (const waiter of this.waiters.splice(0)) waiter({ value: undefined, done: true });
  }
  events(): AsyncIterable<AgentEvent> {
    return {
      [Symbol.asyncIterator]: () => ({
        next: async () => {
          const event = this.queue.shift();
          if (event) return { value: event, done: false };
          if (this.closed) return { value: undefined, done: true };
          return await new Promise<IteratorResult<AgentEvent>>((resolve) =>
            this.waiters.push(resolve),
          );
        },
      }),
    };
  }
  private emit(event: AgentEvent): void {
    const waiter = this.waiters.shift();
    if (waiter) waiter({ value: event, done: false });
    else this.queue.push(event);
  }
  private fault(message: string): void {
    if (this.failed) return;
    this.failed = true;
    this.emit({ kind: 'session.status_idle', payload: { stop_reason: { type: 'end_turn' } } });
    this.emit(terminalError(message));
  }
  private send(command: WorkerCommand): void {
    this.child?.write(JSON.stringify(command) + '\n');
  }
  private waitForBoundary(): Promise<void> {
    let timer: ReturnType<typeof setTimeout>;
    return new Promise<void>((resolve, reject) => {
      this.pending = { resolve, reject };
      timer = setTimeout(
        () => reject(new Error(`${this.sdkName} request timed out`)),
        this.options.timeoutMs ?? 600_000,
      );
    }).finally(() => {
      clearTimeout(timer);
      this.pending = undefined;
    });
  }
  private async readEvents(): Promise<void> {
    try {
      for await (const line of this.child!.lines()) {
        const event = JSON.parse(line) as WorkerEvent;
        if (event.type === 'ready' || event.type === 'done') this.pending?.resolve();
        else if (event.type === 'failure') {
          if (event.fatal) {
            this.fatal = true;
            this.abort?.abort();
            this.pending?.reject(new Error(event.message));
          }
          if (!this.running) this.pending?.reject(new Error(event.message));
          else this.fault(event.message);
        } else if (event.type === 'event') this.mapEvent(event.event);
        else if (event.type === 'checkpoint') {
          this.checkpoint = event.checkpoint;
          this.emit({
            kind: HARNESS_CHECKPOINT_EVENT,
            payload: { provider: this.options.provider ?? 'codex-sdk', state: event.checkpoint },
          });
        } else if (event.type === 'tool_call') {
          const call = this.callTool(event).finally(() => {
            this.toolCalls.delete(call);
          });
          this.toolCalls.add(call);
        }
      }
      if (!this.closed)
        this.pending?.reject(new Error(`${this.sdkName} process exited before completion`));
    } catch {
      this.pending?.reject(new Error(`${this.sdkName} worker stream failed`));
    }
  }
  private mapEvent(event: ThreadEvent): void {
    if (event.type === 'item.completed' && event.item.type === 'agent_message') {
      this.emit({
        kind: 'agent.message',
        payload: { content: [{ type: 'text', text: event.item.text }] },
      });
    } else if (event.type === 'turn.completed') {
      assertCodexTerminalUsage(event.usage);
      this.emit({
        kind: 'agent.usage',
        payload: {
          usage: {
            input_tokens: event.usage.input_tokens - event.usage.cached_input_tokens,
            output_tokens: event.usage.output_tokens,
            cache_read_input_tokens: event.usage.cached_input_tokens,
            cache_creation: {
              ephemeral_5m_input_tokens:
                (event.usage.cache_write_input_tokens ?? 0) -
                (event.usage.cache_write_input_tokens_1h ?? 0),
              ephemeral_1h_input_tokens: event.usage.cache_write_input_tokens_1h ?? 0,
            },
          },
        },
      });
    } else if ((event.type === 'turn.failed' || event.type === 'error') && !this.interrupted) {
      this.fault(event.type === 'error' ? event.message : event.error.message);
    } else if (
      event.type === 'item.started' &&
      ['command_execution', 'file_change', 'web_search'].includes(event.item.type)
    ) {
      this.fault(`${this.sdkName} attempted an unmediated native tool`);
      this.interrupt();
    }
  }
  private async callCustomTool(event: Extract<WorkerEvent, { type: 'tool_call' }>): Promise<void> {
    let result: CallToolResult;
    const id = `evt_${randomUUID().replaceAll('-', '')}`;
    try {
      if (!this.running || this.abort?.signal.aborted || !this.input?.awaitCustomToolResult)
        throw new Error('custom tool callback is unavailable');
      // Park synchronously BEFORE publishing: even an immediate client reply finds
      // its routing slot. The worker's id stays private and never correlates public events.
      const pending = this.input.awaitCustomToolResult(id, this.abort!.signal);
      this.customToolIds.add(id);
      this.emit({
        id,
        kind: 'agent.custom_tool_use',
        payload: { id, name: event.name, input: event.arguments },
      });
      this.emit({
        kind: 'session.status_idle',
        payload: { stop_reason: { type: 'requires_action', event_ids: [...this.customToolIds] } },
      });
      const reply = await pending;
      if (this.abort?.signal.aborted) throw new Error('custom tool callback aborted');
      result = customToolResultToMcp(reply);
    } catch (error) {
      if (error instanceof CustomToolResultConversionError) {
        this.fault(error.message);
        this.interrupt();
      }
      result = toolError(error);
    } finally {
      this.customToolIds.delete(id);
      if (
        !this.closed &&
        !this.failed &&
        !this.abort?.signal.aborted &&
        this.customToolIds.size === 0
      )
        this.emit({ kind: 'session.status_running', payload: {} });
    }
    if (!this.closed && !this.abort?.signal.aborted)
      this.send({ type: 'tool_result', id: event.id, result });
  }

  private async callTool(event: Extract<WorkerEvent, { type: 'tool_call' }>): Promise<void> {
    const binding = this.tools.get(event.name);
    if (binding?.custom) {
      await this.callCustomTool(event);
      return;
    }
    const id = `toolu_${randomUUID()}`;
    let result: CallToolResult;
    let emitted = false;
    this.emit({
      id,
      kind: binding?.serverName ? 'agent.mcp_tool_use' : 'agent.tool_use',
      payload: {
        tool_use_id: id,
        name: binding?.remoteName ?? event.name,
        input: event.arguments,
        ...(binding?.serverName ? { mcp_server_name: binding.serverName } : {}),
      },
    });
    try {
      if (!binding || !this.running || this.abort?.signal.aborted)
        throw new Error('tool is not available');
      let args = event.arguments;
      const policy = this.input!.toolPermissions?.policyFor(binding.name) ?? 'always_ask';
      if (policy !== 'always_allow' && policy !== 'always_ask')
        throw new Error('tool denied by policy');
      if (policy === 'always_ask') {
        if (!this.input!.confirmTool) throw new Error('tool confirmation is unavailable');
        this.emit({
          kind: 'agent.requires_action',
          payload: { action: 'tool_confirmation', tool_use_id: id, tool_name: binding.name },
        });
        const signal = this.abort!.signal;
        let onAbort: () => void = () => {};
        const aborted = new Promise<never>((_, reject) => {
          onAbort = () => reject(new Error('turn interrupted'));
          signal.addEventListener('abort', onAbort, { once: true });
        });
        try {
          const decision = await Promise.race([
            this.input!.confirmTool(binding.name, args, { toolUseId: id }),
            aborted,
          ]);
          if (decision.behavior === 'deny') throw new Error(decision.message);
          args = decision.updatedInput;
        } finally {
          signal.removeEventListener('abort', onAbort);
        }
      }
      if (this.abort?.signal.aborted) throw new Error('turn interrupted');
      const operation = async (): Promise<CallToolResult> => {
        let value: CallToolResult;
        try {
          value = await binding.call(args);
        } catch (error) {
          value = toolError(error);
        }
        this.emit({
          kind: binding?.serverName ? 'agent.mcp_tool_result' : 'agent.tool_result',
          payload: {
            tool_use_id: id,
            ...(binding?.serverName
              ? { mcp_tool_use_id: id, mcp_server_name: binding.serverName }
              : {}),
            content: value.content,
            is_error: value.isError ?? false,
          },
        });
        emitted = true;
        return value;
      };
      if (this.input!.runToolWithResources) {
        try {
          result = await this.input!.runToolWithResources(
            operation,
            (checkpoint, digest) => {
              this.emit({
                kind: RESOURCE_CHECKPOINT_EVENT,
                payload: {
                  checkpoint_id: checkpoint.checkpoint_id,
                  manifest_sha256: digest,
                  revision: checkpoint.revision,
                },
              });
            },
            this.abort!.signal,
          );
        } catch (error) {
          // Persistence failure must never become a recoverable model tool error.
          this.fatal = true;
          this.fault(error instanceof Error ? error.message : 'resource persistence failed');
          this.abort?.abort();
          this.pending?.reject(new Error('resource persistence failed'));
          this.child?.kill();
          return;
        }
      } else result = await operation();
    } catch (error) {
      result = toolError(error);
    }
    if (!emitted)
      this.emit({
        kind: binding?.serverName ? 'agent.mcp_tool_result' : 'agent.tool_result',
        payload: {
          tool_use_id: id,
          ...(binding?.serverName
            ? { mcp_tool_use_id: id, mcp_server_name: binding.serverName }
            : {}),
          content: result.content,
          is_error: result.isError ?? false,
        },
      });
    this.send({ type: 'tool_result', id: event.id, result });
  }
}

function toolError(error: unknown): CallToolResult {
  return {
    isError: true,
    content: [{ type: 'text', text: error instanceof Error ? error.message : 'tool failed' }],
  };
}

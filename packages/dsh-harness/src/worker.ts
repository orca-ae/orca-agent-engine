// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { spawn, type ChildProcessWithoutNullStreams } from 'node:child_process';
import { createRequire } from 'node:module';
import { mkdtemp, mkdir, cp, readFile, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { JsonRpcLineTransport } from '@deepseek-ai/dsh-sdk-protocol';
import {
  assertSdkTerminalUsage,
  sdkExecutionBudgetMs,
  sdkTimeoutSignal,
  type SdkWorker,
  type SdkTurnOptions,
} from '@orca/sdk-harness';
import { dshModel, validateHarnessModel } from '@orca/harness-catalog';
import {
  assertDshCheckpointInstructions,
  decodeCheckpoint,
  assertDshCheckpointModel,
  DSH_SDK_VERSION,
  NATIVE_ID,
} from './checkpoint.js';
import { gatewayBase, assertDshProtocol } from './transport.js';
import { boundedLines } from './bounds.js';
import {
  nativeTools,
  nativeArguments,
  type DshWorkerCommand,
  type DshWorkerEvent,
  type DshStartCommand,
  type NativeName,
} from './orchestration.js';
import { archivedSubpaths, assertNotArchived } from './archive.js';

export class DshSdkWorker implements SdkWorker {
  private child: ChildProcessWithoutNullStreams | undefined;
  private rpc: JsonRpcLineTransport | undefined;
  private home: string | undefined;
  private input?: DshStartCommand;
  private archived = new Set<string>();
  private active: Promise<void> | undefined;
  private interrupts = new Set<Promise<unknown>>();
  private closing?: Promise<void>;
  private starting: Promise<void> | undefined;
  private reaping: Promise<void> | undefined;
  private started = false;
  private fatal = false;
  private pending = new Map<
    string,
    { resolve: (value: unknown) => void; subpath: string; authorization: boolean }
  >();
  private parents = new Map<string, string>();
  constructor(private readonly emit: (event: DshWorkerEvent) => void) {}

  async handle(
    command: DshWorkerCommand & { sourceEventId?: string; threadId?: string },
  ): Promise<void> {
    if (command.type === 'orchestration_result') {
      const pending = this.pending.get(command.id);
      if (pending?.authorization) {
        pending.resolve({ allowed: command.allowed === true, reason: command.reason });
        this.pending.delete(command.id);
      }
      return;
    }
    if (command.type === 'tool_result') {
      const pending = this.pending.get(command.id);
      if (pending && !pending.authorization) {
        pending.resolve(command.result);
        this.pending.delete(command.id);
      }
      return;
    }
    if (command.type === 'archive_fence') {
      if (!this.rpc || this.fatal || this.closing)
        throw new Error('DSH archive fence requires live worker');
      if (
        typeof command.threadId !== 'string' ||
        !NATIVE_ID.test(command.threadId) ||
        !this.parents.has('subagents/' + command.threadId)
      )
        throw new Error('DSH archive fence target outside tree');
      this.archived.add('subagents/' + command.threadId);
      // Fence-only: do not cancel tools, drain native work, or wait for the active submit.
      await this.rpc.request(
        'archive_fence',
        { threadId: command.threadId },
        AbortSignal.timeout(10000),
      );
      return;
    }
    if (command.type === 'stop') return this.close();
    if (command.type === 'interrupt') {
      if (command.archived !== undefined && typeof command.archived !== 'boolean')
        throw new Error('Invalid DSH archive command');
      if (command.archived === true) {
        if (!this.rpc || this.fatal || this.closing)
          throw new Error('DSH archive requires live worker');
        if (!command.threadId) throw new Error('DSH archive requires a thread target');
        const target = 'subagents/' + command.threadId;
        if (!this.parents.has(target)) throw new Error('DSH archive target outside tree');
        this.archived.add(target);
      }
      if (command.archived !== true)
        this.cancelTools(command.threadId ? 'subagents/' + command.threadId : undefined);
      if (this.active || command.archived === true) {
        try {
          const interrupted = this.rpc!.request(
            'interrupt',
            { threadId: command.threadId, archived: command.archived },
            // Targeted host cleanup allows 10s plus transport overhead; do not race its deadline.
            AbortSignal.timeout(command.threadId ? 15000 : 10000),
          );
          this.interrupts.add(interrupted);
          // Write the archive fence before denied tool replies can wake native queued work.
          if (command.archived === true)
            this.cancelTools(command.threadId ? 'subagents/' + command.threadId : undefined);
          try {
            await interrupted;
          } finally {
            this.interrupts.delete(interrupted);
          }
        } catch (error) {
          if (command.threadId)
            throw new Error('DSH target interrupt failed; sibling work remains active', {
              cause: error,
            });
          await this.close();
          throw new Error('DSH interrupt failed; process reaped', { cause: error });
        }
        if (!command.threadId) await this.active;
      }
      return;
    }
    if (command.type === 'start') {
      if (this.started || this.starting || this.closing)
        throw new Error('DSH already started or closed');
      this.started = true;
      this.starting = this.start(command);
      try {
        await this.starting;
        this.emit({ type: 'ready' });
      } catch (error) {
        try {
          await this.reap();
        } catch (cleanup) {
          throw new AggregateError([error, cleanup], 'DSH startup and cleanup failed');
        }
        throw error;
      } finally {
        this.starting = undefined;
      }
      return;
    }
    if (!this.rpc || !this.input || this.active || this.closing || this.fatal)
      throw new Error('DSH not ready for a turn');
    if (!command.sourceEventId?.startsWith('evt_') || command.sourceEventId.length <= 4)
      throw new Error('DSH requires accepted root sourceEventId');
    assertNotArchived(
      command.threadId ? 'subagents/' + command.threadId : '',
      this.archived,
      (path) => this.parents.get(path),
    );
    const budget = sdkExecutionBudgetMs(command.executionBudgetMs);
    const watchdog = sdkTimeoutSignal(
      budget + (command.executionBudgetMs === undefined ? 0 : 30_000),
    );
    this.active = this.rpc
      .request(
        'submit',
        {
          text: command.text,
          sourceEventId: command.sourceEventId,
          threadId: command.threadId,
          executionBudgetMs: command.executionBudgetMs,
        },
        watchdog.signal,
      )
      .then(async () => {
        if (this.fatal) {
          // Submit can finish before an in-flight interrupt ACK reaches the host.
          await Promise.allSettled([...this.interrupts]);
          await this.reap();
        }
      })
      .catch(async () => {
        this.fatal = true;
        this.emit({ type: 'failure', fatal: true, message: 'DSH worker transport failed' });
        this.emit({ type: 'done' });
        await this.reap();
      })
      .finally(() => {
        watchdog.dispose();
        this.active = undefined;
      });
    await this.active;
  }

  private async start(input: DshStartCommand): Promise<void> {
    if (input.tools.some((tool) => nativeTools.has(tool.name)))
      throw new Error('Reserved DSH native tool name');
    this.archived = archivedSubpaths(input.archivedThreadSubpaths);
    // Host admission requires effective always_deny for web_fetch; never expose its executable.
    input = {
      ...input,
      modelProvider: input.modelProvider ?? 'deepseek',
      tools: input.tools.filter((tool) => tool.name !== 'web_fetch'),
    };
    gatewayBase(input.dshGatewayUrl);
    if (!input.apiKey || input.baseUrl)
      throw new Error('DSH requires scoped credentials and Gateway origin');
    const error = validateHarnessModel(
      { harness: 'dsh_sdk' },
      {
        provider: input.modelProvider!,
        id: input.model,
        ...(input.effort ? { effort: input.effort } : {}),
      },
    );
    if (error) throw new Error(error);
    assertDshProtocol(dshModel(input.modelProvider!, input.model)!.api);
    if (input.checkpoint) {
      assertDshCheckpointInstructions(input.checkpoint, input.system);
      assertDshCheckpointModel(input.checkpoint, input.modelProvider!, input.model);
    }
    this.input = input;
    const require = createRequire(import.meta.url);
    this.home = await mkdtemp(join(tmpdir(), 'orca-dsh-'));
    const profile = join(this.home, 'profiles/orca-managed');
    await cp(
      dirname(require.resolve('@orca/dsh-harness/profile/orca-managed/package.json')),
      profile,
      {
        recursive: true,
      },
    );
    const patch = join(profile, 'cordis.patch.yml');
    await writeFile(
      patch,
      (await readFile(patch, 'utf8')).replace(
        'ORCA_DSH_PLUGIN',
        JSON.stringify(require.resolve('@orca/dsh-harness/plugin')),
      ),
    );
    if (input.checkpoint) {
      for (const [name, data] of Object.entries(decodeCheckpoint(input.checkpoint))) {
        if (name === 'orca.json') continue;
        const path = join(this.home, 'sessions', name);
        await mkdir(dirname(path), { recursive: true });
        await writeFile(path, data, { flag: 'wx' });
      }
    }
    const installed = JSON.parse(
      await readFile(require.resolve('@deepseek-ai/dsh/package.json'), 'utf8'),
    );
    if (installed.version !== DSH_SDK_VERSION)
      throw new Error('Incompatible installed DSH runtime');
    const cli = require.resolve('@deepseek-ai/dsh/lib/bin.js');
    const child = spawn(process.execPath, [cli, '--profile', 'orca-managed'], {
      cwd: input.root,
      detached: process.platform !== 'win32',
      env: {
        PATH: process.env.PATH ?? '',
        HOME: this.home,
        DSH_HOME: this.home,
        DSH_TELEMETRY_DISABLED: '1',
        ORCA_DSH_SYSTEM: input.system,
      },
      stdio: ['pipe', 'pipe', 'pipe'],
    });
    this.child = child;
    child.stderr.resume(); // Never copy native history or credentials into public events.
    const stream = child.stdout.pipe(boundedLines());
    this.rpc = new JsonRpcLineTransport(stream, child.stdin);
    const rpc = this.rpc;
    stream.on('error', () => {
      rpc.close();
      void this.reap();
    });
    child.on('error', () => rpc.close());
    child.on('exit', () => {
      rpc.close();
      this.cancelTools();
      this.fatal = true;
    });
    rpc.onNotification((method, value) => {
      if (method !== 'orca.event') return;
      try {
        const event = value as unknown as DshWorkerEvent;
        if (!['event', 'canonical_event', 'checkpoint', 'failure', 'done'].includes(event.type))
          throw new Error('Invalid DSH worker event');
        if (event.type === ('canonical_event' as string)) {
          const canonical = (
            value as unknown as {
              event: { id: string; subpath: string; kind: string; payload: unknown };
            }
          ).event;
          if (
            !canonical?.id?.startsWith('evt_') ||
            !(
              canonical.subpath === '' ||
              /^subagents\/[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/.test(canonical.subpath)
            ) ||
            ![
              'agent.message',
              'agent.thinking',
              'agent.tool_use',
              'agent.tool_result',
              'agent.thread_message_sent',
              'agent.thread_message_received',
              'session.thread_created',
              'session.thread_status_running',
              'session.thread_status_idle',
              'span.model_request_start',
              'span.model_request_end',
            ].includes(canonical.kind) ||
            !canonical.payload ||
            typeof canonical.payload !== 'object' ||
            Array.isArray(canonical.payload)
          )
            throw new Error('Invalid DSH canonical event');
          if (canonical.kind === 'session.thread_created') {
            const parent = (canonical.payload as { parent_subpath?: unknown }).parent_subpath;
            if (
              typeof parent !== 'string' ||
              (parent !== '' && !this.parents.has(parent)) ||
              !canonical.subpath ||
              this.parents.has(canonical.subpath)
            )
              throw new Error('Invalid DSH live lineage');
            this.parents.set(canonical.subpath, parent);
          }
        }
        if (event.type === 'failure') {
          if (typeof event.message !== 'string') throw new Error('Invalid DSH failure');
          if (event.fatal) this.fatal = true;
        }
        if (event.type === 'done') this.cancelTools();
        if (event.type === 'checkpoint')
          assertDshCheckpointInstructions(event.checkpoint, input.system);
        if (event.type === 'event') {
          const normalized = event.event;
          if (normalized.type === 'turn.completed') assertSdkTerminalUsage(normalized.usage);
          else if (normalized.type === 'item.completed') {
            if (
              !['agent_message', 'reasoning'].includes(normalized.item.type) ||
              typeof normalized.item.id !== 'string' ||
              typeof normalized.item.text !== 'string'
            )
              throw new Error('Invalid DSH message');
          } else if (normalized.type !== 'turn.started')
            throw new Error('Invalid DSH normalized event');
        }
        this.emit(event);
      } catch {
        this.fatal = true;
        rpc.close();
        void this.reap();
      }
    });
    rpc.onRequest(async (method, value) => {
      const authorization = method === 'orca.authorize';
      if (
        (method !== 'orca.tool' && !authorization) ||
        value.name === 'web_fetch' ||
        !this.active ||
        this.closing ||
        this.fatal ||
        (authorization
          ? !nativeTools.has(String(value.name))
          : nativeTools.has(String(value.name)) ||
            !input.tools.some((tool) => tool.name === value.name))
      )
        throw new Error('DSH tool is unavailable');
      const id = String(value.id);
      const rawOrigin = value.origin as
        | { eventId?: unknown; subpath?: unknown; parentSubpath?: unknown }
        | undefined;
      if (
        typeof rawOrigin?.eventId !== 'string' ||
        !rawOrigin.eventId.startsWith('evt_') ||
        rawOrigin.eventId.length <= 4 ||
        typeof rawOrigin.subpath !== 'string' ||
        !(
          rawOrigin.subpath === '' ||
          /^subagents\/[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/.test(rawOrigin.subpath)
        ) ||
        (rawOrigin.parentSubpath !== undefined &&
          (typeof rawOrigin.parentSubpath !== 'string' ||
            !(
              rawOrigin.parentSubpath === '' ||
              /^subagents\/[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/.test(
                rawOrigin.parentSubpath,
              )
            ))) ||
        this.pending.has(id) ||
        this.pending.size >= 128 ||
        typeof value.id !== 'string' ||
        !id.startsWith('evt_') ||
        id.length <= 4 ||
        !value.arguments ||
        typeof value.arguments !== 'object' ||
        Array.isArray(value.arguments)
      )
        throw new Error('Invalid DSH tool call');
      const origin = {
        eventId: rawOrigin.eventId,
        subpath: rawOrigin.subpath,
        ...(typeof rawOrigin.parentSubpath === 'string'
          ? { parentSubpath: rawOrigin.parentSubpath }
          : {}),
      };
      if (
        (origin.subpath !== '' && !this.parents.has(origin.subpath)) ||
        (origin.subpath !== '' && this.parents.get(origin.subpath) !== origin.parentSubpath) ||
        (origin.subpath === '' && origin.parentSubpath !== undefined)
      )
        throw new Error('DSH call origin outside owned tree');
      assertNotArchived(origin.subpath, this.archived, (path) => this.parents.get(path));
      const args = authorization
        ? nativeArguments(String(value.name), value.arguments)
        : (value.arguments as Record<string, unknown>);
      return new Promise((resolve) => {
        this.pending.set(id, { resolve, subpath: origin.subpath, authorization });
        this.emit({
          type: authorization ? 'orchestration_request' : 'tool_call',
          id,
          name: String(value.name) as NativeName,
          arguments: args,
          origin,
        });
      });
    });
    rpc.start();
    const initialized = (await rpc.request('initialize', input, AbortSignal.timeout(30000))) as {
      threadId: string;
      threads: Array<{ subpath: string; parentSubpath: string }>;
    };
    const reached = new Set(['']);
    if (!Array.isArray(initialized.threads) || initialized.threads.length > 127)
      throw new Error('Invalid DSH private lineage');
    for (const thread of initialized.threads) {
      if (
        !/^subagents\/[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/.test(thread.subpath) ||
        !reached.has(thread.parentSubpath) ||
        reached.has(thread.subpath)
      )
        throw new Error('Invalid DSH private lineage order');
      reached.add(thread.subpath);
      this.parents.set(thread.subpath, thread.parentSubpath);
    }
    this.emit({
      type: 'event',
      event: { type: 'thread.started', thread_id: initialized.threadId },
    });
    this.emit({ type: 'thread_tree', threads: initialized.threads });
  }

  async refreshOptions(options: SdkTurnOptions): Promise<void> {
    if (!this.input || !this.rpc || this.active || this.closing || this.fatal)
      throw new Error('DSH refresh requires idle');
    if (
      !options.apiKey ||
      options.baseUrl ||
      (options.system !== undefined && options.system !== this.input.system)
    )
      throw new Error('DSH credentials/instructions invalid');
    gatewayBase(options.dshGatewayUrl ?? this.input.dshGatewayUrl);
    await this.rpc.request('refresh', options, AbortSignal.timeout(10000));
    this.input = { ...this.input, ...options };
  }
  private cancelTools(subpath?: string): void {
    const affected = new Set(subpath === undefined ? [] : [subpath]);
    for (const [child, parent] of this.parents) if (affected.has(parent)) affected.add(child);
    for (const [id, pending] of this.pending) {
      if (subpath !== undefined && !affected.has(pending.subpath)) continue;
      pending.resolve(
        pending.authorization
          ? { allowed: false, reason: 'turn interrupted' }
          : { isError: true, content: [{ type: 'text', text: 'turn interrupted' }] },
      );
      this.pending.delete(id);
    }
  }
  close(): Promise<void> {
    this.closing ??= (async () => {
      await this.starting?.catch(() => undefined);
      this.cancelTools();
      await this.reap();
      await this.active;
    })();
    return this.closing;
  }
  private reap(): Promise<void> {
    this.reaping ??= this.disposeChild();
    return this.reaping;
  }
  private async disposeChild(): Promise<void> {
    const child = this.child;
    this.child = undefined;
    this.rpc?.close();
    this.rpc = undefined;
    if (child && child.exitCode === null && child.signalCode === null) {
      const exited = new Promise<void>((resolve) => child.once('exit', () => resolve()));
      const kill = (signal: NodeJS.Signals): void => {
        try {
          if (process.platform === 'win32') child.kill(signal);
          else if (child.pid) process.kill(-child.pid, signal);
        } catch (error) {
          if ((error as NodeJS.ErrnoException).code === 'EPERM') child.kill(signal);
          else if ((error as NodeJS.ErrnoException).code !== 'ESRCH') throw error;
        }
      };
      kill('SIGTERM');
      const timer = setTimeout(() => kill('SIGKILL'), 3000);
      try {
        await exited;
        // A descendant can outlive the CLI or ignore TERM. Drain the owned group too.
        kill('SIGKILL');
      } finally {
        clearTimeout(timer);
      }
    }
    if (this.home) {
      await rm(this.home, { recursive: true, force: true });
      this.home = undefined;
    }
  }
}

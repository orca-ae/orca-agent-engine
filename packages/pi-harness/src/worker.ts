// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { piProviderFetch, PiGoogleTransport } from './transport.js';
import { randomUUID } from 'node:crypto';
import {
  createAgentSession,
  createExtensionRuntime,
  ModelRuntime,
  SessionManager,
  SettingsManager,
  type AgentSession,
  type ResourceLoader,
  type ToolDefinition,
} from '@earendil-works/pi-coding-agent';
import {
  InMemoryCredentialStore,
  type TextContent,
  type ImageContent,
  type Model,
  type Api,
} from '@earendil-works/pi-ai';
import type { ThinkingLevel } from '@earendil-works/pi-agent-core';
import type { TSchema } from 'typebox';
import type { CallToolResult } from '@modelcontextprotocol/sdk/types.js';
import {
  assertSdkTerminalUsage,
  type SdkWorker,
  type StartCommand,
  type WorkerCommand,
  type WorkerEvent,
  type SdkTurnOptions,
  type SdkTerminalUsage,
} from '@orca/sdk-harness';
import {
  CODEX_SDK_MODELS,
  piModelEfforts,
  piModelApi,
  validateHarnessModel,
} from '@orca/harness-catalog';
import {
  assertPiCheckpointInstructions,
  capturePiCheckpoint,
  decodePiCheckpoint,
} from './checkpoint.js';

/** The embedded Pi loop has no executable tools except the host's explicit allowlist. */
export class PiSdkWorker implements SdkWorker {
  private session: AgentSession | undefined;
  private runtime: ModelRuntime | undefined;
  private googleTransport: PiGoogleTransport | undefined;
  private input: StartCommand | undefined;
  private active: Promise<void> | undefined;
  private starting: Promise<void> | undefined;
  private closing?: Promise<void>;
  private started = false;
  private fatal = false;
  private interrupted = false;
  private pending = new Map<string, (result: CallToolResult) => void>();
  constructor(private readonly emit: (event: WorkerEvent) => void) {}

  async handle(command: WorkerCommand): Promise<void> {
    switch (command.type) {
      case 'tool_result':
        this.pending.get(command.id)?.(command.result);
        this.pending.delete(command.id);
        return;
      case 'interrupt':
        this.interrupted = true;
        this.cancelTools();
        this.googleTransport?.abort();
        await this.session?.abort();
        return;
      case 'stop':
        return this.close();
      case 'start':
        if (this.started || this.closing) throw new Error('Pi SDK already started or closed');
        this.started = true;
        this.starting = this.start(command);
        try {
          await this.starting;
          if (this.closing) throw new Error('Pi SDK closed during startup');
          this.emit({ type: 'ready' });
        } catch (error) {
          await this.googleTransport?.close();
          this.googleTransport = undefined;
          this.session?.dispose();
          this.session = undefined;
          this.runtime = undefined;
          this.input = undefined;
          this.started = false;
          throw error;
        } finally {
          this.starting = undefined;
        }
        return;
      case 'submit':
        if (!this.session || this.active || this.closing || this.fatal)
          throw new Error('Pi SDK is not ready for a turn');
        this.active = this.run(command.text).finally(() => {
          this.active = undefined;
        });
        await this.active;
    }
  }

  private async start(input: StartCommand): Promise<void> {
    // Decode first so a retired model in a 0.87.0 checkpoint has a clear
    // migration error instead of the generic current-catalog admission error.
    const restored = input.checkpoint ? decodePiCheckpoint(input.checkpoint) : undefined;
    const error = validateHarnessModel(
      { harness: 'pi_sdk' },
      {
        provider: input.modelProvider ?? 'openai',
        id: input.model,
        ...(input.effort ? { effort: input.effort } : {}),
      },
    );
    if (error) throw new Error(error);
    if (!input.apiKey) throw new Error('Pi SDK requires explicit LLM credentials');
    this.input = input;
    if (input.checkpoint) assertPiCheckpointInstructions(input.checkpoint, input.system);
    if (
      restored &&
      (restored.model !== input.model ||
        (restored.modelProvider ?? 'openai') !== (input.modelProvider ?? 'openai'))
    )
      throw new Error('Pi checkpoint model differs from pinned Agent');
    this.runtime = await ModelRuntime.create({
      credentials: new InMemoryCredentialStore(),
      modelsPath: null,
      refreshOnCreate: false,
    });
    await this.configureProvider(input);
    const model = this.runtime.getModel(input.modelProvider ?? 'openai', input.model);
    if (!model) throw new Error('Pi managed model is unavailable');
    // No discovery, packages, commands, context files, credentials, or extensions from the host/project.
    const extensionRuntime = createExtensionRuntime();
    const resourceLoader: ResourceLoader = {
      getExtensions: () => ({ extensions: [], errors: [], runtime: extensionRuntime }),
      getSkills: () => ({ skills: [], diagnostics: [] }),
      getPrompts: () => ({ prompts: [], diagnostics: [] }),
      getThemes: () => ({ themes: [], diagnostics: [] }),
      getAgentsFiles: () => ({ agentsFiles: [] }),
      getSystemPrompt: () => this.input!.system,
      getSystemPromptSource: () => undefined,
      getAppendSystemPrompt: () => [],
      getAppendSystemPromptSources: () => [],
      extendResources: () => {
        throw new Error('Pi resource discovery is disabled');
      },
      reload: async () => {},
    };
    const customTools: ToolDefinition[] = input.tools.map((tool) => ({
      name: tool.name,
      label: tool.name,
      description: tool.description ?? tool.name,
      parameters: tool.inputSchema as TSchema,
      execute: async (_id, args, signal) => {
        if (this.interrupted || signal?.aborted || this.closing)
          throw new Error('turn interrupted');
        const id = randomUUID();
        const result = await new Promise<CallToolResult>((resolve) => {
          this.pending.set(id, resolve);
          this.emit({
            type: 'tool_call',
            id,
            name: tool.name,
            arguments: args as Record<string, unknown>,
          });
        });
        const content = result.content.map((block): TextContent | ImageContent => {
          if (block.type === 'text') return { type: 'text', text: block.text };
          if (block.type === 'image')
            return { type: 'image', data: block.data, mimeType: block.mimeType };
          return { type: 'text', text: JSON.stringify(block) };
        });
        // Pi sets toolResult.isError on thrown execution errors.
        if (result.isError)
          throw new Error(
            content.map((block) => (block.type === 'text' ? block.text : '[image]')).join('\n'),
          );
        return { content, details: {} };
      },
    }));
    const { session } = await createAgentSession({
      cwd: input.root,
      agentDir: input.root,
      model,
      modelRuntime: this.runtime,
      thinkingLevel: (input.effort === 'ultra'
        ? 'max'
        : (input.effort ??
          (piModelEfforts(input.modelProvider ?? 'openai', input.model)?.includes('medium')
            ? 'medium'
            : (piModelEfforts(input.modelProvider ?? 'openai', input.model)?.[0] ??
              'off')))) as ThinkingLevel,
      noTools: 'builtin',
      tools: customTools.map((tool) => tool.name),
      customTools,
      resourceLoader,
      sessionManager: SessionManager.inMemory(input.root, undefined, restored?.entries),
      settingsManager: SettingsManager.inMemory({
        cacheWarming: 'off',
        compaction: { enabled: false },
        retry: { enabled: false, provider: { maxRetries: 0 } },
        transport: 'sse',
      }),
    });
    // Keep Pi's model loop and request settings; reject lossy usage normalization.
    const stream = session.agent.streamFunction;
    session.agent.streamFunction = (model, context, options) =>
      stream(model, context, {
        ...options,
        ...(model.api === 'google-generative-ai'
          ? {}
          : {
              fetch: piProviderFetch({
                provider: model.provider,
                api: model.api,
                apiKey: this.input!.apiKey,
                sessionId: this.input!.sessionId,
                gatewayUrl: this.input!.piGatewayUrl,
              }),
            }),
        headers: { ...options?.headers, 'X-Orca-Session-Id': this.input!.sessionId },
        cacheRetention: 'short',
        env: {},
      });
    this.session = session;
  }

  private async configureProvider(input: StartCommand): Promise<void> {
    const provider = input.modelProvider ?? 'openai';
    const native = this.runtime!.getProvider(provider);
    if (!native?.auth.apiKey) throw new Error('Pi provider requires static API-key support');
    const known = this.runtime!.getModel(provider, input.model);
    const legacyEfforts = provider === 'openai' ? CODEX_SDK_MODELS[input.model] : undefined;
    const api = piModelApi(provider, input.model)!;
    const model: Model<Api> = {
      ...(known ?? {
        id: input.model,
        provider,
        name: input.model,
        reasoning: true,
        input: ['text', 'image'],
        contextWindow: 128000,
        maxTokens: 16384,
        baseUrl: 'https://api.openai.com/v1',
      }),
      api,
      ...(legacyEfforts
        ? {
            thinkingLevelMap: {
              off: null,
              minimal: null,
              low: 'low',
              medium: 'medium',
              high: 'high',
              xhigh: legacyEfforts.includes('xhigh') ? 'xhigh' : null,
              max:
                input.effort === 'ultra' ? 'ultra' : legacyEfforts.includes('max') ? 'max' : null,
            },
          }
        : {}),
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
    };
    if (api === 'google-generative-ai') {
      this.googleTransport = new PiGoogleTransport(() => ({
        provider,
        api,
        apiKey: this.input!.apiKey,
        sessionId: this.input!.sessionId,
        gatewayUrl: this.input!.piGatewayUrl,
        baseUrl: this.input!.baseUrl ?? model.baseUrl,
      }));
      await this.googleTransport.start();
    }
    // Preserve the official provider's dispatch, compatibility flags and model API.
    // Replace only auth: no ambient env, credential files, OAuth or catalog refresh.
    this.runtime!.registerNativeProvider({
      id: native.id,
      name: native.name,
      ...(native.headers ? { headers: native.headers } : {}),
      getModels: () => [model],
      auth: {
        apiKey: {
          name: 'Orca explicit API key',
          resolve: async () => ({
            auth: {
              apiKey:
                this.googleTransport?.apiKey ??
                (this.input!.piGatewayUrl ? 'orca-managed' : this.input!.apiKey),
              ...(this.googleTransport
                ? { baseUrl: this.googleTransport.modelBaseUrl() }
                : this.input!.baseUrl
                  ? {
                      baseUrl:
                        api === 'anthropic-messages'
                          ? this.input!.baseUrl.replace(/\/v1\/?$/, '')
                          : this.input!.baseUrl,
                    }
                  : {}),
            },
            env: {},
          }),
        },
      },
      stream: native.stream.bind(native),
      streamSimple: native.streamSimple.bind(native),
    });
  }

  async refreshOptions(options: SdkTurnOptions): Promise<void> {
    if (!this.input || !this.session || this.active || this.closing || this.fatal)
      throw new Error('Pi SDK credentials can only refresh while idle');
    if (!options.apiKey) throw new Error('Pi SDK requires explicit credentials');
    if (options.system !== undefined && options.system !== this.input.system)
      throw new Error('Pi SDK instructions are pinned');
    this.input = { ...this.input, ...options };
    await this.session.setModel(
      this.runtime!.getModel(this.input.modelProvider ?? 'openai', this.input.model)!,
    );
  }

  private async run(text: string): Promise<void> {
    this.interrupted = false;
    const usage: SdkTerminalUsage = {
      input_tokens: 0,
      output_tokens: 0,
      cached_input_tokens: 0,
      cache_write_input_tokens: 0,
      cache_write_input_tokens_1h: 0,
    };
    let usageKnown = true;
    let failure: string | undefined;
    let aborted = false;
    const unsubscribe = this.session!.subscribe((event) => {
      if (event.type !== 'message_end' || event.message.role !== 'assistant') return;
      const message = event.message;
      if (message.stopReason === 'error' || message.stopReason === 'aborted') {
        // A failed/aborted stream cannot prove the provider's final spend. Do not commit a checkpoint.
        // Pi may label a cancellation during request preparation as 'error'.
        aborted = this.interrupted || message.stopReason === 'aborted';
        usageKnown = false;
        failure = message.errorMessage ?? 'Pi model request did not complete';
        return;
      }
      const counts = message.usage;
      try {
        for (const count of [counts.input, counts.output, counts.cacheRead, counts.cacheWrite]) {
          if (!Number.isSafeInteger(count) || count < 0) throw new Error('invalid usage');
        }
        usage.input_tokens += counts.input + counts.cacheRead;
        usage.output_tokens += counts.output;
        usage.cached_input_tokens += counts.cacheRead;
        usage.cache_write_input_tokens! += counts.cacheWrite;
        usage.cache_write_input_tokens_1h! += counts.cacheWrite1h ?? 0;
        assertSdkTerminalUsage(usage);
      } catch {
        usageKnown = false;
        failure = 'Pi returned invalid usage';
      }
      for (const block of message.content)
        if (block.type === 'text') {
          this.emit({
            type: 'event',
            event: {
              type: 'item.completed',
              item: { id: randomUUID(), type: 'agent_message', text: block.text },
            },
          });
        }
    });
    try {
      this.emit({ type: 'event', event: { type: 'turn.started' } });
      await this.session!.prompt(text, { expandPromptTemplates: false });
      if (!usageKnown) throw new Error(failure ?? 'Pi usage unavailable');
      this.emit({ type: 'event', event: { type: 'turn.completed', usage } });
      const manager = this.session!.sessionManager;
      this.emit({
        type: 'checkpoint',
        checkpoint: capturePiCheckpoint(manager.getSessionId(), {
          model: this.input!.model,
          modelProvider: this.input!.modelProvider ?? 'openai',
          system: this.input!.system,
          entries: [manager.getHeader()!, ...manager.getEntries()],
        }),
      });
    } catch (error) {
      // User cancellation is recoverable. Unknown spend still emits no usage
      // or checkpoint, so budgeted sessions retain their accounting barrier.
      this.fatal = !(
        this.interrupted &&
        (aborted || (error instanceof Error && error.name === 'AbortError'))
      );
      this.emit({
        type: 'failure',
        ...(this.fatal ? { fatal: true as const } : {}),
        message: error instanceof Error ? error.message : 'Pi SDK failed',
      });
    } finally {
      unsubscribe();
      this.cancelTools();
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
    await this.starting?.catch(() => undefined);
    this.interrupted = true;
    this.cancelTools();
    this.googleTransport?.abort();
    await this.session?.abort();
    await this.active;
    this.session?.dispose();
    this.session = undefined;
    await this.googleTransport?.close();
    this.googleTransport = undefined;
    // ModelRuntime has no disposal API; with in-memory credentials and refresh
    // disabled it owns no background resources. Release its credentials here.
    this.runtime = undefined;
    this.input = undefined;
  }
}

// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { randomUUID } from 'node:crypto';
import { composeSkillsCatalog } from '@orca/sandbox-runtime';
import type { LlmEgress } from '../config.js';
import type { Consumer, EachMessagePayload, Kafka } from 'kafkajs';
import type { S3Client } from '@aws-sdk/client-s3';
import { v5 as uuidv5, v7 as uuidv7 } from 'uuid';
import {
  BUILTIN_EVALUATORS,
  SKILL_LOAD_TOOL,
  evaluateGuardrails,
  type PreparedGuardrail,
} from '@orca/guardrails';
import {
  createKafkaTranscriptCodec,
  matchSessionTopic,
  RetryableSessionEventError,
  SessionEventBarrierError,
  sessionTopicPattern,
  type Event,
  type KafkaTranscriptCodec,
  type SessionEventSourceStatus,
  type TranscriptStore,
} from '@orca/transcript-store';
import type { FileStore } from '@orca/file-store';
import type { SkillStore } from '@orca/skill-store';
import { InternalTranscriptEventKind, userEventProcessedPayload } from '@orca/agent-event-contract';
import { ClaudeAgentSdkAdapter } from '../harness/claude/session-adapter.js';
import { ClaudeAgentSdkHarness, type ClaudeHarnessOptions } from '../harness/claude/index.js';
import { RemoteCodexSdkWorker } from '../harness/codex-sdk/remote-worker.js';
import { CodexSdkHarness } from '../harness/codex-sdk/index.js';
import {
  recoverCodexTurn,
  receiptTerminalEvents,
  type CodexTurnStore,
} from '../harness/codex-sdk/turn-receipt.js';
import { publicEntryToEvent } from '../harness/claude/event-mapper.js';
import type { HarnessTurnReceipt, HarnessTurnSnapshot } from '@orca/harness-catalog';
import type { CodexCheckpoint } from '@orca/codex-harness';
import { ORCA_MCP_SERVER_NAME, ORCA_MCP_TOOL_LOGICAL_NAMES } from '../harness/claude/mcp-tools.js';
import {
  GuardrailPolicyDeniedError,
  DurableHarnessStateError,
  SettledHarnessFailureError,
  GuardrailUsageUnavailableError,
  UnappliedUserEventError,
  SessionEventKind,
  sessionErrorPayload,
  sessionIdlePayload,
  type AgentHarness,
  type CustomToolDefinition,
  type ModelEffort,
  type ModelSpeed,
  type RemoteMcpToolsetPolicy,
  type SubmitHooks,
  type SessionGuardrail,
} from '../harness/agent-harness.js';
import {
  resolveHarnessAnnotation,
  HARNESS_CATALOG,
  resolveHarnessCapabilities,
  validateSdkCheckpoint,
  enabledMcpToolsetServerNames,
  managedToolPermissionPolicyName,
  resolvedToolsetConfig,
  toolsetPermissionPolicies,
  toolsetConfigEntries,
  toolsetConfigValue,
  type HarnessType,
} from '@orca/harness-catalog';
import {
  selectHarness,
  createHarnessProviderRegistry,
  type HarnessProviderRegistry,
} from '../harness/registry.js';
import { InSandboxHarness } from '../harness/in-sandbox/index.js';
import {
  assertHarnessServerCanRunColocated,
  harnessToProvider,
} from '../harness/in-sandbox/provider-map.js';
import { buildLlmGatewayEnv } from '../harness/in-sandbox/llm-env.js';
import { buildReplayTurns, type ReplayTurn } from '../harness/in-sandbox/replay.js';
import { resolveClaudeSessionSpeed } from '../harness/model-controls.js';
import {
  SessionRunner,
  SessionRunnerStoppingError,
  type RunnerInput,
  type TerminalCompletion,
} from './session-runner.js';
import {
  harnessEventSubmitTotal,
  harnessGitCloneTotal,
  harnessKafkaActiveSubscribedTopics,
  harnessKafkaAssignedPartitions,
  harnessKafkaAssignedTopics,
  harnessKafkaConsumerReady,
  harnessKafkaDiscoveredTopics,
  harnessKafkaDiscoveryTotal,
  harnessKafkaOffsetLag,
  harnessKafkaTransitionTotal,
  harnessRunnerSpawnAttemptsTotal,
  harnessRunnerSpawnFailuresTotal,
  harnessAcceptedEventToStatusRunningSeconds,
  harnessSandboxWriteDeniedTotal,
  harnessSandboxWritePolicySetupTotal,
} from '../metrics.js';
import {
  RegistryClient,
  RegistryInvalidRuntimeBindingError,
  type AgentRecord,
  type AgentToolEntry,
  type PreparedExecutionV2,
  type SessionRecord,
  type SkillDescriptor,
  type PreparedGuardrailRecord,
} from '../clients/registry.js';
import { rewriteMcpServers, type RewrittenServer } from '../mcp/rewrite.js';
import { SessionJwtProvider } from '../mcp/session-jwt-provider.js';
import type { SandboxHandle, SandboxRuntime, EnvironmentSpec } from '../sandbox/sandbox-runtime.js';
import { buildEnvironmentSpec } from '../sandbox/environment-spec.js';
import type { ToolDefinition } from '../sandbox/agent-toolset.js';
import { buildAgentToolset } from '../sandbox/agent-toolset.js';
import { pickStrategy, type PickStrategyInput } from '../sandbox/mounts/strategy-factory.js';
import {
  materializeResources,
  type ActiveMount,
  type ResourceMountStrategyOverride,
} from '../sandbox/materialize.js';
import type { MountResource } from '../sandbox/mounts/mount-strategy.js';
import {
  ensureSessionOutputRoot,
  mountSessionOutputs,
  type OutputMountHandle,
} from '../sandbox/outputs/output-mount.js';
import { OutputIndexer, type IndexSessionResult } from '../sandbox/outputs/output-indexer.js';
import {
  OUTPUT_CAPTURE_DIRECTORY,
  withOutputCaptureInstruction,
} from '../sandbox/outputs/output-instructions.js';
import type { MemoryStoreGrant, SessionCredsMinter, SessionS3Creds } from '../auth/sts-creds.js';
import { MemoryVersionWatcher, type WatchedStore } from '../sandbox/memory/version-watcher.js';
import type { GitWorker } from '../git/git-worker.js';
import type { WorkDirManager } from '../git/work-dir.js';
import {
  buildBubblewrapCommand,
  buildSandboxWritePolicy,
  buildSkillsAliasProbeCommand,
  createPolicyEnforcedSandbox,
  encodeSandboxWritePolicy,
  networkAllowedDomainsFromUrls,
  normalizeWritePolicyAccess,
  OUTPUT_WRITABLE_PATH,
  SANDBOX_WRITE_POLICY_ENV,
} from '../sandbox/write-policy.js';
import {
  assertCanonicalPathOutsideSkillsRoot,
  assertMountPathOutsideSkillsRoot,
  materializeSkills,
  SKILLS_ROOT,
  validateSkillDescriptor,
} from '../sandbox/skills/materialize.js';
import { assertSandboxEnvironmentTrust } from '../sandbox/environment-trust.js';

// The MCP-toolset composition primitives live in `@orca/harness-catalog` so the
// registry's snapshot build and this one run ONE implementation. The enabled +
// reserved-`orca` filters on `allowed_mcp_server_names` are a security control:
// a lossy second copy here would deny a server on one execution path and permit
// it on the other, with no log and no error. Re-exported because the dispatcher
// is the import surface the harness-server unit suite drives this through.
export { enabledMcpToolsetServerNames };

interface RuntimeAgentSnapshot {
  id?: string;
  name?: string;
  version?: number;
  model_provider?: string;
  model_id?: string;
  model_speed?: ModelSpeed;
  model_effort?: ModelEffort;
  system?: string;
  allowed_tool_names?: string[];
  allowed_mcp_server_names?: string[];
  tool_permission_policies?: Record<string, 'always_allow' | 'always_ask' | 'always_deny'>;
  custom_tools?: CustomToolDefinition[];
  multiagent?: {
    type: 'coordinator';
    agents: RuntimeSubagentSnapshot[];
  };
}

interface RuntimeSubagentSnapshot {
  id: string;
  name: string;
  version: number;
  model_provider?: string;
  model_id?: string;
  model_speed?: ModelSpeed;
  model_effort?: ModelEffort;
  system?: string;
  allowed_tool_names?: string[];
  tool_permission_policies?: Record<string, 'always_allow' | 'always_ask' | 'always_deny'>;
}

const RESERVED_CUSTOM_TOOL_NAMES = new Set<string>(ORCA_MCP_TOOL_LOGICAL_NAMES);
const ORCA_MCP_TOOL_LOGICAL_NAME_SET = new Set<string>(ORCA_MCP_TOOL_LOGICAL_NAMES);
const RUNTIME_ORCA_TOOL_LOGICAL_NAMES = [
  'bash',
  'read',
  'write',
  'edit',
  'glob',
  'grep',
  'list',
  'delete',
] as const;

type SetupFailurePhase =
  | 'model_config'
  | 'execution_preparation'
  /** Resolving the agent's harness annotation, or building the harness for it. */
  | 'harness_selection'
  | 'sandbox_setup'
  | 'skill_setup'
  | 'output_mount'
  | 'resource_mount'
  | 'memory_setup'
  | 'repo_setup'
  | 'write_policy';

class RequiredSetupError extends Error {
  readonly original: unknown;

  constructor(
    readonly phase: SetupFailurePhase,
    cause: unknown,
  ) {
    super(errorMessage(cause));
    this.name = 'RequiredSetupError';
    this.original = cause;
  }
}

class DroppedSessionEvent extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'DroppedSessionEvent';
  }
}

class DispatcherStoppingError extends RetryableSessionEventError {
  constructor(options?: ErrorOptions) {
    super('dispatcher is stopping', options);
    this.name = 'DispatcherStoppingError';
  }
}

class UserEventAcceptanceError extends RetryableSessionEventError {
  constructor(
    readonly kind: string,
    options: ErrorOptions,
  ) {
    super(`failed to persist acceptance for ${kind}`, options);
    this.name = 'UserEventAcceptanceError';
  }
}

class CompletedUserEventReadError extends RetryableSessionEventError {
  constructor(workspaceId: string, sessionId: string, options: ErrorOptions) {
    super(`failed to rebuild completed user-event cache for ${workspaceId}/${sessionId}`, options);
  }
}

class UserEventDeferralError extends RetryableSessionEventError {
  constructor(options: ErrorOptions) {
    super('failed to persist deferred user.message', options);
    this.name = 'UserEventDeferralError';
  }
}

class DeferredDrainBlockedError extends RetryableSessionEventError {
  constructor(workspaceId: string, sessionId: string, options?: ErrorOptions) {
    super(`deferred user messages remain blocked for ${workspaceId}/${sessionId}`, options);
    this.name = 'DeferredDrainBlockedError';
  }
}

class SessionConfigRefreshError extends RetryableSessionEventError {
  constructor(workspaceId: string, sessionId: string, options: ErrorOptions) {
    super(`failed to refresh runtime config for ${workspaceId}/${sessionId}`, options);
    this.name = 'SessionConfigRefreshError';
  }
}

class SessionPreparationError extends RetryableSessionEventError {
  constructor(workspaceId: string, sessionId: string, options: ErrorOptions) {
    super(`failed to prepare runtime config for ${workspaceId}/${sessionId}`, options);
    this.name = 'SessionPreparationError';
  }
}

class CompanionSystemMessageReadError extends RetryableSessionEventError {
  constructor(workspaceId: string, sessionId: string, options: ErrorOptions) {
    super(`failed to inspect companion system.message for ${workspaceId}/${sessionId}`, options);
    this.name = 'CompanionSystemMessageReadError';
  }
}

class SystemMessageReplayReadError extends RetryableSessionEventError {
  constructor(workspaceId: string, sessionId: string, options: ErrorOptions) {
    super(`failed to rebuild system.message context for ${workspaceId}/${sessionId}`, options);
    this.name = 'SystemMessageReplayReadError';
  }
}

const AGENT_TOOLSET_LOGICAL_TO_ORCA_MCP = new Map<string, string>([
  ['bash', 'bash'],
  ['read', 'read'],
  ['write', 'write'],
  ['edit', 'edit'],
  ['glob', 'glob'],
  ['grep', 'grep'],
  ['web_fetch', 'web_fetch'],
  ['web_search', 'web_search'],
  ['list', 'list'],
  ['delete', 'delete'],
  ['agent_toolset', 'agent_toolset'],
  ['agent_toolset_20260401', 'agent_toolset'],
]);
const MCP_SERVER_NAME_ALLOWLIST_PATTERN = /^[A-Za-z0-9][A-Za-z0-9_-]*$/;
const DEFERRED_USER_MESSAGE_KIND = 'session.deferred_user_message';
const DEFERRED_USER_MESSAGE_SUBMITTED_KIND = 'session.deferred_user_message_submitted';
const USER_EVENT_COMPLETED_KIND = InternalTranscriptEventKind.userEventCompleted;
const USER_EVENT_COMPLETED_NAMESPACE = 'a9a85d09-77c2-4bd8-9516-1d6df0c07cc8';
const COMPANION_SYSTEM_EVENT_ID_FIELD = '_orca_companion_system_event_id';
const IN_SANDBOX_HARNESS_READY_PATH = '/tmp/orca-sandbox-harness-ready';
const KAFKA_CONSUMER_SESSION_TIMEOUT_MS = 30_000;
const KAFKA_EACH_MESSAGE_HEARTBEAT_MS = 5_000;
const KAFKA_DEFAULT_TOPIC_REDISCOVER_INTERVAL_MS = 30_000;
const KAFKA_GROUP_JOIN_TIMEOUT_MS = 30_000;
const DISPATCHER_SHUTDOWN_GRACE_MS = 10_000;
const KAFKA_RETRY_BASE_MS = 250;
const KAFKA_RETRY_MAX_MS = 5_000;

export interface DispatcherReadiness {
  ready: boolean;
  reasons: string[];
}

interface KafkaGroupJoinWaiter {
  promise: Promise<void>;
  resolve: () => void;
  reject: (error: unknown) => void;
}

interface KafkaConsumerLifecycle {
  consumer: Consumer;
  topics: string[];
  groupJoin: KafkaGroupJoinWaiter;
  disconnectPromise: Promise<void> | null;
  generation: number;
  assignmentEpoch: number;
  assignedPartitions: Set<string>;
}

interface KafkaSourceSettlement {
  promise: Promise<void>;
  resolve(): void;
  reject(error: unknown): void;
}

interface UserEventSource {
  userEventId?: string;
  seq?: number;
  producedAt?: string;
}

interface StatusRunningLatencyEvent {
  userEventId: string;
  producedAtMs: number;
}
const GUARDRAIL_STATE_WRITE_TIMEOUT_MS = 5_000;

interface DeferredUserMessage {
  userEventId: string;
  payload: unknown;
}

interface DeferredUserMessageQueue {
  loadedFromTranscript: boolean;
  items: DeferredUserMessage[];
}

interface CompanionSystemMessage {
  eventId: string;
  payload: unknown;
}

type DeferredDrainResult = 'complete' | 'blocked' | 'failed';

export type HarnessFactory = (
  workspaceId: string,
  sessionId: string,
  adapter: ClaudeAgentSdkAdapter,
) => AgentHarness;

export interface SessionEventSource {
  start(handler: (event: Event) => Promise<void>): Promise<void>;
  stop(): Promise<void>;
  /** Optional so legacy/custom sources retain start-based readiness behavior. */
  status?(): SessionEventSourceStatus;
  /** Optional terminal-failure monitor used by harness main for built-in sources. */
  whenFailed?(): Promise<void>;
}

export interface DispatcherOptions {
  kafka?: Kafka;
  /** Shared Kafka codec; composition root owns its lifecycle. Defaults to raw. */
  codec?: KafkaTranscriptCodec;
  eventSource?: SessionEventSource;
  groupId: string;
  store: TranscriptStore;
  anthropicApiKey: string;
  anthropicBaseURL?: string;
  piProviderCredentials?: import('@orca/pi-harness').PiProviderCredentials;
  openaiApiKey?: string;
  openaiBaseURL?: string;
  modelDefault: string;
  /** Override the harness implementation. Tests inject a `FakeHarness`. */
  harnessFactory?: HarnessFactory;
  /**
   * Optional topic filter for test/specialized dispatchers. Defaults to all
   * managed-agent session topics. Applied to bare topic names (without
   * `topicPrefix`).
   */
  topicPattern?: RegExp;
  /**
   * Registry client used to fetch the session/agent snapshot and mint the
   * per-session JWT for ai-gateway. Optional so existing tests that only
   * exercise the chat-only path don't need to plumb HTTP fixtures.
   */
  registry?: RegistryClient;
  /** Public ai-gateway MCP URL (each rewritten server points here). */
  gatewayMcpUrl?: string;
  /**
   * Public ai-gateway LLM base URL. A separate Session resolved to Gateway
   * egress by the deployment default or metadata uses it for model calls.
   * When set together with `registry`, the dispatcher also mints an
   * ai-gateway session JWT and injects it into
   * `EnvironmentSpec.harnessEnv` (as `LITELLM_API_BASE` + `LITELLM_API_KEY`)
   * for `colocated` agents so the in-sandbox harness can reach the LLM
   * through the gateway without holding a provider key. Absent → no env
   * injection (runtimes treat `harnessEnv` as `{}`).
   */
  gatewayLlmUrl?: string;
  /**
   * Default egress for separate Sessions whose metadata omits
   * `orca_llm_egress`. Session metadata always takes precedence.
   */
  llmEgressDefault?: LlmEgress;
  /**
   * File-store used by the per-session TarballPrefetchStrategy to
   * stream file bytes into the sandbox. Required alongside `sandboxRuntime`
   * to enable the resource-materialization path. Chat-only and MCP-only
   * flows work when both are absent.
   */
  fileStore?: FileStore;
  /** Host-side immutable Skill bundle reader. Sandboxes never receive its credentials. */
  skillStore?: SkillStore;
  /**
   * Pluggable sandbox runtime selected by the harness process at startup.
   */
  sandboxRuntime?: SandboxRuntime;
  /**
   * Per-harness override for `HARNESS_CATALOG[harness].defaultImage`, applied
   * in `buildEnvironmentSpec` below the (env-level) `Environment.image` pin.
   * Sourced from `ServiceConfig` (`SANDBOX_HARNESS_CLAUDE_CODE_IMAGE` etc.),
   * itself wired from chart `images.sandboxHarness.*` values — this is how a
   * release points fresh `colocated` sessions at the sandbox-harness image
   * that release actually published instead of the catalog's build-time
   * default.
   */
  defaultImageOverrides?: Partial<Record<HarnessType, string>>;

  // ---- Per-session creds minter + output capture wiring ----
  /**
   * Mints runner-generation-scoped credentials for outputs and attached
   * memory stores. File blobs are deliberately excluded from this grant.
   */
  credsMinter?: Pick<SessionCredsMinter, 'mint'>;
  /** Workspace blob bucket. Same value passed to `LocalFileStore`'s S3BlobStore. */
  s3Bucket?: string;
  /** S3 endpoint — required by both s3fs (`-o url=`) and the indexer's S3Client. */
  s3Endpoint?: string;
  /** S3/s3fs addressing mode. Defaults to path-style when omitted. */
  s3ForcePathStyle?: boolean;
  /** Canonical root shared by files, memory, and execution outputs. */
  s3KeyPrefix?: string;
  /**
   * Override the memory version watcher's poll interval (ms).
   * Production keeps the 2000ms default (matches the watcher's intrinsic
   * default). Integration tests pass a smaller value (e.g. 200ms) so the
   * watcher detects writes within the assertion timeout without bloating the
   * test runtime. Forwarded verbatim to `MemoryVersionWatcher` constructor.
   */
  memoryWatcherIntervalMs?: number;
  s3Region?: string;
  /**
   * S3 client used by `OutputIndexer` for `ListObjectsV2` + `GetObject` over
   * the current runner generation's output prefix. Reuses the same client instance the
   * file-store's S3BlobStore is built on (constructed in `main.ts`).
   */
  s3Client?: S3Client;

  // ---- github_repository wiring ----
  /**
   * Host-side simple-git wrapper used by `GitCloneStrategy`. Constructed at
   * boot via `makeGitWorker()`; tests inject a fake worker that bypasses the
   * `--filter=blob:none` flag so a `file://` remote works. Required for any
   * session with `github_repository` resources; absent => such sessions fail
   * setup (`repo_setup`).
   */
  gitWorker?: GitWorker;
  /**
   * Owns the per-session ephemeral host-side work dir (rm -rf'd at session
   * stop). Required alongside {@link gitWorker} for github_repository
   * resources.
   */
  workDir?: WorkDirManager;
  /**
   * Public-internet URL of the registry's `POST /v1/git-creds` route. Wired
   * into the sandbox via `/etc/profile.d/orca-git-creds.sh` (`ORCA_GIT_CREDS_URL`)
   * so the in-sandbox `orca-git-creds` credential helper knows where to call.
   * Required when a session has at least one `github_repository` resource —
   * the dispatcher fails fast at session-spawn if unset, but at config-load
   * time it stays optional so chat-only deployments don't need to set it.
   */
  gitCredsPublicUrl?: string;

  /**
   * Kafka topic-discovery interval. Every discovered snapshot becomes one
   * explicit `consumer.subscribe({ topics })` list; Kafkajs regex expansion
   * is deliberately never used because it is a one-time metadata lookup.
   * Production config validates this as a positive value. Unit tests may use
   * a short interval to deterministically exercise replacement.
   */
  topicRediscoverIntervalMs?: number;
  /** Bounded wait for Kafka's actual GROUP_JOIN event. Unit tests may shorten it. */
  kafkaGroupJoinTimeoutMs?: number;
  /**
   * Test-only override for the single dispatcher-wide shutdown deadline.
   * Production uses the fixed 10-second default and does not expose this
   * through config.
   */
  shutdownGraceMs?: number;
  /**
   * @deprecated Use `shutdownGraceMs`. Kept only for direct-construction
   * compatibility with existing Kafka lifecycle tests.
   */
  kafkaShutdownGraceMs?: number;
  /**
   * Optional Kafka topic prefix (dot-terminated segments, e.g.
   * `public.default.`). Both bare and prefixed paths discover via
   * `admin.listTopics()` and use explicit canonical topic lists. Kafka-on-Pulsar
   * (KoP) endpoints list session topics in the default
   * tenant/namespace by their BARE local name while fetches require prefixed
   * names, so `matchSessionTopic()` canonicalizes that snapshot before it is
   * subscribed.
   */
  topicPrefix?: string;
  /**
   * Milliseconds to keep a SessionRunner warm after a submitted user event
   * finishes. When the timer fires the runner is stopped, its sandbox is
   * destroyed, and the registry session is marked idle. Defaults to 60000.
   */
  sessionIdleTimeoutMs?: number;
  /**
   * Delay before retrying a deferred message whose acceptance marker could
   * not be persisted. Defaults to 1000ms. Tests may use a shorter delay.
   */
  deferredDrainRetryDelayMs?: number;
  /**
   * Maximum in-process retries after the first deferred-message acceptance
   * failure. Defaults to 3; exhaustion leaves the durable message queued for
   * the next runner activation instead of blocking the source loop forever.
   */
  deferredDrainMaxAcceptanceRetries?: number;
}

/**
 * Kafka consumer-group dispatcher.
 *
 * Discovers canonical session topics through `admin.listTopics()` and passes
 * that exact snapshot to one explicit consumer-group subscription. For each
 * `produced_by=client`, `kind=user.*` message, looks up (or spawns) a `SessionRunner` for that
 * `(workspace_id, session_id)` and forwards the parsed payload via
 * `runner.submit`.
 *
 * Harness-emitted events (`produced_by=harness`) loop back through Kafka and
 * are intentionally ignored to avoid echoing our own work.
 *
 * The harness factory is pluggable so integration tests can inject
 * a `FakeHarness` without touching the real Anthropic SDK.
 */
export class Dispatcher {
  private readonly runners = new Map<string, SessionRunner>();
  private consumer: Consumer | null = null;
  private activeConsumer: KafkaConsumerLifecycle | null = null;
  /** Candidate under connect/subscribe/group-join transition, if any. */
  private candidateConsumer: KafkaConsumerLifecycle | null = null;
  private transitionTarget: KafkaConsumerLifecycle | null = null;
  /**
   * Topics whose subscription has completed a Kafka GROUP_JOIN. This is not
   * discovery state: it changes only from GROUP_JOIN, never from a later
   * `admin.listTopics()` call.
   */
  private readonly subscribedTopics = new Set<string>();
  /** Last successful canonical topic-discovery snapshot. */
  private readonly knownTopics = new Set<string>();
  private kafkaDiscoveryAt: number | null = null;
  private kafkaDiscoveryFailed = true;
  private consumerReady = false;
  private consumerCrashed = false;
  private consumerRebalancing = false;
  private assignedTopicCount = 0;
  private assignedPartitionCount = 0;
  private kafkaOffsetLag = 0;
  private transitionFailed = false;
  private eventSourceStarted = false;
  /**
   * Handle for the rediscover loop's `setTimeout`. Cleared in `stop()` so a
   * shutdown doesn't leave a dangling timer that races against the next
   * boot's consumer.
   */
  private rediscoverTimer: NodeJS.Timeout | null = null;
  private readonly idleTimers = new Map<string, NodeJS.Timeout>();
  /** Idle requests waiting for current runner's terminal append to become durable. */
  private readonly idleAfterTerminal = new Map<string, SessionRunner>();
  private readonly runnerSessionConfigKeys = new Map<string, string>();
  private readonly deferredUserMessagePayloads = new Map<string, Map<string, unknown>>();
  private readonly deferredUserMessageQueues = new Map<string, DeferredUserMessageQueue>();
  private readonly deferredDrainRetryWakeups = new Set<() => void>();
  /** Turn-driving source IDs accepted into not-yet-durably-completed turns. */
  private readonly activeTurnUserEventIds = new Map<string, string[]>();
  /** Per-session cache rebuilt from durable completion markers on first source delivery. */
  private readonly completedUserEventIds = new Map<string, Set<string>>();
  private readonly completedUserEventLoads = new Map<string, Promise<Set<string>>>();
  /**
   * One pending source timestamp per accepted user.message turn. Entries are
   * registered at source ingestion, before runner setup or Kafka dispatch can
   * add latency, and consumed only after a durable status_running append.
   */
  private readonly pendingStatusRunningLatency = new Map<string, StatusRunningLatencyEvent[]>();
  /** Started turns remain keyed until their terminal status to dedupe redelivery. */
  private readonly activeStatusRunningLatency = new Map<string, StatusRunningLatencyEvent[]>();
  /** One shared turn task per durable Kafka source event across consumer replacements. */
  private readonly kafkaMessageWork = new Map<string, Promise<void>>();
  /** Durable source-settlement waiters keyed by stable user event id. */
  private readonly kafkaSourceSettlements = new Map<string, KafkaSourceSettlement>();
  /** Accepted source events participating in the current turn, including replies. */
  private readonly activeTurnSettlementIds = new Map<string, string[]>();
  private readonly kafkaRetryAttempts = new Map<string, number>();
  private readonly kafkaRetryWakeups = new Set<() => void>();
  /**
   * Set during a re-subscribe transition so the rediscover loop is a no-op
   * while the consumer is already in flight (avoids two concurrent
   * `consumer.stop()` calls on the same instance, which can leave kafkajs
   * in an inconsistent state).
   */
  private kafkaTransitionPromise: Promise<void> | null = null;
  private shutdownGraceExpired = false;
  /**
   * Set by `stop()` so any in-flight rediscover poll exits early instead of
   * resurrecting the consumer. Without this, a poll that started just before
   * stop could subscribe after we'd already torn the consumer down.
   */
  private stopping = false;
  /** Incremented before teardown so late callbacks cannot revive retired state. */
  private lifecycleGeneration = 0;
  private stopPromise: Promise<void> | null = null;

  private readonly codec: KafkaTranscriptCodec;
  private readonly harnessProviders: HarnessProviderRegistry;
  private readonly kafkaDecodeWakeups = new Set<() => void>();

  constructor(private readonly opts: DispatcherOptions) {
    this.codec = opts.codec ?? createKafkaTranscriptCodec();
    // Cloud Claude and Codex SDKs share the HTTP bridge; Registry-owned sessions never
    // reach it. An SDK added to the catalog does not implicitly become Claude.
    this.harnessProviders = createHarnessProviderRegistry([
      {
        id: 'claude_agent_sdk',
        build: ({ workspaceId, sessionId, session }) =>
          this.buildClaudeHarness(
            workspaceId,
            sessionId,
            new ClaudeAgentSdkAdapter(this.opts.store, workspaceId),
            session,
          ),
      },
      {
        id: 'codex_sdk',
        modes: ['separate', 'colocated'],
        build: ({ workspaceId, sessionId, session, selection }) =>
          this.buildCodexHarness(workspaceId, sessionId, session, selection?.mode === 'colocated'),
      },
      {
        id: 'pi_sdk',
        modes: ['separate', 'colocated'],
        build: ({ workspaceId, sessionId, session, selection, modelProvider }) =>
          this.buildCodexHarness(
            workspaceId,
            sessionId,
            session,
            selection?.mode === 'colocated',
            'pi_sdk',
            modelProvider,
          ),
      },
      ...(['claude_code', 'codex', 'cursor', 'pi', 'custom'] as const).map((id) => ({
        id,
        build: ({ workspaceId, sessionId }: { workspaceId: string; sessionId: string }) => {
          assertHarnessServerCanRunColocated(id);
          return new InSandboxHarness({
            providerId: harnessToProvider(id),
            port: HARNESS_CATALOG[id].port!,
            replaySource: () => this.readInSandboxReplay(workspaceId, sessionId),
            ...(this.opts.registry
              ? {
                  onGuardrailState: this.guardrailStateWriter(workspaceId, sessionId)!,
                  refreshGuardrailSubjectWindow: this.guardrailSubjectWindowRefresher(
                    workspaceId,
                    sessionId,
                  )!,
                }
              : {}),
          });
        },
      })),
    ]);
  }

  private async readInSandboxReplay(workspaceId: string, sessionId: string): Promise<ReplayTurn[]> {
    const entries: Array<Record<string, unknown>> = [];
    try {
      for await (const e of this.opts.store.read(workspaceId, sessionId, {
        fromCursor: '',
        maxEvents: 0,
        subpath: '',
      })) {
        try {
          entries.push(
            JSON.parse(Buffer.from(e.payload).toString('utf8')) as Record<string, unknown>,
          );
        } catch {
          // skip unparseable payloads
        }
      }
    } catch (err) {
      console.error(`dispatcher: readInSandboxReplay ${sessionId} failed`, err);
      return [];
    }
    return buildReplayTurns(entries);
  }

  private buildClaudeHarness(
    workspaceId: string,
    sessionId: string,
    adapter: ClaudeAgentSdkAdapter,
    session?: SessionRecord | null,
  ): AgentHarness {
    const llmEgress = this.resolveLlmEgress(session);
    const harnessOpts: ClaudeHarnessOptions = {
      apiKey: llmEgress === 'gateway' ? '' : this.opts.anthropicApiKey,
      modelDefault: this.opts.modelDefault,
      adapter,
      workspaceId,
      sessionId,
    };
    if (llmEgress === 'gateway') {
      if (!this.opts.registry || !this.opts.gatewayLlmUrl) {
        throw new Error('gateway LLM egress requires Registry and LLM_GATEWAY_URL');
      }
      const registry = this.opts.registry;
      // The SDK and outcome evaluator append /v1/messages. The local gateway
      // default ends in /v1; deployed LLM proxies can instead end in /v1/llm.
      harnessOpts.baseURL = this.opts.gatewayLlmUrl.replace(/\/+$/, '').replace(/\/v1$/, '');
      harnessOpts.llmGatewayJwtProvider = new SessionJwtProvider((signal) =>
        registry.mintLlmGatewayJwt(workspaceId, sessionId, signal),
      );
    } else if (this.opts.anthropicBaseURL !== undefined) {
      harnessOpts.baseURL = this.opts.anthropicBaseURL;
    }
    const guardrailStateWriter = this.guardrailStateWriter(workspaceId, sessionId);
    if (guardrailStateWriter) harnessOpts.onGuardrailState = guardrailStateWriter;
    const guardrailUsageRecorder = this.guardrailUsageRecorder(workspaceId, sessionId);
    if (guardrailUsageRecorder) harnessOpts.onUsage = guardrailUsageRecorder;
    const subjectWindowRefresher = this.guardrailSubjectWindowRefresher(workspaceId, sessionId);
    if (subjectWindowRefresher) {
      harnessOpts.refreshGuardrailSubjectWindow = subjectWindowRefresher;
    }
    return new ClaudeAgentSdkHarness(harnessOpts);
  }

  private buildCodexHarness(
    workspaceId: string,
    sessionId: string,
    session?: SessionRecord | null,
    colocated = false,
    harness: 'codex_sdk' | 'pi_sdk' = 'codex_sdk',
    modelProvider = 'openai',
  ): AgentHarness {
    const registry = this.opts.registry;
    if (!registry || !session?.runtime_revision)
      throw new Error('codex_sdk requires prepared Registry state');
    const egress = this.resolveLlmEgress(session, colocated);
    if (egress === 'gateway' && !this.opts.gatewayLlmUrl)
      throw new Error('gateway LLM egress requires LLM_GATEWAY_URL');

    const direct =
      harness === 'pi_sdk' ? this.opts.piProviderCredentials?.[modelProvider] : undefined;
    const directKey =
      direct?.apiKey ??
      (modelProvider === 'openai'
        ? this.opts.openaiApiKey
        : modelProvider === 'anthropic'
          ? this.opts.anthropicApiKey
          : '');
    const directUrl =
      direct?.baseUrl ??
      (modelProvider === 'openai'
        ? this.opts.openaiBaseURL
        : modelProvider === 'anthropic'
          ? this.opts.anthropicBaseURL
          : undefined);
    if (session.harness_state) validateSdkCheckpoint(session.harness_state, harness);
    return new CodexSdkHarness({
      harness,
      ...(colocated
        ? {
            createWorker: (emit, input) =>
              new RemoteCodexSdkWorker(
                emit,
                input,
                undefined,
                harness === 'pi_sdk' ? 'pi-sdk' : 'codex-sdk',
              ),
          }
        : {}),
      apiKey: egress === 'gateway' ? '' : (directKey ?? ''),
      ...(egress === 'gateway'
        ? {
            // The Responses SDK appends /responses; retain the gateway's /v1 prefix.
            ...(harness === 'pi_sdk'
              ? { piGatewayUrl: this.opts.gatewayLlmUrl! }
              : { baseUrl: this.opts.gatewayLlmUrl!.replace(/\/+$/, '') }),
            llmGatewayJwtProvider: new SessionJwtProvider(
              (signal) => registry.mintLlmGatewayJwt(workspaceId, sessionId, signal),
              // One SDK bearer spans the ten-minute turn, including client waits.
              630_000,
            ),
          }
        : directUrl
          ? { baseUrl: directUrl }
          : {}),
      ...(session.harness_state ? { checkpoint: session.harness_state as CodexCheckpoint } : {}),
      onGuardrailState: this.guardrailStateWriter(workspaceId, sessionId),
      onUsage: this.guardrailUsageRecorder(workspaceId, sessionId, modelProvider),
      refreshGuardrailSubjectWindow: this.guardrailSubjectWindowRefresher(workspaceId, sessionId),
      turns: this.codexTurnStore(
        workspaceId,
        sessionId,
        session.runtime_revision,
        session.harness_ownership_revision ?? 0,
      ),
    });
  }

  private resolveLlmEgress(session?: SessionRecord | null, colocated = false): LlmEgress {
    const configured = session?.metadata?.['orca_llm_egress'];
    if (configured !== undefined && configured !== 'direct' && configured !== 'gateway') {
      throw new Error(`invalid session metadata.orca_llm_egress: ${String(configured)}`);
    }
    // Colocated cloud harnesses cannot hold provider credentials and therefore
    // always use the Gateway. For separate execution, an explicit Session
    // override wins over the deployment-wide default.
    return colocated ? 'gateway' : (configured ?? this.opts.llmEgressDefault ?? 'direct');
  }

  private codexTurnStore(
    workspaceId: string,
    sessionId: string,
    runtimeRevision: number,
    expectedOwnershipRevision: number,
  ): CodexTurnStore {
    const ownerToken = randomUUID();
    let ownershipRevision = expectedOwnershipRevision;
    const generation = this.lifecycleGeneration;
    const request = async (
      action: import('@orca/harness-catalog').HarnessTurnAction,
    ): Promise<HarnessTurnSnapshot> =>
      this.withHarnessTurnRecovery(async () => {
        this.assertLifecycleCurrent(generation);
        const result = await this.opts.registry!.harnessTurn({
          workspaceId,
          sessionId,
          request: { runtimeRevision, ownershipRevision, ownerToken, action },
        });
        this.assertLifecycleCurrent(generation);
        if (!result) throw new Error('Registry did not acknowledge harness ownership');
        ownershipRevision = result.ownershipRevision;
        return result;
      });
    return {
      claim: () => request({ type: 'claim', expectedOwnershipRevision }),
      update: request,
      recover: (receipt) =>
        this.withHarnessTurnRecovery(() =>
          this.persistRecoveredHarnessTurn(workspaceId, sessionId, receipt, generation),
        ),
    };
  }

  private async withHarnessTurnRecovery<T>(operation: () => Promise<T>): Promise<T> {
    try {
      return await operation();
    } catch (error) {
      if (
        error instanceof RegistryInvalidRuntimeBindingError ||
        error instanceof RetryableSessionEventError
      )
        throw error;
      throw new RetryableSessionEventError('Codex durable turn recovery unavailable', {
        cause: error,
      });
    }
  }

  private async persistRecoveredHarnessTurn(
    workspaceId: string,
    sessionId: string,
    receipt: HarnessTurnReceipt,
    generation: number,
  ): Promise<void> {
    const events = receiptTerminalEvents(receipt).map((event) =>
      publicEntryToEvent({
        workspaceId,
        sessionId,
        subpath: event.subpath,
        producedBy: 'harness',
        eventId: event.id,
        idempotencyKey: event.id,
        now: () => new Date(receipt.producedAt),
        entry: { ...(event.payload as Record<string, unknown>), type: event.kind },
      }),
    );
    events.push(
      ...receipt.sourceIds.map((id) => ({
        ...completedUserEventMarker(workspaceId, sessionId, id),
        producedAt: receipt.producedAt,
      })),
    );
    const missing = new Set(events.map(({ id }) => id));
    for await (const event of this.opts.store.read(workspaceId, sessionId, {
      fromCursor: '',
      maxEvents: 0,
      subpath: '*',
    }))
      missing.delete(event.id);
    for (const event of events) {
      this.assertLifecycleCurrent(generation);
      if (missing.has(event.id)) await this.opts.store.append(workspaceId, sessionId, [event]);
    }
    this.assertLifecycleCurrent(generation);
    this.completionForTerminal(
      workspaceId,
      sessionId,
      generation,
      receipt.sourceIds,
    )?.onPersisted();
    this.clearCompletedUserEventCache(runnerKeyFor(workspaceId, sessionId));
  }

  private async recoverColdHarnessTurn(
    workspaceId: string,
    sessionId: string,
    generation: number,
  ): Promise<void> {
    // This route reads the pinned binding without execution preparation, so a cold
    // interrupt still works after its Agent is archived or its setup becomes invalid.
    const registry = this.opts.registry;
    if (!registry?.harnessTurn) return;
    const snapshot = await this.withHarnessTurnRecovery(() =>
      registry.harnessTurn({
        workspaceId,
        sessionId,
        request: { action: { type: 'inspect' } },
      }),
    );
    this.assertLifecycleCurrent(generation);
    if (!snapshot?.receipt || snapshot.receipt.phase === 'settled') return;
    const store = this.codexTurnStore(
      workspaceId,
      sessionId,
      snapshot.runtimeRevision,
      snapshot.ownershipRevision,
    );
    await recoverCodexTurn(store, await store.claim());
  }

  private guardrailUsageRecorder(
    workspaceId: string,
    sessionId: string,
    provider = 'anthropic',
  ): ClaudeHarnessOptions['onUsage'] {
    const registry = this.opts.registry;
    if (!registry) return undefined;
    return async (usage, model, subagentId, turnEventId, usageEventId) => {
      const session = await registry.recordSessionUsageInternal({
        workspaceId,
        sessionId,
        provider,
        usage,
        model,
        ...(subagentId ? { subagentId } : {}),
        ...(turnEventId ? { turnEventId } : {}),
        ...(usageEventId ? { usageEventId } : {}),
      });
      if (!session) throw new Error(`Registry session ${sessionId} disappeared during usage flush`);
      return {
        ...(session.guardrail_usage_state ?? {}),
        ...(session.guardrail_subject_window_state ?? {}),
      };
    };
  }

  private guardrailSubjectWindowRefresher(
    workspaceId: string,
    sessionId: string,
  ): ClaudeHarnessOptions['refreshGuardrailSubjectWindow'] {
    const registry = this.opts.registry;
    if (!registry) return undefined;
    return async (turnEventId) =>
      await registry.refreshGuardrailSubjectWindowInternal({
        workspaceId,
        sessionId,
        turnEventId,
      });
  }

  private guardrailStateWriter(
    workspaceId: string,
    sessionId: string,
  ): ClaudeHarnessOptions['onGuardrailState'] {
    const registry = this.opts.registry;
    if (!registry) return undefined;
    // Guardrail decisions can be requested concurrently by the SDK. Keep one
    // FIFO write-through lane per harness so persistent `set`/`append` updates
    // cannot overtake each other. This is intentionally synchronous rather
    // than a background flusher: the guarded action is not released, nor is
    // local state advanced, until Registry acknowledges the durable write.
    let writeTail: Promise<void> = Promise.resolve();
    return async (updates) => {
      const subjectWindowUpdate = updates.find((update) => update.scope === 'subject_window');
      if (subjectWindowUpdate) {
        throw new Error(
          'subject_window guardrail updates must be derived and persisted by Registry from authenticated turn usage',
        );
      }
      const persistent = updates.filter(
        (update): update is typeof update & { scope: 'session' } => update.scope === 'session',
      );
      if (persistent.length === 0) return;
      const write = writeTail.then(async () => {
        await registry.applyGuardrailStateInternal({
          workspaceId,
          sessionId,
          updates: persistent,
          signal: AbortSignal.timeout(GUARDRAIL_STATE_WRITE_TIMEOUT_MS),
        });
      });
      // A rejected write fails the current guarded action, but must not poison
      // the lane for a later independent decision.
      writeTail = write.catch(() => undefined);
      await write;
    };
  }

  async start(): Promise<void> {
    this.lifecycleGeneration += 1;
    this.stopping = false;
    this.shutdownGraceExpired = false;
    this.stopPromise = null;
    if (this.opts.eventSource) {
      await this.opts.eventSource.start(this.onTranscriptEvent);
      this.eventSourceStarted = true;
      return;
    }
    if (!this.opts.kafka) {
      throw new Error('Dispatcher requires either kafka or eventSource');
    }
    await this.runKafkaTransition((generation) => this.discoverKafkaTopics(generation));
    this.scheduleRediscover();
  }

  /** Effective topic prefix ('' = canonical bare-name subscriptions). */
  private get topicPrefix(): string {
    return this.opts.topicPrefix ?? '';
  }

  private get sessionTopicPattern(): RegExp {
    return this.opts.topicPattern ?? sessionTopicPattern(this.codec.encoding ?? 'raw');
  }

  /**
   * Returns the broker's current list of topics matching
   * the selected encoding's session topic pattern, or `null` if the admin call failed (caller
   * decides whether to retry next tick or surface the error). The admin
   * client is created + closed per call — `listTopics` is rare enough that
   * a long-lived admin connection isn't worth the lifecycle complexity.
   */
  private async listMatchingSessionTopics(): Promise<string[] | null> {
    if (!this.opts.kafka) return null;
    const admin = this.opts.kafka.admin();
    try {
      await admin.connect();
      const all = await admin.listTopics();
      if (this.topicPrefix === '') {
        return all
          .filter(
            (t) =>
              matchSessionTopic(t, '', this.codec.encoding ?? 'raw') !== null &&
              matchesTopic(this.sessionTopicPattern, t),
          )
          .sort((left, right) => left.localeCompare(right));
      }
      // Prefixed path: plain Kafka lists the full prefixed names, while KoP
      // lists only local bare names for the selected namespace. Prefer real
      // prefixed listings when present so unrelated bare topics from other
      // tests/services are not rewritten into non-existent prefixed topics.
      const prefixed = new Set<string>();
      const bare = new Set<string>();
      for (const t of all) {
        if (t.startsWith(this.topicPrefix)) {
          const match = matchSessionTopic(t, this.topicPrefix, this.codec.encoding ?? 'raw');
          if (match && matchesTopic(this.sessionTopicPattern, t.slice(this.topicPrefix.length))) {
            prefixed.add(match.canonicalTopic);
          }
          continue;
        }
        const match = matchSessionTopic(t, this.topicPrefix, this.codec.encoding ?? 'raw');
        if (match && matchesTopic(this.sessionTopicPattern, t)) bare.add(match.canonicalTopic);
      }
      return (prefixed.size > 0 ? [...prefixed] : [...bare]).sort((left, right) =>
        left.localeCompare(right),
      );
    } catch (e) {
      console.error('dispatcher: rediscover listTopics failed', e);
      return null;
    } finally {
      await admin.disconnect().catch(() => {});
    }
  }

  /**
   * Return liveness-independent dispatcher state for `/readyz`. Legacy event
   * sources are ready after start; sources with status() report their current
   * state. Kafka requires a fresh metadata snapshot and a joined consumer
   * whenever that snapshot has topics.
   */
  readiness(): DispatcherReadiness {
    if (this.stopping) return { ready: false, reasons: ['dispatcher_stopping'] };
    if (this.opts.eventSource) {
      if (!this.eventSourceStarted) {
        return { ready: false, reasons: ['event_source_not_started'] };
      }
      const status = this.eventSourceStatus();
      if (status?.ready !== false) return { ready: true, reasons: [] };
      return {
        ready: false,
        reasons: [status?.state === 'failed' ? 'event_source_failed' : 'event_source_stopped'],
      };
    }
    if (!this.opts.kafka) return { ready: false, reasons: ['event_source_not_configured'] };

    const reasons: string[] = [];
    if (this.kafkaDiscoveryIsStale()) reasons.push('kafka_discovery_stale');
    if (
      this.knownTopics.size > 0 &&
      (!this.consumerReady ||
        this.consumerRebalancing ||
        !sameTopicSets(this.knownTopics, this.subscribedTopics))
    ) {
      reasons.push('kafka_topics_unjoined');
    }
    if (this.transitionFailed) reasons.push('kafka_transition_failed');
    return { ready: reasons.length === 0, reasons };
  }

  /**
   * Schedule exactly one later discovery. `setTimeout` makes slow broker
   * calls serialize rather than pile up. The default is a safe 30 seconds for
   * direct construction; production config validates its own positive value.
   */
  private scheduleRediscover(): void {
    if (this.stopping || this.rediscoverTimer !== null) return;
    this.rediscoverTimer = setTimeout(() => {
      this.rediscoverTimer = null;
      void this.rediscoverTopics();
    }, this.kafkaRediscoverIntervalMs);
    this.rediscoverTimer.unref?.();
  }

  private get kafkaRediscoverIntervalMs(): number {
    const configured = this.opts.topicRediscoverIntervalMs;
    if (configured === undefined || !Number.isFinite(configured) || configured <= 0) {
      return KAFKA_DEFAULT_TOPIC_REDISCOVER_INTERVAL_MS;
    }
    return Math.max(1, Math.floor(configured));
  }

  private get kafkaGroupJoinTimeoutMs(): number {
    const configured = this.opts.kafkaGroupJoinTimeoutMs;
    if (configured === undefined || !Number.isFinite(configured) || configured <= 0) {
      return KAFKA_GROUP_JOIN_TIMEOUT_MS;
    }
    return Math.max(1, Math.floor(configured));
  }

  private get shutdownGraceMs(): number {
    const configured = this.opts.shutdownGraceMs ?? this.opts.kafkaShutdownGraceMs;
    if (configured === undefined || !Number.isFinite(configured) || configured <= 0) {
      return DISPATCHER_SHUTDOWN_GRACE_MS;
    }
    return Math.max(1, Math.floor(configured));
  }

  private isLifecycleCurrent(generation: number): boolean {
    return !this.stopping && this.lifecycleGeneration === generation;
  }

  private assertLifecycleCurrent(generation: number): void {
    if (!this.isLifecycleCurrent(generation)) throw new DispatcherStoppingError();
  }

  private eventSourceStatus(): SessionEventSourceStatus | null {
    try {
      return this.opts.eventSource?.status?.() ?? null;
    } catch {
      // A health callback must not make /readyz throw or disclose source details.
      return { ready: false, state: 'failed' };
    }
  }

  private kafkaDiscoveryIsStale(): boolean {
    if (this.kafkaDiscoveryFailed || this.kafkaDiscoveryAt === null) return true;
    return Date.now() - this.kafkaDiscoveryAt > Math.max(this.kafkaRediscoverIntervalMs * 2, 1_000);
  }

  /** One serialized metadata-discovery/reconciliation tick. */
  private async rediscoverTopics(): Promise<void> {
    const lifecycleGeneration = this.lifecycleGeneration;
    if (!this.isLifecycleCurrent(lifecycleGeneration)) return;
    if (this.kafkaTransitionPromise !== null) {
      this.scheduleRediscover();
      return;
    }
    try {
      await this.runKafkaTransition((generation) => this.discoverKafkaTopics(generation));
    } catch (e) {
      if (this.isLifecycleCurrent(lifecycleGeneration)) {
        console.error('dispatcher: rediscover tick failed', e);
      }
    } finally {
      if (this.isLifecycleCurrent(lifecycleGeneration)) this.scheduleRediscover();
    }
  }

  /**
   * Serializes discovery and replacement. `stop()` observes this promise only
   * up to its Kafka shutdown grace; stopping guards prevent a late candidate
   * from restoring consumer pointers after that bound expires.
   */
  private async runKafkaTransition(work: (generation: number) => Promise<void>): Promise<void> {
    if (this.kafkaTransitionPromise !== null) return this.kafkaTransitionPromise;
    const lifecycleGeneration = this.lifecycleGeneration;
    this.assertLifecycleCurrent(lifecycleGeneration);
    const transition = Promise.resolve().then(async () => await work(lifecycleGeneration));
    this.kafkaTransitionPromise = transition;
    try {
      await transition;
    } finally {
      if (this.kafkaTransitionPromise === transition) this.kafkaTransitionPromise = null;
    }
  }

  /**
   * Treat one `listTopics()` result as the authoritative discovery snapshot.
   * It is copied to `knownTopics` and, if reconciliation is needed, passed
   * unchanged to `consumer.subscribe({ topics })`.
   */
  private async discoverKafkaTopics(lifecycleGeneration: number): Promise<void> {
    const matched = await this.listMatchingSessionTopics();
    this.assertLifecycleCurrent(lifecycleGeneration);
    if (matched === null) {
      this.kafkaDiscoveryFailed = true;
      harnessKafkaDiscoveryTotal.inc({ result: 'error' });
      this.updateKafkaMetrics();
      return;
    }

    this.kafkaDiscoveryAt = Date.now();
    this.kafkaDiscoveryFailed = false;
    replaceTopicSet(this.knownTopics, matched);
    harnessKafkaDiscoveryTotal.inc({ result: 'ok' });
    this.updateKafkaMetrics();
    await this.reconcileKafkaTopics(matched, lifecycleGeneration);
  }

  private async reconcileKafkaTopics(
    desiredTopics: string[],
    lifecycleGeneration: number,
  ): Promise<void> {
    this.assertLifecycleCurrent(lifecycleGeneration);
    if (desiredTopics.length === 0) {
      await this.deactivateKafkaConsumerForEmptySnapshot(lifecycleGeneration);
      return;
    }

    const topicSetMatches = sameTopicSets(desiredTopics, this.subscribedTopics);
    if (topicSetMatches && this.consumer !== null && !this.consumerCrashed) {
      // KafkaJS owns normal rebalances and emits GROUP_JOIN when assignment is
      // restored. Do not create a second runner while that consumer recovers.
      if (this.consumerReady) this.transitionFailed = false;
      this.updateKafkaMetrics();
      return;
    }

    await this.replaceKafkaConsumer(desiredTopics, lifecycleGeneration);
  }

  private async deactivateKafkaConsumerForEmptySnapshot(
    lifecycleGeneration: number,
  ): Promise<void> {
    this.assertLifecycleCurrent(lifecycleGeneration);
    const previous = this.activeConsumer;
    if (!previous) {
      replaceTopicSet(this.subscribedTopics, []);
      this.consumerReady = false;
      this.consumerCrashed = false;
      this.consumerRebalancing = false;
      this.transitionFailed = false;
      this.clearKafkaAssignment();
      this.updateKafkaMetrics();
      return;
    }

    try {
      await this.disconnectActiveKafkaConsumer(previous);
      this.assertLifecycleCurrent(lifecycleGeneration);
      replaceTopicSet(this.subscribedTopics, []);
      this.consumerCrashed = false;
      this.consumerRebalancing = false;
      this.transitionFailed = false;
      harnessKafkaTransitionTotal.inc({ result: 'ok' });
    } catch (error) {
      if (
        !this.isLifecycleCurrent(lifecycleGeneration) ||
        error instanceof DispatcherStoppingError
      ) {
        return;
      }
      this.transitionFailed = true;
      harnessKafkaTransitionTotal.inc({ result: 'error' });
      console.error(
        'dispatcher: could not disconnect Kafka consumer for empty topic snapshot',
        error,
      );
    }
    this.assertLifecycleCurrent(lifecycleGeneration);
    this.updateKafkaMetrics();
  }

  /**
   * Prepare a replacement while the old consumer remains active. Candidate
   * `run()` starts only after old `disconnect()` has completed, avoiding two
   * consumers executing the same in-flight turn in one group.
   */
  private async replaceKafkaConsumer(
    desiredTopics: string[],
    lifecycleGeneration: number,
  ): Promise<void> {
    this.assertLifecycleCurrent(lifecycleGeneration);
    const previous = this.activeConsumer;
    const previousTopics = [...this.subscribedTopics];
    let candidate: KafkaConsumerLifecycle | null = null;

    try {
      candidate = this.createKafkaConsumer(desiredTopics, lifecycleGeneration);
      this.candidateConsumer = candidate;
      await candidate.consumer.connect();
      if (!this.isLifecycleCurrent(lifecycleGeneration)) {
        await this.disconnectKafkaCandidate(candidate);
        return;
      }
      await candidate.consumer.subscribe({ topics: candidate.topics, fromBeginning: true });
    } catch (error) {
      await this.disconnectKafkaCandidate(candidate);
      if (!this.isLifecycleCurrent(lifecycleGeneration)) return;
      await this.handleKafkaTransitionFailure(
        error,
        previous,
        previousTopics,
        false,
        lifecycleGeneration,
      );
      return;
    }

    if (!this.isLifecycleCurrent(lifecycleGeneration)) {
      await this.disconnectKafkaCandidate(candidate);
      return;
    }

    let previousDisconnected = previous === null;
    if (previous) {
      try {
        await this.disconnectActiveKafkaConsumer(previous);
        this.assertLifecycleCurrent(lifecycleGeneration);
        previousDisconnected = true;
      } catch (error) {
        await this.disconnectKafkaCandidate(candidate);
        if (!this.isLifecycleCurrent(lifecycleGeneration)) return;
        await this.handleKafkaTransitionFailure(
          error,
          previous,
          previousTopics,
          false,
          lifecycleGeneration,
        );
        return;
      }
    }

    // Shutdown may have begun while `disconnect()` waited for an in-flight
    // handler. Never invoke candidate.run() after that wait unless this
    // dispatcher still owns the transition.
    if (!this.isLifecycleCurrent(lifecycleGeneration)) {
      await this.disconnectKafkaCandidate(candidate);
      return;
    }

    try {
      await this.runKafkaConsumer(candidate);
      this.assertLifecycleCurrent(lifecycleGeneration);
      harnessKafkaTransitionTotal.inc({ result: 'ok' });
    } catch (error) {
      await this.disconnectKafkaCandidate(candidate);
      if (!this.isLifecycleCurrent(lifecycleGeneration)) return;
      await this.handleKafkaTransitionFailure(
        error,
        previous,
        previousTopics,
        previousDisconnected,
        lifecycleGeneration,
      );
    }
  }

  private createKafkaConsumer(
    topics: string[],
    lifecycleGeneration: number,
  ): KafkaConsumerLifecycle {
    if (!this.opts.kafka) throw new Error('Kafka dispatcher is not configured');
    const consumer = this.opts.kafka.consumer({
      groupId: this.opts.groupId,
      sessionTimeout: KAFKA_CONSUMER_SESSION_TIMEOUT_MS,
      // Dispatcher replaces crashed consumers. KafkaJS auto-restart can finish
      // joining after disconnect() and steal partitions from that replacement.
      retry: { restartOnFailure: async () => false },
    });
    const lifecycle: KafkaConsumerLifecycle = {
      consumer,
      topics: [...topics],
      groupJoin: createKafkaGroupJoinWaiter(),
      disconnectPromise: null,
      generation: lifecycleGeneration,
      assignmentEpoch: 0,
      assignedPartitions: new Set(),
    };

    consumer.on(consumer.events.GROUP_JOIN, (event) => {
      lifecycle.groupJoin.resolve();
      if (!this.isLifecycleCurrent(lifecycle.generation) || this.consumer !== consumer) return;

      const assignment = event.payload.memberAssignment ?? {};
      lifecycle.assignmentEpoch += 1;
      lifecycle.assignedPartitions.clear();
      this.wakeKafkaRetryWaiters();
      for (const [topic, partitions] of Object.entries(assignment)) {
        for (const partition of partitions) {
          lifecycle.assignedPartitions.add(kafkaTopicPartitionKey(topic, partition));
        }
      }
      this.assignedTopicCount = Object.values(assignment).filter(
        (partitions) => partitions.length > 0,
      ).length;
      this.assignedPartitionCount = Object.values(assignment).reduce(
        (count, partitions) => count + partitions.length,
        0,
      );
      if (this.assignedPartitionCount === 0) this.kafkaOffsetLag = 0;
      this.consumerReady = true;
      this.consumerCrashed = false;
      this.consumerRebalancing = false;
      replaceTopicSet(this.subscribedTopics, lifecycle.topics);
      if (this.candidateConsumer === lifecycle) {
        this.candidateConsumer = null;
      }
      if (this.transitionTarget === lifecycle) {
        this.transitionTarget = null;
      }
      this.transitionFailed = !sameTopicSets(this.knownTopics, lifecycle.topics);
      this.updateKafkaMetrics();
    });
    consumer.on(consumer.events.REBALANCING, () => {
      if (!this.isLifecycleCurrent(lifecycle.generation) || this.consumer !== consumer) return;
      lifecycle.assignmentEpoch += 1;
      lifecycle.assignedPartitions.clear();
      this.wakeKafkaRetryWaiters();
      this.consumerReady = false;
      this.consumerRebalancing = true;
      this.clearKafkaAssignment();
      this.updateKafkaMetrics();
    });
    consumer.on(consumer.events.CRASH, (event) => {
      lifecycle.groupJoin.reject(event.payload.error);
      if (!this.isLifecycleCurrent(lifecycle.generation) || this.consumer !== consumer) return;
      lifecycle.assignmentEpoch += 1;
      lifecycle.assignedPartitions.clear();
      this.wakeKafkaRetryWaiters();
      this.consumerReady = false;
      this.consumerCrashed = true;
      this.consumerRebalancing = false;
      this.transitionFailed = true;
      this.clearKafkaAssignment();
      this.updateKafkaMetrics();
      this.scheduleRediscover();
    });
    consumer.on(consumer.events.END_BATCH_PROCESS, (event) => {
      if (
        !this.isLifecycleCurrent(lifecycle.generation) ||
        this.consumer !== consumer ||
        this.assignedPartitionCount === 0
      )
        return;
      const lag = Number(event.payload.offsetLag);
      if (!Number.isFinite(lag) || lag < 0) return;
      this.kafkaOffsetLag = lag;
      this.updateKafkaMetrics();
    });
    return lifecycle;
  }

  private async runKafkaConsumer(lifecycle: KafkaConsumerLifecycle): Promise<void> {
    this.assertLifecycleCurrent(lifecycle.generation);
    this.consumer = lifecycle.consumer;
    this.activeConsumer = lifecycle;
    this.transitionTarget = lifecycle;
    this.consumerReady = false;
    this.consumerCrashed = false;
    this.consumerRebalancing = false;
    this.clearKafkaAssignment();
    this.updateKafkaMetrics();
    await lifecycle.consumer.run({
      autoCommit: false,
      eachMessage: async (payload) => await this.onKafkaMessage(lifecycle, payload),
    });
    this.assertLifecycleCurrent(lifecycle.generation);
    await this.waitForKafkaGroupJoin(lifecycle);
    this.assertLifecycleCurrent(lifecycle.generation);
  }

  private async waitForKafkaGroupJoin(lifecycle: KafkaConsumerLifecycle): Promise<void> {
    await new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => {
        reject(
          new Error(
            `Kafka consumer did not emit GROUP_JOIN within ${this.kafkaGroupJoinTimeoutMs}ms`,
          ),
        );
      }, this.kafkaGroupJoinTimeoutMs);
      timer.unref?.();
      void lifecycle.groupJoin.promise.then(
        () => {
          clearTimeout(timer);
          resolve();
        },
        (error: unknown) => {
          clearTimeout(timer);
          reject(error);
        },
      );
    });
  }

  private cancelKafkaGroupJoinWaiter(): void {
    this.candidateConsumer?.groupJoin.reject(new DispatcherStoppingError());
    this.transitionTarget?.groupJoin.reject(new DispatcherStoppingError());
  }

  private async handleKafkaTransitionFailure(
    error: unknown,
    previous: KafkaConsumerLifecycle | null,
    previousTopics: string[],
    previousDisconnected: boolean,
    lifecycleGeneration: number,
  ): Promise<void> {
    this.assertLifecycleCurrent(lifecycleGeneration);
    this.transitionFailed = true;
    harnessKafkaTransitionTotal.inc({ result: 'error' });
    console.error('dispatcher: Kafka consumer transition failed', error);
    // The old consumer remains tracked and active when candidate preparation
    // or retirement fails. Do not disconnect it a second time or create a
    // fallback beside an unknown old state.
    if (!previousDisconnected && previous && this.consumer === previous.consumer) {
      this.updateKafkaMetrics();
      return;
    }
    await this.restorePreviousKafkaConsumer(previousTopics, lifecycleGeneration);
    this.assertLifecycleCurrent(lifecycleGeneration);
    this.updateKafkaMetrics();
  }

  /** Restore a fresh consumer for the last joined topic list after a failed replacement. */
  private async restorePreviousKafkaConsumer(
    previousTopics: string[],
    lifecycleGeneration: number,
  ): Promise<void> {
    if (!this.isLifecycleCurrent(lifecycleGeneration) || previousTopics.length === 0) return;
    if (this.consumer !== null) return;

    let fallback: KafkaConsumerLifecycle | null = null;
    try {
      fallback = this.createKafkaConsumer(previousTopics, lifecycleGeneration);
      this.candidateConsumer = fallback;
      await fallback.consumer.connect();
      if (!this.isLifecycleCurrent(lifecycleGeneration)) {
        await this.disconnectKafkaCandidate(fallback);
        return;
      }
      await fallback.consumer.subscribe({ topics: fallback.topics, fromBeginning: true });
      if (!this.isLifecycleCurrent(lifecycleGeneration)) {
        await this.disconnectKafkaCandidate(fallback);
        return;
      }
      await this.runKafkaConsumer(fallback);
      this.assertLifecycleCurrent(lifecycleGeneration);
      harnessKafkaTransitionTotal.inc({ result: 'ok' });
    } catch (error) {
      await this.disconnectKafkaCandidate(fallback);
      if (!this.isLifecycleCurrent(lifecycleGeneration)) return;
      harnessKafkaTransitionTotal.inc({ result: 'error' });
      console.error('dispatcher: Kafka fallback consumer failed', error);
    }
  }

  private async disconnectActiveKafkaConsumer(lifecycle: KafkaConsumerLifecycle): Promise<void> {
    // Retirement revokes ownership before KafkaJS begins its asynchronous leave.
    // Detached work may settle while disconnect() is in flight; it must defer
    // commit/seek/resume to replacement redelivery rather than race group leave.
    lifecycle.assignmentEpoch += 1;
    lifecycle.assignedPartitions.clear();
    this.wakeKafkaRetryWaiters();
    await this.requestKafkaConsumerDisconnect(lifecycle);
    if (this.candidateConsumer === lifecycle) this.candidateConsumer = null;
    if (this.consumer !== lifecycle.consumer) return;
    this.consumer = null;
    this.activeConsumer = null;
    if (this.transitionTarget === lifecycle) this.transitionTarget = null;
    this.consumerReady = false;
    this.clearKafkaAssignment();
    this.updateKafkaMetrics();
  }

  private async disconnectKafkaCandidate(lifecycle: KafkaConsumerLifecycle | null): Promise<void> {
    if (!lifecycle) return;
    try {
      await this.requestKafkaConsumerDisconnect(lifecycle);
    } catch (error) {
      // An active candidate can still be processing if KafkaJS reports a
      // disconnect error. Keep it as the tracked active consumer so a later
      // transition retires it before any replacement is allowed to run.
      if (!this.stopping) console.error('dispatcher: Kafka candidate disconnect failed', error);
      return;
    }
    if (this.candidateConsumer === lifecycle) this.candidateConsumer = null;
    if (this.consumer === lifecycle.consumer) {
      this.consumer = null;
      this.activeConsumer = null;
      if (this.transitionTarget === lifecycle) this.transitionTarget = null;
      this.consumerReady = false;
      this.clearKafkaAssignment();
      this.updateKafkaMetrics();
    }
  }

  /** Reuse an in-flight disconnect instead of issuing concurrent KafkaJS calls. */
  private requestKafkaConsumerDisconnect(lifecycle: KafkaConsumerLifecycle): Promise<void> {
    if (lifecycle.disconnectPromise !== null) return lifecycle.disconnectPromise;

    let disconnect: Promise<void>;
    try {
      disconnect = Promise.resolve(lifecycle.consumer.disconnect());
    } catch (error) {
      disconnect = Promise.reject(error);
    }
    lifecycle.disconnectPromise = disconnect;
    void disconnect.then(
      () => {
        if (lifecycle.disconnectPromise === disconnect) lifecycle.disconnectPromise = null;
      },
      () => {
        if (lifecycle.disconnectPromise === disconnect) lifecycle.disconnectPromise = null;
      },
    );
    return disconnect;
  }

  private clearKafkaAssignment(): void {
    this.assignedTopicCount = 0;
    this.assignedPartitionCount = 0;
    this.kafkaOffsetLag = 0;
  }

  private updateKafkaMetrics(): void {
    harnessKafkaDiscoveredTopics.set(this.knownTopics.size);
    harnessKafkaActiveSubscribedTopics.set(this.subscribedTopics.size);
    harnessKafkaAssignedTopics.set(this.assignedTopicCount);
    harnessKafkaAssignedPartitions.set(this.assignedPartitionCount);
    harnessKafkaOffsetLag.set(this.kafkaOffsetLag);
    harnessKafkaConsumerReady.set(this.consumerReady ? 1 : 0);
  }

  /**
   * Return KafkaJS's callback immediately after pausing this session partition.
   * Turn execution remains tracked outside KafkaJS, so consumer replacement is
   * not blocked by a multi-minute model/tool turn. The source offset is committed
   * only after the durable terminal/completion append; crash before that leaves
   * it redeliverable. A replacement consumer reuses the same in-process work
   * promise when Kafka re-delivers before the old turn settles.
   */
  private async onKafkaMessage(
    lifecycle: KafkaConsumerLifecycle,
    payload: EachMessagePayload,
  ): Promise<void> {
    this.assertLifecycleCurrent(lifecycle.generation);
    const assignmentEpoch = lifecycle.assignmentEpoch;
    if (!this.kafkaDeliveryOwnsPartition(lifecycle, payload, assignmentEpoch)) return;
    const key = kafkaMessageWorkKey(payload);
    // Decode before settlement bookkeeping, control events, or turn dispatch.
    // Pending lookups are assignment-local, unlike already accepted turn work.
    let event: Event | null = null;
    const abort = new AbortController();
    const checkOwner = (): void => {
      if (!this.kafkaDeliveryOwnsPartition(lifecycle, payload, assignmentEpoch)) {
        abort.abort(new DispatcherStoppingError());
      }
    };
    this.kafkaDecodeWakeups.add(checkOwner);
    try {
      const route = matchSessionTopic(
        payload.topic,
        this.topicPrefix,
        this.codec.encoding ?? 'raw',
      );
      if (route) {
        await this.withKafkaHeartbeat(
          payload,
          async () => {
            abort.signal.throwIfAborted();
            event = await this.codec.decode(payload.message, route, abort.signal);
            abort.signal.throwIfAborted();
          },
          abort,
        );
      }
    } catch {
      if (!this.kafkaDeliveryOwnsPartition(lifecycle, payload, assignmentEpoch)) return;
      // Fail closed without logging schema bodies, credentials, or payload bytes.
      console.error('dispatcher: Kafka transcript decode failed; scheduling retry');
      const resume = payload.pause();
      try {
        await this.retryKafkaDelivery(lifecycle, payload, assignmentEpoch, key);
      } finally {
        if (this.kafkaDeliveryOwnsPartition(lifecycle, payload, assignmentEpoch)) resume();
      }
      return;
    } finally {
      this.kafkaDecodeWakeups.delete(checkOwner);
    }
    if (!this.kafkaDeliveryOwnsPartition(lifecycle, payload, assignmentEpoch)) return;
    const dispatch = async (): Promise<void> => {
      // withKafkaHeartbeat awaits once more after decoding; fence that await too.
      if (!this.kafkaDeliveryOwnsPartition(lifecycle, payload, assignmentEpoch)) {
        throw new DispatcherStoppingError();
      }
      await this.onMessage(payload, event);
    };
    if (!isKafkaTurnDrivingDelivery(payload, this.topicPrefix, this.codec.encoding ?? 'raw')) {
      await this.withKafkaHeartbeat(payload, dispatch);
      const committed = await this.commitKafkaDelivery(lifecycle, payload, assignmentEpoch, key);
      if (committed) this.kafkaRetryAttempts.delete(key);
      return;
    }
    let work = this.kafkaMessageWork.get(key);
    if (!work) {
      const sourceEventId = kafkaSourceEventId(payload);
      const settlement = this.getOrCreateKafkaSourceSettlement(sourceEventId);
      work = (async () => {
        try {
          await this.withKafkaHeartbeat(payload, dispatch);
          await settlement.promise;
        } catch (error) {
          this.rejectKafkaSourceEvent(sourceEventId, error);
          throw error;
        }
      })();
      this.kafkaMessageWork.set(key, work);
      void work.catch(() => {});
    }

    let resume: () => void = () => {};
    try {
      resume = payload.pause();
    } catch {
      // KafkaJS always supplies pause(); keep diagnostic fakes/custom clients safe.
    }

    const trackedWork = work;
    const delivery = work.then(
      async () => {
        const committed = await this.commitKafkaDelivery(lifecycle, payload, assignmentEpoch, key);
        if (committed && this.kafkaMessageWork.get(key) === trackedWork) {
          this.kafkaMessageWork.delete(key);
          this.kafkaRetryAttempts.delete(key);
        }
      },
      async () => {
        if (this.kafkaMessageWork.get(key) === trackedWork) this.kafkaMessageWork.delete(key);
        await this.retryKafkaDelivery(lifecycle, payload, assignmentEpoch, key);
      },
    );
    void delivery
      .finally(() => {
        if (this.kafkaDeliveryOwnsPartition(lifecycle, payload, assignmentEpoch)) {
          try {
            resume();
          } catch {
            // Retired consumer or stale partition pause; replacement owns retry.
          }
        }
      })
      .catch(() => {});
  }

  private async commitKafkaDelivery(
    lifecycle: KafkaConsumerLifecycle,
    payload: EachMessagePayload,
    assignmentEpoch: number,
    key: string,
  ): Promise<boolean> {
    if (!this.kafkaDeliveryOwnsPartition(lifecycle, payload, assignmentEpoch)) {
      return false;
    }
    try {
      await lifecycle.consumer.commitOffsets([
        {
          topic: payload.topic,
          partition: payload.partition,
          offset: nextKafkaOffset(payload.message.offset),
        },
      ]);
      // KafkaJS resolves commitOffsets() without error when its runner stopped
      // before the request was sent. Re-check ownership before treating that
      // resolution as durable and evicting shared in-process work.
      return this.kafkaDeliveryOwnsPartition(lifecycle, payload, assignmentEpoch);
    } catch {
      await this.retryKafkaDelivery(lifecycle, payload, assignmentEpoch, key);
      console.error('dispatcher: Kafka source offset commit failed; scheduling retry');
      return false;
    }
  }

  private async retryKafkaDelivery(
    lifecycle: KafkaConsumerLifecycle,
    payload: EachMessagePayload,
    assignmentEpoch: number,
    key: string,
  ): Promise<void> {
    if (!this.kafkaDeliveryOwnsPartition(lifecycle, payload, assignmentEpoch)) {
      return;
    }
    const attempt = (this.kafkaRetryAttempts.get(key) ?? 0) + 1;
    this.kafkaRetryAttempts.set(key, attempt);
    const delayMs = Math.min(
      KAFKA_RETRY_BASE_MS * 2 ** Math.min(attempt - 1, 8),
      KAFKA_RETRY_MAX_MS,
    );
    await this.waitForKafkaRetryBackoff(delayMs, lifecycle, payload, assignmentEpoch);
    if (!this.kafkaDeliveryOwnsPartition(lifecycle, payload, assignmentEpoch)) return;
    try {
      lifecycle.consumer.seek({
        topic: payload.topic,
        partition: payload.partition,
        offset: payload.message.offset,
      });
    } catch {
      console.error('dispatcher: Kafka source seek failed; replacement must redeliver');
    }
  }

  private async waitForKafkaRetryBackoff(
    delayMs: number,
    lifecycle: KafkaConsumerLifecycle,
    payload: EachMessagePayload,
    assignmentEpoch: number,
  ): Promise<void> {
    await new Promise<void>((resolve) => {
      const finish = (): void => {
        clearTimeout(timer);
        this.kafkaRetryWakeups.delete(finish);
        resolve();
      };
      this.kafkaRetryWakeups.add(finish);
      const timer = setTimeout(finish, delayMs);
      timer.unref?.();
      if (!this.kafkaDeliveryOwnsPartition(lifecycle, payload, assignmentEpoch)) finish();
    });
  }

  private wakeKafkaRetryWaiters(): void {
    for (const wake of Array.from(this.kafkaRetryWakeups)) wake();
    for (const wake of Array.from(this.kafkaDecodeWakeups)) wake();
  }

  private kafkaDeliveryOwnsPartition(
    lifecycle: KafkaConsumerLifecycle,
    payload: EachMessagePayload,
    assignmentEpoch: number,
  ): boolean {
    return (
      this.isLifecycleCurrent(lifecycle.generation) &&
      this.consumer === lifecycle.consumer &&
      lifecycle.assignmentEpoch === assignmentEpoch &&
      lifecycle.assignedPartitions.has(kafkaTopicPartitionKey(payload.topic, payload.partition))
    );
  }

  private async withKafkaHeartbeat(
    payload: EachMessagePayload,
    work: () => Promise<void>,
    abort?: AbortController,
  ): Promise<void> {
    const signal = abort?.signal;
    let stopped = false;
    let heartbeatInFlight: Promise<void> | undefined;
    const heartbeat = (beforeWork = false): Promise<void> => {
      if (stopped) return Promise.resolve();
      if (heartbeatInFlight) return heartbeatInFlight;
      heartbeatInFlight = (async () => {
        try {
          await payload.heartbeat();
        } catch (err) {
          if (!stopped && abort) {
            abort.abort(new Error('Kafka heartbeat failed during transcript decode'));
          } else if (beforeWork) {
            // No side effects have started: broker membership failure is fatal
            // even when KafkaJS has not emitted a local assignment change yet.
            throw new Error('Kafka heartbeat failed before message handling');
          } else if (!stopped) {
            // Once dispatch starts, retain the accepted turn's durable settlement.
            console.warn('dispatcher: kafka heartbeat failed during message handling', err);
          }
        } finally {
          heartbeatInFlight = undefined;
        }
      })();
      return heartbeatInFlight;
    };
    const timer = setInterval(() => {
      if (!heartbeatInFlight) void heartbeat();
    }, KAFKA_EACH_MESSAGE_HEARTBEAT_MS);
    timer.unref?.();
    let onAbort: (() => void) | undefined;
    const cancelled = new Promise<never>((_resolve, reject) => {
      onAbort = () => reject(signal?.reason);
      signal?.addEventListener('abort', onAbort, { once: true });
      if (signal?.aborted) onAbort();
    });
    try {
      await Promise.race([
        (async () => {
          await heartbeat(true);
          signal?.throwIfAborted();
          await work();
          if (abort) {
            // Decode is pure. Stop scheduling beats and join any membership
            // check it started before allowing the caller to dispatch effects.
            clearInterval(timer);
            await heartbeatInFlight;
            signal?.throwIfAborted();
          }
        })(),
        cancelled,
      ]);
    } finally {
      stopped = true;
      clearInterval(timer);
      if (onAbort) signal?.removeEventListener('abort', onAbort);
    }
  }

  private onMessage = async (
    delivery: EachMessagePayload,
    decoded?: Event | null,
  ): Promise<void> => {
    const { topic, message } = delivery;
    const sourceEventId = kafkaSourceEventId(delivery);
    const ignore = (): void => this.settleKafkaSourceEvent(sourceEventId);
    const lifecycleGeneration = this.lifecycleGeneration;
    this.assertLifecycleCurrent(lifecycleGeneration);
    const ids = matchSessionTopic(topic, this.topicPrefix, this.codec.encoding ?? 'raw');
    if (!ids) {
      ignore();
      return;
    }
    // Codec errors are infrastructure/wire failures, never ignorable business JSON.
    const event = decoded === undefined ? await this.codec.decode(message, ids) : decoded;
    this.assertLifecycleCurrent(lifecycleGeneration);
    if (!event) {
      ignore();
      return;
    }
    if (event.kind === 'session.archived' || event.kind === 'session.deleted') {
      await this.stopArchivedRunner(ids.workspaceId, ids.sessionId);
      ignore();
      return;
    }
    if (event.producedBy !== 'client') {
      ignore();
      return;
    }
    const kind = event.kind;
    if (!kind.startsWith('user.') || event.payload.length === 0) {
      ignore();
      return;
    }
    let payload: unknown;
    try {
      payload = JSON.parse(Buffer.from(event.payload).toString('utf8'));
    } catch {
      ignore();
      return;
    }

    await this.handleUserEvent(
      ids.workspaceId,
      ids.sessionId,
      kind,
      payload,
      {
        userEventId: sourceEventId,
        seq: Number(message.offset),
        producedAt: event.producedAt,
      },
      lifecycleGeneration,
    );
  };

  private onTranscriptEvent = async (event: Event): Promise<void> => {
    const lifecycleGeneration = this.lifecycleGeneration;
    this.assertLifecycleCurrent(lifecycleGeneration);
    if (event.kind === 'session.archived' || event.kind === 'session.deleted') {
      await this.stopArchivedRunner(event.workspaceId, event.sessionId);
      return;
    }
    if (event.producedBy !== 'client') return;
    const kind = event.kind;
    if (!kind.startsWith('user.')) return;
    if (event.payload.length === 0) return;
    let payload: unknown;
    try {
      payload = JSON.parse(Buffer.from(event.payload).toString('utf8'));
    } catch {
      return;
    }
    await this.handleUserEvent(
      event.workspaceId,
      event.sessionId,
      kind,
      payload,
      {
        userEventId: event.id,
        seq: event.seq,
        producedAt: event.producedAt,
      },
      lifecycleGeneration,
    );
  };

  private async stopArchivedRunner(workspaceId: string, sessionId: string): Promise<void> {
    const runnerKey = `${workspaceId}/${sessionId}`;
    const runner = this.runners.get(runnerKey);
    this.rejectTurnSourceEvents(
      runnerKey,
      new DroppedSessionEvent(
        `session archived during active turn for ${workspaceId}/${sessionId}`,
      ),
    );
    this.cancelIdleTimer(runnerKey);
    this.runnerSessionConfigKeys.delete(runnerKey);
    this.clearDeferredUserMessages(runnerKey);
    if (!runner) {
      this.clearActiveTurnUserEvents(runnerKey);
      this.clearCompletedUserEventCache(runnerKey);
      return;
    }
    this.runners.delete(runnerKey);
    await runner.stop('client.archived').catch((error) => {
      console.error(
        `dispatcher: runner stop failed for archived session ${workspaceId}/${sessionId}`,
        error,
      );
    });
    this.clearActiveTurnUserEvents(runnerKey);
    this.clearCompletedUserEventCache(runnerKey);
  }

  /** Do not make an in-flight source handler wait on stale runner teardown. */
  private stopStaleRunner(runner: SessionRunner): void {
    const stop = Promise.resolve().then(async () => await runner.stop('replica.shutting_down'));
    void stop.catch(() => {});
  }

  private async handleUserEvent(
    workspaceId: string,
    sessionId: string,
    kind: string,
    payload: unknown,
    source?: UserEventSource,
    lifecycleGeneration = this.lifecycleGeneration,
  ): Promise<void> {
    const runnerKey = `${workspaceId}/${sessionId}`;
    this.assertLifecycleCurrent(lifecycleGeneration);
    // Resolve ownership for every event, including cold interrupts. Do not cache:
    // session bindings can be updated independently of the immutable Agent harness.
    if (this.opts.registry) {
      const owner = await this.opts.registry.getExecutionOwner({ workspaceId, sessionId });
      this.assertLifecycleCurrent(lifecycleGeneration);
      if (owner !== 'harness-server') {
        const stale = this.runners.get(runnerKey);
        if (stale) {
          this.runners.delete(runnerKey);
          this.cancelIdleTimer(runnerKey);
          this.runnerSessionConfigKeys.delete(runnerKey);
          this.clearDeferredUserMessages(runnerKey);
          this.clearActiveTurnUserEvents(runnerKey);
          this.stopStaleRunner(stale);
        }
        this.clearPendingStatusRunningLatency(runnerKey, source?.userEventId);
        this.settleKafkaSourceEvent(source?.userEventId);
        return;
      }
    }
    if (kind === 'user.message') {
      this.enqueueStatusRunningLatency(runnerKey, source?.userEventId, source?.producedAt);
    }
    const companionSystemMessage = await this.findCompanionSystemMessage(
      workspaceId,
      sessionId,
      payload,
      source?.seq,
    );
    this.assertLifecycleCurrent(lifecycleGeneration);
    if (!this.runners.has(runnerKey)) {
      try {
        await this.recoverColdHarnessTurn(workspaceId, sessionId, lifecycleGeneration);
      } catch (error) {
        if (!(error instanceof RegistryInvalidRuntimeBindingError)) throw error;
        await this.completeUnstartedUserEvent(
          workspaceId,
          sessionId,
          source?.userEventId,
          lifecycleGeneration,
          error,
        );
        this.clearPendingStatusRunningLatency(runnerKey, source?.userEventId);
        this.settleKafkaSourceEvent(source?.userEventId);
        return;
      }
    }
    const completed = await this.isCompletedUserEvent(workspaceId, sessionId, source?.userEventId);
    this.assertLifecycleCurrent(lifecycleGeneration);
    if (completed) {
      this.clearPendingStatusRunningLatency(runnerKey, source?.userEventId);
      this.settleKafkaSourceEvent(source?.userEventId);
      return;
    }
    const harnessPayload = withoutCompanionSystemCorrelation(payload);
    this.cancelIdleTimer(runnerKey);
    let runner = this.runners.get(runnerKey);
    if (!runner) {
      if (kind === 'user.interrupt') {
        await this.completeUnstartedUserEvent(
          workspaceId,
          sessionId,
          source?.userEventId,
          lifecycleGeneration,
        );
        this.settleKafkaSourceEvent(source?.userEventId);
        return;
      }
      try {
        runner = await this.spawnRunnerWithMetrics(workspaceId, sessionId);
      } catch (e) {
        if (!this.isLifecycleCurrent(lifecycleGeneration)) {
          throw new DispatcherStoppingError({ cause: e });
        }
        if (e instanceof DroppedSessionEvent) {
          this.clearPendingStatusRunningLatency(runnerKey, source?.userEventId);
          this.settleKafkaSourceEvent(source?.userEventId);
          console.warn(
            `dispatcher: skipped user event for ${workspaceId}/${sessionId}: ${e.message}`,
          );
          return;
        }
        console.error(`dispatcher: failed to spawn runner for ${workspaceId}/${sessionId}`, e);
        if (e instanceof RegistryInvalidRuntimeBindingError) {
          await this.completeUnstartedUserEvent(
            workspaceId,
            sessionId,
            source?.userEventId,
            lifecycleGeneration,
            e,
          );
        } else if (!(e instanceof RequiredSetupError)) {
          throw e;
        }
        this.clearPendingStatusRunningLatency(runnerKey, source?.userEventId);
        this.settleKafkaSourceEvent(source?.userEventId);
        return;
      }
      if (!this.isLifecycleCurrent(lifecycleGeneration)) {
        this.clearPendingStatusRunningLatency(runnerKey, source?.userEventId);
        this.stopStaleRunner(runner);
        throw new DispatcherStoppingError();
      }
      this.runners.set(runnerKey, runner);
    } else {
      const refreshed = await this.respawnRunnerIfSessionConfigChanged(
        workspaceId,
        sessionId,
        runnerKey,
        kind,
        runner,
        lifecycleGeneration,
        source?.userEventId,
      );
      this.assertLifecycleCurrent(lifecycleGeneration);
      if (!refreshed) {
        this.clearPendingStatusRunningLatency(runnerKey, source?.userEventId);
        this.settleKafkaSourceEvent(source?.userEventId);
        return;
      }
      runner = refreshed;
      this.assertLifecycleCurrent(lifecycleGeneration);
      await this.markSessionRunning(workspaceId, sessionId, runner.sandboxId).catch((stateErr) => {
        console.error(`dispatcher: failed to mark ${sessionId} running`, stateErr);
      });
      this.assertLifecycleCurrent(lifecycleGeneration);
    }
    // start() can recover a receipt claimed after the initial cold inspection.
    if (await this.isCompletedUserEvent(workspaceId, sessionId, source?.userEventId)) {
      this.settleKafkaSourceEvent(source?.userEventId);
      return;
    }
    if (kind === 'user.message' && !runner.hasPendingRequiredAction()) {
      let preDrainResult: DeferredDrainResult;
      try {
        preDrainResult = await this.drainDeferredUserMessages(
          workspaceId,
          sessionId,
          runnerKey,
          runner,
          lifecycleGeneration,
        );
      } catch (cause) {
        if (!this.isLifecycleCurrent(lifecycleGeneration)) {
          throw new DispatcherStoppingError({ cause });
        }
        // The newer source event has not reached its acceptance hook. Store
        // failures while discovering or settling older deferred work must keep
        // that event retryable instead of letting Pulsar poison-message handling
        // eventually ACK and drop it.
        throw new DeferredDrainBlockedError(workspaceId, sessionId, { cause });
      }
      if (preDrainResult === 'failed') {
        this.assertLifecycleCurrent(lifecycleGeneration);
        // The current message has not reached its acceptance hook. A failure
        // while processing older deferred work must not ACK and lose it.
        throw new DeferredDrainBlockedError(workspaceId, sessionId);
      }
      if (preDrainResult === 'blocked') {
        this.assertLifecycleCurrent(lifecycleGeneration);
        await this.markSessionIdle(workspaceId, sessionId, runner.sandboxId).catch((stateErr) => {
          console.error(
            `dispatcher: failed to mark ${sessionId} idle after deferred drain exhaustion`,
            stateErr,
          );
        });
        this.assertLifecycleCurrent(lifecycleGeneration);
        this.scheduleIdleTimer(runnerKey, workspaceId, sessionId, runner);
        // This newer source event has not been accepted. Keep it retryable so
        // it cannot overtake the older durable deferred message.
        throw new DeferredDrainBlockedError(workspaceId, sessionId);
      }
    }
    harnessEventSubmitTotal.inc({ workspace_id: workspaceId, kind });
    try {
      this.assertLifecycleCurrent(lifecycleGeneration);
      const submitResult = await runner.submit(
        {
          ...(source?.userEventId ? { id: source.userEventId } : {}),
          kind,
          payload: harnessPayload,
          ...(companionSystemMessage === undefined
            ? {}
            : { systemMessage: companionSystemMessage.payload }),
        },
        this.acceptanceHooks(
          workspaceId,
          sessionId,
          kind,
          source?.userEventId,
          companionSystemMessage?.eventId,
          lifecycleGeneration,
        ),
      );
      this.assertLifecycleCurrent(lifecycleGeneration);
      if (kind === 'user.message' && submitResult === 'deferred') {
        await this.deferUserMessage(
          workspaceId,
          sessionId,
          runnerKey,
          source?.userEventId,
          payload,
          lifecycleGeneration,
        );
        this.assertLifecycleCurrent(lifecycleGeneration);
        await this.markSessionIdle(workspaceId, sessionId, runner.sandboxId).catch((stateErr) => {
          console.error(
            `dispatcher: failed to mark ${sessionId} idle after deferred user message`,
            stateErr,
          );
        });
        this.assertLifecycleCurrent(lifecycleGeneration);
        this.settleKafkaSourceEvent(source?.userEventId);
        return;
      }
    } catch (e) {
      if (
        !this.isLifecycleCurrent(lifecycleGeneration) ||
        e instanceof DispatcherStoppingError ||
        e instanceof SessionRunnerStoppingError
      ) {
        this.clearPendingStatusRunningLatency(runnerKey, source?.userEventId);
        throw new DispatcherStoppingError({ cause: e });
      }
      if (e instanceof DurableHarnessStateError) {
        this.handleRunnerEventPersistenceFailure(
          workspaceId,
          sessionId,
          lifecycleGeneration,
          runner,
          e,
        );
        throw new RetryableSessionEventError('Codex durable turn requires recovery', { cause: e });
      }
      if (e instanceof UserEventAcceptanceError || e instanceof UserEventDeferralError) {
        console.error(
          `dispatcher: durable user-event transition failed for ${workspaceId}/${sessionId} (${kind})`,
          e.cause,
        );
        await this.markSessionIdle(workspaceId, sessionId, runner.sandboxId).catch((stateErr) => {
          console.error(
            `dispatcher: failed to mark ${sessionId} idle after acceptance failure`,
            stateErr,
          );
        });
        this.assertLifecycleCurrent(lifecycleGeneration);
        // A required-action runner intentionally stays warm while it waits for
        // the matching client event. Other runners need their idle timer back:
        // handleUserEvent canceled it before attempting this delivery.
        if (!runner.hasPendingRequiredAction()) {
          this.scheduleIdleTimer(runnerKey, workspaceId, sessionId, runner);
        }
        // The harness contract guarantees that agent work has not started yet.
        // Let the event source retain/nack the original message for retry.
        throw e;
      }
      if (e instanceof GuardrailPolicyDeniedError) {
        await this.appendInternalEvent(
          workspaceId,
          sessionId,
          SessionEventKind.error,
          sessionErrorPayload({
            type: 'policy_denied',
            message: e.message,
            willRetry: false,
            extra: { reasons: [...e.reasons] },
          }),
        );
        await this.appendInternalTerminalEvent(
          workspaceId,
          sessionId,
          SessionEventKind.statusIdle,
          sessionIdlePayload('end_turn'),
          lifecycleGeneration,
        );
        this.settleTurnSourceEvents(runnerKey);
        // Request policy can reject before onAccepted registers the source.
        this.settleKafkaSourceEvent(source?.userEventId);
        await this.markSessionIdle(workspaceId, sessionId, runner.sandboxId).catch((stateErr) => {
          console.error(
            `dispatcher: failed to mark ${sessionId} idle after policy denial`,
            stateErr,
          );
        });
        this.scheduleIdleTimer(runnerKey, workspaceId, sessionId, runner);
        return;
      }
      if (e instanceof GuardrailUsageUnavailableError) {
        await this.appendInternalEvent(
          workspaceId,
          sessionId,
          SessionEventKind.error,
          sessionErrorPayload({
            type: 'guardrail_usage_unavailable',
            message: e.message,
            willRetry: false,
          }),
        );
        await this.appendInternalTerminalEvent(
          workspaceId,
          sessionId,
          SessionEventKind.statusIdle,
          sessionIdlePayload('retries_exhausted'),
          lifecycleGeneration,
        );
        this.settleTurnSourceEvents(runnerKey);
        // A pending-usage retry can also fail before accepting a new request.
        // Release Kafka only after the failed turn is durable so queued
        // retries and lifecycle sentinels can reach the retained runner.
        this.settleKafkaSourceEvent(source?.userEventId);
        await this.markSessionIdle(workspaceId, sessionId, runner.sandboxId).catch((stateErr) => {
          console.error(
            `dispatcher: failed to mark ${sessionId} idle after usage guardrail lock`,
            stateErr,
          );
        });
        // Keep the runner and its stable pending usage event IDs alive. A later
        // action retries those deltas before the harness can accept new work.
        return;
      }
      if (e instanceof UnappliedUserEventError) {
        console.warn(
          `dispatcher: runner could not apply ${kind} for ${workspaceId}/${sessionId}`,
          e,
        );
        await this.markSessionIdle(workspaceId, sessionId, runner.sandboxId).catch((stateErr) => {
          console.error(
            `dispatcher: failed to mark ${sessionId} idle after unapplied user event`,
            stateErr,
          );
        });
        this.assertLifecycleCurrent(lifecycleGeneration);
        // Non-retryable by construction: an UnappliedUserEventError means the client
        // event (e.g. a stray user.tool_confirmation / user.custom_tool_result) does
        // not match any pending required-action state on the runner, and that mismatch
        // cannot be resolved by redelivering the identical event. We therefore ack/commit
        // it here on both backends (matches the Kafka path, which never rethrew). Pulsar
        // previously rethrew to trigger a nack+redelivery retry, but since the mismatch
        // is permanent that produced an infinite redelivery loop — one session.error
        // appended per cycle — and the periodic consumer-rediscover teardown racing a
        // pending nack redelivery timer crashed the native pulsar client (SIGSEGV).
        await this.emitUnappliedUserEvent(workspaceId, sessionId, kind, e);
        this.assertLifecycleCurrent(lifecycleGeneration);
        if (!runner.hasPendingRequiredAction()) {
          this.scheduleIdleTimer(runnerKey, workspaceId, sessionId, runner);
        }
        this.settleKafkaSourceEvent(source?.userEventId);
        return;
      }
      console.error(
        `dispatcher: runner submit failed for ${workspaceId}/${sessionId} (${kind})`,
        e,
      );
      await this.handleRunnerSubmitFailure(
        workspaceId,
        sessionId,
        runnerKey,
        runner,
        lifecycleGeneration,
        source?.userEventId,
        e instanceof SettledHarnessFailureError,
      );
      this.assertLifecycleCurrent(lifecycleGeneration);
      return;
    }
    if (!runner.hasPendingRequiredAction()) {
      const drainSucceeded = await this.drainDeferredUserMessages(
        workspaceId,
        sessionId,
        runnerKey,
        runner,
        lifecycleGeneration,
      );
      this.assertLifecycleCurrent(lifecycleGeneration);
      if (drainSucceeded === 'failed') return;
    }
    this.assertLifecycleCurrent(lifecycleGeneration);
    await this.markSessionIdle(workspaceId, sessionId, runner.sandboxId).catch((stateErr) => {
      console.error(`dispatcher: failed to mark ${sessionId} idle after turn`, stateErr);
    });
    this.assertLifecycleCurrent(lifecycleGeneration);
    if (runner.hasPendingRequiredAction()) {
      return;
    }
    this.requestIdleTimerAfterDurableTerminal(runnerKey, workspaceId, sessionId, runner);
  }

  private async findCompanionSystemMessage(
    workspaceId: string,
    sessionId: string,
    payload: unknown,
    sourceSeq: number | undefined,
  ): Promise<CompanionSystemMessage | undefined> {
    const companionEventId =
      payload && typeof payload === 'object' && !Array.isArray(payload)
        ? (payload as Record<string, unknown>)[COMPANION_SYSTEM_EVENT_ID_FIELD]
        : undefined;
    if (typeof companionEventId !== 'string' || !companionEventId.startsWith('evt_')) {
      return undefined;
    }

    try {
      for await (const event of this.opts.store.read(workspaceId, sessionId, {
        fromCursor: sourceSeq === undefined ? '' : String(sourceSeq + 1),
        maxEvents: 0,
        subpath: '*',
      })) {
        // Some test/fallback stores do not enforce the cursor themselves.
        if (sourceSeq !== undefined && event.seq <= sourceSeq) continue;
        if (event.id !== companionEventId) continue;
        if (event.kind !== 'system.message' || event.producedBy !== 'client') {
          throw new Error(`correlated event ${companionEventId} is not a client system.message`);
        }
        try {
          return {
            eventId: event.id,
            payload: JSON.parse(Buffer.from(event.payload).toString('utf8')),
          };
        } catch {
          throw new Error(`correlated system.message ${companionEventId} has invalid JSON`);
        }
      }
      throw new Error(`correlated system.message ${companionEventId} was not found`);
    } catch (cause) {
      throw new CompanionSystemMessageReadError(workspaceId, sessionId, { cause });
    }
  }

  private async loadAppliedSystemMessages(
    workspaceId: string,
    sessionId: string,
  ): Promise<string[]> {
    const systemMessages = new Map<string, { seq: number; text: string }>();
    const processedEventIds = new Set<string>();
    try {
      for await (const event of this.opts.store.read(workspaceId, sessionId, {
        fromCursor: '',
        maxEvents: 0,
        subpath: '*',
      })) {
        if (event.producedBy === 'client' && event.kind === 'system.message') {
          const text = systemMessageText(parseJsonPayload(event.payload));
          if (text) systemMessages.set(event.id, { seq: event.seq, text });
          continue;
        }
        if (
          event.producedBy !== 'harness' ||
          event.kind !== InternalTranscriptEventKind.userEventProcessed
        ) {
          continue;
        }
        const marker = parseJsonPayload(event.payload);
        const eventId =
          marker && typeof marker === 'object' && !Array.isArray(marker)
            ? (marker as Record<string, unknown>)['user_event_id']
            : undefined;
        if (typeof eventId === 'string') processedEventIds.add(eventId);
      }
    } catch (cause) {
      throw new SystemMessageReplayReadError(workspaceId, sessionId, { cause });
    }
    return [...systemMessages.entries()]
      .filter(([eventId]) => processedEventIds.has(eventId))
      .sort((left, right) => left[1].seq - right[1].seq)
      .map(([, message]) => message.text);
  }

  private async respawnRunnerIfSessionConfigChanged(
    workspaceId: string,
    sessionId: string,
    runnerKey: string,
    kind: string,
    runner: SessionRunner,
    lifecycleGeneration: number,
    userEventId?: string,
  ): Promise<SessionRunner | null> {
    this.assertLifecycleCurrent(lifecycleGeneration);
    // UpdateSession overrides are guaranteed for the next user turn, not just
    // the next cold runner. If a runner is parked on a required-action gate,
    // keep it alive so the matching tool/custom result can still complete.
    if (kind !== 'user.message' || runner.hasPendingRequiredAction() || !this.opts.registry) {
      return runner;
    }
    let prepared: PreparedExecutionV2 | null = null;
    try {
      prepared = await this.opts.registry.prepareExecution({ workspaceId, sessionId });
      this.assertLifecycleCurrent(lifecycleGeneration);
      if (prepared) {
        prepared = validatePreparedExecution(workspaceId, sessionId, prepared);
      }
    } catch (e) {
      if (!this.isLifecycleCurrent(lifecycleGeneration) || e instanceof DispatcherStoppingError) {
        throw new DispatcherStoppingError({ cause: e });
      }
      console.error(`dispatcher: refresh prepareExecution ${workspaceId}/${sessionId} failed`, e);
      this.runners.delete(runnerKey);
      this.runnerSessionConfigKeys.delete(runnerKey);
      this.clearDeferredUserMessages(runnerKey);
      this.cancelIdleTimer(runnerKey);
      await runner.stop('session.updated').catch((stopErr) => {
        console.error(
          `dispatcher: runner stop failed for ${workspaceId}/${sessionId} after config refresh error`,
          stopErr,
        );
      });
      this.assertLifecycleCurrent(lifecycleGeneration);
      this.clearActiveTurnUserEvents(runnerKey);
      this.clearCompletedUserEventCache(runnerKey);
      if (e instanceof DroppedSessionEvent) return null;
      if (e instanceof RegistryInvalidRuntimeBindingError) {
        await this.completeUnstartedUserEvent(
          workspaceId,
          sessionId,
          userEventId,
          lifecycleGeneration,
          e,
        );
        return null;
      }
      await this.markSessionIdle(workspaceId, sessionId, null).catch((stateErr) => {
        console.error(
          `dispatcher: failed to mark ${sessionId} idle after config refresh error`,
          stateErr,
        );
      });
      this.assertLifecycleCurrent(lifecycleGeneration);
      // The source user event has not reached its acceptance hook. Keep it
      // retryable so Registry recovery can create a fresh runner; never let a
      // warm runner continue with a stale or unvalidated tenant snapshot.
      throw new SessionConfigRefreshError(workspaceId, sessionId, { cause: e });
    }
    if (!prepared) {
      this.runners.delete(runnerKey);
      this.runnerSessionConfigKeys.delete(runnerKey);
      this.clearDeferredUserMessages(runnerKey);
      await runner.stop('client.archived').catch((stopErr) => {
        console.error(
          `dispatcher: runner stop failed for deleted session ${workspaceId}/${sessionId}`,
          stopErr,
        );
      });
      this.assertLifecycleCurrent(lifecycleGeneration);
      this.clearActiveTurnUserEvents(runnerKey);
      this.clearCompletedUserEventCache(runnerKey);
      console.warn(
        `dispatcher: skipped user event for ${workspaceId}/${sessionId}: registry session not found`,
      );
      return null;
    }
    const nextKey = preparedExecutionRuntimeConfigKey(prepared);
    const currentKey = this.runnerSessionConfigKeys.get(runnerKey);
    if (currentKey === undefined || currentKey === nextKey) {
      // prepareExecution is the authoritative pre-turn snapshot. Refresh a
      // reused runner before submit() evaluates request guardrails. A changed
      // configuration starts a fresh runner with this same prepared state.
      runner.applyGuardrailUsageState(prepared.guardrail_state ?? {});
      if (currentKey === undefined) this.runnerSessionConfigKeys.set(runnerKey, nextKey);
      return runner;
    }

    this.runners.delete(runnerKey);
    this.runnerSessionConfigKeys.delete(runnerKey);
    await runner.stop('session.updated').catch((stopErr) => {
      console.error(
        `dispatcher: runner stop failed for ${workspaceId}/${sessionId} after session update`,
        stopErr,
      );
    });
    this.assertLifecycleCurrent(lifecycleGeneration);
    this.clearActiveTurnUserEvents(runnerKey);
    try {
      const fresh = await this.spawnRunnerWithMetrics(workspaceId, sessionId, prepared);
      this.assertLifecycleCurrent(lifecycleGeneration);
      this.runners.set(runnerKey, fresh);
      return fresh;
    } catch (e) {
      if (!this.isLifecycleCurrent(lifecycleGeneration) || e instanceof DispatcherStoppingError) {
        throw new DispatcherStoppingError({ cause: e });
      }
      if (e instanceof DroppedSessionEvent) {
        console.warn(
          `dispatcher: skipped user event for ${workspaceId}/${sessionId}: ${e.message}`,
        );
        return null;
      }
      console.error(`dispatcher: failed to respawn runner for ${workspaceId}/${sessionId}`, e);
      if (!(e instanceof RequiredSetupError)) throw e;
      await this.markSessionIdle(workspaceId, sessionId, null).catch((stateErr) => {
        console.error(
          `dispatcher: failed to mark ${sessionId} idle after session-update respawn error`,
          stateErr,
        );
      });
      this.assertLifecycleCurrent(lifecycleGeneration);
      return null;
    }
  }

  private async handleRunnerSubmitFailure(
    workspaceId: string,
    sessionId: string,
    runnerKey: string,
    runner: SessionRunner,
    lifecycleGeneration: number,
    sourceEventId?: string,
    terminalPersisted = false,
  ): Promise<void> {
    this.assertLifecycleCurrent(lifecycleGeneration);
    await runner.stop('replica.shutting_down').catch((stopErr) => {
      console.error(
        `dispatcher: runner stop failed for ${workspaceId}/${sessionId} after submit error`,
        stopErr,
      );
    });
    this.assertLifecycleCurrent(lifecycleGeneration);
    this.runners.delete(runnerKey);
    this.runnerSessionConfigKeys.delete(runnerKey);
    this.clearDeferredUserMessages(runnerKey);
    this.clearCompletedUserEventCache(runnerKey);
    this.cancelIdleTimer(runnerKey);
    if (!terminalPersisted) {
      // A hard submit failure (harness crash, query subprocess exit) was silent
      // before; surface it as session.error + a terminal session.status_idle so a
      // stream consumer sees the turn end. The session stays idle (recoverable on
      // the next event), not terminated.
      await this.appendInternalEvent(
        workspaceId,
        sessionId,
        SessionEventKind.error,
        sessionErrorPayload({
          type: 'runner_error',
          message: 'runner submit failed',
          willRetry: false,
        }),
      ).catch((appendErr) => {
        console.error(`dispatcher: failed to emit session.error for ${sessionId}`, appendErr);
      });
      this.assertLifecycleCurrent(lifecycleGeneration);
      try {
        await this.appendInternalTerminalEvent(
          workspaceId,
          sessionId,
          SessionEventKind.statusIdle,
          sessionIdlePayload('retries_exhausted'),
          lifecycleGeneration,
        );
      } catch (appendErr) {
        this.rejectTurnSourceEvents(runnerKey, appendErr);
        this.rejectKafkaSourceEvent(sourceEventId, appendErr);
        console.error(`dispatcher: failed to emit session.status_idle for ${sessionId}`);
        throw appendErr;
      }
    }
    this.settleTurnSourceEvents(runnerKey);
    this.settleKafkaSourceEvent(sourceEventId);
    this.assertLifecycleCurrent(lifecycleGeneration);
    await this.markSessionIdle(workspaceId, sessionId, null).catch((stateErr) => {
      console.error(`dispatcher: failed to mark ${sessionId} idle after submit error`, stateErr);
    });
    this.assertLifecycleCurrent(lifecycleGeneration);
  }

  private async deferUserMessage(
    workspaceId: string,
    sessionId: string,
    runnerKey: string,
    userEventId: string | undefined,
    payload: unknown,
    lifecycleGeneration: number,
  ): Promise<void> {
    const deferredUserEventId =
      userEventId && userEventId.length > 0 ? userEventId : `evt_${uuidv7()}`;
    // Durability precedes in-memory visibility. If this append fails, the
    // source handler rejects and no later confirmation can drain a queue item
    // that would disappear on process restart.
    await this.appendInternalEvent(workspaceId, sessionId, DEFERRED_USER_MESSAGE_KIND, {
      user_event_id: deferredUserEventId,
    }).catch((cause: unknown) => {
      throw new UserEventDeferralError({ cause });
    });
    this.assertLifecycleCurrent(lifecycleGeneration);
    let bySession = this.deferredUserMessagePayloads.get(runnerKey);
    if (!bySession) {
      bySession = new Map();
      this.deferredUserMessagePayloads.set(runnerKey, bySession);
    }
    bySession.set(deferredUserEventId, payload);
    const queue =
      this.deferredUserMessageQueues.get(runnerKey) ?? this.createDeferredQueue(runnerKey, true);
    queue.items.push({ userEventId: deferredUserEventId, payload });
  }

  private async drainDeferredUserMessages(
    workspaceId: string,
    sessionId: string,
    runnerKey: string,
    runner: SessionRunner,
    lifecycleGeneration: number,
  ): Promise<DeferredDrainResult> {
    const configuredMaxRetries = this.opts.deferredDrainMaxAcceptanceRetries ?? 3;
    const maxAcceptanceRetries = Number.isFinite(configuredMaxRetries)
      ? Math.max(0, Math.floor(configuredMaxRetries))
      : 3;
    while (!runner.hasPendingRequiredAction()) {
      this.assertLifecycleCurrent(lifecycleGeneration);
      const next = await this.nextDeferredUserMessage(workspaceId, sessionId, runnerKey);
      this.assertLifecycleCurrent(lifecycleGeneration);
      if (!next) return 'complete';
      // Receipt recovery may complete a deferred source without its submitted
      // marker, including after the bounded receipt has moved to a newer turn.
      const completed = await this.isCompletedUserEvent(workspaceId, sessionId, next.userEventId);
      this.assertLifecycleCurrent(lifecycleGeneration);
      if (completed) {
        this.markDeferredUserMessageSubmitted(runnerKey, next.userEventId);
        continue;
      }
      const refreshed = await this.refreshDeferredGuardrailUsageState(
        workspaceId,
        sessionId,
        runner,
        lifecycleGeneration,
      );
      this.assertLifecycleCurrent(lifecycleGeneration);
      if (!refreshed) {
        // The durable deferral marker remains authoritative. Restore the
        // in-memory FIFO position and fail closed until Registry can provide
        // the counters used by request-phase budgets.
        this.requeueDeferredUserMessage(runnerKey, next);
        return 'blocked';
      }
      let submitResult: Awaited<ReturnType<SessionRunner['submit']>>;
      let acceptanceRetries = 0;
      for (;;) {
        try {
          const companionSystemMessage = await this.findCompanionSystemMessage(
            workspaceId,
            sessionId,
            next.payload,
            undefined,
          );
          this.assertLifecycleCurrent(lifecycleGeneration);
          submitResult = await runner.submit(
            {
              id: next.userEventId,
              kind: 'user.message',
              payload: withoutCompanionSystemCorrelation(next.payload),
              ...(companionSystemMessage === undefined
                ? {}
                : { systemMessage: companionSystemMessage.payload }),
            },
            this.acceptanceHooks(
              workspaceId,
              sessionId,
              'user.message',
              next.userEventId,
              companionSystemMessage?.eventId,
              lifecycleGeneration,
            ),
          );
          this.assertLifecycleCurrent(lifecycleGeneration);
          break;
        } catch (e) {
          if (
            !this.isLifecycleCurrent(lifecycleGeneration) ||
            e instanceof DispatcherStoppingError ||
            e instanceof SessionRunnerStoppingError
          ) {
            throw new DispatcherStoppingError({ cause: e });
          }
          if (e instanceof CompanionSystemMessageReadError) throw e;
          if (e instanceof DurableHarnessStateError) {
            this.handleRunnerEventPersistenceFailure(
              workspaceId,
              sessionId,
              lifecycleGeneration,
              runner,
              e,
            );
            throw new RetryableSessionEventError('Codex durable turn requires recovery', {
              cause: e,
            });
          }
          if (!(e instanceof UserEventAcceptanceError)) {
            console.error(
              `dispatcher: deferred user message submit failed for ${workspaceId}/${sessionId}`,
              e,
            );
            await this.handleRunnerSubmitFailure(
              workspaceId,
              sessionId,
              runnerKey,
              runner,
              lifecycleGeneration,
              next.userEventId,
              e instanceof SettledHarnessFailureError,
            );
            return 'failed';
          }
          this.assertLifecycleCurrent(lifecycleGeneration);
          if (acceptanceRetries >= maxAcceptanceRetries) {
            this.requeueDeferredUserMessage(runnerKey, next);
            console.error(
              `dispatcher: deferred user message acceptance retries exhausted for ${workspaceId}/${sessionId}; leaving ${next.userEventId} queued`,
              e.cause,
            );
            return 'blocked';
          }
          acceptanceRetries += 1;
          console.error(
            `dispatcher: deferred user message acceptance failed for ${workspaceId}/${sessionId}; retrying ${acceptanceRetries}/${maxAcceptanceRetries}`,
            e.cause,
          );
          await this.waitForDeferredDrainRetry();
          this.assertLifecycleCurrent(lifecycleGeneration);
        }
      }
      if (submitResult === 'deferred') {
        this.requeueDeferredUserMessage(runnerKey, next);
        return 'complete';
      }
      await this.appendInternalEvent(workspaceId, sessionId, DEFERRED_USER_MESSAGE_SUBMITTED_KIND, {
        user_event_id: next.userEventId,
      });
      this.assertLifecycleCurrent(lifecycleGeneration);
      this.markDeferredUserMessageSubmitted(runnerKey, next.userEventId);
    }
    return 'complete';
  }

  private async refreshDeferredGuardrailUsageState(
    workspaceId: string,
    sessionId: string,
    runner: SessionRunner,
    lifecycleGeneration: number,
  ): Promise<boolean> {
    if (!this.opts.registry) return true;
    try {
      const prepared = await this.opts.registry.prepareExecution({ workspaceId, sessionId });
      this.assertLifecycleCurrent(lifecycleGeneration);
      if (!prepared) {
        console.warn(
          `dispatcher: deferred usage refresh skipped for deleted session ${workspaceId}/${sessionId}`,
        );
        return false;
      }
      const validated = validatePreparedExecution(workspaceId, sessionId, prepared);
      runner.applyGuardrailUsageState(validated.guardrail_state ?? {});
      return true;
    } catch (cause) {
      if (
        !this.isLifecycleCurrent(lifecycleGeneration) ||
        cause instanceof DispatcherStoppingError
      ) {
        throw new DispatcherStoppingError({ cause });
      }
      console.error(
        `dispatcher: deferred usage refresh failed for ${workspaceId}/${sessionId}; leaving user message queued`,
        cause,
      );
      return false;
    }
  }

  private async waitForDeferredDrainRetry(): Promise<void> {
    const delayMs = this.opts.deferredDrainRetryDelayMs ?? 1_000;
    if (delayMs <= 0) return;
    await new Promise<void>((resolve) => {
      const finish = (): void => {
        clearTimeout(timer);
        this.deferredDrainRetryWakeups.delete(finish);
        resolve();
      };
      this.deferredDrainRetryWakeups.add(finish);
      const timer = setTimeout(finish, delayMs);
      if (this.stopping) finish();
    });
  }

  private wakeDeferredDrainRetryWaiters(): void {
    for (const wake of Array.from(this.deferredDrainRetryWakeups)) wake();
  }

  private async nextDeferredUserMessage(
    workspaceId: string,
    sessionId: string,
    runnerKey: string,
  ): Promise<DeferredUserMessage | null> {
    const queue = await this.getDeferredUserMessageQueue(workspaceId, sessionId, runnerKey);
    const next = queue.items.shift();
    if (!next) {
      this.clearEmptyDeferredUserMessages(runnerKey);
      return null;
    }
    return next;
  }

  private createDeferredQueue(
    runnerKey: string,
    loadedFromTranscript: boolean,
  ): DeferredUserMessageQueue {
    const queue: DeferredUserMessageQueue = {
      loadedFromTranscript,
      items: [],
    };
    this.deferredUserMessageQueues.set(runnerKey, queue);
    return queue;
  }

  private async getDeferredUserMessageQueue(
    workspaceId: string,
    sessionId: string,
    runnerKey: string,
  ): Promise<DeferredUserMessageQueue> {
    const existing = this.deferredUserMessageQueues.get(runnerKey);
    if (existing?.loadedFromTranscript) return existing;

    const userMessages = new Map<string, unknown>();
    const deferred: Array<{ seq: number; userEventId: string }> = [];
    const submitted = new Set<string>();

    for await (const event of this.opts.store.read(workspaceId, sessionId, {
      fromCursor: '',
      maxEvents: 0,
      subpath: '*',
    })) {
      if (event.producedBy === 'client' && event.kind === 'user.message') {
        const payload = parseJsonPayload(event.payload);
        if (payload !== null) userMessages.set(event.id, payload);
        continue;
      }
      if (event.producedBy !== 'harness') continue;
      if (event.kind === DEFERRED_USER_MESSAGE_KIND) {
        const payload = parseJsonPayload(event.payload);
        const userEventId =
          payload && typeof payload === 'object'
            ? (payload as Record<string, unknown>)['user_event_id']
            : null;
        if (typeof userEventId === 'string') {
          deferred.push({ seq: event.seq, userEventId });
        }
        continue;
      }
      if (
        event.kind === DEFERRED_USER_MESSAGE_SUBMITTED_KIND ||
        event.kind === USER_EVENT_COMPLETED_KIND
      ) {
        const payload = parseJsonPayload(event.payload);
        const userEventId =
          payload && typeof payload === 'object'
            ? (payload as Record<string, unknown>)['user_event_id']
            : null;
        if (typeof userEventId === 'string') submitted.add(userEventId);
      }
    }

    deferred.sort((a, b) => a.seq - b.seq);
    const memoryPayloads = this.deferredUserMessagePayloads.get(runnerKey);
    const queue = this.createDeferredQueue(runnerKey, true);
    for (const item of deferred) {
      if (submitted.has(item.userEventId)) continue;
      const payload = memoryPayloads?.get(item.userEventId) ?? userMessages.get(item.userEventId);
      if (payload !== undefined) queue.items.push({ userEventId: item.userEventId, payload });
    }
    return queue;
  }

  private markDeferredUserMessageSubmitted(runnerKey: string, userEventId: string): void {
    const queue = this.deferredUserMessageQueues.get(runnerKey);
    if (queue) queue.items = queue.items.filter((item) => item.userEventId !== userEventId);
    this.deferredUserMessagePayloads.get(runnerKey)?.delete(userEventId);
    this.clearEmptyDeferredUserMessages(runnerKey);
  }

  private requeueDeferredUserMessage(runnerKey: string, message: DeferredUserMessage): void {
    const queue =
      this.deferredUserMessageQueues.get(runnerKey) ?? this.createDeferredQueue(runnerKey, true);
    if (!queue.items.some((item) => item.userEventId === message.userEventId)) {
      queue.items.unshift(message);
    }
  }

  private clearEmptyDeferredUserMessages(runnerKey: string): void {
    const payloads = this.deferredUserMessagePayloads.get(runnerKey);
    if (payloads?.size === 0) this.deferredUserMessagePayloads.delete(runnerKey);
    // Keep an empty loaded queue cached for the lifetime of the runner. New
    // user.message events pre-drain this queue to preserve ordering, and the
    // cache avoids rescanning the full transcript when no deferred items exist.
  }

  private clearDeferredUserMessages(runnerKey: string): void {
    this.deferredUserMessagePayloads.delete(runnerKey);
    this.deferredUserMessageQueues.delete(runnerKey);
    this.clearPendingStatusRunningLatency(runnerKey);
  }

  private async isCompletedUserEvent(
    workspaceId: string,
    sessionId: string,
    userEventId: string | undefined,
  ): Promise<boolean> {
    if (!userEventId) return false;
    const completed = await this.loadCompletedUserEventIds(workspaceId, sessionId);
    return completed.has(userEventId);
  }

  private async loadCompletedUserEventIds(
    workspaceId: string,
    sessionId: string,
  ): Promise<Set<string>> {
    const runnerKey = runnerKeyFor(workspaceId, sessionId);
    const loading = this.completedUserEventLoads.get(runnerKey);
    if (loading) return await loading;

    const existing = this.completedUserEventIds.get(runnerKey);
    if (existing) return existing;

    const completed = new Set<string>();
    this.completedUserEventIds.set(runnerKey, completed);
    const load = (async (): Promise<Set<string>> => {
      try {
        for await (const event of this.opts.store.read(workspaceId, sessionId, {
          fromCursor: '',
          maxEvents: 0,
          subpath: '*',
        })) {
          if (event.kind !== USER_EVENT_COMPLETED_KIND) continue;
          const payload = parseJsonPayload(event.payload);
          const userEventId =
            payload && typeof payload === 'object' && !Array.isArray(payload)
              ? (payload as Record<string, unknown>)['user_event_id']
              : undefined;
          if (typeof userEventId === 'string' && userEventId.length > 0) {
            completed.add(userEventId);
          }
        }
        return completed;
      } catch (cause) {
        if (this.completedUserEventIds.get(runnerKey) === completed) {
          this.completedUserEventIds.delete(runnerKey);
        }
        throw new CompletedUserEventReadError(workspaceId, sessionId, { cause });
      }
    })();
    this.completedUserEventLoads.set(runnerKey, load);
    try {
      return await load;
    } finally {
      if (this.completedUserEventLoads.get(runnerKey) === load) {
        this.completedUserEventLoads.delete(runnerKey);
      }
    }
  }

  private getOrCreateKafkaSourceSettlement(userEventId: string): KafkaSourceSettlement {
    const existing = this.kafkaSourceSettlements.get(userEventId);
    if (existing) return existing;
    let resolvePromise: () => void = () => {};
    let rejectPromise: (error: unknown) => void = () => {};
    let settled = false;
    const promise = new Promise<void>((resolve, reject) => {
      resolvePromise = resolve;
      rejectPromise = reject;
    });
    void promise.catch(() => {});
    const settlement: KafkaSourceSettlement = {
      promise,
      resolve: () => {
        if (settled) return;
        settled = true;
        resolvePromise();
      },
      reject: (error) => {
        if (settled) return;
        settled = true;
        rejectPromise(error);
      },
    };
    this.kafkaSourceSettlements.set(userEventId, settlement);
    return settlement;
  }

  private settleKafkaSourceEvent(userEventId: string | undefined): void {
    if (!userEventId) return;
    this.removeActiveTurnSettlementId(userEventId);
    const settlement = this.kafkaSourceSettlements.get(userEventId);
    if (!settlement) return;
    this.kafkaSourceSettlements.delete(userEventId);
    settlement.resolve();
  }

  private rejectKafkaSourceEvent(userEventId: string | undefined, error: unknown): void {
    if (!userEventId) return;
    this.removeActiveTurnSettlementId(userEventId);
    const settlement = this.kafkaSourceSettlements.get(userEventId);
    if (!settlement) return;
    this.kafkaSourceSettlements.delete(userEventId);
    settlement.reject(error);
  }

  private removeActiveTurnSettlementId(userEventId: string): void {
    for (const [runnerKey, active] of this.activeTurnSettlementIds) {
      const remaining = active.filter((candidate) => candidate !== userEventId);
      if (remaining.length === active.length) continue;
      if (remaining.length === 0) this.activeTurnSettlementIds.delete(runnerKey);
      else this.activeTurnSettlementIds.set(runnerKey, remaining);
    }
  }

  private recordAcceptedTurnSettlement(
    workspaceId: string,
    sessionId: string,
    userEventId: string,
    lifecycleGeneration: number,
  ): void {
    if (!this.isLifecycleCurrent(lifecycleGeneration)) return;
    const runnerKey = runnerKeyFor(workspaceId, sessionId);
    const active = this.activeTurnSettlementIds.get(runnerKey) ?? [];
    if (!active.includes(userEventId)) active.push(userEventId);
    this.activeTurnSettlementIds.set(runnerKey, active);
  }

  private settleTurnSourceEvents(runnerKey: string): void {
    const active = this.activeTurnSettlementIds.get(runnerKey) ?? [];
    this.activeTurnSettlementIds.delete(runnerKey);
    for (const userEventId of active) this.settleKafkaSourceEvent(userEventId);
  }

  private rejectTurnSourceEvents(runnerKey: string, error: unknown): void {
    const active = this.activeTurnSettlementIds.get(runnerKey) ?? [];
    this.activeTurnSettlementIds.delete(runnerKey);
    for (const userEventId of active) this.rejectKafkaSourceEvent(userEventId, error);
  }

  private handleRunnerEventPersistenceFailure(
    workspaceId: string,
    sessionId: string,
    lifecycleGeneration: number,
    originatingRunner: SessionRunner,
    error: unknown,
  ): void {
    const runnerKey = runnerKeyFor(workspaceId, sessionId);
    if (!this.isLifecycleCurrent(lifecycleGeneration)) return;
    const runner = this.runners.get(runnerKey);
    if (runner !== originatingRunner) return;
    this.rejectTurnSourceEvents(runnerKey, error);
    this.runners.delete(runnerKey);
    this.runnerSessionConfigKeys.delete(runnerKey);
    this.clearDeferredUserMessages(runnerKey);
    this.clearCompletedUserEventCache(runnerKey);
    this.cancelIdleTimer(runnerKey);
    // Do not await from inside runner's own event pump. stop() marks runner
    // non-accepting synchronously, then joins pump after this callback unwinds.
    void runner.stop('replica.shutting_down').catch((stopError) => {
      console.error(
        `dispatcher: runner stop failed after transcript persistence error for ${workspaceId}/${sessionId}`,
        stopError,
      );
    });
  }

  private rejectAllKafkaSourceSettlements(error: unknown): void {
    for (const [userEventId, settlement] of this.kafkaSourceSettlements) {
      this.kafkaSourceSettlements.delete(userEventId);
      settlement.reject(error);
    }
    this.activeTurnSettlementIds.clear();
  }

  private recordAcceptedTurnUserEvent(
    workspaceId: string,
    sessionId: string,
    userEventId: string,
    lifecycleGeneration: number,
  ): void {
    if (!this.isLifecycleCurrent(lifecycleGeneration)) return;
    const runnerKey = runnerKeyFor(workspaceId, sessionId);
    const active = this.activeTurnUserEventIds.get(runnerKey) ?? [];
    if (!active.includes(userEventId)) active.push(userEventId);
    this.activeTurnUserEventIds.set(runnerKey, active);
  }

  private completionForTerminal(
    workspaceId: string,
    sessionId: string,
    lifecycleGeneration = this.lifecycleGeneration,
    explicitSourceIds?: string[],
  ): TerminalCompletion | undefined {
    if (!this.isLifecycleCurrent(lifecycleGeneration)) return undefined;
    const runnerKey = runnerKeyFor(workspaceId, sessionId);
    const active = this.activeTurnUserEventIds.get(runnerKey);
    if (!active?.length && !explicitSourceIds?.length) return undefined;

    // Keep this snapshot in FIFO order until the terminal+marker append
    // succeeds. A failed append must leave every source retryable.
    const userEventIds = explicitSourceIds ? [...explicitSourceIds] : [active![0]!];
    return {
      events: userEventIds.map((userEventId) =>
        completedUserEventMarker(workspaceId, sessionId, userEventId),
      ),
      onPersisted: () => {
        // A terminal append can settle after stop() has retired all in-memory
        // bookkeeping. Its durable result is immutable; its callback must not
        // recreate the completion cache or active-turn queue.
        if (!this.isLifecycleCurrent(lifecycleGeneration)) return;
        const pending = this.activeTurnUserEventIds.get(runnerKey);
        if (pending) {
          for (const userEventId of userEventIds) {
            const index = pending.indexOf(userEventId);
            if (index >= 0) pending.splice(index, 1);
          }
          if (pending.length === 0) this.activeTurnUserEventIds.delete(runnerKey);
        }
        const completed = this.completedUserEventIds.get(runnerKey) ?? new Set<string>();
        for (const userEventId of userEventIds) completed.add(userEventId);
        this.completedUserEventIds.set(runnerKey, completed);
      },
    };
  }

  private clearActiveTurnUserEvents(runnerKey: string): void {
    this.activeTurnUserEventIds.delete(runnerKey);
  }

  private clearCompletedUserEventCache(runnerKey: string): void {
    this.completedUserEventIds.delete(runnerKey);
    this.completedUserEventLoads.delete(runnerKey);
  }

  private internalEvent(
    workspaceId: string,
    sessionId: string,
    kind: string,
    payload: unknown,
  ): Event {
    return {
      id: `evt_${uuidv7()}`,
      workspaceId,
      sessionId,
      subpath: '',
      seq: 0,
      producedAt: new Date().toISOString(),
      producedBy: 'harness',
      kind,
      payload: new TextEncoder().encode(JSON.stringify(payload)),
      idempotencyKey: '',
    };
  }

  private async appendInternalEvent(
    workspaceId: string,
    sessionId: string,
    kind: string,
    payload: unknown,
  ): Promise<void> {
    await this.opts.store.append(workspaceId, sessionId, [
      this.internalEvent(workspaceId, sessionId, kind, payload),
    ]);
  }

  private async appendInternalTerminalEvent(
    workspaceId: string,
    sessionId: string,
    kind: string,
    payload: unknown,
    lifecycleGeneration = this.lifecycleGeneration,
  ): Promise<void> {
    this.assertLifecycleCurrent(lifecycleGeneration);
    const completion = this.completionForTerminal(workspaceId, sessionId, lifecycleGeneration);
    await this.opts.store.append(workspaceId, sessionId, [
      this.internalEvent(workspaceId, sessionId, kind, payload),
      ...(completion?.events ?? []),
    ]);
    this.assertLifecycleCurrent(lifecycleGeneration);
    completion?.onPersisted();
  }

  private acceptanceHooks(
    workspaceId: string,
    sessionId: string,
    kind: string,
    userEventId: string | undefined,
    companionSystemEventId?: string,
    lifecycleGeneration = this.lifecycleGeneration,
  ): SubmitHooks | undefined {
    if (!userEventId) return undefined;
    let append: Promise<void> | null = null;
    return {
      onAccepted: () => {
        this.assertLifecycleCurrent(lifecycleGeneration);
        append ??= this.appendUserEventProcessedMarkers(
          workspaceId,
          sessionId,
          companionSystemEventId ? [userEventId, companionSystemEventId] : [userEventId],
        )
          .then(() => {
            this.assertLifecycleCurrent(lifecycleGeneration);
            this.recordAcceptedTurnSettlement(
              workspaceId,
              sessionId,
              userEventId,
              lifecycleGeneration,
            );
            if (isCompletionTrackedUserEvent(kind)) {
              this.recordAcceptedTurnUserEvent(
                workspaceId,
                sessionId,
                userEventId,
                lifecycleGeneration,
              );
            }
          })
          .catch((cause: unknown) => {
            if (
              !this.isLifecycleCurrent(lifecycleGeneration) ||
              cause instanceof DispatcherStoppingError
            ) {
              throw new DispatcherStoppingError({ cause });
            }
            throw new UserEventAcceptanceError(kind, { cause });
          });
        return append;
      },
    };
  }

  private enqueueStatusRunningLatency(
    runnerKey: string,
    userEventId: string | undefined,
    producedAt: string | undefined,
  ): void {
    if (!userEventId || !producedAt) return;
    const producedAtMs = Date.parse(producedAt);
    if (!Number.isFinite(producedAtMs)) return;
    const queue = this.pendingStatusRunningLatency.get(runnerKey) ?? [];
    // A source retry during this turn's pending/running lifecycle gets one sample.
    if (
      queue.some((pending) => pending.userEventId === userEventId) ||
      this.activeStatusRunningLatency
        .get(runnerKey)
        ?.some((pending) => pending.userEventId === userEventId)
    ) {
      return;
    }
    queue.push({ userEventId, producedAtMs });
    this.pendingStatusRunningLatency.set(runnerKey, queue);
  }

  private recordProducedEventToStatusRunning(runnerKey: string, lifecycleGeneration: number): void {
    if (!this.isLifecycleCurrent(lifecycleGeneration)) return;
    const queue = this.pendingStatusRunningLatency.get(runnerKey);
    const pending = queue?.shift();
    if (queue?.length === 0) this.pendingStatusRunningLatency.delete(runnerKey);
    if (!pending) return;
    const active = this.activeStatusRunningLatency.get(runnerKey) ?? [];
    active.push(pending);
    this.activeStatusRunningLatency.set(runnerKey, active);
    // Producer clocks can be ahead of this replica. Preserve a valid
    // non-negative Prometheus observation rather than emitting a negative lag.
    harnessAcceptedEventToStatusRunningSeconds.observe(
      Math.max(0, (Date.now() - pending.producedAtMs) / 1_000),
    );
  }

  private discardPendingStatusRunningLatency(runnerKey: string, lifecycleGeneration: number): void {
    if (!this.isLifecycleCurrent(lifecycleGeneration)) return;
    const active = this.activeStatusRunningLatency.get(runnerKey);
    if (active?.length) {
      active.shift();
      if (active.length === 0) this.activeStatusRunningLatency.delete(runnerKey);
      return;
    }
    const queue = this.pendingStatusRunningLatency.get(runnerKey);
    queue?.shift();
    if (queue?.length === 0) this.pendingStatusRunningLatency.delete(runnerKey);
  }

  private clearPendingStatusRunningLatency(runnerKey: string, userEventId?: string): void {
    if (userEventId === undefined) {
      this.pendingStatusRunningLatency.delete(runnerKey);
      this.activeStatusRunningLatency.delete(runnerKey);
      return;
    }
    const queue = this.pendingStatusRunningLatency.get(runnerKey);
    if (queue) {
      const remaining = queue.filter((pending) => pending.userEventId !== userEventId);
      if (remaining.length === 0) this.pendingStatusRunningLatency.delete(runnerKey);
      else this.pendingStatusRunningLatency.set(runnerKey, remaining);
    }
    const active = this.activeStatusRunningLatency.get(runnerKey);
    if (active) {
      const remainingActive = active.filter((pending) => pending.userEventId !== userEventId);
      if (remainingActive.length === 0) this.activeStatusRunningLatency.delete(runnerKey);
      else this.activeStatusRunningLatency.set(runnerKey, remainingActive);
    }
  }

  private async appendUserEventProcessedMarkers(
    workspaceId: string,
    sessionId: string,
    eventIds: string[],
  ): Promise<void> {
    const producedAt = new Date().toISOString();
    const events = eventIds.map(
      (eventId, index): Event => ({
        id: `evt_${uuidv7()}`,
        workspaceId,
        sessionId,
        subpath: '',
        seq: 0,
        producedAt,
        producedBy: 'harness',
        kind: InternalTranscriptEventKind.userEventProcessed,
        payload: new TextEncoder().encode(JSON.stringify(userEventProcessedPayload(eventId))),
        idempotencyKey: `${sessionId}:processed:${eventId}:${index}`,
      }),
    );
    await this.opts.store.append(workspaceId, sessionId, events);
  }

  private async emitUnappliedUserEvent(
    workspaceId: string,
    sessionId: string,
    kind: string,
    error: UnappliedUserEventError,
  ): Promise<void> {
    await this.appendInternalEvent(
      workspaceId,
      sessionId,
      SessionEventKind.error,
      sessionErrorPayload({
        type: 'unapplied_event',
        message: error.message,
        willRetry: false,
        extra: { user_event_kind: kind },
      }),
    ).catch((appendErr) => {
      console.error(
        `dispatcher: failed to emit session.error for unapplied ${kind} in ${workspaceId}/${sessionId}`,
        appendErr,
      );
    });
  }

  private async waitForKafkaShutdownOperation(
    operation: Promise<void>,
    shutdownDeadline: number,
  ): Promise<BoundedWaitOutcome> {
    const outcome = await waitForBounded(operation, shutdownDeadline - Date.now());
    if (outcome !== 'timed_out') return outcome;

    this.detachHandledLatePromise(operation);
    if (!this.shutdownGraceExpired) {
      this.shutdownGraceExpired = true;
      // Do not include a broker/client error here: readiness and shutdown
      // diagnostics can be exposed through platform logs.
      console.error('dispatcher: Kafka shutdown grace exceeded; continuing cleanup');
    }
    return outcome;
  }

  private async waitForShutdownOperation(
    operation: Promise<void>,
    shutdownDeadline: number,
    timedOutDiagnostic: string,
    failedDiagnostic: string,
  ): Promise<BoundedWaitOutcome> {
    const outcome = await waitForBounded(operation, shutdownDeadline - Date.now());
    if (outcome === 'timed_out') {
      this.detachHandledLatePromise(operation);
      console.error(timedOutDiagnostic);
    } else if (outcome === 'rejected') {
      // Source and runner errors can include broker URLs, SQL, prompts, or
      // tokens. Shutdown diagnostics deliberately carry no error object.
      console.error(failedDiagnostic);
    }
    return outcome;
  }

  private detachHandledLatePromise(operation: Promise<unknown>): void {
    void operation.catch(() => {});
  }

  private detachKafkaTransition(transition: Promise<void>): void {
    // A timed-out transition can still settle after stop() returns. Observe its
    // rejection and clear the ownership pointer now; its callbacks already
    // check `stopping` before changing consumer/readiness state.
    void transition.catch(() => {});
    if (this.kafkaTransitionPromise === transition) this.kafkaTransitionPromise = null;
  }

  private async disconnectKafkaConsumersDuringShutdown(shutdownDeadline: number): Promise<void> {
    const consumers: KafkaConsumerLifecycle[] = [];
    for (const lifecycle of [this.candidateConsumer, this.activeConsumer, this.transitionTarget]) {
      if (lifecycle && !consumers.includes(lifecycle)) consumers.push(lifecycle);
    }

    for (const lifecycle of consumers) {
      const operation =
        this.activeConsumer === lifecycle
          ? this.disconnectActiveKafkaConsumer(lifecycle)
          : this.disconnectKafkaCandidate(lifecycle);
      await this.waitForKafkaShutdownOperation(operation, shutdownDeadline);
    }
  }

  async stop(): Promise<void> {
    if (this.stopPromise) return await this.stopPromise;
    this.stopPromise = this.stopInternal();
    return await this.stopPromise;
  }

  private async stopInternal(): Promise<void> {
    const shutdownDeadline = Date.now() + this.shutdownGraceMs;
    this.stopping = true;
    // Advance before any await so stale source callbacks and runner callbacks
    // cannot repopulate maps cleared below, even if the dispatcher is started
    // again after this stop has returned.
    this.lifecycleGeneration += 1;
    const shutdownGeneration = this.lifecycleGeneration;
    this.rejectAllKafkaSourceSettlements(new DispatcherStoppingError());
    this.cancelKafkaGroupJoinWaiter();
    this.wakeDeferredDrainRetryWaiters();
    this.wakeKafkaRetryWaiters();
    if (this.rediscoverTimer !== null) {
      clearTimeout(this.rediscoverTimer);
      this.rediscoverTimer = null;
    }
    for (const timer of this.idleTimers.values()) {
      clearTimeout(timer);
    }
    this.idleTimers.clear();
    this.idleAfterTerminal.clear();

    const runners = Array.from(this.runners.entries());
    for (const [runnerKey] of runners) {
      this.runners.delete(runnerKey);
      this.runnerSessionConfigKeys.delete(runnerKey);
      this.clearDeferredUserMessages(runnerKey);
      this.clearActiveTurnUserEvents(runnerKey);
      this.clearCompletedUserEventCache(runnerKey);
    }
    this.runners.clear();
    this.runnerSessionConfigKeys.clear();
    this.activeTurnUserEventIds.clear();
    this.completedUserEventIds.clear();
    this.completedUserEventLoads.clear();
    this.pendingStatusRunningLatency.clear();
    this.activeStatusRunningLatency.clear();
    this.kafkaMessageWork.clear();
    this.kafkaSourceSettlements.clear();
    this.activeTurnSettlementIds.clear();
    this.kafkaRetryAttempts.clear();
    this.kafkaRetryWakeups.clear();

    // Begin source quiesce and runner cancellation together. A Postgres or
    // Pulsar source can be awaiting the in-flight handler; that handler must
    // observe runner cancellation before source.stop() waits for it to join.
    const sourceStop = this.opts.eventSource
      ? Promise.resolve().then(async () => await this.opts.eventSource!.stop())
      : null;
    this.eventSourceStarted = false;
    const runnerCleanup = this.cleanupRunnersDuringShutdown(runners, shutdownGeneration);
    const kafkaCleanup = this.opts.kafka
      ? this.stopKafkaConsumersDuringShutdown(shutdownDeadline)
      : null;

    const waits: Promise<unknown>[] = [
      this.waitForShutdownOperation(
        runnerCleanup,
        shutdownDeadline,
        'dispatcher: runner shutdown grace exceeded; continuing cleanup',
        'dispatcher: runner shutdown failed; continuing cleanup',
      ),
    ];
    if (sourceStop) {
      waits.push(
        this.waitForShutdownOperation(
          sourceStop,
          shutdownDeadline,
          'dispatcher: event source shutdown grace exceeded; continuing cleanup',
          'dispatcher: event source shutdown failed; continuing cleanup',
        ),
      );
    }
    if (kafkaCleanup) waits.push(kafkaCleanup);
    await Promise.all(waits);

    this.consumer = null;
    this.activeConsumer = null;
    this.candidateConsumer = null;
    this.transitionTarget = null;
    this.consumerReady = false;
    this.consumerCrashed = false;
    this.consumerRebalancing = false;
    replaceTopicSet(this.subscribedTopics, []);
    this.clearKafkaAssignment();
    this.updateKafkaMetrics();
  }

  private async stopKafkaConsumersDuringShutdown(shutdownDeadline: number): Promise<void> {
    if (this.opts.kafka) {
      const transition = this.kafkaTransitionPromise;
      if (transition) {
        const outcome = await this.waitForKafkaShutdownOperation(transition, shutdownDeadline);
        if (outcome === 'timed_out') {
          this.cancelKafkaGroupJoinWaiter();
          this.detachKafkaTransition(transition);
        }
      }
      // A timed-out transition can be blocked in connect(), or in KafkaJS's
      // disconnect wait for an in-flight eachMessage handler. Make one
      // best-effort, deadline-bounded pass over every tracked lifecycle before
      // runner cleanup; never let a stuck broker client hold shutdown forever.
      await this.disconnectKafkaConsumersDuringShutdown(shutdownDeadline);
    }
  }

  private async cleanupRunnersDuringShutdown(
    runners: Array<[string, SessionRunner]>,
    shutdownGeneration: number,
  ): Promise<void> {
    await Promise.all(
      runners.map(async ([runnerKey, runner]) => {
        try {
          await runner.stop('replica.shutting_down');
        } catch {
          console.error('dispatcher: runner shutdown failed; continuing cleanup');
        }
        if (!this.stopping || this.lifecycleGeneration !== shutdownGeneration) return;
        try {
          await this.markSessionIdle(
            workspaceIdFromRunnerKey(runnerKey),
            sessionIdFromRunnerKey(runnerKey),
            null,
          );
        } catch {
          console.error('dispatcher: runner shutdown state cleanup failed; continuing cleanup');
        }
      }),
    );
  }

  private scheduleIdleTimer(
    runnerKey: string,
    workspaceId: string,
    sessionId: string,
    runner: SessionRunner,
  ): void {
    this.idleAfterTerminal.delete(runnerKey);
    const timeoutMs = this.opts.sessionIdleTimeoutMs ?? 60_000;
    if (timeoutMs <= 0 || this.stopping) return;
    const timer = setTimeout(() => {
      void this.idleOutRunner(runnerKey, workspaceId, sessionId, runner);
    }, timeoutMs);
    this.idleTimers.set(runnerKey, timer);
  }

  private cancelIdleTimer(runnerKey: string): void {
    this.idleAfterTerminal.delete(runnerKey);
    const timer = this.idleTimers.get(runnerKey);
    if (!timer) return;
    clearTimeout(timer);
    this.idleTimers.delete(runnerKey);
  }

  private requestIdleTimerAfterDurableTerminal(
    runnerKey: string,
    workspaceId: string,
    sessionId: string,
    runner: SessionRunner,
  ): void {
    if (this.stopping || this.runners.get(runnerKey) !== runner) return;
    const pendingKafkaSettlement = (this.activeTurnSettlementIds.get(runnerKey) ?? []).some(
      (userEventId) => this.kafkaSourceSettlements.has(userEventId),
    );
    if (!pendingKafkaSettlement) {
      this.scheduleIdleTimer(runnerKey, workspaceId, sessionId, runner);
      return;
    }
    this.idleAfterTerminal.set(runnerKey, runner);
  }

  private recordRunnerTerminalPersisted(
    runnerKey: string,
    workspaceId: string,
    sessionId: string,
    runner: SessionRunner,
  ): void {
    if (this.idleAfterTerminal.get(runnerKey) !== runner) return;
    this.idleAfterTerminal.delete(runnerKey);
    if (this.runners.get(runnerKey) !== runner || runner.hasPendingRequiredAction()) return;
    this.scheduleIdleTimer(runnerKey, workspaceId, sessionId, runner);
  }

  private async idleOutRunner(
    runnerKey: string,
    workspaceId: string,
    sessionId: string,
    runner: SessionRunner,
  ): Promise<void> {
    const current = this.runners.get(runnerKey);
    if (current !== runner || this.stopping) return;
    this.idleTimers.delete(runnerKey);
    // Usage deltas that Registry has not acknowledged are part of the
    // fail-closed state. Do not discard them by idling out the warm runner.
    if (runner.hasPendingGuardrailUsage()) return;
    this.runners.delete(runnerKey);
    this.runnerSessionConfigKeys.delete(runnerKey);
    this.clearDeferredUserMessages(runnerKey);
    try {
      await runner.stop('idle.timeout');
    } catch (e) {
      console.error(`dispatcher: idle stop failed for ${workspaceId}/${sessionId}`, e);
    } finally {
      if (!this.runners.has(runnerKey)) {
        this.clearActiveTurnUserEvents(runnerKey);
        this.clearCompletedUserEventCache(runnerKey);
        await this.markSessionIdle(workspaceId, sessionId, null).catch((stateErr) => {
          console.error(`dispatcher: failed to mark ${sessionId} idle`, stateErr);
        });
      }
    }
  }

  private async markSessionRunning(
    workspaceId: string,
    sessionId: string,
    sandboxHandleId: string | null,
  ): Promise<void> {
    if (!this.opts.registry) return;
    await this.opts.registry.updateSessionStateInternal({
      workspaceId,
      sessionId,
      status: 'running',
      sandboxHandleId,
    });
  }

  private async markSessionIdle(
    workspaceId: string,
    sessionId: string,
    sandboxHandleId: string | null,
  ): Promise<void> {
    if (!this.opts.registry) return;
    await this.opts.registry.updateSessionStateInternal({
      workspaceId,
      sessionId,
      status: 'idle',
      sandboxHandleId,
    });
  }

  private async emitModelConfigSetupFailure(
    workspaceId: string,
    sessionId: string,
    error: unknown,
  ): Promise<void> {
    try {
      await this.emitSetupFailed(workspaceId, sessionId, 'model_config', error);
    } finally {
      await this.markSessionIdle(workspaceId, sessionId, null).catch((stateErr) => {
        console.error(
          `dispatcher: failed to mark ${sessionId} idle after model config error`,
          stateErr,
        );
      });
    }
  }

  /** Settle work that cannot start without passing it through a harness. */
  private async completeUnstartedUserEvent(
    workspaceId: string,
    sessionId: string,
    userEventId: string | undefined,
    lifecycleGeneration: number,
    error?: RegistryInvalidRuntimeBindingError,
  ): Promise<void> {
    this.assertLifecycleCurrent(lifecycleGeneration);
    try {
      // Persist Registry state before the completion marker. Otherwise a source
      // retry could see completion and skip a failed idle update forever.
      if (this.opts.registry) {
        const session = await this.opts.registry.updateSessionStateInternal({
          workspaceId,
          sessionId,
          status: 'idle',
          sandboxHandleId: null,
        });
        this.assertLifecycleCurrent(lifecycleGeneration);
        // Registry protects missing, archived and terminated sessions. Do not
        // publish a new idle result for a session it no longer allows to run.
        if (!session) return;
      }
      const events: Event[] = [];
      if (error) {
        const phase: SetupFailurePhase =
          error.resourceType === 'model_config' ? 'model_config' : 'execution_preparation';
        events.push(
          this.internalEvent(
            workspaceId,
            sessionId,
            SessionEventKind.error,
            sessionErrorPayload({
              type: 'setup_failed',
              message: errorMessage(error),
              willRetry: false,
              extra: { phase },
            }),
          ),
        );
      }
      events.push(
        this.internalEvent(
          workspaceId,
          sessionId,
          SessionEventKind.statusIdle,
          sessionIdlePayload(error ? 'retries_exhausted' : 'end_turn'),
        ),
      );
      if (userEventId) {
        events.unshift(
          this.internalEvent(
            workspaceId,
            sessionId,
            InternalTranscriptEventKind.userEventProcessed,
            userEventProcessedPayload(userEventId),
          ),
        );
        // Stable IDs locate committed outcomes after a crash or lost response.
        for (const event of events) {
          const uuid = uuidv5(
            `${workspaceId}\0${sessionId}\0${userEventId}\0${event.kind}`,
            USER_EVENT_COMPLETED_NAMESPACE,
          );
          event.id = `evt_${uuid}`;
          event.idempotencyKey = `unstarted-user-event:${uuid}`;
        }
        events.push(completedUserEventMarker(workspaceId, sessionId, userEventId));
      }
      const missingIds = new Set(events.map(({ id }) => id));
      if (userEventId) {
        // Kafka/Pulsar event-ID caches do not survive client restarts. Do not
        // rewrite committed identities with new timestamps or error details.
        for await (const persisted of this.opts.store.read(workspaceId, sessionId, {
          fromCursor: '',
          maxEvents: 0,
          subpath: '*',
        })) {
          missingIds.delete(persisted.id);
        }
        this.assertLifecycleCurrent(lifecycleGeneration);
      }
      // Publish a durable prefix: acceptance, error (if any), idle, completion.
      // A multi-event Pulsar append can commit later sends when an earlier one
      // fails, so each prerequisite must succeed before publishing the next.
      for (const event of events) {
        if (!missingIds.has(event.id)) continue;
        await this.opts.store.append(workspaceId, sessionId, [event]);
        this.assertLifecycleCurrent(lifecycleGeneration);
      }
      if (userEventId) {
        const runnerKey = runnerKeyFor(workspaceId, sessionId);
        // Warm runner teardown invalidates the cache. Leave it unloaded so
        // the next delivery also discovers completion markers for older turns.
        this.completedUserEventIds.get(runnerKey)?.add(userEventId);
      }
    } catch (cause) {
      if (
        !this.isLifecycleCurrent(lifecycleGeneration) ||
        cause instanceof DispatcherStoppingError
      ) {
        throw new DispatcherStoppingError({ cause });
      }
      // The append may have committed before its response failed. Rebuild
      // completion from the transcript before attempting preparation again.
      this.clearCompletedUserEventCache(runnerKeyFor(workspaceId, sessionId));
      throw new SessionEventBarrierError(
        `failed to persist unstarted user event for ${workspaceId}/${sessionId}`,
        async () => {
          await this.completeUnstartedUserEvent(
            workspaceId,
            sessionId,
            userEventId,
            lifecycleGeneration,
            error,
          );
          this.clearPendingStatusRunningLatency(runnerKeyFor(workspaceId, sessionId), userEventId);
          this.settleKafkaSourceEvent(userEventId);
        },
        { cause },
      );
    }
  }

  private async spawnRunnerWithMetrics(
    workspaceId: string,
    sessionId: string,
    preparedInput?: PreparedExecutionV2,
  ): Promise<SessionRunner> {
    harnessRunnerSpawnAttemptsTotal.inc();
    try {
      return await this.spawnRunner(workspaceId, sessionId, preparedInput);
    } catch (error) {
      if (!(error instanceof DroppedSessionEvent)) harnessRunnerSpawnFailuresTotal.inc();
      throw error;
    }
  }

  private async spawnRunner(
    workspaceId: string,
    sessionId: string,
    preparedInput?: PreparedExecutionV2,
  ): Promise<SessionRunner> {
    const lifecycleGeneration = this.lifecycleGeneration;
    this.assertLifecycleCurrent(lifecycleGeneration);
    const appliedSystemMessages = await this.loadAppliedSystemMessages(workspaceId, sessionId);

    // Registry session fetch — used by both the MCP-rewrite path and the Phase
    // 5+ sandbox/resource-materialization path. When a registry is configured,
    // a missing session means the Kafka/Pulsar subscription has picked up a
    // stale or cross-workspace event; skip it instead of starting a chat-only
    // runner with no agent snapshot.
    //
    // Per Anthropic Managed Agents semantics, credentials bind to MCP server
    // URLs via `Credential.auth.mcp_server_url`. Prepared credentials supply an
    // initial header hint for policy/exact static overrides. Registry-backed
    // wildcard dispatch resolves the live binding on every request, so this
    // startup map never becomes credential authority.
    let prepared: PreparedExecutionV2 | null = preparedInput ?? null;
    let session: SessionRecord | null = null;
    let agent: AgentRecord | null = null;
    let agentSnapshot: RuntimeAgentSnapshot = {};
    let mcpServers: Record<string, RewrittenServer> | undefined;
    let mcpJwtProvider: SessionJwtProvider | undefined;
    let remoteMcpToolsets: RemoteMcpToolsetPolicy[] | undefined;
    if (this.opts.registry) {
      if (!prepared) {
        try {
          prepared = await this.opts.registry.prepareExecution({ workspaceId, sessionId });
        } catch (e) {
          console.error(`prepareExecution ${workspaceId}/${sessionId} failed`, e);
          if (e instanceof RegistryInvalidRuntimeBindingError) throw e;
          throw new SessionPreparationError(workspaceId, sessionId, { cause: e });
        }
      }
      if (!prepared) {
        throw new DroppedSessionEvent('registry session not found');
      }
      prepared = validatePreparedExecution(workspaceId, sessionId, prepared);
      session = prepared.session;
      agent = applySessionAgentOverrides(prepared.primary_agent, session);
      agentSnapshot = this.buildRuntimeAgentSnapshot(agent, prepared);
    }
    // An unreadable harness annotation is a SETUP FAILURE, not a default. Falling
    // back to claude_agent_sdk/separate silently ran the agent on a different
    // harness, in a different topology, with a different tool surface than the
    // operator pinned — evidenced only by a stderr line no API caller ever sees.
    const annotation = resolveHarnessAnnotation(agent?.metadata ?? undefined);
    if ('error' in annotation) {
      const setupError = new RequiredSetupError(
        'harness_selection',
        new Error(
          `invalid harness annotation on agent ${agent?.id ?? 'unknown'}: ${annotation.error}`,
        ),
      );
      await this.failSessionSetup(workspaceId, sessionId, setupError);
      throw setupError;
    }
    const selection = annotation;
    const capabilities = resolveHarnessCapabilities(selection);
    const sessionSkills = prepared ? collectPreparedExecutionSkills(prepared) : [];
    try {
      if (prepared && agent) {
        assertAgentsSkillsReadable([agent, ...prepared.subagents], selection.mode);
      }
      if (sessionSkills.length > 0 && !this.opts.sandboxRuntime) {
        throw new RequiredSetupError(
          'skill_setup',
          new Error('sessions with skills require a sandbox runtime'),
        );
      }
      if (sessionSkills.length > 0 && !this.opts.skillStore) {
        throw new RequiredSetupError(
          'skill_setup',
          new Error('sessions with skills require SkillStore'),
        );
      }
    } catch (error) {
      const setupError =
        error instanceof RequiredSetupError ? error : new RequiredSetupError('skill_setup', error);
      await this.failSessionSetup(workspaceId, sessionId, setupError);
      throw setupError;
    }
    if (capabilities.modelControls === 'claude') {
      try {
        resolveClaudeSessionSpeed(agentSnapshot);
      } catch (e) {
        await this.emitModelConfigSetupFailure(workspaceId, sessionId, e);
        throw new RequiredSetupError('model_config', e);
      }
    }
    // Harness selection sits OUTSIDE every other try in this method, so anything
    // it throws used to escape `spawnRunner` entirely — skipping `emitSetupFailed`
    // and `markSessionIdle`, surfacing as a bare console.error, and wedging the
    // session with no `setup_failed` and no idle transition for the client.
    let harness: AgentHarness;
    try {
      if (selection.mode === 'colocated' && !this.opts.harnessFactory) {
        assertHarnessServerCanRunColocated(selection.harness);
      }
      harness = selectHarness({
        override: this.opts.harnessFactory
          ? () =>
              this.opts.harnessFactory!(
                workspaceId,
                sessionId,
                new ClaudeAgentSdkAdapter(this.opts.store, workspaceId),
              )
          : undefined,
        selection,
        providers: this.harnessProviders,
        context: { workspaceId, sessionId, session, modelProvider: agentSnapshot.model_provider },
      });
    } catch (error) {
      const setupError =
        error instanceof RequiredSetupError
          ? error
          : new RequiredSetupError('harness_selection', error);
      await this.failSessionSetup(workspaceId, sessionId, setupError);
      throw setupError;
    }

    // MCP rewrite + JWT mint. Skip cleanly when the gateway isn't configured
    // (chat-only / test paths). Capabilities select adapters that consume the
    // host-managed MCP map, including Codex with its SDK worker in the sandbox.
    if (
      capabilities.needsMcpRewrite &&
      session &&
      prepared &&
      agent &&
      this.opts.registry &&
      this.opts.gatewayMcpUrl
    ) {
      try {
        {
          const serverNames = agent.mcp_servers.map((s) => s.name);
          const enabledMcpServerNames = new Set(agentSnapshot.allowed_mcp_server_names ?? []);
          const sessionVaultIds = session.vault_ids ?? [];

          const credentials = prepared.vault_credentials;

          // Build URL→credential_id map. Registry order follows
          // session.vault_ids[] then credential created_at/id; first match wins.
          const credentialByUrl = new Map<string, string>();
          for (const c of credentials) {
            // environment_variable creds are not MCP-server bound (no url);
            // their sandbox env-var injection is handled separately.
            if (!c.mcp_server_url) continue;
            if (!credentialByUrl.has(c.mcp_server_url)) {
              credentialByUrl.set(c.mcp_server_url, c.credential_id);
            }
          }
          if (serverNames.length > 0) {
            // Registry derives startup credential metadata from the scoped
            // session; the harness never supplies credential ids as authority.
            const registry = this.opts.registry;
            mcpJwtProvider = new SessionJwtProvider((signal) =>
              registry.mintSessionJwt(workspaceId, sessionId, serverNames, sessionVaultIds, signal),
            );
            const minted = await mcpJwtProvider.getValidToken();
            mcpServers = rewriteMcpServers(
              agent.mcp_servers,
              sessionId,
              this.opts.gatewayMcpUrl,
              minted.token,
              credentialByUrl,
            );
            if (enabledMcpServerNames.size > 0) {
              remoteMcpToolsets = [...enabledMcpServerNames].map((serverName) => ({
                serverName,
                permissionPolicy:
                  agentSnapshot.tool_permission_policies?.[`mcp__${serverName}__*`] ?? 'always_ask',
              }));
            }
          }
        }
      } catch (e) {
        // Soft-fail: log and continue without MCP. Chat-only sessions still
        // work, and we don't want a registry hiccup to nuke a live turn.
        console.error('mcp rewrite failed', e);
        mcpJwtProvider?.close();
        mcpJwtProvider = undefined;
        mcpServers = undefined;
        remoteMcpToolsets = undefined;
      }
    }

    // Acquire one sandbox for either topology. `separate` uses it as the tool
    // executor; `colocated` also boots the harness process inside the same
    // filesystem. Session resources therefore share the exact mount and
    // teardown lifecycle in both modes.
    let sandbox: SandboxHandle | undefined;
    let agentSandbox: SandboxHandle | undefined;
    let tools: ToolDefinition[] | undefined;
    let mounts: ActiveMount[] | undefined;
    let resourceMountEvents: Event[] = [];
    let outputMount: OutputMountHandle | undefined;
    let creds: SessionS3Creds | undefined;
    let credsMintError: unknown | undefined;
    let memoryWatcher: MemoryVersionWatcher | undefined;
    let setupPhase: SetupFailurePhase = 'sandbox_setup';
    /**
     * Cleanup closure that rm -rf's the host-side per-session git work dir at
     * runner stop. Set when the github_repository setup branch succeeds; left
     * undefined when there are no repo resources. A failed repo setup releases
     * the work dir in its own catch and leaves a no-op here, so the outer
     * setup-failure path does not release it twice. Threaded into
     * `RunnerInput.workDirReleases` so the runner runs it between the
     * memory-watcher stop and the output indexer.
     */
    let repoCleanupReleases: (() => Promise<void>) | undefined;
    if (
      this.opts.sandboxRuntime &&
      session &&
      // Preserve the existing separate-mode dependency gate: historically a
      // runtime without a FileStore stayed sandbox-less. In-sandbox mode must
      // still acquire its harness sandbox, including for memory/repo-only
      // sessions that do not need a FileStore.
      (selection.mode === 'colocated' || this.opts.fileStore || sessionSkills.length > 0)
    ) {
      const runtime = this.opts.sandboxRuntime;
      const fileStore = this.opts.fileStore;
      const skillStore = this.opts.skillStore;
      const resources = prepared?.resources ?? session.resources ?? [];
      const fileResources = resources.filter((r) => r.type === 'file');
      const memoryStoreGrants = buildMemoryStoreGrants(resources);
      const repoResources = resources.filter((r) => r.type === 'github_repository');
      const repoUrls = repoResources
        .map((r) => (r.repo_ref as { url?: string } | null)?.url)
        .filter((url): url is string => typeof url === 'string' && url.length > 0);
      let inSandboxGitCredsToken: string | undefined;
      let inSandboxWritePolicy: ReturnType<typeof buildSandboxWritePolicy> | undefined;
      const generationId = `run_${uuidv7()}`;

      // Mint per-session creds BEFORE sandbox acquire so
      // that downstream FUSE mounts (output capture + per-resource memory_fuse)
      // see scoped credentials. The failure remains soft until we know whether
      // this runtime actually needs FUSE credentials for requested setup.
      if (this.opts.credsMinter) {
        try {
          creds = await this.opts.credsMinter.mint({
            workspaceId,
            sessionId,
            generationId,
            memoryStores: memoryStoreGrants,
          });
        } catch (e) {
          credsMintError = e;
          console.error('credsMinter.mint failed', e);
        }
      }

      let environmentSpec: EnvironmentSpec = {};
      try {
        environmentSpec = buildEnvironmentSpec(
          prepared?.environment ?? null,
          selection,
          this.opts.defaultImageOverrides,
        );
      } catch (e) {
        // A colocated harness that resolves to no image cannot boot a sandbox.
        // Surface it to the client rather than letting it escape the spawn.
        const setupError = new RequiredSetupError('sandbox_setup', e);
        await this.failSessionSetup(workspaceId, sessionId, setupError);
        throw setupError;
      }
      const outputCaptureConfigured =
        !!this.opts.s3Bucket && !!this.opts.s3Endpoint && this.opts.s3KeyPrefix !== undefined;

      if (selection.mode === 'colocated') {
        // The in-sandbox process receives these before it starts. Its provider
        // uses the output directory to steer downloadable artifacts into the
        // runner's local index path.
        if (this.opts.registry) {
          environmentSpec.harnessEnv = {
            ...environmentSpec.harnessEnv,
            ORCA_OUTPUT_CAPTURE_DIRECTORY: OUTPUT_CAPTURE_DIRECTORY,
          };
        }
        if (this.opts.registry && this.opts.gatewayLlmUrl) {
          try {
            const minted = await this.opts.registry.mintLlmGatewayJwt(workspaceId, sessionId);
            const llmEnv = buildLlmGatewayEnv({
              baseUrl: this.opts.gatewayLlmUrl,
              token: minted.token,
              sessionId,
              ...(agentSnapshot.model_id ? { model: agentSnapshot.model_id } : {}),
            });
            environmentSpec.harnessEnv = { ...environmentSpec.harnessEnv, ...llmEnv };
          } catch (e) {
            console.error(`dispatcher: mintLlmGatewayJwt failed for ${sessionId}`, e);
          }
        }
      }

      try {
        setupPhase = sessionSkills.length > 0 ? 'skill_setup' : 'write_policy';
        assertSandboxEnvironmentTrust({
          mode: selection.mode,
          image: prepared?.environment?.image,
          packages: prepared?.environment?.packages,
        });

        if (selection.mode === 'colocated') {
          setupPhase = 'write_policy';
          inSandboxWritePolicy = buildSandboxWritePolicy(
            resources.map((resource) => ({
              path: resource.mount_path,
              kind: resource.type,
              access: normalizeWritePolicyAccess(resource.access),
            })),
            {
              includeSkillsRoot: sessionSkills.length > 0,
              networkAllowedDomains:
                repoUrls.length > 0 && this.opts.gitCredsPublicUrl
                  ? networkAllowedDomainsFromUrls([...repoUrls, this.opts.gitCredsPublicUrl])
                  : [],
            },
          );
          environmentSpec.harnessEnv = {
            ...environmentSpec.harnessEnv,
            [SANDBOX_WRITE_POLICY_ENV]: encodeSandboxWritePolicy(inSandboxWritePolicy),
          };
        }

        if (selection.mode === 'colocated' && repoResources.length > 0) {
          setupPhase = 'repo_setup';
          if (!this.opts.registry || !this.opts.gitCredsPublicUrl) {
            throw new RequiredSetupError(
              'repo_setup',
              new Error('github_repository resources require registry and GIT_CREDS_PUBLIC_URL'),
            );
          }
          const gitJwt = await this.opts.registry.mintGitCredsJwt({
            workspaceId,
            sessionId,
            repoUrls,
          });
          inSandboxGitCredsToken = gitJwt.token;
          environmentSpec.harnessEnv = {
            ...environmentSpec.harnessEnv,
            ORCA_GIT_CREDS_URL: this.opts.gitCredsPublicUrl,
            ORCA_GIT_CREDS_TOKEN: gitJwt.token,
          };
        }

        setupPhase = 'sandbox_setup';
        sandbox = await runtime.acquire(environmentSpec);

        // This must be the first sandbox-side setup operation. Remote
        // image-backed runtimes prove that every future materialization root
        // is canonical and contains no pre-existing mount before trusted bytes
        // or FUSE mounts are placed there.
        try {
          setupPhase = 'write_policy';
          if (!sandbox.prepareFilesystemRoots) {
            throw new Error(
              `${sandboxRuntimeLabel(runtime)} does not support filesystem-root preflight`,
            );
          }
          await sandbox.prepareFilesystemRoots([
            OUTPUT_WRITABLE_PATH,
            ...resources.map((resource) => resource.mount_path),
            ...(sessionSkills.length > 0 ? [SKILLS_ROOT] : []),
          ]);
        } catch (e) {
          throw new RequiredSetupError('write_policy', e);
        }

        // The separate-mode write policy always binds and chdirs to the
        // session output root, even when S3 capture/indexing is disabled.
        // Create that scratch root as a policy precondition instead of
        // relying on the optional output-mount branch below.
        try {
          setupPhase = 'write_policy';
          await ensureSessionOutputRoot(sandbox);
        } catch (e) {
          throw new RequiredSetupError('write_policy', e);
        }

        // Mount a fresh runner-generation output prefix BEFORE
        // materializing resources so the agent's `write` tool can land bytes
        // straight at `/mnt/session/outputs/`. The InMemory path is a single
        // `mkdir`; the FUSE path issues an `s3fs` mount via `runPrivileged`
        // that needs the per-session creds.
        //
        const supportsFuse = runtime.capabilities.supportsFuse;
        // In-sandbox sessions without object-store wiring still use their local
        // output directory + Registry upload path. When S3 is configured, both
        // topologies use verified runtime capability: FUSE when available,
        // local indexing otherwise.
        const forceLocalOutputs = selection.mode === 'colocated' && !outputCaptureConfigured;
        if (outputCaptureConfigured && supportsFuse && !forceLocalOutputs && creds === undefined) {
          throw new RequiredSetupError(
            'output_mount',
            credsMintError ?? new Error('output capture requires per-session S3 credentials'),
          );
        }
        if (outputCaptureConfigured || (forceLocalOutputs && this.opts.registry)) {
          try {
            setupPhase = 'output_mount';
            const mountInput: Parameters<typeof mountSessionOutputs>[0] = {
              sandbox,
              workspaceId,
              sessionId,
              generationId,
              supportsFuse,
            };
            if (forceLocalOutputs) mountInput.forceLocal = true;
            if (this.opts.s3Bucket !== undefined) mountInput.bucket = this.opts.s3Bucket;
            if (this.opts.s3Endpoint !== undefined) mountInput.endpoint = this.opts.s3Endpoint;
            if (this.opts.s3KeyPrefix !== undefined) {
              mountInput.outputsRoot = this.opts.s3KeyPrefix;
            }
            if (creds !== undefined) mountInput.creds = creds;
            if (this.opts.s3Region !== undefined) mountInput.region = this.opts.s3Region;
            if (this.opts.s3ForcePathStyle !== undefined) {
              mountInput.forcePathStyle = this.opts.s3ForcePathStyle;
            }
            outputMount = await mountSessionOutputs(mountInput);
          } catch (e) {
            throw new RequiredSetupError('output_mount', e);
          }
        }

        if (fileResources.length > 0) {
          if (!fileStore) {
            throw new RequiredSetupError(
              'resource_mount',
              new Error('file resources require fileStore'),
            );
          }
          // Multi-workspace isolation: file resources are ALWAYS materialized
          // host-side via `tarball_prefetch`, on every runtime. Any
          // per-resource `mount_strategy` from the prepared execution is
          // ignored in favor of the hardcoded override below.
          const chooseStrategy = (
            resource: MountResource,
            _mountStrategy: ResourceMountStrategyOverride,
          ) => {
            const factoryInput: PickStrategyInput = {
              resource,
              // Execution credentials grant no file-blob access. Always copy
              // file bytes through the trusted host-side FileStore instead.
              overrideStrategy: 'tarball_prefetch',
              runtime,
              workspaceId,
              sessionId,
              fileStore,
            };
            return pickStrategy(factoryInput);
          };

          try {
            setupPhase = 'resource_mount';
            const materialized = await materializeResources({
              workspaceId,
              sessionId,
              sandbox,
              resources: fileResources,
              chooseStrategy,
            });
            mounts = materialized.mounts;
            resourceMountEvents = materialized.events;
          } catch (e) {
            throw new RequiredSetupError('resource_mount', e);
          }
        }

        // Handle `memory_store` resources separately from
        // `materializeResources` (which is file-only). Each store gets its
        // own FUSE root mount (or local-tmpdir mount on InMemory), and after
        // mounts are activated the dispatcher starts a single per-session
        // `MemoryVersionWatcher` covering all attached stores. The watcher's
        // start() seeds its sha cache from the registry's truth so writes
        // that pre-existed at session-spawn don't re-register.
        //
        const memoryResources = resources.filter((r) => r.type === 'memory_store');
        if (memoryResources.length > 0 && this.opts.registry) {
          const watchedStores: WatchedStore[] = [];
          const memoryMountHandles: ActiveMount[] = [];
          try {
            setupPhase = 'memory_setup';
            for (const r of memoryResources) {
              const memStoreId = r.memory_store_id;
              if (!memStoreId) {
                throw new Error(`memory_store resource ${r.id} is missing memory_store_id`);
              }
              const storeRecord = r.memory_store;
              if (!storeRecord) {
                throw new Error(`memory_store ${memStoreId} missing from prepared execution`);
              }
              // Prepared executions normally carry Registry's canonical
              // mount path. The ID-based fallback avoids turning an
              // arbitrary store name into a filesystem path on contract
              // drift or older internal callers.
              const mountPath = r.mount_path ? r.mount_path : `/mnt/memory/${memStoreId}/`;
              const resource: MountResource = {
                id: r.id,
                type: 'memory_store',
                memoryStoreId: memStoreId,
                storeName: storeRecord.name,
                mountPath,
                access: r.access,
              };
              // Memory resources don't carry a strategy override in the
              // current registry contract — `pickStrategy`'s memory branch
              // auto-picks `memory_fuse` when the runtime supports FUSE +
              // wiring is complete, else `local_memory`.
              const factoryInput: PickStrategyInput = {
                resource,
                overrideStrategy: null,
                runtime,
                workspaceId,
                sessionId,
              };
              if (creds !== undefined) factoryInput.creds = creds;
              if (this.opts.s3Bucket !== undefined) factoryInput.bucket = this.opts.s3Bucket;
              if (this.opts.s3Endpoint !== undefined) factoryInput.endpoint = this.opts.s3Endpoint;
              if (this.opts.s3KeyPrefix !== undefined) {
                factoryInput.memoryKeyPrefix = this.opts.s3KeyPrefix;
              }
              if (this.opts.s3Region !== undefined) factoryInput.region = this.opts.s3Region;
              if (this.opts.s3ForcePathStyle !== undefined) {
                factoryInput.forcePathStyle = this.opts.s3ForcePathStyle;
              }
              const strategy = pickStrategy(factoryInput);
              await assertCanonicalPathOutsideSkillsRoot(sandbox, resource.mountPath);
              const handle = await strategy.activate(sandbox, resource);
              memoryMountHandles.push({ strategy, handle });
              watchedStores.push({
                storeId: memStoreId,
                storeName: storeRecord.name,
                mountPath,
              });
            }

            // Append memory mounts onto the existing `mounts` array so the
            // runner's `stop()` deactivate-loop covers them uniformly with
            // file mounts. ActiveMount carries strategy + handle; deactivate
            // dispatches polymorphically.
            if (memoryMountHandles.length > 0) {
              mounts = mounts ? [...mounts, ...memoryMountHandles] : memoryMountHandles;
            }

            // Start the per-session memory watcher AFTER mounts are live.
            // The S3 path is taken when the runtime supports FUSE AND the
            // dispatcher is wired with an S3Client + bucket; the InMemory
            // path otherwise. Either path covers ALL attached stores in a
            // single watcher instance — the dispatcher does not pre-emptively
            // start a watcher per store.
            if (watchedStores.length > 0) {
              const watcherOpts: ConstructorParameters<typeof MemoryVersionWatcher>[0] = {
                workspaceId,
                sessionId,
                stores: watchedStores,
                registry: this.opts.registry,
                store: this.opts.store,
              };
              const watcherS3 = this.opts.s3Client;
              const watcherBucket = this.opts.s3Bucket;
              const useS3Branch =
                runtime.capabilities.supportsFuse &&
                watcherS3 !== undefined &&
                watcherBucket !== undefined;
              if (useS3Branch) {
                watcherOpts.s3 = watcherS3;
                watcherOpts.bucket = watcherBucket;
                watcherOpts.memoryKeyPrefix = this.opts.s3KeyPrefix ?? '';
              } else {
                watcherOpts.sandbox = sandbox;
              }
              if (this.opts.memoryWatcherIntervalMs !== undefined) {
                watcherOpts.intervalMs = this.opts.memoryWatcherIntervalMs;
              }

              // Cross-session persistence on the InMemory
              // path requires seeding the sandbox tmpdir with each existing
              // memory's bytes BEFORE the watcher starts. The InMemory
              // runtime's tmpdir is fresh per `acquire()`, so a Session B
              // that reads `/mnt/memory/{store}/foo.txt` would otherwise see
              // ENOENT — even though Session A's write durably landed in
              // the registry/blob-store. The S3/FUSE path doesn't need this:
              // bytes already live in the bucket and the FUSE mount surfaces
              // them on first access.
              //
              // Seeding happens BEFORE `watcher.start()` so the cache the
              // watcher seeds from `registry.listSessionMemories` matches the bytes
              // actually present on disk; otherwise the next poll would
              // re-register every seeded path as a brand-new write.
              //
              // Seeding is required setup: running without existing memory
              // bytes would violate the mounted memory-store contract.
              if (!useS3Branch) {
                for (const ws of watchedStores) {
                  const memories = await this.opts.registry.listSessionMemories({
                    workspaceId,
                    sessionId,
                    storeId: ws.storeId,
                  });
                  for (const m of memories) {
                    const bytes = await this.opts.registry.getSessionMemoryContent({
                      workspaceId,
                      sessionId,
                      storeId: ws.storeId,
                      memoryId: m.id,
                    });
                    if (bytes === null) continue;
                    const root = ws.mountPath.endsWith('/')
                      ? ws.mountPath.slice(0, -1)
                      : ws.mountPath;
                    await sandbox.files.write(`${root}/${m.path}`, bytes);
                  }
                }
              }

              memoryWatcher = new MemoryVersionWatcher(watcherOpts);
              await memoryWatcher.start();
            }
          } catch (e) {
            console.error('memory_store mount setup failed', e);
            if (memoryWatcher) {
              await memoryWatcher.stop().catch(() => {});
              memoryWatcher = undefined;
            }
            for (const m of memoryMountHandles) {
              await m.strategy.deactivate(sandbox, m.handle).catch(() => {});
            }
            if (mounts && memoryMountHandles.length > 0) {
              const partials = new Set(memoryMountHandles.map((m) => m.handle.id));
              mounts = mounts.filter((m) => !partials.has(m.handle.id));
              if (mounts.length === 0) mounts = undefined;
            }
            throw new RequiredSetupError('memory_setup', e);
          }
        }

        // github_repository resources. The harness clones each
        // repo host-side via `simple-git`, then streams the working tree (plus
        // `.git/`) into the sandbox at `mount_path`. The PAT is resolved
        // per-clone and never reaches the sandbox; per-call git operations
        // from inside the sandbox flow through the credential helper, which
        // calls back to `/v1/git-creds`.
        //
        if (
          repoResources.length > 0 &&
          this.opts.registry &&
          this.opts.gitWorker &&
          this.opts.workDir
        ) {
          const registry = this.opts.registry;
          const gitWorker = this.opts.gitWorker;
          const workDir = this.opts.workDir;
          const repoMountHandles: ActiveMount[] = [];
          let repoSetupSucceeded = false;
          // Track whether any strategy.activate() ran. The
          // strategy itself increments the ok/error counter for each call it
          // sees, so we only need to record errors for repos whose setup
          // failed BEFORE entering the activate loop (mint JWT, profile
          // write, runPrivileged, etc.). When the catch fires inside the
          // loop, the strategy already counted the failing repo.
          let activateLoopEntered = false;
          try {
            setupPhase = 'repo_setup';
            // 1. Mint a session-scoped JWT with `aud='git-creds'` and the
            //    repo URL allowlist baked in. The JWT is the only thing the
            //    in-sandbox helper needs to identify itself; bytes never
            //    cross workspace boundaries because the registry side
            //    enforces workspace scoping on every credential lookup.
            const repoUrls = repoResources
              .map((r) => (r.repo_ref as { url?: string } | null)?.url)
              .filter((u): u is string => typeof u === 'string' && u.length > 0);
            const gitJwt = inSandboxGitCredsToken
              ? { token: inSandboxGitCredsToken }
              : await registry.mintGitCredsJwt({ workspaceId, sessionId, repoUrls });

            // 2. Separate-mode templates are already running at this point,
            //    so inject the helper config via `/etc/profile.d`. The custom
            //    image registers the root-owned system Git helper at build
            //    time; no post-start sudo path is needed. In-sandbox images
            //    received the same URL/token in EnvironmentSpec.harnessEnv
            //    before acquire and configure the helper in the image itself.
            if (selection.mode === 'separate') {
              const credsUrl = this.opts.gitCredsPublicUrl;
              if (!credsUrl) {
                throw new Error('dispatcher: GIT_CREDS_PUBLIC_URL not configured');
              }
              const profileScript =
                `export ORCA_GIT_CREDS_URL=${shQuote(credsUrl)}\n` +
                `export ORCA_GIT_CREDS_TOKEN=${shQuote(gitJwt.token)}\n`;
              await sandbox.files.write(
                '/etc/profile.d/orca-git-creds.sh',
                Buffer.from(profileScript, 'utf8'),
              );
            }

            // 3. Activate `GitCloneStrategy` per repo. Each strategy clones
            //    into a per-(session, repoIdx) work dir + streams files into
            //    the sandbox at `mount_path`.
            activateLoopEntered = true;
            for (const [idx, r] of repoResources.entries()) {
              const repoRef = r.repo_ref as {
                git_credential_id: string;
                url: string;
                checkout?: { type: 'branch' | 'commit'; value: string };
              } | null;
              if (!repoRef) continue;
              const mountPath =
                r.mount_path && r.mount_path.length > 0
                  ? r.mount_path
                  : defaultRepoMountPath(repoRef.url);
              const resource: MountResource = {
                id: r.id,
                type: 'github_repository',
                url: repoRef.url,
                mountPath,
                access: r.access,
                gitCredentialId: repoRef.git_credential_id,
                repoIdx: idx,
                ...(repoRef.checkout ? { checkout: repoRef.checkout } : {}),
              };
              const factoryInput: PickStrategyInput = {
                resource,
                overrideStrategy: null,
                runtime,
                workspaceId,
                sessionId,
                gitWorker,
                workDir,
                resolvePat: (gitCredentialId: string) =>
                  registry.resolveGitCredentialSecret({
                    workspaceId,
                    sessionId,
                    gitCredentialId,
                  }),
              };
              const strategy = pickStrategy(factoryInput);
              await assertCanonicalPathOutsideSkillsRoot(sandbox, resource.mountPath);
              const handle = await strategy.activate(sandbox, resource);
              repoMountHandles.push({ strategy, handle });
            }

            if (repoMountHandles.length > 0) {
              mounts = mounts ? [...mounts, ...repoMountHandles] : repoMountHandles;
            }
            repoSetupSucceeded = true;
          } catch (e) {
            console.error('github_repository setup failed', e);
            // When the failure happened BEFORE the
            // activate loop ran (mint JWT, profile write, runPrivileged),
            // GitCloneStrategy never observed the error so the counter
            // was not incremented. Record one error per repo here so the
            // signal isn't lost. When the loop did run, the strategy's
            // own try/catch already counted the failing repo (and ok'd
            // any successful ones earlier in the loop) — skip to avoid
            // double-counting.
            if (!activateLoopEntered) {
              for (const _r of repoResources) {
                harnessGitCloneTotal.inc({ workspace_id: workspaceId, result: 'error' });
              }
            }
            // Drop any partial repo handles from `mounts` so the runner
            // doesn't try to deactivate something we already cleaned up.
            if (mounts && repoMountHandles.length > 0) {
              const partials = new Set(repoMountHandles.map((m) => m.handle.id));
              mounts = mounts.filter((m) => !partials.has(m.handle.id));
              if (mounts.length === 0) mounts = undefined;
            }
            // Best-effort: rm -rf the partial work dir so a half-clone
            // can't outlive the session.
            await this.opts.workDir.releaseSession(workspaceId, sessionId).catch(() => {});
            repoCleanupReleases = async () => {};
            throw new RequiredSetupError('repo_setup', e);
          }
          if (repoSetupSucceeded) {
            // Capture the cleanup so SessionRunner.stop() can run it after
            // memoryWatcher.stop() and before indexOutputs(). `workDir` is
            // captured by ref above so a future Dispatcher option mutation
            // can't repoint the cleanup at the wrong manager.
            repoCleanupReleases = async () => {
              await workDir.releaseSession(workspaceId, sessionId);
            };
          }
        } else if (repoResources.length > 0) {
          throw new RequiredSetupError(
            'repo_setup',
            new Error('github_repository resources require registry, gitWorker, and workDir'),
          );
        }

        // Materialize the trusted Skill tree only after every tenant resource
        // has been activated. Besides the syntactic reserved-path checks at
        // Registry and Harness boundaries, this final delete-and-rebuild
        // closes aliases through image-provided symlinks: a resource cannot
        // remain the last writer to /workspace/skills. A mount that hides or
        // prevents rebuilding the reserved tree fails setup closed here.
        try {
          setupPhase = 'skill_setup';
          await materializeSkills({
            workspaceId,
            sandbox,
            skillStore,
            skills: sessionSkills,
          });
        } catch (e) {
          throw new RequiredSetupError('skill_setup', e);
        }

        if (selection.mode === 'colocated') {
          setupPhase = 'write_policy';
          const runtimeLabel = sandboxRuntimeLabel(runtime);
          try {
            if (!inSandboxWritePolicy) {
              throw new Error('in-sandbox write policy was not initialized');
            }
            const filesystemBoundaryProbe = await sandbox.run({
              tool: 'bash',
              args: {
                command: buildSkillsAliasProbeCommand(inSandboxWritePolicy),
              },
            });
            if (filesystemBoundaryProbe.exit_code !== 0) {
              throw new Error(
                `in-sandbox filesystem-boundary probe failed: ${filesystemBoundaryProbe.stderr ?? ''}`,
              );
            }
            const probe = await sandbox.run({
              tool: 'bash',
              args: {
                command: buildBubblewrapCommand('true', inSandboxWritePolicy),
              },
            });
            if (probe.exit_code !== 0) {
              throw new Error(`in-sandbox write-policy probe failed: ${probe.stderr ?? ''}`);
            }
            // The image entrypoint waits for this marker before creating its
            // outer read-only mount namespace. Releasing it only after all
            // resource/FUSE mounts are live makes those mounts visible inside
            // the long-running harness process.
            await assertCanonicalPathOutsideSkillsRoot(sandbox, IN_SANDBOX_HARNESS_READY_PATH);
            await sandbox.files.write(IN_SANDBOX_HARNESS_READY_PATH, Buffer.alloc(0));
            harnessSandboxWritePolicySetupTotal.inc({ runtime: runtimeLabel, result: 'ok' });
          } catch (e) {
            harnessSandboxWritePolicySetupTotal.inc({ runtime: runtimeLabel, result: 'error' });
            throw new RequiredSetupError('write_policy', e);
          }
        }
        if (selection.mode === 'separate' || capabilities.needsSandboxToolset) {
          // Agent write isolation protects every mounted resource, not just
          // output capture. A session without S3/output wiring must therefore
          // receive the same restricted handle as an output-enabled session.
          setupPhase = 'write_policy';
          const runtimeLabel = sandboxRuntimeLabel(runtime);
          if (runtime.capabilities.supportsWritePolicy !== true) {
            harnessSandboxWritePolicySetupTotal.inc({ runtime: runtimeLabel, result: 'error' });
            throw new RequiredSetupError(
              'write_policy',
              new Error(`${runtimeLabel} does not support sandbox write-policy enforcement`),
            );
          }
          const resourceById = new Map(resources.map((resource) => [resource.id, resource]));
          const policy = buildSandboxWritePolicy(
            (mounts ?? []).map(({ handle }) => {
              const resource = resourceById.get(handle.resourceId);
              return {
                path: handle.mountPath,
                kind: handle.resourceType,
                access: normalizeWritePolicyAccess(resource?.access),
              };
            }),
            { includeSkillsRoot: sessionSkills.length > 0 },
          );
          try {
            agentSandbox = await createPolicyEnforcedSandbox(
              sandbox,
              policy,
              ({ path, operation }) => {
                harnessSandboxWriteDeniedTotal.inc({ runtime: runtimeLabel, kind: operation });
                console.warn('sandbox write denied', {
                  workspaceId,
                  sessionId,
                  runtime: runtimeLabel,
                  operation,
                  path,
                });
              },
            );
            harnessSandboxWritePolicySetupTotal.inc({ runtime: runtimeLabel, result: 'ok' });
            if (capabilities.needsSandboxToolset) tools = buildAgentToolset(agentSandbox);
          } catch (e) {
            harnessSandboxWritePolicySetupTotal.inc({ runtime: runtimeLabel, result: 'error' });
            throw new RequiredSetupError('write_policy', e);
          }
        }
      } catch (e) {
        console.error('sandbox setup failed', e);
        if (memoryWatcher) {
          await memoryWatcher.stop().catch(() => {});
          memoryWatcher = undefined;
        }
        if (sandbox && mounts) {
          for (const { strategy, handle } of mounts) {
            await strategy.deactivate(sandbox, handle).catch(() => {});
          }
        }
        // Best-effort sweep of any host-side work dir we
        // managed to create before the sandbox-level catch fired. The cleanup
        // closure itself is dropped — there's no runner to invoke it.
        if (repoCleanupReleases) {
          await repoCleanupReleases().catch(() => {});
          repoCleanupReleases = undefined;
        } else if (this.opts.workDir) {
          await this.opts.workDir.releaseSession(workspaceId, sessionId).catch(() => {});
        }
        agentSandbox = undefined;
        if (sandbox) {
          await sandbox.destroy().catch(() => {});
          sandbox = undefined;
        }
        tools = undefined;
        mounts = undefined;
        resourceMountEvents = [];
        outputMount = undefined;
        const phase = e instanceof RequiredSetupError ? e.phase : setupPhase;
        try {
          await this.emitSetupFailed(workspaceId, sessionId, phase, e);
        } finally {
          await this.markSessionIdle(workspaceId, sessionId, null).catch((stateErr) => {
            console.error(
              `dispatcher: failed to mark ${sessionId} idle after setup error`,
              stateErr,
            );
          });
        }
        throw e instanceof RequiredSetupError ? e : new RequiredSetupError(phase, e);
      }
    }

    const runnerRef: { current?: SessionRunner } = {};
    const onEventPersistenceFailed = (error: unknown): void => {
      const originatingRunner = runnerRef.current;
      if (!originatingRunner) return;
      this.handleRunnerEventPersistenceFailure(
        workspaceId,
        sessionId,
        lifecycleGeneration,
        originatingRunner,
        error,
      );
    };
    const runnerOpts: RunnerInput = {
      workspaceId,
      sessionId,
      harness,
      store: this.opts.store,
      isLifecycleCurrent: () => this.isLifecycleCurrent(lifecycleGeneration),
      onStatusRunning: () =>
        this.recordProducedEventToStatusRunning(
          runnerKeyFor(workspaceId, sessionId),
          lifecycleGeneration,
        ),
      completionForTerminal: (sourceIds) =>
        this.completionForTerminal(workspaceId, sessionId, lifecycleGeneration, sourceIds),
      onTurnTerminal: () => {
        const runnerKey = runnerKeyFor(workspaceId, sessionId);
        this.discardPendingStatusRunningLatency(runnerKey, lifecycleGeneration);
        this.settleTurnSourceEvents(runnerKey);
        const originatingRunner = runnerRef.current;
        if (originatingRunner) {
          this.recordRunnerTerminalPersisted(runnerKey, workspaceId, sessionId, originatingRunner);
        }
      },
      onTurnTerminalFailed: onEventPersistenceFailed,
      onEventPersistenceFailed,
    };
    if (
      prepared?.environment?.target === 'self_hosted' &&
      capabilities.supportsClientToolExecution &&
      selection.mode === 'separate'
    ) {
      runnerOpts.clientToolExecution = true;
    }
    if (mcpServers) runnerOpts.mcpServers = mcpServers;
    if (mcpJwtProvider) runnerOpts.mcpJwtProvider = mcpJwtProvider;
    if (remoteMcpToolsets) runnerOpts.remoteMcpToolsets = remoteMcpToolsets;
    if (sandbox) runnerOpts.sandbox = sandbox;
    if (agentSandbox) runnerOpts.agentSandbox = agentSandbox;
    if (tools) runnerOpts.tools = tools;
    if (mounts) runnerOpts.mounts = mounts;
    if (outputMount) runnerOpts.outputMount = outputMount;
    if (memoryWatcher) runnerOpts.memoryWatcher = memoryWatcher;
    if (repoCleanupReleases) runnerOpts.workDirReleases = repoCleanupReleases;
    // Registry coordinates the writer with the deployed Gateway sink. Missing
    // authority defaults to Harness for backward compatibility and prevents a
    // colocated deployment without that optional sink from silently dropping
    // all usage.
    if (this.opts.registry && (prepared?.session.usage_writer ?? 'harness') === 'harness') {
      const registry = this.opts.registry;
      runnerOpts.recordUsage = async (usage, model, subagentId, turnEventId, usageEventId) => {
        const session = await registry.recordSessionUsageInternal({
          workspaceId,
          sessionId,
          usage,
          ...(model ? { model } : {}),
          ...(agentSnapshot.model_provider ? { provider: agentSnapshot.model_provider } : {}),
          ...(subagentId ? { subagentId } : {}),
          ...(turnEventId ? { turnEventId } : {}),
          ...(usageEventId ? { usageEventId } : {}),
        });
        if (!session) {
          throw new Error(`Registry session ${sessionId} disappeared during usage flush`);
        }
        return {
          ...(session.guardrail_usage_state ?? {}),
          ...(session.guardrail_subject_window_state ?? {}),
        };
      };
    }

    // Build one live + shutdown indexer closure for this
    // runner generation. The runner doesn't need to know about its wiring;
    // the closure captures registry, store, sandbox, S3 client, and the
    // generation-scoped prefix. SessionRunner invokes it after tool results
    // and once more before deactivating mounts during shutdown.
    if (
      outputMount &&
      sandbox &&
      this.opts.registry &&
      (outputMount.kind === 'inmemory_local' || this.opts.s3Client)
    ) {
      const registry = this.opts.registry;
      const store = this.opts.store;
      const indexer = new OutputIndexer();
      const capturedSandbox = sandbox;
      const capturedMount = outputMount;
      const s3Client = this.opts.s3Client;
      runnerOpts.indexOutputs = async (signal?: AbortSignal): Promise<IndexSessionResult> => {
        const indexInput: Parameters<OutputIndexer['indexSession']>[0] = {
          workspaceId,
          sessionId,
          mount: capturedMount,
          registry,
          store,
          ...(signal ? { signal } : {}),
        };
        if (capturedMount.kind === 's3' && s3Client) indexInput.s3 = s3Client;
        if (capturedMount.kind === 'inmemory_local') indexInput.sandbox = capturedSandbox;
        return await indexer.indexSession(indexInput);
      };
    }

    const runner = new SessionRunner(runnerOpts);
    runnerRef.current = runner;
    try {
      if (resourceMountEvents.length > 0) {
        await this.opts.store
          .append(workspaceId, sessionId, resourceMountEvents)
          .catch((e) => console.error('emit resource_mounted failed', e));
      }
      this.assertLifecycleCurrent(lifecycleGeneration);
      const baseAgentSnapshot = runnerOpts.indexOutputs
        ? { ...agentSnapshot, system: withOutputCaptureInstruction(agentSnapshot.system) }
        : agentSnapshot;
      const replayedSystemPrompt = appendSystemMessages(
        baseAgentSnapshot.system,
        appliedSystemMessages,
      );
      await runner.start({
        agentSnapshot:
          replayedSystemPrompt === baseAgentSnapshot.system
            ? baseAgentSnapshot
            : { ...baseAgentSnapshot, system: replayedSystemPrompt },
        // `prepared` is nullable here and genuinely null on the paths that
        // reuse an already-running runner, so read it defensively rather than
        // relying on narrowing that does not survive the intervening awaits.
        ...(prepared?.guardrails && prepared.guardrails.length > 0
          ? { guardrails: prepared.guardrails.map(toSessionGuardrail) }
          : {}),
        ...(prepared?.guardrail_state ? { guardrailState: prepared.guardrail_state } : {}),
      });
      this.assertLifecycleCurrent(lifecycleGeneration);
      await this.markSessionRunning(workspaceId, sessionId, runner.sandboxId).catch((stateErr) => {
        console.error(`dispatcher: failed to mark ${sessionId} running`, stateErr);
      });
      this.assertLifecycleCurrent(lifecycleGeneration);
    } catch (e) {
      if (
        !this.isLifecycleCurrent(lifecycleGeneration) ||
        e instanceof DispatcherStoppingError ||
        e instanceof SessionRunnerStoppingError
      ) {
        this.runnerSessionConfigKeys.delete(runnerKeyFor(workspaceId, sessionId));
        this.stopStaleRunner(runner);
        throw new DispatcherStoppingError({ cause: e });
      }
      if (e instanceof DurableHarnessStateError) {
        if (e.cause instanceof RegistryInvalidRuntimeBindingError) throw e.cause;
        throw new RetryableSessionEventError('Codex durable turn startup requires recovery', {
          cause: e,
        });
      }
      if (selection.mode !== 'colocated') throw e;
      console.error(`dispatcher: colocated setup failed for ${sessionId}`, e);
      try {
        await this.emitSetupFailed(workspaceId, sessionId, 'sandbox_setup', e);
      } finally {
        await this.markSessionIdle(workspaceId, sessionId, null).catch((stateErr) => {
          console.error(
            `dispatcher: failed to mark ${sessionId} idle after colocated setup error`,
            stateErr,
          );
        });
      }
      throw new RequiredSetupError('sandbox_setup', e);
    }
    this.runnerSessionConfigKeys.set(
      runnerKeyFor(workspaceId, sessionId),
      prepared ? preparedExecutionRuntimeConfigKey(prepared) : sessionRuntimeConfigKey(session),
    );
    return runner;
  }

  private buildRuntimeAgentSnapshot(
    agent: AgentRecord,
    prepared: PreparedExecutionV2,
  ): RuntimeAgentSnapshot {
    const base = this.buildSingleRuntimeAgentSnapshot(agent, prepared);
    if (agent.multiagent?.type !== 'coordinator') return base;

    const subagents: RuntimeSubagentSnapshot[] = [];
    for (const ref of agent.multiagent.agents) {
      const child =
        ref.id === agent.id && ref.version === agent.version
          ? agent
          : prepared.subagents.find(
              (candidate) => candidate.id === ref.id && candidate.version === ref.version,
            );
      if (!child) {
        throw new Error(`prepared execution missing subagent ${ref.id} version ${ref.version}`);
      }
      const childSnapshot = this.buildSingleRuntimeAgentSnapshot(child, prepared);
      subagents.push({
        id: child.id,
        name: child.name,
        version: child.version,
        ...(childSnapshot.model_provider ? { model_provider: childSnapshot.model_provider } : {}),
        ...(childSnapshot.model_id ? { model_id: childSnapshot.model_id } : {}),
        ...(childSnapshot.model_speed ? { model_speed: childSnapshot.model_speed } : {}),
        ...(childSnapshot.model_effort ? { model_effort: childSnapshot.model_effort } : {}),
        ...(childSnapshot.system ? { system: childSnapshot.system } : {}),
        ...(childSnapshot.allowed_tool_names
          ? { allowed_tool_names: childSnapshot.allowed_tool_names }
          : {}),
        ...(childSnapshot.tool_permission_policies
          ? { tool_permission_policies: childSnapshot.tool_permission_policies }
          : {}),
      });
    }

    return {
      ...base,
      multiagent: { type: 'coordinator', agents: subagents },
    };
  }

  private buildSingleRuntimeAgentSnapshot(
    agent: AgentRecord,
    prepared: PreparedExecutionV2,
  ): RuntimeAgentSnapshot {
    const skills = agent.skills ?? [];
    for (const skill of skills) validateSkillDescriptor(skill);

    // Advertise only bundles that the same execution will actually materialize.
    const system = composeSkillsCatalog(
      agent.system ?? '',
      skills.filter((skill) => !skillIsBlocked(prepared, skill)),
    );
    const agentToolNames = extractAgentToolNames(agent.tools ?? []);
    const allowedMcpServerNames = enabledMcpToolsetServerNames(agent.tools ?? []);
    return {
      ...buildBaseAgentSnapshot(agent),
      system,
      allowed_tool_names: enabledOrcaToolNames(agent.tools ?? []),
      allowed_mcp_server_names: [...allowedMcpServerNames],
      tool_permission_policies: buildToolPermissionPolicies(agent, allowedMcpServerNames),
      custom_tools: extractCustomToolDefinitions(agent.tools ?? [], agentToolNames),
    };
  }

  /**
   * Surface a required-setup failure to the client and leave the session idle:
   * emit `session.error{type:setup_failed}` + `session.status_idle`, then mark
   * the row idle. The `finally` is load-bearing — the session must not stay
   * `running` even when the transcript append itself fails (that error still
   * propagates to the caller).
   */
  private async failSessionSetup(
    workspaceId: string,
    sessionId: string,
    error: RequiredSetupError,
  ): Promise<void> {
    try {
      await this.emitSetupFailed(workspaceId, sessionId, error.phase, error);
    } finally {
      await this.markSessionIdle(workspaceId, sessionId, null).catch((stateErr) => {
        console.error(
          `dispatcher: failed to mark ${sessionId} idle after ${error.phase} setup error`,
          stateErr,
        );
      });
    }
  }

  private async emitSetupFailed(
    workspaceId: string,
    sessionId: string,
    phase: SetupFailurePhase,
    error: unknown,
  ): Promise<void> {
    const message = errorMessage(error);
    // The non-Claude `session.setup_failed` event is replaced by the Claude
    // RECEIVED pair session.error + session.status_idle{retries_exhausted}.
    // Setup failures are recoverable on the next event in
    // this architecture (the runner respawns and re-attempts provisioning), so
    // the session stays idle — consistent with the DB status (markSessionIdle at
    // the call sites) and with handleRunnerSubmitFailure — rather than emitting a
    // terminal session.status_terminated that would contradict the retryable
    // behavior and the REST `idle` status.
    await this.appendInternalEvent(
      workspaceId,
      sessionId,
      SessionEventKind.error,
      sessionErrorPayload({ type: 'setup_failed', message, willRetry: false, extra: { phase } }),
    );
    await this.appendInternalEvent(
      workspaceId,
      sessionId,
      SessionEventKind.statusIdle,
      sessionIdlePayload('retries_exhausted'),
    );
  }
}

function completedUserEventMarker(
  workspaceId: string,
  sessionId: string,
  userEventId: string,
): Event {
  const markerUuid = uuidv5(
    `${workspaceId}\0${sessionId}\0${userEventId}`,
    USER_EVENT_COMPLETED_NAMESPACE,
  );
  return {
    id: `evt_${markerUuid}`,
    workspaceId,
    sessionId,
    subpath: '',
    seq: 0,
    producedAt: new Date().toISOString(),
    producedBy: 'harness',
    kind: USER_EVENT_COMPLETED_KIND,
    payload: new TextEncoder().encode(JSON.stringify({ user_event_id: userEventId })),
    idempotencyKey: `turn-completed:${markerUuid}`,
  };
}

function isCompletionTrackedUserEvent(kind: string): boolean {
  return kind === 'user.message' || kind === 'user.define_outcome';
}

function sandboxRuntimeLabel(runtime: SandboxRuntime | undefined): string {
  if (!runtime) return 'none';
  return runtime.constructor.name
    .replace(/SandboxRuntime$/, '')
    .replace(/([a-z0-9])([A-Z])/g, '$1_$2')
    .toLowerCase();
}

function errorMessage(error: unknown): string {
  if (error instanceof RequiredSetupError) return errorMessage(error.original);
  const message = error instanceof Error ? error.message : String(error);
  return sanitizePublicErrorMessage(message);
}

function sanitizePublicErrorMessage(message: string): string {
  return message
    .replace(/\b([a-z][a-z0-9+.-]*:\/\/)([^/\s:@]+):([^@\s/]+)@/gi, '$1$2:***@')
    .replace(
      /([?&](?:access_token|api_key|client_secret|key|password|refresh_token|secret|session_token|token)=)[^&\s]+/gi,
      '$1***',
    )
    .replace(
      /\b(access_token|api_key|client_secret|password|refresh_token|secret|session_token|token)=([^\s]+)/gi,
      '$1=***',
    )
    .replace(/\b(authorization:\s*(?:bearer|basic)\s+)[^\s]+/gi, '$1***')
    .replace(/\b(?:gh[pousr]_[A-Za-z0-9_]+|github_pat_[A-Za-z0-9_]+)\b/g, '***');
}

function sessionIdFromRunnerKey(runnerKey: string): string {
  return runnerKey.slice(runnerKey.lastIndexOf('/') + 1);
}

function workspaceIdFromRunnerKey(runnerKey: string): string {
  return runnerKey.slice(0, runnerKey.lastIndexOf('/'));
}

function runnerKeyFor(workspaceId: string, sessionId: string): string {
  return `${workspaceId}/${sessionId}`;
}

function sessionRuntimeConfigKey(session: SessionRecord | null): string {
  // Hash every registry field that feeds runner boot. Some fields are create-time
  // today, but any future/internal change must respawn before the next user turn.
  return stableJson({
    agent_id: session?.agent_id ?? null,
    agent_version: session?.agent_version ?? null,
    environment_id: session?.environment_id ?? null,
    tools: session?.tools ?? null,
    mcp_servers: session?.mcp_servers ?? null,
    vault_ids: session?.vault_ids ?? [],
    resources: sessionResourcesRuntimeConfig(session?.resources ?? []),
  });
}

function preparedExecutionRuntimeConfigKey(prepared: PreparedExecutionV2): string {
  return stableJson({
    session: {
      runtime_revision: prepared.session.runtime_revision ?? null,
      usage_writer: prepared.session.usage_writer ?? 'harness',
      agent_id: prepared.session.agent_id,
      agent_version: prepared.session.agent_version,
      environment_id: prepared.session.environment_id ?? null,
      tools: prepared.session.tools ?? null,
      mcp_servers: prepared.session.mcp_servers ?? null,
      vault_ids: prepared.session.vault_ids ?? [],
    },
    primary_agent: prepared.primary_agent,
    subagents: [...prepared.subagents].sort((a, b) =>
      `${a.id}:${a.version}`.localeCompare(`${b.id}:${b.version}`),
    ),
    environment: prepared.environment,
    vault_credentials: runtimeAffectingVaultCredentials(prepared.vault_credentials),
    resources: sessionResourcesRuntimeConfig(prepared.resources),
    // Guardrails are part of the runtime's configuration: editing one must
    // restart the runner rather than let an in-flight session keep enforcing
    // the rule it started with. Omitting this would make a guardrail change
    // take effect only on the next session, silently.
    guardrails: [...(prepared.guardrails ?? [])].sort((a, b) => a.id.localeCompare(b.id)),
  });
}

function runtimeAffectingVaultCredentials(
  credentials: PreparedExecutionV2['vault_credentials'],
): PreparedExecutionV2['vault_credentials'] {
  // MCP credential ids are advisory startup hints. ai-gateway resolves the
  // current live credential from Registry on every request, so archive and
  // replacement must propagate without tearing down a warm runner. Retain
  // non-MCP credentials because they may affect future sandbox/runtime setup.
  return credentials
    .filter((credential) => !credential.mcp_server_url)
    .sort((a, b) => a.credential_id.localeCompare(b.credential_id));
}

/** Wire shape to runtime shape: snake_case crossing the service boundary. */
function toSessionGuardrail(record: PreparedGuardrailRecord): SessionGuardrail {
  return {
    id: record.id,
    name: record.name,
    tier: record.tier,
    phases: record.phases,
    rule: record.rule,
    stateful: record.stateful,
    ...(record.state_scope ? { stateScope: record.state_scope } : {}),
    ...(record.subagent_id ? { subagentId: record.subagent_id } : {}),
  };
}

function sessionResourcesRuntimeConfig(
  resources: SessionRecord['resources'],
): SessionRecord['resources'] {
  return [...(resources ?? [])].sort((a, b) => a.id.localeCompare(b.id));
}

function buildMemoryStoreGrants(resources: PreparedExecutionV2['resources']): MemoryStoreGrant[] {
  const grants = new Map<string, MemoryStoreGrant['access']>();
  for (const resource of resources) {
    if (resource.type !== 'memory_store' || !resource.memory_store_id) continue;
    const access = resource.access;
    const previous = grants.get(resource.memory_store_id);
    // Multiple mount points for one store must not produce duplicate policy
    // entries. Preserve the broader of the requested grants.
    if (previous !== 'read_write') grants.set(resource.memory_store_id, access);
  }
  return [...grants].map(([storeId, access]) => ({ storeId, access }));
}

function collectPreparedExecutionSkills(prepared: PreparedExecutionV2): SkillDescriptor[] {
  const skills = [
    ...(prepared.primary_agent.skills ?? []),
    ...prepared.subagents.flatMap((agent) => agent.skills ?? []),
  ];
  return skills.filter((skill) => !skillIsBlocked(prepared, skill));
}

/**
 * Whether a guardrail blocks this Skill from being materialized at all.
 *
 * `block_skills` is written against the tool that loads a Skill, but nothing
 * in this runtime exposes one — Skills are files staged into the sandbox, so a
 * rule keyed on a tool call had no call to key on and could never fire. The
 * enforcement point that does exist is this list: a Skill left out of it is
 * never staged, which is a strictly stronger outcome than denying the tool
 * that would have read it.
 *
 * Only the stateless pass runs here. The list is built once, before the
 * session has a state store, and a name-keyed rule needs no state — the same
 * reason tool exposure resolves this way for a sandboxed session.
 */
function skillIsBlocked(prepared: PreparedExecutionV2, skill: SkillDescriptor): boolean {
  const guardrails = prepared.guardrails ?? [];
  if (guardrails.length === 0) return false;
  const decision = evaluateGuardrails(
    guardrails.map((g) => ({
      guardrail: {
        id: g.id,
        name: g.name,
        enabled: true,
        phases: g.phases as PreparedGuardrail['guardrail']['phases'],
        scope: 'explicit',
        rule: g.rule as PreparedGuardrail['guardrail']['rule'],
      },
      tier: g.tier,
      stateful: g.stateful,
      ...(g.stateScope ? { stateScope: g.stateScope } : {}),
      ...(g.subagentId ? { subagentId: g.subagentId } : {}),
    })),
    {
      phase: 'tool_call',
      sessionId: prepared.session.id,
      tool: { name: SKILL_LOAD_TOOL, input: { skill: skill.name } },
    },
    { builtins: BUILTIN_EVALUATORS, readOnly: true },
  );
  return decision.verdict === 'deny';
}

function validatePreparedExecution(
  expectedWorkspaceId: string,
  expectedSessionId: string,
  prepared: PreparedExecutionV2,
): PreparedExecutionV2 {
  if (prepared.schema_version !== 2) {
    throw new DroppedSessionEvent(
      `prepared schema version is invalid for ${expectedWorkspaceId}/${expectedSessionId}`,
    );
  }
  if (prepared.workspace_id !== expectedWorkspaceId) {
    throw new DroppedSessionEvent(
      `prepared workspace mismatch for ${expectedWorkspaceId}/${expectedSessionId}`,
    );
  }
  if (prepared.session.id !== expectedSessionId) {
    throw new DroppedSessionEvent(
      `prepared session mismatch for ${expectedWorkspaceId}/${expectedSessionId}`,
    );
  }
  if (prepared.session.workspace_id !== expectedWorkspaceId) {
    throw new DroppedSessionEvent(
      `prepared session workspace mismatch for ${expectedWorkspaceId}/${expectedSessionId}`,
    );
  }
  if (
    prepared.primary_agent.id !== prepared.session.agent_id ||
    prepared.primary_agent.version !== prepared.session.agent_version
  ) {
    throw new DroppedSessionEvent(
      `prepared primary agent mismatch for ${expectedWorkspaceId}/${expectedSessionId}`,
    );
  }
  for (const subagent of prepared.subagents) {
    if (subagent.multiagent !== null) {
      throw new DroppedSessionEvent(
        `prepared nested coordinator ${subagent.id}@${subagent.version} is unsupported`,
      );
    }
  }
  const workspaceOwned = [
    prepared.primary_agent,
    ...prepared.subagents,
    ...(prepared.environment ? [prepared.environment] : []),
    ...prepared.resources.flatMap((resource) =>
      resource.memory_store ? [resource.memory_store] : [],
    ),
  ];
  for (const resource of workspaceOwned) {
    if (resource.workspace_id !== expectedWorkspaceId) {
      throw new DroppedSessionEvent(
        `prepared resource workspace mismatch for ${expectedWorkspaceId}/${expectedSessionId}`,
      );
    }
  }
  for (const resource of prepared.resources) {
    try {
      assertMountPathOutsideSkillsRoot(resource.mount_path);
    } catch {
      throw new DroppedSessionEvent(
        `prepared resource mount path overlaps the reserved Skill root for ${expectedWorkspaceId}/${expectedSessionId}`,
      );
    }
    if (resource.access !== 'read_only' && resource.access !== 'read_write') {
      throw new DroppedSessionEvent(
        `prepared resource access is invalid for ${expectedWorkspaceId}/${expectedSessionId}`,
      );
    }
    if (
      resource.type === 'memory_store' &&
      resource.memory_store &&
      resource.memory_store.id !== resource.memory_store_id
    ) {
      throw new DroppedSessionEvent(
        `prepared memory resource mismatch for ${expectedWorkspaceId}/${expectedSessionId}`,
      );
    }
  }
  const preparedSkills = collectPreparedExecutionSkills(prepared);
  if (preparedSkills.length > 500) {
    throw new DroppedSessionEvent(
      `prepared execution has too many skill bindings for ${expectedWorkspaceId}/${expectedSessionId}`,
    );
  }
  for (const skill of preparedSkills) {
    try {
      validateSkillDescriptor(skill);
    } catch {
      throw new DroppedSessionEvent(
        `prepared skill descriptor is invalid for ${expectedWorkspaceId}/${expectedSessionId}`,
      );
    }
  }
  return prepared;
}

function stableJson(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(stableJson).join(',')}]`;
  if (value && typeof value === 'object') {
    const obj = value as Record<string, unknown>;
    return `{${Object.keys(obj)
      .sort()
      .map((key) => `${JSON.stringify(key)}:${stableJson(obj[key])}`)
      .join(',')}}`;
  }
  return JSON.stringify(value);
}

/**
 * Apply a session-local override of `tools` / `mcp_servers` onto the pinned
 * agent record (managed-agents-2026-04-01 UpdateSession). A non-null session
 * override is a FULL REPLACEMENT; `null`/absent falls back to the agent. The
 * dispatcher reads the session fresh each turn, so re-materializing the
 * snapshot from the merged record is what makes a `POST /v1/sessions/:id`
 * override take effect on the next turn without bumping the agent version.
 */
export function applySessionAgentOverrides(
  agent: AgentRecord,
  session: SessionRecord,
): AgentRecord {
  const tools = session.tools ?? agent.tools;
  const mcpServers = session.mcp_servers ?? agent.mcp_servers;
  if (tools === agent.tools && mcpServers === agent.mcp_servers) return agent;
  return { ...agent, tools, mcp_servers: mcpServers };
}

function buildBaseAgentSnapshot(agent: AgentRecord): RuntimeAgentSnapshot {
  const agentToolNames = extractAgentToolNames(agent.tools ?? []);
  const allowedMcpServerNames = enabledMcpToolsetServerNames(agent.tools ?? []);
  const snapshot: RuntimeAgentSnapshot = {
    id: agent.id,
    name: agent.name,
    version: agent.version,
    system: agent.system ?? '',
    allowed_tool_names: enabledOrcaToolNames(agent.tools ?? []),
    allowed_mcp_server_names: [...allowedMcpServerNames],
    tool_permission_policies: buildToolPermissionPolicies(agent, allowedMcpServerNames),
    custom_tools: extractCustomToolDefinitions(agent.tools ?? [], agentToolNames),
  };
  const modelProvider = agent.model?.provider ?? agent.model_provider;
  if (modelProvider !== undefined) snapshot.model_provider = modelProvider;
  const modelId = agent.model?.id ?? agent.model_id;
  if (modelId !== undefined) snapshot.model_id = modelId;
  const modelSpeed = agent.model?.speed ?? agent.model_speed;
  if (modelSpeed !== undefined) snapshot.model_speed = modelSpeed;
  const modelEffort = agent.model?.effort ?? agent.model_effort;
  if (modelEffort !== undefined) snapshot.model_effort = modelEffort;
  return snapshot;
}

export function buildToolPermissionPolicies(
  agent: Pick<AgentRecord, 'tools' | 'mcp_servers'>,
  enabledMcpServerNames: Iterable<string> = enabledMcpToolsetServerNames(agent.tools ?? []),
): Record<string, 'always_allow' | 'always_ask' | 'always_deny'> {
  const policies: Record<string, 'always_allow' | 'always_ask' | 'always_deny'> = {};
  Object.assign(policies, orcaAgentToolsetPermissionPolicies(agent.tools ?? []));
  const serverByName = new Map((agent.mcp_servers ?? []).map((server) => [server.name, server]));
  for (const serverName of enabledMcpServerNames) {
    if (!serverName || !MCP_SERVER_NAME_ALLOWLIST_PATTERN.test(serverName)) continue;
    if (serverName === ORCA_MCP_SERVER_NAME) continue;
    const server = serverByName.get(serverName);
    if (!server) continue;
    const toolsetPolicies = mcpToolsetPermissionPolicies(agent.tools ?? [], server.name);
    policies[`mcp__${serverName}__*`] = toolsetPolicies[`mcp__${serverName}__*`] ?? 'always_ask';
    Object.assign(policies, toolsetPolicies);
  }
  return policies;
}

function mcpToolsetPermissionPolicies(
  tools: AgentToolEntry[],
  serverName: string,
): Record<string, 'always_allow' | 'always_ask' | 'always_deny'> {
  return toolsetPermissionPolicies(
    tools,
    serverName,
    (tool) => tool.type === 'mcp_toolset' && tool['mcp_server_name'] === serverName,
  );
}

function orcaAgentToolsetPermissionPolicies(
  tools: AgentToolEntry[],
): Record<string, 'always_allow' | 'always_ask' | 'always_deny'> {
  return toolsetPermissionPolicies(
    tools,
    ORCA_MCP_SERVER_NAME,
    (tool) => tool.type === 'agent_toolset' || tool.type === 'agent_toolset_20260401',
  );
}

export { composeSkillsCatalog } from '@orca/sandbox-runtime';

function appendSystemMessages(base: string | undefined, messages: string[]): string | undefined {
  const parts = [base, ...messages].filter(
    (part): part is string => typeof part === 'string' && part.length > 0,
  );
  return parts.length > 0 ? parts.join('\n\n') : undefined;
}

function systemMessageText(payload: unknown): string | null {
  if (!payload || typeof payload !== 'object' || Array.isArray(payload)) return null;
  const content = (payload as Record<string, unknown>)['content'];
  if (!Array.isArray(content)) return null;
  const text = content
    .map((block) =>
      block &&
      typeof block === 'object' &&
      !Array.isArray(block) &&
      (block as Record<string, unknown>)['type'] === 'text' &&
      typeof (block as Record<string, unknown>)['text'] === 'string'
        ? ((block as Record<string, unknown>)['text'] as string)
        : '',
    )
    .filter((part) => part.length > 0)
    .join('\n');
  return text.length > 0 ? text : null;
}

export function hasEnabledReadTool(tools: readonly AgentToolEntry[]): boolean {
  return enabledOrcaToolNames(tools).includes('read');
}

function assertAgentsSkillsReadable(
  agents: readonly AgentRecord[],
  mode: 'separate' | 'colocated',
): void {
  for (const agent of agents) {
    if ((agent.skills ?? []).length === 0) continue;
    if (!hasEnabledReadTool(agent.tools ?? [])) {
      throw new RequiredSetupError(
        'skill_setup',
        new Error(`agent ${agent.id} has skills but no enabled read tool`),
      );
    }
    if (mode !== 'colocated') continue;
    const policies = buildToolPermissionPolicies(agent);
    const policy =
      policies[`mcp__${ORCA_MCP_SERVER_NAME}__read`] ??
      policies['read'] ??
      policies['Read'] ??
      policies[`mcp__${ORCA_MCP_SERVER_NAME}__*`] ??
      'always_ask';
    if (policy !== 'always_allow') {
      throw new RequiredSetupError(
        'skill_setup',
        new Error(`in-sandbox agent ${agent.id} requires read=always_allow to load skills`),
      );
    }
  }
}

function extractAgentToolNames(tools: AgentToolEntry[]): string[] {
  const names: string[] = [];
  for (const tool of tools) {
    if (tool.type === 'custom') {
      if (
        typeof tool.name === 'string' &&
        tool.name.length > 0 &&
        !RESERVED_CUSTOM_TOOL_NAMES.has(tool.name)
      ) {
        names.push(tool.name);
      }
      continue;
    }
    const normalized = normalizeToolName(tool.type);
    if (normalized) names.push(normalized);
  }
  return names;
}

function extractCustomToolDefinitions(
  tools: AgentToolEntry[],
  allowedToolNames: readonly string[],
): CustomToolDefinition[] {
  const allowed = new Set(allowedToolNames);
  const out: CustomToolDefinition[] = [];
  for (const tool of tools) {
    if (tool.type !== 'custom') continue;
    if (typeof tool.name !== 'string' || tool.name.length === 0) continue;
    if (RESERVED_CUSTOM_TOOL_NAMES.has(tool.name)) continue;
    if (!allowed.has(tool.name)) continue;
    const def: CustomToolDefinition = { name: tool.name };
    if (typeof tool.description === 'string') def.description = tool.description;
    if (
      tool.input_schema &&
      typeof tool.input_schema === 'object' &&
      !Array.isArray(tool.input_schema)
    ) {
      def.input_schema = tool.input_schema as Record<string, unknown>;
    }
    out.push(def);
  }
  return out;
}

function normalizeToolName(name: string): string | null {
  return AGENT_TOOLSET_LOGICAL_TO_ORCA_MCP.get(name) ?? name;
}

export function enabledOrcaToolNames(tools: readonly AgentToolEntry[]): string[] {
  const enabled = new Set<string>();
  for (const tool of tools) {
    if (tool.type === 'agent_toolset' || tool.type === 'agent_toolset_20260401') {
      const defaultConfig = resolvedToolsetConfig(toolsetConfigValue(tool['default_config']));
      const configByName = new Map<string, Record<string, unknown>>();
      for (const config of toolsetConfigEntries(tool['configs'])) {
        const logicalName = orcaLogicalToolName(config.name);
        if (logicalName) configByName.set(logicalName, config.value);
      }
      for (const logicalName of RUNTIME_ORCA_TOOL_LOGICAL_NAMES) {
        const config = configByName.get(logicalName);
        const resolved = config ? resolvedToolsetConfig(config, defaultConfig) : defaultConfig;
        if (resolved.enabled && resolved.permissionPolicy !== 'always_deny') {
          enabled.add(logicalName);
        }
      }
      continue;
    }

    const logicalName = normalizeToolName(tool.type);
    if (
      logicalName &&
      ORCA_MCP_TOOL_LOGICAL_NAME_SET.has(logicalName) &&
      tool['enabled'] !== false &&
      managedToolPermissionPolicyName(tool['permission_policy']) !== 'always_deny'
    ) {
      enabled.add(logicalName);
    }
  }
  return RUNTIME_ORCA_TOOL_LOGICAL_NAMES.filter((name) => enabled.has(name));
}

function orcaLogicalToolName(configuredName: string): string | null {
  const prefix = `mcp__${ORCA_MCP_SERVER_NAME}__`;
  const logicalName = configuredName.startsWith(prefix)
    ? configuredName.slice(prefix.length)
    : normalizeToolName(configuredName);
  return logicalName && ORCA_MCP_TOOL_LOGICAL_NAME_SET.has(logicalName) ? logicalName : null;
}

/**
 * Derive a default `mountPath` when the registry's session_resource
 * row left it unset. Mirrors `git clone <url>`'s default destination naming:
 * the last path segment of the URL with a trailing `.git` stripped, anchored
 * under `/workspace/`. Falls back to `/workspace/repo/` when the URL can't be
 * parsed (defensive — the registry already validates URL shape, so this only
 * fires on a contract drift).
 */
function defaultRepoMountPath(repoUrl: string): string {
  let last: string;
  try {
    const u = new URL(repoUrl);
    const parts = u.pathname.split('/').filter((p) => p.length > 0);
    last = parts.length > 0 ? (parts[parts.length - 1] ?? 'repo') : 'repo';
  } catch {
    last = 'repo';
  }
  last = last.replace(/\.git$/, '');
  if (last.length === 0) last = 'repo';
  return `/workspace/${last}/`;
}

function parseJsonPayload(payload: Uint8Array): unknown | null {
  try {
    return JSON.parse(Buffer.from(payload).toString('utf8'));
  } catch {
    return null;
  }
}

function withoutCompanionSystemCorrelation(payload: unknown): unknown {
  if (!payload || typeof payload !== 'object' || Array.isArray(payload)) return payload;
  const { [COMPANION_SYSTEM_EVENT_ID_FIELD]: _companionSystemEventId, ...publicPayload } =
    payload as Record<string, unknown>;
  return publicPayload;
}

type BoundedWaitOutcome = 'fulfilled' | 'rejected' | 'timed_out';

/**
 * Observe a promise without allowing an unavailable dependency to block the
 * caller indefinitely. Rejections stay handled even when the timeout wins.
 */
function waitForBounded(operation: Promise<void>, timeoutMs: number): Promise<BoundedWaitOutcome> {
  return new Promise((resolve) => {
    let settled = false;
    let timer: NodeJS.Timeout | null = null;
    const finish = (outcome: BoundedWaitOutcome): void => {
      if (settled) return;
      settled = true;
      if (timer !== null) clearTimeout(timer);
      resolve(outcome);
    };

    void operation.then(
      () => finish('fulfilled'),
      () => finish('rejected'),
    );
    if (timeoutMs <= 0) {
      finish('timed_out');
      return;
    }
    timer = setTimeout(() => finish('timed_out'), timeoutMs);
    timer.unref?.();
  });
}

function createKafkaGroupJoinWaiter(): KafkaGroupJoinWaiter {
  let resolvePromise: () => void = () => {};
  let rejectPromise: (error: unknown) => void = () => {};
  let settled = false;
  const promise = new Promise<void>((resolve, reject) => {
    resolvePromise = resolve;
    rejectPromise = reject;
  });
  // A consumer can crash synchronously from `run()` before the transition
  // caller begins its bounded wait. Keep that rejection handled until the
  // caller observes it through the same promise.
  void promise.catch(() => {});
  return {
    promise,
    resolve: () => {
      if (settled) return;
      settled = true;
      resolvePromise();
    },
    reject: (error) => {
      if (settled) return;
      settled = true;
      rejectPromise(error);
    },
  };
}

function replaceTopicSet(target: Set<string>, topics: Iterable<string>): void {
  target.clear();
  for (const topic of topics) target.add(topic);
}

function sameTopicSets(left: Iterable<string>, right: Iterable<string>): boolean {
  const leftSet = left instanceof Set ? left : new Set(left);
  const rightSet = right instanceof Set ? right : new Set(right);
  if (leftSet.size !== rightSet.size) return false;
  for (const topic of leftSet) {
    if (!rightSet.has(topic)) return false;
  }
  return true;
}

function matchesTopic(pattern: RegExp, topic: string): boolean {
  pattern.lastIndex = 0;
  return pattern.test(topic);
}

function kafkaMessageWorkKey(payload: EachMessagePayload): string {
  const eventId = kafkaSourceEventId(payload);
  return `${payload.topic}\0${eventId}`;
}

function isKafkaTurnDrivingDelivery(
  payload: EachMessagePayload,
  topicPrefix: string,
  encoding: 'raw' | 'avro',
): boolean {
  const ids = matchSessionTopic(payload.topic, topicPrefix, encoding);
  if (!ids || !payload.message.value) return false;
  const headers = (payload.message.headers ?? {}) as Record<string, Buffer | string | undefined>;
  const header = (name: string): string => {
    const value = headers[name];
    if (value === undefined || value === null) return '';
    return Buffer.isBuffer(value) ? value.toString('utf8') : String(value);
  };
  return (
    header('workspace_id') === ids.workspaceId &&
    header('session_id') === ids.sessionId &&
    header('produced_by') === 'client' &&
    header('kind').startsWith('user.')
  );
}

function kafkaSourceEventId(payload: EachMessagePayload): string {
  const rawId = (payload.message.headers ?? {})['id'];
  const fallback = `${payload.topic}:${payload.partition}:${payload.message.offset}`;
  if (rawId === undefined || rawId === null) return fallback;
  const eventId = Buffer.isBuffer(rawId) ? rawId.toString('utf8') : String(rawId);
  return eventId || fallback;
}

function nextKafkaOffset(offset: string): string {
  try {
    return (BigInt(offset) + 1n).toString();
  } catch {
    const parsed = Number(offset);
    return Number.isSafeInteger(parsed) && parsed >= 0 ? String(parsed + 1) : offset;
  }
}

function kafkaTopicPartitionKey(topic: string, partition: number): string {
  return `${topic}\0${partition}`;
}

/**
 * Minimal POSIX single-quote escape used to embed dynamic strings in
 * the bash `export` lines we write into `/etc/profile.d/orca-git-creds.sh`.
 * Wraps the input in `'...'` and escapes any embedded `'` as `'\''`. Keeps
 * the dispatcher self-contained (no `child_process` shell-escape dep).
 */
function shQuote(s: string): string {
  return `'${s.replace(/'/g, "'\\''")}'`;
}

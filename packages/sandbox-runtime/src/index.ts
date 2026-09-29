// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

export type {
  ToolCall,
  ToolResult,
  SandboxFiles,
  SpawnHandle,
  SandboxHandle,
  SandboxCapabilities,
  EnvironmentSpec,
  SandboxRuntime,
  WritablePathKind,
  WritablePath,
  SandboxWritePolicy,
  ValidatedSandboxWritePolicy,
  WritePolicyCapableHandle,
  SandboxReadConstraint,
  SandboxFileMode,
  PackageManager,
  Packages,
  BashToolArgs,
  SearchToolArgs,
  ParsedToolCall,
} from './sandbox-runtime.js';
export { packageManagers, parseToolCall, hasWritePolicyEnforcement } from './sandbox-runtime.js';

export {
  readUtf8Page,
  readUtf8FilePage,
  resolveOpenedDescriptorPath,
  resolveSandboxPathUnderRoot,
  buildSandboxReadPageCommand,
  parseSandboxReadPageResult,
  buildSandboxReadPrerequisiteProbeCommand,
  DEFAULT_READ_LIMIT_BYTES,
  MAX_READ_LIMIT_BYTES,
  SANDBOX_READ_TIMEOUT_MS,
  SANDBOX_READ_COMMAND_ENVS,
} from './read-page.js';
export type { ReadPage, ReadPageInput, ReadPageMetadata } from './read-page.js';

export {
  serializeSandboxFileModes,
  buildSandboxChmodManyCommand,
  SANDBOX_CHMOD_MANY_TIMEOUT_MS,
} from './chmod-many.js';

export {
  SKILLS_ROOT,
  GIT_PROXY_AUTH_ROOT,
  OUTPUT_WRITABLE_PATH,
  SANDBOX_WRITE_POLICY_ENV,
  SANDBOX_FILESYSTEM_ROOT_PREFLIGHT_TIMEOUT_MS,
  normalizeWritePolicyAccess,
  buildSandboxWritePolicy,
  validateSandboxWritePolicy,
  SandboxPathEscapeError,
  encodeSandboxWritePolicy,
  networkAllowedDomainsFromUrls,
  canonicalAbsolutePath,
  writablePathFor,
  readablePathFor,
  createPolicyEnforcedSandbox,
  buildBubblewrapCommand,
  assertMappedToolPolicyRoots,
  buildMappedToolBubblewrapCommand,
  buildSandboxFilesystemRootPreflightCommand,
  buildSkillsAliasProbeCommand,
  canonicalizeVirtualPathUnderRoot,
} from './write-policy.js';
export type {
  WritePolicyMount,
  WritePolicyDenial,
  BuildSandboxWritePolicyOptions,
  MappedToolRuntimeBind,
} from './write-policy.js';

export {
  MAX_SKILL_BINDINGS,
  uniqueSkillMaterializations,
  uniqueSkillExactPins,
  skillExactPinKey,
  validateSkillDescriptor,
  isSafeSkillName,
  assertSafeSkillName,
  validateSkillBundlePath,
  composeSkillsCatalog,
  isPinnedSkillsCatalog,
  validateSkillBundle,
  planSkillBundleChmod,
  skillBundleDirectories,
} from './skills-materialize.js';
export type {
  MaterializableSkillDescriptor,
  MaterializableBundle,
  MaterializableBundleFile,
  MaterializableBundleManifestEntry,
} from './skills-materialize.js';

export { InMemorySandboxRuntime, asInMemoryHandle } from './in-memory/runtime.js';

export {
  LocalSandboxRuntime,
  asLocalSandboxHandle,
  createManagedToolSandboxManager,
} from './local/runtime.js';
export type {
  LocalSandboxRuntimeOptions,
  SandboxManagerLike,
  SandboxManagerInitConfig,
} from './local/runtime.js';

// Re-export the upstream `SandboxManager` (the concrete `srt`-backed manager the
// `LocalSandboxRuntime` drives) so a consumer that wires the Local runtime — the
// harness-server AND the session-runner — gets it from THIS package rather than
// taking its own direct dependency on `@anthropic-ai/sandbox-runtime`. That keeps
// the sandbox boundary (and its single upstream pin) owned in one place, which is
// the whole point of extracting this package. It structurally satisfies
// {@link SandboxManagerLike} (static `initialize` + `wrapWithSandbox`).
export { SandboxManager } from '@anthropic-ai/sandbox-runtime';
export { acquireSessionWorkDir, releaseSessionWorkDir } from './local/materialize.js';
export type { SessionWorkDirLayout, SessionWorkDirOptions } from './local/materialize.js';

export {
  RESOURCE_CHUNK_BYTES,
  RESOURCE_MAX_FILE_BYTES,
  RESOURCE_MAX_TOTAL_BYTES,
  RESOURCE_MAX_FILES,
  RESOURCE_MAX_MOUNTS,
  RESOURCE_CHECKPOINT_EVENT,
  OUTPUT_RESOURCE_ID,
  parseResourceCheckpoint,
  resourceCheckpointDigest,
  parseResourceManifest,
  resourceManifestDigest,
  resourceFilePath,
  decodeResourceChunk,
} from './resource-transfer.js';
export type {
  ResourceManifest,
  GitProxyCapability,
  ResourceMountDescriptor,
  ResourceFileDescriptor,
  ResourcePush,
  ResourceCheckpoint,
  ChangedResourceFile,
  DeletedResourceFile,
} from './resource-transfer.js';

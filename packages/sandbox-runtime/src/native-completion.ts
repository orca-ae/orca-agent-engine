// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';

export const NATIVE_USAGE_SOURCE = 'pi-0.87.1-anthropic-files-v1';
export const NATIVE_COMPLETION_EVENT = 'orca.native_completion';
export const NATIVE_REPORT_EVENT = 'orca.native_usage';
export const NATIVE_MAX_REPORTS = 1000;
export const NATIVE_FILE_TOOLS = ['read', 'write', 'edit', 'glob', 'grep', 'list'] as const;
const PUBLIC_EVENTS = new Set([
  'agent.message',
  'agent.tool_use',
  'agent.tool_result',
  'agent.requires_action',
  'agent.status',
  'agent.error',
]);

export interface NativeUsageReport {
  id: string;
  provider: 'anthropic';
  model?: string;
  semantics: 'delta';
  complete: boolean;
  usage?: {
    input_tokens: number;
    output_tokens: number;
    cache_read_input_tokens: number;
    cache_creation: { ephemeral_5m_input_tokens: number; ephemeral_1h_input_tokens: number };
  };
}

export interface NativeCompletion {
  version: 1;
  source: typeof NATIVE_USAGE_SOURCE;
  turn_id: string;
  execution_id: string;
  placement_id: string;
  outcome: 'success' | 'failed' | 'interrupted';
  report_set_sealed: boolean;
  events?: Array<Record<string, unknown>>;
  error?: string;
  expected_reports: string[];
  reports: NativeUsageReport[];
}

function object(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value))
    throw new Error('Invalid native completion');
  return value as Record<string, unknown>;
}
function identity(value: unknown): string {
  if (typeof value !== 'string' || !/^[A-Za-z0-9_-]{1,128}$/.test(value))
    throw new Error('Invalid native identity');
  return value;
}
function count(value: unknown): number {
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < 0)
    throw new Error('Invalid native usage');
  return value;
}
function onlyKeys(value: Record<string, unknown>, allowed: string[]): void {
  if (Object.keys(value).some((key) => !allowed.includes(key)))
    throw new Error('Unknown native evidence field');
}
function canonical(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(canonical);
  if (value && typeof value === 'object')
    return Object.fromEntries(
      Object.entries(value)
        .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
        .map(([key, item]) => [key, canonical(item)]),
    );
  if (typeof value === 'number' && !Number.isFinite(value)) throw new Error('Invalid native JSON');
  return value;
}
export function parseNativeUsage(value: unknown): NativeUsageReport {
  const raw = object(value);
  onlyKeys(raw, ['id', 'provider', 'model', 'semantics', 'complete', 'usage']);
  if (
    raw.provider !== 'anthropic' ||
    raw.semantics !== 'delta' ||
    typeof raw.complete !== 'boolean'
  )
    throw new Error('Unsupported native usage source');
  if (
    raw.model !== undefined &&
    (typeof raw.model !== 'string' || !/^[a-zA-Z0-9._:/-]{1,256}$/.test(raw.model))
  )
    throw new Error('Invalid native model');
  const result: NativeUsageReport = {
    id: identity(raw.id),
    provider: 'anthropic',
    semantics: 'delta',
    complete: raw.complete,
    ...(typeof raw.model === 'string' ? { model: raw.model } : {}),
  };
  if (raw.usage !== undefined) {
    const usage = object(raw.usage),
      cache = object(usage.cache_creation);
    result.usage = {
      input_tokens: count(usage.input_tokens),
      output_tokens: count(usage.output_tokens),
      cache_read_input_tokens: count(usage.cache_read_input_tokens),
      cache_creation: {
        ephemeral_5m_input_tokens: count(cache.ephemeral_5m_input_tokens),
        ephemeral_1h_input_tokens: count(cache.ephemeral_1h_input_tokens),
      },
    };
    if (
      !Number.isSafeInteger(
        result.usage.input_tokens +
          result.usage.output_tokens +
          result.usage.cache_read_input_tokens +
          result.usage.cache_creation.ephemeral_5m_input_tokens +
          result.usage.cache_creation.ephemeral_1h_input_tokens,
      )
    )
      throw new Error('Native usage overflow');
  }
  if (result.complete && (!result.usage || !result.model))
    throw new Error('Native usage evidence missing');
  return result;
}

export function parseNativeCompletion(value: unknown): NativeCompletion {
  const raw = object(value);
  onlyKeys(raw, [
    'version',
    'source',
    'turn_id',
    'execution_id',
    'placement_id',
    'outcome',
    'report_set_sealed',
    'error',
    'expected_reports',
    'reports',
    'events',
  ]);
  if (
    raw.version !== 1 ||
    raw.source !== NATIVE_USAGE_SOURCE ||
    typeof raw.report_set_sealed !== 'boolean' ||
    !['success', 'failed', 'interrupted'].includes(String(raw.outcome)) ||
    !Array.isArray(raw.expected_reports) ||
    !Array.isArray(raw.reports) ||
    raw.expected_reports.length > NATIVE_MAX_REPORTS ||
    raw.reports.length > NATIVE_MAX_REPORTS
  )
    throw new Error('Unsupported native completion');
  const expected = raw.expected_reports.map(identity);
  if (new Set(expected).size !== expected.length) throw new Error('Duplicate expected report');
  const reports = raw.reports.map(parseNativeUsage);
  if (
    new Set(reports.map(({ id }) => id)).size !== reports.length ||
    reports.some(({ id }) => !expected.includes(id))
  )
    throw new Error('Native report set differs');
  if (raw.error !== undefined && (typeof raw.error !== 'string' || raw.error.length > 4096))
    throw new Error('Invalid native outcome');
  let events: Array<Record<string, unknown>> | undefined;
  if (raw.events !== undefined) {
    if (!Array.isArray(raw.events) || raw.events.length > 10000)
      throw new Error('Native event replay exceeds limits');
    const ids = new Set<string>();
    events = raw.events.map((value) => {
      const event = object(value);
      const id = identity(event.id);
      if (
        !id.startsWith('evt_') ||
        ids.has(id) ||
        typeof event.type !== 'string' ||
        !PUBLIC_EVENTS.has(event.type) ||
        event.subpath
      )
        throw new Error('Invalid native public event');
      ids.add(id);
      return canonical(event) as Record<string, unknown>;
    });
  }
  return {
    version: 1,
    source: NATIVE_USAGE_SOURCE,
    turn_id: identity(raw.turn_id),
    execution_id: identity(raw.execution_id),
    placement_id: identity(raw.placement_id),
    outcome: raw.outcome as NativeCompletion['outcome'],
    report_set_sealed: raw.report_set_sealed,
    ...(events ? { events } : {}),
    ...(typeof raw.error === 'string' ? { error: raw.error } : {}),
    expected_reports: [...expected].sort(),
    reports: reports.sort((a, b) => a.id.localeCompare(b.id)),
  };
}

/** Compare executable policy and bindings without restarting native history for a JWT refresh. */
export function nativeSnapshotFingerprint(snapshot: {
  provider: string;
  model: unknown;
  system: string;
  allowed_tool_names: readonly string[];
  allowed_mcp_server_names: readonly string[];
  tool_permissions?: unknown;
  default_tool_permission?: unknown;
  guardrails?: unknown;
  sandbox_resources?: unknown;
  egress: unknown;
}): string {
  const egress = snapshot.egress as { mode?: unknown; gateway?: { llm_base_url?: unknown } } | null;
  return createHash('sha256')
    .update(
      JSON.stringify(
        canonical({
          provider: snapshot.provider,
          model: snapshot.model,
          system: snapshot.system,
          tools: snapshot.allowed_tool_names,
          mcp: snapshot.allowed_mcp_server_names,
          permissions: snapshot.tool_permissions ?? {},
          defaultPermission: snapshot.default_tool_permission ?? 'always_ask',
          guardrails: snapshot.guardrails ?? [],
          resources: snapshot.sandbox_resources,
          egress: { mode: egress?.mode, baseURL: egress?.gateway?.llm_base_url },
        }),
      ),
    )
    .digest('hex');
}

export function nativeCompletionIsComplete(value: NativeCompletion): boolean {
  return (
    value.report_set_sealed &&
    (value.outcome !== 'success' || value.reports.length > 0) &&
    value.expected_reports.length === value.reports.length &&
    value.reports.every((report) => report.complete)
  );
}

export function nativeCompletionDigest(value: NativeCompletion): string {
  return createHash('sha256')
    .update(JSON.stringify(parseNativeCompletion(value)))
    .digest('hex');
}

/** Inspect the effective surface; never remove requested tools to qualify a session. */
export function supportsNativeFileCompletion(snapshot: {
  provider: string;
  model: { provider: string };
  allowed_tool_names: readonly string[];
  allowed_mcp_server_names: readonly string[];
  custom_tools?: readonly unknown[];
  multiagent?: unknown;
  skills?: readonly unknown[];
  skills_plugin_dir?: string;
  custom_spec?: unknown;
}): boolean {
  return (
    snapshot.provider === 'pi' &&
    snapshot.model.provider === 'anthropic' &&
    snapshot.allowed_tool_names.every((name) =>
      (NATIVE_FILE_TOOLS as readonly string[]).includes(name),
    ) &&
    snapshot.allowed_mcp_server_names.length === 0 &&
    !snapshot.custom_tools?.length &&
    !snapshot.multiagent &&
    !snapshot.skills?.length &&
    snapshot.skills_plugin_dir === undefined &&
    snapshot.custom_spec === undefined
  );
}

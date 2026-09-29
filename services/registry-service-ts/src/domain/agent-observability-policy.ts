// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/** Shared non-secret observability policy vocabulary. */
// Ordered capabilities; legacy redacted_io never grants unredacted body capture.
export const AGENT_OBSERVABILITY_CAPTURE_MODES = [
  'metadata_only',
  'redacted_io',
  'raw_io',
] as const;
export const AGENT_OBSERVABILITY_ADAPTERS = ['otlp_http', 'langfuse_sdk'] as const;
export const AGENT_OBSERVABILITY_ENDPOINT_KINDS = ['traces_endpoint', 'base_endpoint'] as const;
export const AGENT_OBSERVABILITY_ENDPOINT_CLASSES = ['public', 'private'] as const;
export const AGENT_OBSERVABILITY_BINDING_STATUSES = [
  'active',
  'draining',
  'disabled',
  'archived',
] as const;
export const AGENT_OBSERVABILITY_SEMANTIC_PROFILES = ['otel_genai', 'langfuse'] as const;
export const AGENT_OBSERVABILITY_PROTOCOLS = ['http/protobuf', 'http/json', 'sdk'] as const;
export const AGENT_OBSERVABILITY_COMPRESSIONS = ['none', 'gzip'] as const;
export const AGENT_OBSERVABILITY_WORKSPACE_MODES = ['inherit', 'disabled', 'custom'] as const;
export const AGENT_OBSERVABILITY_DISABLED_REASONS = [
  'no_default_binding',
  'no_organization_default',
  'workspace_disabled',
  'organization_archived',
  'workspace_archived',
  'binding_draining',
  'binding_disabled',
  'binding_archived',
  'binding_configuration_invalid',
  'platform_adapter_disallowed',
  'platform_endpoint_class_disallowed',
  'credential_not_configured',
] as const;

export type AgentObservabilityCaptureMode = (typeof AGENT_OBSERVABILITY_CAPTURE_MODES)[number];
export type AgentObservabilityAdapter = (typeof AGENT_OBSERVABILITY_ADAPTERS)[number];
export type AgentObservabilityEndpointKind = (typeof AGENT_OBSERVABILITY_ENDPOINT_KINDS)[number];
export type AgentObservabilityEndpointClass = (typeof AGENT_OBSERVABILITY_ENDPOINT_CLASSES)[number];
export type AgentObservabilityBindingStatus = (typeof AGENT_OBSERVABILITY_BINDING_STATUSES)[number];
export type AgentObservabilitySemanticProfile =
  (typeof AGENT_OBSERVABILITY_SEMANTIC_PROFILES)[number];
export type AgentObservabilityProtocol = (typeof AGENT_OBSERVABILITY_PROTOCOLS)[number];
export type AgentObservabilityCompression = (typeof AGENT_OBSERVABILITY_COMPRESSIONS)[number];
export type WorkspaceObservabilityMode = (typeof AGENT_OBSERVABILITY_WORKSPACE_MODES)[number];
export type AgentObservabilityDisabledReason =
  (typeof AGENT_OBSERVABILITY_DISABLED_REASONS)[number];

function isMember<T extends readonly string[]>(values: T, value: unknown): value is T[number] {
  return typeof value === 'string' && (values as readonly string[]).includes(value);
}

export function isAgentObservabilityCaptureMode(
  value: unknown,
): value is AgentObservabilityCaptureMode {
  return isMember(AGENT_OBSERVABILITY_CAPTURE_MODES, value);
}

export function isAgentObservabilityAdapter(value: unknown): value is AgentObservabilityAdapter {
  return isMember(AGENT_OBSERVABILITY_ADAPTERS, value);
}

export function isAgentObservabilityEndpointKind(
  value: unknown,
): value is AgentObservabilityEndpointKind {
  return isMember(AGENT_OBSERVABILITY_ENDPOINT_KINDS, value);
}

export function isAgentObservabilityEndpointClass(
  value: unknown,
): value is AgentObservabilityEndpointClass {
  return isMember(AGENT_OBSERVABILITY_ENDPOINT_CLASSES, value);
}

export function isAgentObservabilityBindingStatus(
  value: unknown,
): value is AgentObservabilityBindingStatus {
  return isMember(AGENT_OBSERVABILITY_BINDING_STATUSES, value);
}

export function isAgentObservabilitySemanticProfile(
  value: unknown,
): value is AgentObservabilitySemanticProfile {
  return isMember(AGENT_OBSERVABILITY_SEMANTIC_PROFILES, value);
}

export function isAgentObservabilityProtocol(value: unknown): value is AgentObservabilityProtocol {
  return isMember(AGENT_OBSERVABILITY_PROTOCOLS, value);
}

export function isAgentObservabilityCompression(
  value: unknown,
): value is AgentObservabilityCompression {
  return isMember(AGENT_OBSERVABILITY_COMPRESSIONS, value);
}

export function isWorkspaceObservabilityMode(value: unknown): value is WorkspaceObservabilityMode {
  return isMember(AGENT_OBSERVABILITY_WORKSPACE_MODES, value);
}

/** Lowest requested/allowed capture level wins. */
export function effectiveCaptureMode(
  requested: AgentObservabilityCaptureMode,
  platformMaximum: AgentObservabilityCaptureMode,
  organizationCeiling: AgentObservabilityCaptureMode,
  workspaceCeiling?: AgentObservabilityCaptureMode,
): AgentObservabilityCaptureMode {
  const modes = [requested, platformMaximum, organizationCeiling, workspaceCeiling].filter(
    (mode): mode is AgentObservabilityCaptureMode => mode !== undefined,
  );
  return modes.reduce((minimum, mode) =>
    isAgentObservabilityCaptureModeAtMost(mode, minimum) ? mode : minimum,
  );
}

/** Whether one concrete capture mode is no broader than a configured ceiling. */
export function isAgentObservabilityCaptureModeAtMost(
  captureMode: AgentObservabilityCaptureMode,
  ceiling: AgentObservabilityCaptureMode,
): boolean {
  return (
    AGENT_OBSERVABILITY_CAPTURE_MODES.indexOf(captureMode) <=
    AGENT_OBSERVABILITY_CAPTURE_MODES.indexOf(ceiling)
  );
}

/** Only scope-ceiling reductions advance the sticky capture restriction epoch. */
export function captureRestrictionEpochAfterReplacement(
  previous: AgentObservabilityCaptureMode,
  next: AgentObservabilityCaptureMode,
  epoch: number,
): number {
  return isAgentObservabilityCaptureModeAtMost(previous, next) ? epoch : epoch + 1;
}

/** Adapter/profile/protocol/project combinations supported by released policy. */
export function isValidAgentObservabilityAdapterConfiguration(
  adapter: AgentObservabilityAdapter,
  endpointKind: AgentObservabilityEndpointKind,
  externalProjectId: string | null,
  profile: AgentObservabilitySemanticProfile,
  protocol: AgentObservabilityProtocol,
): boolean {
  return (
    (adapter === 'otlp_http' &&
      (profile === 'otel_genai' || profile === 'langfuse') &&
      (protocol === 'http/protobuf' || protocol === 'http/json') &&
      (profile !== 'langfuse' || externalProjectId !== null)) ||
    (adapter === 'langfuse_sdk' &&
      endpointKind === 'base_endpoint' &&
      externalProjectId !== null &&
      profile === 'langfuse' &&
      protocol === 'sdk')
  );
}

export function disabledReasonForBindingStatus(
  status: Exclude<AgentObservabilityBindingStatus, 'active'>,
): Extract<AgentObservabilityDisabledReason, `binding_${string}`> {
  switch (status) {
    case 'draining':
      return 'binding_draining';
    case 'disabled':
      return 'binding_disabled';
    case 'archived':
      return 'binding_archived';
  }
}

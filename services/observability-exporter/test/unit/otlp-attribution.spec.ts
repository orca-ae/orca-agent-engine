// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { encodeLangfuseOtlpJson } from '../../src/otlp-json.js';
import { completedProjectedTrace } from '../support/events.js';

const context = {
  organizationId: 'org_registry',
  bindingId: 'aob_registry',
  bindingVersion: 2,
  adapterType: 'otlp_http',
  endpointKind: 'traces_endpoint',
  endpointClass: 'public',
  endpointUrl: 'https://collector.example/api/public/otel/v1/traces',
  semanticProfile: 'langfuse',
  protocol: 'http/json',
  compression: 'none',
  timeoutMs: 1000,
  captureMode: 'metadata_only',
  sampleRate: 1,
  configSchemaVersion: 1,
  agentId: 'agt_registry',
  agentVersion: 7,
  harness: 'claude_code',
  harnessMode: 'colocated',
  environment: 'test',
  release: 'release-2026.09',
};

describe('pinned trace attribution', () => {
  it('propagates trusted agent, harness and binding configuration to every observation', () => {
    const trace = completedProjectedTrace();
    const spans = encodeLangfuseOtlpJson(trace, context).resourceSpans[0]!.scopeSpans[0]!.spans;
    for (const span of spans) {
      expect(span.attributes).toEqual(
        expect.arrayContaining([
          {
            key: 'langfuse.observation.metadata.orca.agent.id',
            value: { stringValue: 'agt_registry' },
          },
          { key: 'langfuse.observation.metadata.orca.agent.version', value: { intValue: '7' } },
          {
            key: 'langfuse.observation.metadata.orca.harness.name',
            value: { stringValue: 'claude_code' },
          },
          {
            key: 'langfuse.observation.metadata.orca.harness.mode',
            value: { stringValue: 'colocated' },
          },
          {
            key: 'langfuse.observation.metadata.orca.deployment.environment',
            value: { stringValue: 'test' },
          },
          {
            key: 'langfuse.observation.metadata.orca.deployment.release',
            value: { stringValue: 'release-2026.09' },
          },
          {
            key: 'langfuse.observation.metadata.orca.observability.binding_id',
            value: { stringValue: 'aob_registry' },
          },
          {
            key: 'langfuse.observation.metadata.orca.observability.binding_version',
            value: { intValue: '2' },
          },
          {
            key: 'langfuse.observation.metadata.orca.observability.config_schema_version',
            value: { intValue: '1' },
          },
          { key: 'langfuse.version', value: { stringValue: '7' } },
          { key: 'langfuse.environment', value: { stringValue: 'test' } },
          { key: 'langfuse.release', value: { stringValue: 'release-2026.09' } },
        ]),
      );
    }
    expect(JSON.stringify(spans)).not.toContain('orca.harness.version');
    expect(trace.root.metadata).not.toHaveProperty('orca.agent.id');
  });

  it('does not invent agent/config versions for legacy callers without a pinned context', () => {
    const wire = JSON.stringify(encodeLangfuseOtlpJson(completedProjectedTrace()));
    expect(wire).not.toContain('orca.agent.version');
    expect(wire).not.toContain('langfuse.version');
    expect(wire).not.toContain('langfuse.release');
    expect(wire).not.toContain('langfuse.environment');
  });

  it('does not enrich an old queued context which predates attribution capture', () => {
    const { agentId, agentVersion, harness, harnessMode, environment, release, ...legacy } =
      context;
    void [agentId, agentVersion, harness, harnessMode, environment, release];
    const trace = completedProjectedTrace();
    expect(encodeLangfuseOtlpJson(trace, legacy)).toEqual(encodeLangfuseOtlpJson(trace));
  });
});

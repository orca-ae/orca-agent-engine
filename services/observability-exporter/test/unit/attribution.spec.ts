// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { Pool } from 'pg';
import { describe, expect, it, vi } from 'vitest';
import { parsePinnedDeliveryContext } from '../../src/canonical-validation.js';
import {
  initialKafkaCheckpoint,
  parseKafkaCheckpoint,
  parseKafkaDeliveryRecord,
} from '../../src/kafka-state.js';
import { initialKafkaState, parseKafkaStateHead } from '../../src/kafka-state-v2.js';
import { ObservabilityExporterRepository } from '../../src/persistence.js';
import { RegistryObservabilityClient } from '../../src/registry-client.js';
import { completedProjectedTrace } from '../support/events.js';
import { enabledRegistryContext } from '../support/registry.js';

async function registryContext() {
  const raw = enabledRegistryContext();
  const config = (raw.binding as { config: Record<string, unknown> }).config;
  config.environment = 'prod';
  config.release = 'v1.2.3+build-42';
  const client = new RegistryObservabilityClient({
    internalBaseUrl: 'https://registry.internal',
    tokenProvider: async () => 'test-token',
    fetchImpl: async () => Response.json(raw),
  });
  const result = await client.resolveContext({
    workspaceId: 'ws_registry',
    sessionId: 'ses_registry',
  });
  if (result.status !== 'enabled') throw new Error('expected enabled');
  return result.deliveryContext;
}

describe('pinned attribution', () => {
  it('round trips Registry labels through both Kafka checkpoint formats and delivery records', async () => {
    const context = await registryContext();
    const trace = completedProjectedTrace();
    const route = {
      workspaceId: trace.workspaceId,
      sessionId: trace.sessionId,
      topic: 'transcript',
    };
    const expected = {
      ...context,
      agentId: 'agt_registry',
      agentVersion: 1,
      harness: 'claude_code',
      harnessMode: 'colocated',
      environment: 'prod',
      release: 'v1.2.3+build-42',
    };
    const checkpoint = initialKafkaCheckpoint(route, context);
    expect(
      parseKafkaCheckpoint(JSON.parse(JSON.stringify(checkpoint)), route).deliveryContext,
    ).toEqual(expected);
    const state = initialKafkaState(route, context, 'checkpoint-key');
    expect(parseKafkaStateHead(state.headValue, route).deliveryContext).toEqual(expected);
    const record = { version: 1, trace, deliveryContext: context };
    expect(parseKafkaDeliveryRecord(JSON.parse(JSON.stringify(record))).deliveryContext).toEqual(
      expected,
    );
  });

  it('preserves attribution on a fresh SQL repository claiming a persisted JSONB outbox row', async () => {
    const context = await registryContext();
    const trace = completedProjectedTrace();
    const persisted = JSON.stringify({
      id: '1',
      organization_id: context.organizationId,
      workspace_id: trace.workspaceId,
      session_id: trace.sessionId,
      binding_id: context.bindingId,
      binding_version: context.bindingVersion,
      trace_id: trace.traceId,
      canonical_trace: trace,
      delivery_context: context,
      delivery_attempt_count: '1',
      lease_owner: 'worker',
      lease_generation: '2',
    });
    const query = vi.fn(async (sql: string) => {
      if (sql.includes('SELECT o.binding_id')) return { rows: [{ binding_id: context.bindingId }] };
      if (sql.includes('WITH candidate')) return { rows: [JSON.parse(persisted)] };
      return { rows: [], rowCount: 1 };
    });
    for (let restart = 0; restart < 2; restart++) {
      const repository = new ObservabilityExporterRepository({
        connect: async () => ({ query, release: vi.fn() }),
        query,
      } as unknown as Pool);
      const item = await repository.claimOutbox('worker', 60_000);
      expect(item?.deliveryContext).toEqual(context);
    }
  });

  it('preserves old absent fields and rejects unsafe persisted labels rather than rewriting them', async () => {
    const context = await registryContext();
    const { agentId, agentVersion, harness, harnessMode, environment, release, ...legacy } =
      context;
    expect({ agentId, agentVersion, harness, harnessMode, environment, release }).toEqual({
      agentId: 'agt_registry',
      agentVersion: 1,
      harness: 'claude_code',
      harnessMode: 'colocated',
      environment: 'prod',
      release: 'v1.2.3+build-42',
    });
    expect(JSON.stringify(parsePinnedDeliveryContext(legacy))).toBe(JSON.stringify(legacy));
    expect(
      parsePinnedDeliveryContext({ ...legacy, agentId: 'agt_sk-SYNTHETIC_CANARY' }),
    ).toHaveProperty('agentId', 'agt_sk-SYNTHETIC_CANARY');
    for (const key of ['harness', 'harnessMode', 'environment', 'release']) {
      for (const value of ['sk-SYNTHETIC_CANARY', 'ghp_SYNTHETIC', 'eyJhbGci.payload.signature']) {
        expect(parsePinnedDeliveryContext({ ...legacy, [key]: value })).toHaveProperty(key, value);
      }
    }
    for (const key of ['agentId', 'harness', 'harnessMode', 'environment', 'release']) {
      for (const value of [
        null,
        {},
        2,
        '',
        'x'.repeat(129),
        'user@example.com',
        'token=canary',
        'two words',
      ]) {
        expect(() => parsePinnedDeliveryContext({ ...legacy, [key]: value })).toThrow(
          'delivery context is invalid',
        );
      }
    }
    for (const agentVersion of [null, 0, -1, 1.5, Number.MAX_SAFE_INTEGER + 1, '1']) {
      expect(() => parsePinnedDeliveryContext({ ...legacy, agentVersion })).toThrow();
    }
    expect(
      parsePinnedDeliveryContext({
        ...legacy,
        agentVersion: Number.MAX_SAFE_INTEGER,
        environment: 'x'.repeat(128),
        agentId: 'agt_' + 'x'.repeat(124),
      }),
    ).toMatchObject({
      agentVersion: Number.MAX_SAFE_INTEGER,
      environment: 'x'.repeat(128),
      agentId: 'agt_' + 'x'.repeat(124),
    });
  });
});

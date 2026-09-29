// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import {
  triggerCreateBodySchema,
  triggerSourceSchema,
  triggerUpdateBodySchema,
} from '../../src/contracts/triggers.contract.js';

const validCreate = {
  name: 'weekday-report',
  agent: 'agt_01',
  session_mode: 'SESSION_PER_EVENT',
  source: {
    type: 'cron',
    schedule: '0 9 * * 1-5',
    timezone: 'Asia/Shanghai',
    payload: 'Create the daily report.',
  },
  session: { environment_id: 'env_01' },
} as const;

describe('Trigger contract', () => {
  it('accepts only the cron source and session-per-fire v1 shape', () => {
    expect(triggerCreateBodySchema.safeParse(validCreate).success).toBe(true);
    expect(
      triggerCreateBodySchema.safeParse({
        ...validCreate,
        agent: { type: 'agent', id: 'agt_01', version: 3 },
        replicas: 1,
        paused: true,
        session: {
          environment_id: 'env_01',
          title_template: 'Report ${payload}',
          metadata: { owner: 'finance' },
          vault_ids: ['vlt_01'],
        },
      }).success,
    ).toBe(true);

    for (const extension of [
      { source: { type: 'kafka', topic: 'jobs' } },
      { session_mode: 'SHARED' },
      { replicas: 2 },
      { payload: 'flattened payload' },
      { schedule: { type: 'cron', expression: '* * * * *' } },
      { environment_id: 'env_flattened' },
      { resources: [] },
      { workspace_id: 'ws_other' },
    ]) {
      expect(triggerCreateBodySchema.safeParse({ ...validCreate, ...extension }).success).toBe(
        false,
      );
    }
  });

  it('does not allow an update to replace the pinned agent', () => {
    expect(
      triggerUpdateBodySchema.safeParse({
        session_mode: 'SESSION_PER_EVENT',
        source: { type: 'cron', payload: 'new payload' },
        session: { metadata: { owner: 'finance' } },
        replicas: 1,
      }).success,
    ).toBe(true);
    expect(triggerUpdateBodySchema.safeParse({ agent: 'agt_other' }).success).toBe(false);
  });

  it('requires the source discriminator to be cron', () => {
    expect(
      triggerSourceSchema.safeParse({
        type: 'cron',
        schedule: '* * * * *',
        payload: 'tick',
      }).success,
    ).toBe(true);
    expect(
      triggerSourceSchema.safeParse({ type: 'pulsar', schedule: '* * * * *', payload: 'tick' })
        .success,
    ).toBe(false);
  });
});

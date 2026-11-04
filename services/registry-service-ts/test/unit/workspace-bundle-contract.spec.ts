// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { buildOpenApiDocument } from '../../src/contracts/openapi-schema.js';
import { CLAUDE_SESSION_EVENT_TYPES } from '../../src/domain/events.js';
import { shouldEmitClaudeStreamFrame } from '../../src/streaming/sse.js';

describe('workspace bundle HTTP contract', () => {
  it('publishes renewal headers on 200 and bodyless 304 responses', () => {
    const operation = buildOpenApiDocument({ version: 'test' }).paths[
      '/apis/policy.runorca.ai/v1/effectiveguardrails'
    ]!.get!;
    for (const status of ['200', '304']) {
      expect(operation.responses[status]).toMatchObject({
        headers: {
          ETag: { required: true },
          'Orca-Guardrail-Issued-At': { schema: { format: 'date-time' } },
          'Orca-Guardrail-Expires-At': { required: true },
          'Orca-Guardrail-Generation': { required: true },
          'Cache-Control': { schema: { enum: ['no-store'] } },
        },
      });
    }
    expect(operation.responses['304']).not.toHaveProperty('content');
  });
  it('keeps observer evidence outside default Claude event views', () => {
    expect(CLAUDE_SESSION_EVENT_TYPES.has('session.guardrail_decision')).toBe(false);
    expect(shouldEmitClaudeStreamFrame('session.guardrail_decision')).toBe(false);
  });
});

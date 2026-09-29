// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import {
  SESSION_LLM_EGRESS_KEY,
  sessionLlmEgressMetadataError,
} from '../../src/domain/session-llm-egress.js';

describe('Session LLM egress metadata', () => {
  it('keeps direct egress by default and accepts explicit gateway selection', () => {
    expect(sessionLlmEgressMetadataError({})).toBeNull();
    expect(sessionLlmEgressMetadataError({ [SESSION_LLM_EGRESS_KEY]: 'direct' })).toBeNull();
    expect(sessionLlmEgressMetadataError({ [SESSION_LLM_EGRESS_KEY]: 'gateway' })).toBeNull();
  });

  it('rejects an unsupported egress value', () => {
    expect(sessionLlmEgressMetadataError({ [SESSION_LLM_EGRESS_KEY]: 'other' })).toContain(
      'must be "direct" or "gateway"',
    );
  });
});

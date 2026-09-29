// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { acceptedPrefixesFor, toInternalId, toWireId } from '../../src/contracts/id-prefix.js';
import { idString } from '../../src/contracts/common.js';

describe('toInternalId', () => {
  it('rewrites Claude wire prefixes to internal prefixes', () => {
    expect(toInternalId('agent_ABC')).toBe('agt_ABC');
    expect(toInternalId('sesn_XYZ')).toBe('ses_XYZ');
  });

  it('leaves internal and same-prefix ids untouched', () => {
    expect(toInternalId('agt_ABC')).toBe('agt_ABC');
    expect(toInternalId('ses_XYZ')).toBe('ses_XYZ');
    expect(toInternalId('env_1')).toBe('env_1');
    expect(toInternalId('skill_1')).toBe('skill_1');
    expect(toInternalId('skillver_1')).toBe('skillver_1');
    expect(toInternalId('vlt_1')).toBe('vlt_1');
  });
});

describe('toWireId', () => {
  it('rewrites internal prefixes to Claude wire prefixes for default clients', () => {
    expect(toWireId('agt_ABC', false)).toBe('agent_ABC');
    expect(toWireId('ses_XYZ', false)).toBe('sesn_XYZ');
  });

  it('keeps internal ids for orca-beta clients', () => {
    expect(toWireId('agt_ABC', true)).toBe('agt_ABC');
    expect(toWireId('ses_XYZ', true)).toBe('ses_XYZ');
  });
});

describe('acceptedPrefixesFor', () => {
  it('includes the wire alias when it differs from the internal prefix', () => {
    expect(acceptedPrefixesFor('agt')).toEqual(['agt', 'agent']);
    expect(acceptedPrefixesFor('ses')).toEqual(['ses', 'sesn']);
  });

  it('returns only the prefix itself when there is no distinct wire alias', () => {
    expect(acceptedPrefixesFor('vlt')).toEqual(['vlt']);
    expect(acceptedPrefixesFor('env')).toEqual(['env']);
    expect(acceptedPrefixesFor('skill')).toEqual(['skill']);
    expect(acceptedPrefixesFor('skillver')).toEqual(['skillver']);
  });
});

describe('idString accepts both internal and wire encodings', () => {
  it('parses both agt_ and agent_ ids', () => {
    const schema = idString('agt');
    expect(schema.safeParse('agt_ABC').success).toBe(true);
    expect(schema.safeParse('agent_ABC').success).toBe(true);
    expect(schema.safeParse('foo_ABC').success).toBe(false);
  });
});

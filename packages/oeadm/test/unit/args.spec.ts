// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect } from 'vitest';
import { parseArgs } from '../../src/args.js';

describe('parseArgs', () => {
  it('parses long flags with values', () => {
    const parsed = parseArgs(['--agent', 'agt_1', '--environment', 'env_1']);
    expect(parsed.options).toEqual({ agent: 'agt_1', environment: 'env_1' });
    expect(parsed.positionals).toEqual([]);
  });

  it('parses --flag=value form', () => {
    const parsed = parseArgs(['--name=my laptop', '--target=self_hosted']);
    expect(parsed.options).toEqual({ name: 'my laptop', target: 'self_hosted' });
  });

  it('collects positionals separately from flags', () => {
    const parsed = parseArgs(['create', '--name', 'x']);
    expect(parsed.positionals).toEqual(['create']);
    expect(parsed.options).toEqual({ name: 'x' });
  });

  it('treats a trailing valueless flag as a boolean true', () => {
    const parsed = parseArgs(['--verbose']);
    expect(parsed.options).toEqual({ verbose: true });
  });

  it('treats a flag followed by another flag as boolean true', () => {
    const parsed = parseArgs(['--verbose', '--name', 'x']);
    expect(parsed.options).toEqual({ verbose: true, name: 'x' });
  });
});

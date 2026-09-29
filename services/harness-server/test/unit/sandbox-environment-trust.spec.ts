// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { assertSandboxEnvironmentTrust } from '../../src/sandbox/environment-trust.js';

describe('Skill execution environment trust boundary', () => {
  it('rejects a custom in-sandbox image even without Skills', () => {
    expect(() =>
      assertSandboxEnvironmentTrust({
        mode: 'colocated',
        image: 'customer.example/agent:latest',
      }),
    ).toThrow(/custom in-sandbox images/);
  });

  it('rejects package installers before any managed sandbox setup', () => {
    expect(() =>
      assertSandboxEnvironmentTrust({
        mode: 'separate',
        packages: { npm: ['untrusted-package'] },
      }),
    ).toThrow(/package installers/);
  });

  it('allows the operator catalog image and empty package declarations', () => {
    expect(() =>
      assertSandboxEnvironmentTrust({
        mode: 'colocated',
        image: null,
        packages: { npm: [] },
      }),
    ).not.toThrow();
  });

  it('allows separate mode to ignore an Environment image when no installer runs', () => {
    expect(() =>
      assertSandboxEnvironmentTrust({
        mode: 'separate',
        image: 'customer.example/unused-in-separate-mode:latest',
      }),
    ).not.toThrow();
  });
});

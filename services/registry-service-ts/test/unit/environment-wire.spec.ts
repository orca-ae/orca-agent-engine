// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import {
  EnvConfigSchema,
  configToStorage,
  hasOwn,
  legacyAptPackages,
  mergeNetworkingUpdate,
  networkingToApiConfig,
  normalizeLegacyPackages,
  normalizePackages,
  normalizeStoredPackages,
  packagesToApi,
  stripNullFields,
} from '../../src/contracts/environment-wire.js';

describe('EnvConfigSchema', () => {
  it('accepts null for each Claude package-manager request field', () => {
    expect(
      normalizePackages({
        type: 'packages',
        apt: null,
        cargo: null,
        gem: null,
        go: null,
        npm: null,
        pip: null,
      }),
    ).toEqual({});
  });

  it('accepts nullable cloud-only fields in the public request contract', () => {
    expect(
      EnvConfigSchema.parse({
        type: 'cloud',
        networking: null,
        packages: null,
      }),
    ).toEqual({ type: 'cloud' });
  });

  it('keeps nullable cloud-only fields invalid for self-hosted configs', () => {
    expect(() => EnvConfigSchema.parse({ type: 'self_hosted', networking: null })).toThrow(
      /networking is only supported for cloud configs/,
    );
    expect(() => EnvConfigSchema.parse({ type: 'self_hosted', packages: null })).toThrow(
      /packages is only supported for cloud configs/,
    );
  });
});

describe('configToStorage', () => {
  it('normalizes a limited-networking config to {target, networking}', () => {
    expect(
      configToStorage({
        type: 'cloud',
        networking: { type: 'limited', allow_package_managers: true, allowed_hosts: ['a.test'] },
      }),
    ).toEqual({
      target: 'cloud',
      networking: { type: 'limited', allow_package_managers: true, allowed_hosts: ['a.test'] },
    });
  });

  it('accepts allow_mcp_servers as a boolean', () => {
    expect(
      configToStorage({
        type: 'cloud',
        networking: { type: 'limited', allow_mcp_servers: true },
      }),
    ).toEqual({
      target: 'cloud',
      networking: { type: 'limited', allow_mcp_servers: true },
    });
  });

  it('rejects allow_mcp_servers as an array (legacy shape)', () => {
    expect(
      configToStorage({
        type: 'cloud',
        networking: { type: 'limited', allow_mcp_servers: ['some-server'] },
      }),
    ).toEqual({ error: expect.any(String) });
  });

  it('accepts unrestricted networking', () => {
    expect(configToStorage({ type: 'cloud' })).toEqual({
      target: 'cloud',
      networking: { type: 'unrestricted' },
    });
  });

  it('accepts self-hosted config without cloud-only fields', () => {
    expect(configToStorage({ type: 'self_hosted' })).toEqual({
      target: 'self_hosted',
      networking: {},
    });
  });

  it('ignores package-only config for storage projection', () => {
    expect(configToStorage({ packages: { npm: ['typescript'] } })).toBeNull();
  });

  it('treats an explicit config.networking: null as reset-to-unrestricted', () => {
    expect(configToStorage({ type: 'cloud', networking: null })).toEqual({
      target: 'cloud',
      networking: { type: 'unrestricted' },
    });
  });

  it('accepts an explicit null limited sub-field and lets it survive parsing for the merge step', () => {
    expect(
      configToStorage({
        type: 'cloud',
        networking: { type: 'limited', allow_mcp_servers: null, allowed_hosts: ['a.test'] },
      }),
    ).toEqual({
      target: 'cloud',
      networking: { type: 'limited', allow_mcp_servers: null, allowed_hosts: ['a.test'] },
    });
  });

  it('rejects a bad type or bad networking', () => {
    expect(configToStorage({ type: 'CLOUD', networking: { type: 'unrestricted' } })).toEqual({
      error: expect.any(String),
    });
    expect(configToStorage({ type: 'cloud', networking: { type: 'open' } })).toEqual({
      error: expect.any(String),
    });
    expect(configToStorage({ type: 'self_hosted', networking: { type: 'unrestricted' } })).toEqual({
      error: expect.any(String),
    });
  });
});

describe('mergeNetworkingUpdate', () => {
  it('merges a limited update onto an existing limited value, provided keys winning', () => {
    expect(
      mergeNetworkingUpdate(
        {
          type: 'limited',
          allow_mcp_servers: true,
          allow_package_managers: true,
          allowed_hosts: ['a.test'],
        },
        { type: 'limited', allowed_hosts: ['b.test'] },
      ),
    ).toEqual({
      type: 'limited',
      allow_mcp_servers: true,
      allow_package_managers: true,
      allowed_hosts: ['b.test'],
    });
  });

  it('fully replaces on an unrestricted update', () => {
    expect(
      mergeNetworkingUpdate(
        { type: 'limited', allow_mcp_servers: true, allowed_hosts: ['a.test'] },
        { type: 'unrestricted' },
      ),
    ).toEqual({ type: 'unrestricted' });
  });

  it('uses the provided value as-is when limited is applied over a non-limited existing value', () => {
    expect(
      mergeNetworkingUpdate(
        { type: 'unrestricted' },
        { type: 'limited', allowed_hosts: ['a.test'] },
      ),
    ).toEqual({ type: 'limited', allowed_hosts: ['a.test'] });
    expect(
      mergeNetworkingUpdate(
        { allowed_hosts: ['legacy'] },
        { type: 'limited', allowed_hosts: ['a.test'] },
      ),
    ).toEqual({ type: 'limited', allowed_hosts: ['a.test'] });
  });

  it('strips an explicit null sub-field after the merge, resetting it instead of persisting null', () => {
    expect(
      mergeNetworkingUpdate(
        {
          type: 'limited',
          allow_mcp_servers: true,
          allow_package_managers: true,
          allowed_hosts: ['a.test'],
        },
        { type: 'limited', allow_mcp_servers: null },
      ),
    ).toEqual({
      type: 'limited',
      allow_package_managers: true,
      allowed_hosts: ['a.test'],
    });
  });

  it('sanitizes legacy invalid known fields when preserving omitted limited fields', () => {
    expect(
      mergeNetworkingUpdate(
        {
          type: 'limited',
          allow_mcp_servers: ['legacy-server'],
          allow_package_managers: 'yes',
          allowed_hosts: ['a.test', 123, null],
          custom: 'preserved',
        },
        { type: 'limited' },
      ),
    ).toEqual({
      type: 'limited',
      allowed_hosts: ['a.test'],
      custom: 'preserved',
    });
  });

  it('keeps valid provided limited fields when existing known fields are invalid', () => {
    expect(
      mergeNetworkingUpdate(
        {
          type: 'limited',
          allow_mcp_servers: ['legacy-server'],
          allowed_hosts: ['legacy.test'],
        },
        { type: 'limited', allow_mcp_servers: false, allowed_hosts: ['current.test'] },
      ),
    ).toEqual({
      type: 'limited',
      allow_mcp_servers: false,
      allowed_hosts: ['current.test'],
    });
  });
});

describe('stripNullFields', () => {
  it('drops only null-valued keys, keeping false/empty-array/other falsy values', () => {
    expect(stripNullFields({ a: null, b: false, c: [], d: 'x' })).toEqual({
      b: false,
      c: [],
      d: 'x',
    });
  });
});

describe('hasOwn', () => {
  it('is true only for non-array objects owning the key, false for arrays/null/primitives', () => {
    expect(hasOwn({ a: 1 }, 'a')).toBe(true);
    expect(hasOwn({ a: undefined }, 'a')).toBe(true);
    expect(hasOwn({}, 'a')).toBe(false);
    expect(hasOwn(null, 'a')).toBe(false);
    expect(hasOwn(['a'], '0')).toBe(false);
    expect(hasOwn('a', 'length')).toBe(false);
  });
});

describe('networkingToApiConfig', () => {
  it('projects unrestricted networking into the Claude config shape unchanged', () => {
    expect(networkingToApiConfig('cloud', { type: 'unrestricted' })).toEqual({
      type: 'cloud',
      networking: { type: 'unrestricted' },
    });
  });

  it('normalizes sparse limited networking with the required response defaults', () => {
    expect(networkingToApiConfig('cloud', { type: 'limited', allowed_hosts: ['*'] })).toEqual({
      type: 'cloud',
      networking: {
        type: 'limited',
        allow_mcp_servers: false,
        allow_package_managers: false,
        allowed_hosts: ['*'],
      },
    });
  });

  it('lets stored values win over defaults when normalizing limited networking', () => {
    expect(
      networkingToApiConfig('cloud', {
        type: 'limited',
        allow_mcp_servers: true,
        allow_package_managers: false,
        allowed_hosts: [],
      }),
    ).toEqual({
      type: 'cloud',
      networking: {
        type: 'limited',
        allow_mcp_servers: true,
        allow_package_managers: false,
        allowed_hosts: [],
      },
    });
  });

  it('returns null for legacy untyped networking blobs', () => {
    expect(networkingToApiConfig('cloud', { allowed_hosts: ['*'] })).toBeNull();
    expect(networkingToApiConfig(null, {})).toBeNull();
  });

  it('sanitizes a legacy array-valued allow_mcp_servers to false rather than leaking it', () => {
    expect(
      networkingToApiConfig('cloud', {
        type: 'limited',
        allow_mcp_servers: ['some-server'],
        allowed_hosts: ['*'],
      }),
    ).toEqual({
      type: 'cloud',
      networking: {
        type: 'limited',
        allow_mcp_servers: false,
        allow_package_managers: false,
        allowed_hosts: ['*'],
      },
    });
  });

  it('filters non-string entries out of allowed_hosts', () => {
    expect(
      networkingToApiConfig('cloud', {
        type: 'limited',
        allowed_hosts: ['a.test', 123, null],
      }),
    ).toEqual({
      type: 'cloud',
      networking: {
        type: 'limited',
        allow_mcp_servers: false,
        allow_package_managers: false,
        allowed_hosts: ['a.test'],
      },
    });
  });

  it('never exposes unknown legacy networking types or fields in the Claude response', () => {
    expect(networkingToApiConfig('cloud', { type: 'bogus', token: 'secret' })).toBeNull();
    expect(
      networkingToApiConfig('cloud', {
        type: 'unrestricted',
        legacy: 'hidden',
      }),
    ).toEqual({
      type: 'cloud',
      networking: { type: 'unrestricted' },
    });
    expect(
      networkingToApiConfig('cloud', {
        type: 'limited',
        allowed_hosts: ['a.test'],
        legacy: 'hidden',
      }),
    ).toEqual({
      type: 'cloud',
      networking: {
        type: 'limited',
        allow_mcp_servers: false,
        allow_package_managers: false,
        allowed_hosts: ['a.test'],
      },
    });
  });
});

describe('environment package wire helpers', () => {
  it('accepts canonical package managers and drops empty arrays', () => {
    expect(
      normalizePackages({
        type: 'packages',
        apt: ['curl'],
        npm: ['typescript'],
        pip: [],
      }),
    ).toEqual({ apt: ['curl'], npm: ['typescript'] });
  });

  it('resets to empty packages for an explicit null value (config.packages: null)', () => {
    expect(normalizePackages(null)).toEqual({});
  });

  it('rejects unknown managers', () => {
    expect(() => normalizePackages({ brew: ['curl'] })).toThrow(/Unrecognized key/);
  });

  it('rejects a bad packages type discriminant', () => {
    expect(() => normalizePackages({ type: 'tools', npm: ['typescript'] })).toThrow();
  });

  it('maps legacy top-level package arrays to apt', () => {
    expect(normalizeLegacyPackages(['curl', 'jq'])).toEqual({ apt: ['curl', 'jq'] });
    expect(legacyAptPackages({ apt: ['curl'], npm: ['typescript'] })).toEqual(['curl']);
  });

  it('normalizes old stored arrays and new stored objects', () => {
    expect(normalizeStoredPackages(['curl'])).toEqual({ apt: ['curl'] });
    expect(normalizeStoredPackages({ cargo: ['ripgrep'], gem: [] })).toEqual({
      cargo: ['ripgrep'],
    });
  });

  it('projects API packages with all managers and type discriminant', () => {
    expect(packagesToApi({ pip: ['pandas'] })).toEqual({
      type: 'packages',
      apt: [],
      cargo: [],
      gem: [],
      go: [],
      npm: [],
      pip: ['pandas'],
    });
  });
});

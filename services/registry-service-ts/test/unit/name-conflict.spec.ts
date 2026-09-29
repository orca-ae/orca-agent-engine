// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { nameConflictMessage } from '../../src/persistence/postgres/name-conflict.js';

describe('name conflict errors', () => {
  it('recognizes a PostgreSQL organization name violation', () => {
    expect(nameConflictMessage({ code: '23505', constraint: 'organizations_name_idx' })).toBe(
      'organization name already exists',
    );
  });

  it('recognizes a workspace violation wrapped by Drizzle', () => {
    const error = new Error('Failed query', {
      cause: { code: '23505', constraint: 'workspaces_organization_name_idx' },
    });
    expect(nameConflictMessage(error)).toBe('workspace name already exists in this organization');
  });

  it.each([
    { code: '23505', constraint: 'organizations_audience_idx' },
    { code: '23505', constraint: 'workspaces_pkey' },
    { code: '23503', constraint: 'workspaces_organization_fk' },
    new Error('organizations_name_idx: connection lost'),
    null,
  ])('does not turn other database failures into a name conflict: %s', (error) => {
    expect(nameConflictMessage(new Error('Failed query', { cause: error }))).toBeUndefined();
  });
});

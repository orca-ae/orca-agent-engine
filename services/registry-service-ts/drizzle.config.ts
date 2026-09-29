// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { Config } from 'drizzle-kit';

export default {
  schema: './src/persistence/postgres/schema.ts',
  out: './src/persistence/postgres/migrations',
  dialect: 'postgresql',
  dbCredentials: {
    url: process.env['DATABASE_URL'] ?? 'postgres://orca:orca@localhost:5432/registry',
  },
} satisfies Config;

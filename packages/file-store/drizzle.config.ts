// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { defineConfig } from 'drizzle-kit';

export default defineConfig({
  schema: './src/metadata/postgres.ts',
  out: './src/metadata/migrations',
  dialect: 'postgresql',
  dbCredentials: {
    url: process.env['FILESTORE_DATABASE_URL'] ?? 'postgres://orca:orca@localhost:5432/filestore',
  },
});

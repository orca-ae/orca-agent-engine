// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import js from '@eslint/js';
import tseslint from 'typescript-eslint';
import prettier from 'eslint-config-prettier';

export default tseslint.config(
  { ignores: ['**/dist/**', '**/src/generated/**', '**/node_modules/**'] },
  js.configs.recommended,
  ...tseslint.configs.recommended,
  prettier,
  {
    // Build-time generator scripts run under plain Node, so `no-undef` is live
    // for them (typescript-eslint switches it off for `.ts`, where the compiler
    // already answers the question). Declared explicitly rather than pulling in
    // the `globals` package for five names.
    files: ['**/scripts/**/*.mjs'],
    languageOptions: {
      globals: {
        Buffer: 'readonly',
        URL: 'readonly',
        console: 'readonly',
        fetch: 'readonly',
        process: 'readonly',
        structuredClone: 'readonly',
      },
    },
  },
  {
    files: ['**/*.{ts,tsx,mts,cts}'],
    languageOptions: {
      ecmaVersion: 2022,
      sourceType: 'module',
      globals: { node: true, es2022: true },
    },
    rules: {
      '@typescript-eslint/no-unused-vars': [
        'error',
        {
          argsIgnorePattern: '^_',
          varsIgnorePattern: '^_',
          caughtErrorsIgnorePattern: '^_',
        },
      ],
    },
  },
  // Plain ESM `.mjs` modules loaded directly by Node (not compiled by any package build):
  // the shipped pi orca-extension and the CLI test fakes. `js.configs.recommended` enables
  // `no-undef` for these, so their Node runtime globals must be declared here. Flat config
  // does not honor `/* eslint-env node */` comments (they are removed in ESLint 10), and the
  // `globals` package is not a hoisted dependency at the repo root, so the Node globals these
  // files use are declared inline as read-only.
  {
    files: ['**/*.mjs'],
    languageOptions: {
      ecmaVersion: 2022,
      sourceType: 'module',
      globals: {
        process: 'readonly',
        console: 'readonly',
        Buffer: 'readonly',
        URL: 'readonly',
        URLSearchParams: 'readonly',
        TextEncoder: 'readonly',
        TextDecoder: 'readonly',
        structuredClone: 'readonly',
        queueMicrotask: 'readonly',
        setTimeout: 'readonly',
        clearTimeout: 'readonly',
        setInterval: 'readonly',
        clearInterval: 'readonly',
        setImmediate: 'readonly',
        clearImmediate: 'readonly',
        globalThis: 'readonly',
      },
    },
  },
);

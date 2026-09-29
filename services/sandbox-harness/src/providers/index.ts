// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Public barrel for the providers module.
//
// Consumers (session.ts, routes.ts, subprocess-entry.ts) import the provider
// surface from `./providers/index.js`, the single public entry point. The
// implementation is split for clarity — interfaces live in `./types.js`, the
// static registry (resolveProvider / listProviderMetadata) in `./registry.js` —
// and re-exported here so callers depend on one stable specifier rather than
// internal layout.

export * from './types.js';
export * from './registry.js';

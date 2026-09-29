// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Toolset name aliasing — re-exported from the shared `@orca/harness-catalog`
// composition module so the registry and harness-server share ONE canonical
// tool-name mapping. Kept as a stable import path for the registry's existing
// consumers (agent routes + snapshot builder).

export { toCanonicalToolName, toAnthropicWireToolName } from '@orca/harness-catalog';

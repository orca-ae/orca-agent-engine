// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * Edge-translation for resource id prefixes per managed-agents-2026-04-01.
 *
 * Claude wire ids use prefixes like `agent_`/`sesn_`, while orca generates and
 * stores internal prefixes `agt_`/`ses_`. The id SUFFIX is identical — only the
 * prefix differs — so translation is a pure prefix rewrite.
 *
 * Strategy (per the unit brief): ACCEPT Claude encodings on the public HTTP API
 * and normalize to the existing internal representation; keep id GENERATION and
 * the internal/harness-facing contract stable. `toWireId` is provided for an
 * opt-in outbound translation but the default serializers keep internal ids so
 * downstream consumers (harness, mesh-internal routes) are unaffected.
 */

export interface PrefixPair {
  internal: string;
  wire: string;
}

export const PREFIX_MAP: Record<string, PrefixPair> = {
  agent: { internal: 'agt', wire: 'agent' },
  session: { internal: 'ses', wire: 'sesn' },
  environment: { internal: 'env', wire: 'env' },
  // Orca-native resource with no Claude counterpart, so there is no wire alias
  // to translate — the entry exists so `idString`/`acceptedPrefixesFor` can be
  // driven from one table rather than a second, divergent list.
  guardrail: { internal: 'grd', wire: 'grd' },
  skill: { internal: 'skill', wire: 'skill' },
  skillVersion: { internal: 'skillver', wire: 'skillver' },
};

const PAIRS = Object.values(PREFIX_MAP);

/** Rewrite a Claude wire-prefixed id to its internal form (no-op otherwise). */
export function toInternalId(id: string): string {
  if (typeof id !== 'string') return id;
  for (const { internal, wire } of PAIRS) {
    if (wire !== internal && id.startsWith(`${wire}_`)) {
      return `${internal}_${id.slice(wire.length + 1)}`;
    }
  }
  return id;
}

/**
 * Rewrite an internal-prefixed id to its Claude wire form for default clients.
 * `orcaBeta` callers keep internal ids unchanged.
 */
export function toWireId(id: string, orcaBeta: boolean): string {
  if (orcaBeta || typeof id !== 'string') return id;
  for (const { internal, wire } of PAIRS) {
    if (wire !== internal && id.startsWith(`${internal}_`)) {
      return `${wire}_${id.slice(internal.length + 1)}`;
    }
  }
  return id;
}

/**
 * The set of prefixes a request edge should accept for a given internal prefix
 * (the internal prefix plus any distinct Claude wire alias). Used by `idString`
 * so the public contract tolerates both encodings.
 */
export function acceptedPrefixesFor(internalPrefix: string): string[] {
  const pair = PAIRS.find((p) => p.internal === internalPrefix);
  if (pair && pair.wire !== pair.internal) return [pair.internal, pair.wire];
  return [internalPrefix];
}

// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/** Bounded failure labels only: no native identities, model text or tool arguments. */
export function settlementFailures(
  interrupted: boolean,
  rootId: string,
  usage: ReadonlyMap<string, { started: number; completed: number }>,
  terminals: ReadonlyMap<string, string>,
): string[] {
  const reasons: string[] = [];
  if (interrupted) reasons.push('root_interrupted');
  if (!usage.size) reasons.push('no_usage');
  if (usage.has(rootId) && terminals.get(rootId) !== 'completed')
    reasons.push('root_terminal_unsuccessful');
  if ([...usage.keys()].some((id) => !terminals.has(id))) reasons.push('missing_terminal');
  if ([...usage.values()].some((value) => value.started !== value.completed))
    reasons.push('provider_requests_unmatched');
  return reasons;
}

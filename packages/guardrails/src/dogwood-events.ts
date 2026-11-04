// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0
import { mergeEvents, type PolicyEvent } from '@runorca/dogwood';
/** Actual shared-core aggregation: bounded projections, conflict rejection and approval ordering. */
export function mergeDogwoodEvents(events: readonly PolicyEvent[]): PolicyEvent[] {
  if (!events.length) return [];
  const result = mergeEvents([...events]);
  if (result.errors.length) throw new Error(result.errors.join('; '));
  return result.events;
}

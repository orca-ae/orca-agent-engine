// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

export function archivedSubpaths(value: unknown): Set<string> {
  if (value === undefined) return new Set();
  if (
    !Array.isArray(value) ||
    value.length > 128 ||
    value.some(
      (path) =>
        typeof path !== 'string' ||
        (path !== '' && !/^subagents[/][a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/.test(path)),
    )
  )
    throw new Error('Invalid DSH archived thread snapshot');
  return new Set(value as string[]);
}

/** Ancestor fences also cover descendants created or resumed after the archive command. */
export function assertNotArchived(
  subpath: string,
  archived: ReadonlySet<string>,
  parentOf: (subpath: string) => string | undefined,
): void {
  const visited = new Set<string>();
  let current = subpath;
  for (;;) {
    if (archived.has(current)) throw new Error('DSH thread is archived');
    if (current === '') return;
    if (visited.has(current)) throw new Error('Invalid DSH archive lineage');
    visited.add(current);
    const parent = parentOf(current);
    if (parent === undefined) throw new Error('DSH archive lineage unavailable');
    current = parent;
  }
}

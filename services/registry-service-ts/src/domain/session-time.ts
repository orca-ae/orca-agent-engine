// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

export function elapsedSeconds(from: Date, to: Date): number {
  return Math.max(0, Math.floor((to.getTime() - from.getTime()) / 1000));
}

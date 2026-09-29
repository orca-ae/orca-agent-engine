// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * The harness → in-sandbox provider mapping, plus harness-server's own
 * "can I actually boot this colocated?" guard.
 *
 * The MAPPING is not defined here. It lives in {@link HARNESS_CATALOG} and is
 * read through `harnessToProvider`, re-exported below: a parallel switch in this
 * file drifted from the catalog the moment the catalog grew a harness, and the
 * `satisfies never` that was supposed to catch that only ever fired into a `tsc`
 * run no CI job executes. One mapping, one place.
 *
 * The CAPABILITY question is genuinely harness-server-local and stays here: the
 * colocated path boots a `@orca/sandbox-harness` image and talks to its HTTP
 * server, so a harness the catalog marks colocated but gives no image/port
 * (today: `mock`, which the self-hosted session-runner builds in-process) cannot
 * run under harness-server at all. {@link assertHarnessServerCanRunColocated}
 * says so explicitly, and the dispatcher routes that through the session's
 * `setup_failed` path so the caller sees it instead of a wedged session.
 */
import {
  HARNESS_CATALOG,
  harnessToProvider,
  type HarnessCatalogEntry,
  type HarnessType,
} from '@orca/harness-catalog';

export { harnessToProvider };

/**
 * Throw unless harness-server can boot `harness` colocated — i.e. the catalog
 * gives it both an in-sandbox image to run and a port to reach its harness
 * server on.
 *
 * @throws Error naming the harness and why this deployment cannot execute it.
 */
export function assertHarnessServerCanRunColocated(harness: HarnessType): void {
  const entry = HARNESS_CATALOG[harness] as HarnessCatalogEntry | undefined;
  if (entry === undefined) {
    throw new Error(`unsupported harness: ${String(harness)}`);
  }
  if (entry.defaultImage === null || entry.port === null) {
    throw new Error(
      `harness '${harness}' declares no in-sandbox image/port, so harness-server cannot run it ` +
        `colocated; it is a session-runner provider (self-hosted environment target)`,
    );
  }
}

// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * Periodic upstream price refresh.
 *
 * Two properties define this module. A failed fetch changes nothing — the last
 * successful rows stay authoritative, because stale prices are a far better
 * answer than absent ones. And leaving the URL unset is a supported
 * configuration, not a degraded one: an air-gapped deployment runs on its seed
 * and operator entries and never reaches the network.
 *
 * See `docs/managed-agents/pricing.md`.
 */

import { Gauge, Counter } from 'prom-client';
import { SEED_PRICE_PROVIDER, parseModelPriceCatalog } from '@orca/harness-catalog';
import { registry } from '../metrics.js';
import {
  GLOBAL_MODEL_PRICE_SCOPE,
  toPriceEntries,
  type ModelPriceStore,
  type ModelPriceWrite,
} from './store.js';

/**
 * A deployment that has lost egress keeps serving prices, so nothing else
 * surfaces the loss. Exported as a last-success timestamp rather than an age so
 * an alert reads `time() - metric` and "never succeeded" stays distinguishable
 * from "succeeded a moment ago": the gauge is 0 until the first success.
 */
const lastSuccessTimestamp = new Gauge({
  name: 'registry_service_model_price_refresh_last_success_timestamp_seconds',
  help: 'Unix time of the last successful upstream model-price refresh; 0 if none has succeeded.',
  registers: [registry],
});

const refreshTotal = new Counter({
  name: 'registry_service_model_price_refresh_total',
  help: 'Upstream model-price refresh attempts. Labels: result=succeeded|failed|skipped.',
  labelNames: ['result'] as const,
  registers: [registry],
});

export interface ModelPriceRefresherOptions {
  store: ModelPriceStore;
  /** Absent is a supported configuration; the refresher then never fetches. */
  url: string | undefined;
  /**
   * Which provider's catalog this URL serves. One feed prices one vendor, and
   * the replace below is scoped to it, so pointing the refresher at a second
   * vendor's catalog cannot delete the first vendor's rows.
   */
  provider?: string;
  intervalMs: number;
  /** Injected in tests; production passes the global fetch. */
  fetchImpl?: typeof fetch;
  now?: () => Date;
  /** Called with a one-line reason whenever a refresh fails. */
  onFailure?: (message: string) => void;
  /** Aborts a fetch that never answers, so a hung upstream cannot wedge the timer. */
  timeoutMs?: number;
}

export type RefreshOutcome =
  | { status: 'skipped'; reason: 'not_configured' | 'in_flight' }
  | { status: 'succeeded'; entries: number }
  | { status: 'failed'; error: string };

export interface ModelPriceRefresher {
  /** Run one refresh now. Never throws; every failure is a returned outcome. */
  refreshNow(): Promise<RefreshOutcome>;
  /** Begin the periodic schedule. A no-op when no URL is configured. */
  start(): void;
  stop(): void;
  lastSuccessAt(): Date | null;
  /** Milliseconds since the last success, or `null` if none has succeeded. */
  lastSuccessAgeMs(): number | null;
}

const DEFAULT_FETCH_TIMEOUT_MS = 30_000;

export function createModelPriceRefresher(
  options: ModelPriceRefresherOptions,
): ModelPriceRefresher {
  const { store, url, intervalMs } = options;
  const provider = options.provider ?? SEED_PRICE_PROVIDER;
  const fetchImpl = options.fetchImpl ?? fetch;
  const now = options.now ?? (() => new Date());
  const timeoutMs = options.timeoutMs ?? DEFAULT_FETCH_TIMEOUT_MS;

  let lastSuccess: Date | null = null;
  let inFlight = false;
  let timer: ReturnType<typeof setInterval> | undefined;

  const fail = (error: string): RefreshOutcome => {
    refreshTotal.inc({ result: 'failed' });
    options.onFailure?.(error);
    return { status: 'failed', error };
  };

  async function refreshNow(): Promise<RefreshOutcome> {
    if (url === undefined) {
      refreshTotal.inc({ result: 'skipped' });
      return { status: 'skipped', reason: 'not_configured' };
    }
    // A slow upstream must not stack fetches behind each other; the next tick
    // will try again.
    if (inFlight) {
      refreshTotal.inc({ result: 'skipped' });
      return { status: 'skipped', reason: 'in_flight' };
    }
    inFlight = true;
    try {
      let payload: unknown;
      try {
        const response = await fetchImpl(url, {
          signal: AbortSignal.timeout(timeoutMs),
          headers: { accept: 'application/json' },
        });
        if (!response.ok) return fail(`upstream catalog responded ${response.status}`);
        payload = await response.json();
      } catch (cause) {
        const detail = cause instanceof Error ? cause.message : String(cause);
        return fail(`upstream catalog could not be fetched: ${detail}`);
      }

      // The baseline is what makes the out-of-bounds delta guard real: without
      // the rows currently in force there is nothing for a suspicious new rate
      // to be suspicious *against*, and every price would be accepted as a
      // first observation.
      let baseline: ReturnType<typeof toPriceEntries> = [];
      try {
        baseline = toPriceEntries(await store.list(GLOBAL_MODEL_PRICE_SCOPE));
      } catch (cause) {
        const detail = cause instanceof Error ? cause.message : String(cause);
        return fail(`current prices could not be read for comparison: ${detail}`);
      }

      const parsed = parseModelPriceCatalog(payload, { provider, baseline });
      if (!parsed.ok) return fail(parsed.error);
      // A payload that parses to nothing is a shape we did not understand, not
      // an instruction to price nothing. Treated as a failure so the previous
      // rows stay authoritative.
      if (parsed.entries.length === 0) return fail('upstream catalog contained no usable entries');

      const fetchedAt = now();
      const writes = parsed.entries.map(
        (entry): ModelPriceWrite => ({
          provider: entry.provider,
          modelId: entry.modelId,
          inputPerMillionTokens: entry.inputPerMillionTokens,
          outputPerMillionTokens: entry.outputPerMillionTokens,
          cacheReadPerMillionTokens: entry.cacheReadPerMillionTokens ?? null,
          cacheWritePerMillionTokens: entry.cacheWritePerMillionTokens ?? null,
          fetchedAt,
        }),
      );

      try {
        await store.replace(provider, 'upstream', writes, fetchedAt, GLOBAL_MODEL_PRICE_SCOPE);
      } catch (cause) {
        const detail = cause instanceof Error ? cause.message : String(cause);
        return fail(`upstream catalog could not be stored: ${detail}`);
      }

      lastSuccess = fetchedAt;
      lastSuccessTimestamp.set(fetchedAt.getTime() / 1000);
      refreshTotal.inc({ result: 'succeeded' });
      return { status: 'succeeded', entries: writes.length };
    } finally {
      inFlight = false;
    }
  }

  return {
    refreshNow,

    start() {
      if (url === undefined || timer) return;
      void refreshNow();
      timer = setInterval(() => void refreshNow(), intervalMs);
      // The refresh is best-effort background work; it must never be the reason
      // the process stays alive.
      timer.unref();
    },

    stop() {
      if (timer) clearInterval(timer);
      timer = undefined;
    },

    lastSuccessAt() {
      return lastSuccess;
    },

    lastSuccessAgeMs() {
      return lastSuccess === null ? null : now().getTime() - lastSuccess.getTime();
    },
  };
}

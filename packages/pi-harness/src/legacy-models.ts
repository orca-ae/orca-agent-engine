// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { Api, Model } from '@earendil-works/pi-ai';
import data from './legacy-models.json' with { type: 'json' };

const models = data as Readonly<Record<string, Readonly<Record<string, Model<Api>>>>>;

/**
 * Find a retained definition for the exact provider and model ID.
 * The worker prefers it to the installed SDK definition so existing Sessions
 * keep their protocol, endpoint, and supported reasoning efforts.
 */
export function legacyPiModel(provider: string, id: string): Model<Api> | undefined {
  const catalog = Object.hasOwn(models, provider) ? models[provider] : undefined;
  return catalog && Object.hasOwn(catalog, id) ? catalog[id] : undefined;
}

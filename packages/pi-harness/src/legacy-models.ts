// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { Api, Model } from '@earendil-works/pi-ai';
import data from './legacy-models.json' with { type: 'json' };

// Retained definitions for earlier admitted models removed or changed in Pi 1.5.0.
// Keep their native endpoints, compatibility flags and thinking maps for existing Sessions.
const models = data as Readonly<Record<string, Readonly<Record<string, Model<Api>>>>>;
export function legacyPiModel(provider: string, id: string): Model<Api> | undefined {
  const catalog = Object.hasOwn(models, provider) ? models[provider] : undefined;
  return catalog && Object.hasOwn(catalog, id) ? catalog[id] : undefined;
}

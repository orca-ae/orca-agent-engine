// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { readFileSync } from 'node:fs';
import type { SdkCheckpoint } from '@orca/sdk-harness';
import type { TestProvider } from './provider-fixture.js';

export function legacyPiCheckpoint(
  provider: TestProvider,
  sdkVersion: '0.87.1' | '1.0.4' = '0.87.1',
): SdkCheckpoint {
  return JSON.parse(
    readFileSync(new URL(`../fixtures/pi-${sdkVersion}/${provider}.json`, import.meta.url), 'utf8'),
  ) as SdkCheckpoint;
}

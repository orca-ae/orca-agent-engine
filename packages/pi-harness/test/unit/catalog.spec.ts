// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { expect, it } from 'vitest';
import { ModelRuntime } from '@earendil-works/pi-coding-agent';
import { InMemoryCredentialStore, getSupportedThinkingLevels } from '@earendil-works/pi-ai';
import { CODEX_SDK_MODELS, PI_SDK_MODELS, piModelApi } from '@orca/harness-catalog';

it('keeps the admission catalog consistent with the installed native provider models', async () => {
  const runtime = await ModelRuntime.create({
    credentials: new InMemoryCredentialStore(),
    modelsPath: null,
    refreshOnCreate: false,
  });
  for (const [provider, models] of Object.entries(PI_SDK_MODELS)) {
    for (const [id, efforts] of Object.entries(models)) {
      // These explicitly retained aliases are configured by the worker itself.
      if (provider === 'openai' && Object.hasOwn(CODEX_SDK_MODELS, id)) continue;
      const native = runtime.getModel(provider, id);
      expect(native, `${provider}/${id}`).toBeDefined();
      expect(native!.api).toBe(piModelApi(provider, id));
      expect(
        getSupportedThinkingLevels(native!).filter((level) => !['off', 'minimal'].includes(level)),
      ).toEqual(efforts);
    }
  }
});

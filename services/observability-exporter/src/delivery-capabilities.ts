// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { isValidSampleRate } from './sampling.js';
import type { PinnedDeliveryContext } from './types.js';

/** Capability gate for validated Registry context, not a persisted-state validator. */
export function isLangfuseHttpJsonContext(
  context: PinnedDeliveryContext,
  allowRawIo = false,
): boolean {
  return (
    context.adapterType === 'otlp_http' &&
    context.endpointKind === 'traces_endpoint' &&
    context.endpointClass === 'public' &&
    isPublicHttpsTracesEndpoint(context.endpointUrl) &&
    context.semanticProfile === 'langfuse' &&
    context.protocol === 'http/json' &&
    context.compression === 'none' &&
    (context.captureMode === 'metadata_only' || (allowRawIo && context.captureMode === 'raw_io')) &&
    isValidSampleRate(context.sampleRate)
  );
}

function isPublicHttpsTracesEndpoint(value: string): boolean {
  try {
    const endpoint = new URL(value);
    return (
      endpoint.protocol === 'https:' &&
      endpoint.username === '' &&
      endpoint.password === '' &&
      endpoint.search === '' &&
      endpoint.hash === '' &&
      endpoint.pathname === '/api/public/otel/v1/traces'
    );
  } catch {
    return false;
  }
}

// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/** Response headers are part of the route contract, including bodyless renewals. */
export type OpenApiResponseHeaders = Record<
  number,
  Record<string, { description: string; required: boolean; schema: Record<string, unknown> }>
>;

export function responseHeaderMetadata(headers: OpenApiResponseHeaders) {
  return { openApiResponseHeaders: headers };
}

export function readResponseHeaderMetadata(metadata: unknown): OpenApiResponseHeaders | undefined {
  return metadata !== null && typeof metadata === 'object'
    ? (metadata as { openApiResponseHeaders?: OpenApiResponseHeaders }).openApiResponseHeaders
    : undefined;
}

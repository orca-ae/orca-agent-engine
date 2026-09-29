// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * Explicit OpenAPI media types for the routes whose bodies are not JSON.
 *
 * A handful of routes move bytes rather than JSON: the file upload is
 * `multipart/form-data`, and the file, skill-version and memory content routes
 * return raw bytes. ts-rest has no way to describe those, so their contracts
 * type the payload `z.unknown()` and the handler works with the stream
 * directly. `z.unknown()` renders to `{ nullable: true }` — a schema that says
 * nothing at all — so the published document claimed the upload took an empty
 * JSON-ish body and the downloads returned nullable JSON. A client generated
 * from that document could not upload or download a file.
 *
 * The fix is to state the media type where the route is defined, and let
 * `openapi-schema.ts` splice it into the generated operation. Keeping it here
 * rather than in a lookup table keyed by path means the declaration cannot
 * drift away from the route it describes: delete the route and the override
 * goes with it.
 */

/** A JSON Schema fragment, kept structural to avoid depending on `openapi3-ts`. */
export type MediaSchema = Record<string, unknown>;

export interface MediaOverride {
  contentType: string;
  /**
   * Omit to keep the schema the contract already generated and change only the
   * media type. A byte payload has no zod schema to keep, so the binary helpers
   * below always supply one; an SSE frame's payload is ordinary JSON and is
   * better described in the contract with everything else, where it cannot drift
   * from the events the route actually emits.
   */
  schema?: MediaSchema;
  description?: string;
}

export interface OpenApiMediaMetadata {
  /** Replaces the generated request body, which ts-rest cannot describe. */
  requestBody?: MediaOverride;
  /** Replaces the generated content for the given status codes. */
  responses?: Record<number, MediaOverride>;
}

/** Marker key. Namespaced so unrelated route metadata can coexist. */
export interface WithOpenApiMedia {
  openApiMedia: OpenApiMediaMetadata;
}

/**
 * Attach media overrides to a route's `metadata`.
 *
 * Usage: `metadata: openApiMedia({ requestBody: binaryUpload('file') })`.
 */
export function openApiMedia(media: OpenApiMediaMetadata): WithOpenApiMedia {
  return { openApiMedia: media };
}

/** Read the overrides back off a route, if it declared any. */
export function readOpenApiMedia(metadata: unknown): OpenApiMediaMetadata | undefined {
  if (!metadata || typeof metadata !== 'object') return undefined;
  const media = (metadata as Partial<WithOpenApiMedia>).openApiMedia;
  return media && typeof media === 'object' ? media : undefined;
}

/**
 * A `multipart/form-data` body carrying exactly one required binary part.
 *
 * `required` is on the part, not just the body: a multipart upload with no file
 * in it is not a degraded request, it is a different request, and a generated
 * client should refuse to send it.
 */
export function binaryUpload(part: string, description: string): MediaOverride {
  return {
    contentType: 'multipart/form-data',
    schema: {
      type: 'object',
      required: [part],
      properties: {
        [part]: { type: 'string', format: 'binary', description },
      },
    },
  };
}

/**
 * A `multipart/form-data` body carrying a required array of binary parts.
 *
 * Skill bundles are uploaded as many files in one request — Anthropic's pinned
 * schemas require `files` on both create and add-version, and our handler
 * rejects an upload containing none. `extra` carries the optional text parts
 * that accompany them.
 */
export function binaryUploadArray(
  part: string,
  description: string,
  extra: Record<string, MediaSchema> = {},
): MediaOverride {
  return {
    contentType: 'multipart/form-data',
    schema: {
      type: 'object',
      required: [part],
      properties: {
        [part]: { type: 'array', items: { type: 'string', format: 'binary' }, description },
        ...extra,
      },
    },
  };
}

/** A response body of raw bytes under the given media type. */
export function binaryDownload(contentType: string, description: string): MediaOverride {
  return {
    contentType,
    schema: { type: 'string', format: 'binary' },
    description,
  };
}

/**
 * A `text/event-stream` response whose frames carry the contract's own schema.
 *
 * `src/streaming/sse.ts` writes `content-type: text/event-stream` and one frame
 * per event; ts-rest publishes every response as `application/json`, so the
 * three stream routes advertised a media type they never send. OpenAPI has no
 * way to describe SSE framing, so the schema describes the payload of a single
 * frame's `data:` line — which is how Anthropic's own spec models these two
 * operations. No schema is supplied here: the route declares the event type in
 * zod and this only re-keys it under the media type actually written.
 */
export function eventStream(description: string): MediaOverride {
  return { contentType: 'text/event-stream', description };
}

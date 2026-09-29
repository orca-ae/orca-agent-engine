// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import {
  MAX_METADATA_KEY_LENGTH,
  MAX_METADATA_PAIRS,
  MAX_METADATA_VALUE_LENGTH,
} from '../contracts/metadata.js';

export type StringMetadata = Record<string, string>;
export type MetadataPatch = Record<string, string | null>;

export function normalizeStoredMetadata(value: unknown): StringMetadata {
  const metadata = emptyStringMetadata();
  if (!value || typeof value !== 'object' || Array.isArray(value)) return metadata;
  for (const [key, item] of Object.entries(value as Record<string, unknown>)) {
    if (typeof item === 'string') metadata[key] = item;
  }
  return metadata;
}

export function parseMetadata(
  input: unknown,
  field = 'metadata',
): { ok: true; value: StringMetadata } | { ok: false; error: string } {
  if (input === undefined) return { ok: true, value: emptyStringMetadata() };
  if (!input || typeof input !== 'object' || Array.isArray(input)) {
    return { ok: false, error: `${field} must be an object of string values` };
  }
  const metadata = emptyStringMetadata();
  for (const [key, item] of Object.entries(input as Record<string, unknown>)) {
    if (typeof item !== 'string') {
      return { ok: false, error: `${field}.${key} must be a string` };
    }
    metadata[key] = item;
  }
  const limitsError = validateMetadataLimits(metadata, field);
  if (limitsError) return { ok: false, error: limitsError };
  return { ok: true, value: metadata };
}

export function parseMetadataPatch(
  input: unknown,
  field = 'metadata',
): { ok: true; value: MetadataPatch } | { ok: false; error: string } {
  if (input === undefined) return { ok: true, value: emptyMetadataPatch() };
  if (!input || typeof input !== 'object' || Array.isArray(input)) {
    return { ok: false, error: `${field} must be an object of string or null values` };
  }
  const patch = emptyMetadataPatch();
  for (const [key, item] of Object.entries(input as Record<string, unknown>)) {
    if (typeof item !== 'string' && item !== null) {
      return { ok: false, error: `${field}.${key} must be a string or null` };
    }
    patch[key] = item;
  }
  const limitsError = validateMetadataPatchLimits(patch, field);
  if (limitsError) return { ok: false, error: limitsError };
  return { ok: true, value: patch };
}

export function applyMetadataPatch(existing: unknown, patch: MetadataPatch): StringMetadata {
  const metadata = normalizeStoredMetadata(existing);
  for (const [key, item] of Object.entries(patch)) {
    if (item === null) {
      delete metadata[key];
    } else {
      metadata[key] = item;
    }
  }
  return metadata;
}

export function toJsonMetadata(metadata: StringMetadata): StringMetadata {
  const out: StringMetadata = {};
  for (const [key, value] of Object.entries(metadata)) {
    Object.defineProperty(out, key, {
      value,
      enumerable: true,
      writable: true,
      configurable: true,
    });
  }
  return out;
}

export function validateMetadataLimits(
  metadata: StringMetadata,
  field = 'metadata',
): string | null {
  const entries = Object.entries(metadata);
  if (entries.length > MAX_METADATA_PAIRS) {
    return `${field} must contain at most ${MAX_METADATA_PAIRS} pairs`;
  }
  for (const [key, value] of entries) {
    const keyError = validateMetadataKey(key, field);
    if (keyError) return keyError;
    if (value.length > MAX_METADATA_VALUE_LENGTH) {
      return `${field}.${key} must be at most ${MAX_METADATA_VALUE_LENGTH} characters`;
    }
  }
  return null;
}

export function validateMetadataPatchLimits(
  patch: MetadataPatch,
  field = 'metadata',
): string | null {
  for (const [key, value] of Object.entries(patch)) {
    const keyError = validateMetadataKey(key, field);
    if (keyError) return keyError;
    if (typeof value === 'string' && value.length > MAX_METADATA_VALUE_LENGTH) {
      return `${field}.${key} must be at most ${MAX_METADATA_VALUE_LENGTH} characters`;
    }
  }
  return null;
}

function validateMetadataKey(key: string, field: string): string | null {
  if (key.length === 0) {
    return `${field} keys must contain at least 1 character`;
  }
  if (key.length > MAX_METADATA_KEY_LENGTH) {
    return `${field} keys must be at most ${MAX_METADATA_KEY_LENGTH} characters`;
  }
  return null;
}

function emptyStringMetadata(): StringMetadata {
  return Object.create(null) as StringMetadata;
}

function emptyMetadataPatch(): MetadataPatch {
  return Object.create(null) as MetadataPatch;
}

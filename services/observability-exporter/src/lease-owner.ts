// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

export const MAX_LEASE_OWNER_LENGTH = 256;
export const PROJECTOR_LEASE_OWNER_SUFFIX = '-projector';
export const DELIVERY_LEASE_OWNER_SUFFIX = '-delivery';

const LONGEST_LEASE_OWNER_SUFFIX_LENGTH = Math.max(
  PROJECTOR_LEASE_OWNER_SUFFIX.length,
  DELIVERY_LEASE_OWNER_SUFFIX.length,
);

export const MAX_WORKER_ID_LENGTH = MAX_LEASE_OWNER_LENGTH - LONGEST_LEASE_OWNER_SUFFIX_LENGTH;

// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Both persistence backends share exactly the same metadata-only validation boundary.
export {
  parseProjectedTrace as parseKafkaProjectedTrace,
  parsePinnedDeliveryContext as parseKafkaDeliveryContext,
} from './canonical-validation.js';

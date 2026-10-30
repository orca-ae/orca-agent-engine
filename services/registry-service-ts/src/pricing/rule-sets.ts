// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import {
  validatePriceRuleSet,
  priceConditionKey,
  type CatalogPriceRule,
} from '@orca/harness-catalog';
import type { z } from 'zod';
import type { PriceRuleSetCreateSchema } from '../contracts/model-price-rules.contract.js';
import { newId } from '../domain/versioning.js';
import {
  priceDigest,
  PricingConflictError,
  type CatalogDocument,
  type StoredPriceRuleSet,
} from './catalog-store.js';

export type RuleSetWrite = z.infer<typeof PriceRuleSetCreateSchema>;
export class InvalidPriceRuleSetError extends Error {}

/** The activated marker survives withdrawal. Removing a rule cannot resurrect a flat cache. */
export function modelUsesConditionalPrices(
  doc: CatalogDocument,
  provider: string,
  modelId: string,
): boolean {
  return doc.conditional_models.some((m) => m.provider === provider && m.model_id === modelId);
}

export function publishOperatorRuleSet(
  doc: CatalogDocument,
  organizationId: string,
  body: RuleSetWrite,
  now: Date,
  allowActivation: boolean,
  existingId?: string,
  expectedRevision?: number,
): StoredPriceRuleSet {
  if (!organizationId) throw new InvalidPriceRuleSetError('operator rules require an organization');
  const existing = existingId ? doc.rule_sets.find((s) => s.id === existingId) : undefined;
  if (existingId && !existing) throw new PricingConflictError('rule set not found');
  if (existing && existing.revision !== expectedRevision)
    throw new PricingConflictError('stale rule set revision');
  if (existing && (existing.provider !== body.provider || existing.model_id !== body.model_id))
    throw new InvalidPriceRuleSetError('rule set identity cannot change');
  if (
    !existing &&
    doc.rule_sets.some(
      (s) =>
        s.provider === body.provider && s.model_id === body.model_id && s.source === 'operator',
    )
  )
    throw new PricingConflictError('operator rule set already exists');
  if (body.activate && !allowActivation)
    throw new InvalidPriceRuleSetError(
      'conditional activation is disabled; qualify writers and v2 consumers before enabling it',
    );
  if (
    existing &&
    doc.rules.some(
      (r) =>
        r.rule_set_id === existing.id &&
        r.retired_at === null &&
        Date.parse(r.active_from) > now.getTime(),
    )
  )
    throw new PricingConflictError('a scheduled replacement is pending');
  const activated = modelUsesConditionalPrices(doc, body.provider, body.model_id);
  const effective =
    body.provider_effective_at === null
      ? now
      : new Date(Math.max(now.getTime(), Date.parse(body.provider_effective_at)));
  if (body.activate && !activated && effective > now)
    throw new InvalidPriceRuleSetError('first activation cannot precede the effective rule set');
  const id = existing?.id ?? newId('prs');
  const revision = (existing?.revision ?? 0) + 1;
  const old = doc.rules.filter((r) => r.rule_set_id === id && r.retired_at === null);
  const rules: CatalogPriceRule[] = body.rules.map((input) => ({
    ...input,
    rule_set_id: id,
    rule_id:
      old.find((r) => priceConditionKey(r.conditions) === priceConditionKey(input.conditions))
        ?.rule_id ?? newId('prule'),
    revision,
    provider: body.provider,
    model_id: body.model_id,
    source: 'operator',
    organization_id: organizationId,
    usage_profile: body.usage_profile,
    profile_version: body.profile_version,
    provenance: {
      source_id: 'organization-operator',
      document_sha256: priceDigest(body),
      adapter_version: 'operator-v1',
      observed_at: now.toISOString(),
    },
    provider_effective_at: body.provider_effective_at,
    active_from: effective.toISOString(),
    retired_at: null,
    valid_until: null,
  }));
  const error = validatePriceRuleSet(rules);
  if (error) throw new InvalidPriceRuleSetError(error);
  for (const rule of old) rule.retired_at = effective.toISOString();
  doc.rules.push(...rules);
  const set: StoredPriceRuleSet = {
    id,
    revision,
    provider: body.provider,
    model_id: body.model_id,
    source: 'operator',
    organization_id: organizationId,
    rule_ids: rules.map((r) => r.rule_id),
  };
  doc.rule_sets = [...doc.rule_sets.filter((s) => s.id !== id), set];
  if (body.activate && !activated)
    doc.conditional_models.push({ provider: body.provider, model_id: body.model_id });
  return set;
}

export function retireOperatorRuleSet(
  doc: CatalogDocument,
  id: string,
  expectedRevision: number,
  now: Date,
): void {
  const set = doc.rule_sets.find((s) => s.id === id && s.source === 'operator');
  if (!set || set.revision !== expectedRevision)
    throw new PricingConflictError('stale or missing rule set');
  for (const rule of doc.rules)
    if (
      rule.rule_set_id === id &&
      (rule.retired_at === null || Date.parse(rule.retired_at) > now.getTime())
    )
      rule.retired_at = now.toISOString();
  doc.rule_sets = doc.rule_sets.filter((s) => s.id !== id);
}

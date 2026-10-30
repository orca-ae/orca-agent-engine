// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { z } from 'zod';
import { PriceRuleInputSchema } from '../contracts/model-price-rules.contract.js';

const budget = (defaultValue: number, min: number, max: number) =>
  z.number().int().min(min).max(max).default(defaultValue);
export const PriceSourceConfigSchema = z
  .object({
    id: z.string().regex(/^[a-z][a-z0-9_-]{0,63}$/),
    provider: z.string().regex(/^[a-z][a-z0-9_-]{0,63}$/),
    adapter: z.enum(['normalized-v1', 'openai-public-v1', 'anthropic-public-v1']),
    url: z
      .string()
      .url()
      .refine((s) => {
        const u = new URL(s);
        return u.protocol === 'https:' && !u.username && !u.password && !u.search && !u.hash;
      }),
    interval_ms: budget(6 * 60 * 60_000, 60_000, 7 * 24 * 60 * 60_000),
    timeout_ms: budget(30_000, 100, 60_000),
    max_bytes: budget(4 * 1024 * 1024, 1024, 4 * 1024 * 1024),
    max_rules: budget(10_000, 1, 10_000),
    max_pages: budget(100, 1, 100),
    max_age_ms: budget(7 * 24 * 60 * 60_000, 60_000, 7 * 24 * 60 * 60_000),
    /** Reviewed exact document diffs, never workspace-controlled approval. */
    approved_change_digests: z
      .array(z.string().regex(/^[a-f0-9]{64}$/))
      .max(100)
      .default([]),
    approve_empty: z.boolean().default(false),
    replaces_source_id: z
      .string()
      .regex(/^[a-z][a-z0-9_-]{0,63}$/)
      .optional(),
  })
  .strict()
  .superRefine((s, ctx) => {
    const official =
      s.adapter === 'openai-public-v1'
        ? ['openai', 'https://developers.openai.com/api/docs/pricing']
        : s.adapter === 'anthropic-public-v1'
          ? ['anthropic', 'https://platform.claude.com/docs/en/about-claude/pricing']
          : null;
    if (official && (s.provider !== official[0] || s.url !== official[1]))
      ctx.addIssue({
        code: 'custom',
        message: 'official adapter provider and source URL are fixed',
      });
  });
export type PriceSourceConfig = z.infer<typeof PriceSourceConfigSchema>;
export function parsePriceSources(
  raw: string | undefined,
  legacyUrl: string | undefined,
): PriceSourceConfig[] {
  if (!raw) return [];
  if (legacyUrl)
    throw new Error('PRICE_SOURCES_JSON and PRICE_REFRESH_URL cannot be configured together');
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    throw new Error('PRICE_SOURCES_JSON must be a JSON source list');
  }
  const sources = z.array(PriceSourceConfigSchema).max(20).safeParse(parsed);
  if (!sources.success)
    throw new Error('PRICE_SOURCES_JSON contains an invalid source configuration');
  if (
    new Set(sources.data.map((s) => s.provider)).size !== sources.data.length ||
    new Set(sources.data.map((s) => s.id)).size !== sources.data.length
  )
    throw new Error('PRICE_SOURCES_JSON requires unique source IDs and one source per provider');
  return sources.data;
}

export const SourceModelSchema = z
  .object({
    model_id: z
      .string()
      .min(1)
      .max(200)
      .regex(/^[A-Za-z0-9][A-Za-z0-9._:/-]*$/),
    profile_version: z.enum(['literal-v1', 'openai-v1', 'anthropic-v1']),
    usage_profile: z.enum(['exclusive-v1', 'anthropic-cache-v1']),
    provider_effective_at: z.string().datetime({ offset: true }).nullable(),
    rules: z.array(PriceRuleInputSchema).min(1).max(10_000),
  })
  .strict();
export type SourceModel = z.infer<typeof SourceModelSchema>;
const FeedEnvelope = z
  .object({
    schema_version: z.literal(1),
    price_scope: z.literal('public_list'),
    provider: z.string(),
    currency: z.literal('USD'),
    unit: z.literal('per_million_tokens'),
    complete: z.literal(true),
    /** Each model carries its own upstream document digest, in addition to the feed hash. */
    upstream_document_sha256: z.string().regex(/^[a-f0-9]{64}$/),
    models: z.array(z.object({ model_id: z.string().min(1).max(200) }).passthrough()).max(10_000),
    withdrawn_models: z.array(z.string().min(1).max(200)).max(10_000),
    next_page: z.string().url().nullable(),
  })
  .strict();
export interface SourcePage {
  rule_count: number;
  models: { model_id: string; value: SourceModel | null }[];
  withdrawn_models: string[];
  next_page: string | null;
  upstream_digest: string;
}

/** Schema changes fail closed; invalid model sets remain identifiable for quarantine. */
export function parseSourcePage(
  text: string,
  source: PriceSourceConfig,
  digest: string,
): SourcePage {
  if (source.adapter !== 'normalized-v1') return parsePublicPriceDocument(text, source, digest);
  const envelope = FeedEnvelope.parse(JSON.parse(text));
  if (envelope.provider !== source.provider) throw new Error('source provider mismatch');
  return {
    models: envelope.models.map((model) => {
      const parsed = SourceModelSchema.safeParse(model);
      return { model_id: model.model_id, value: parsed.success ? parsed.data : null };
    }),
    rule_count: envelope.models.reduce(
      (sum, m) => sum + (Array.isArray(m['rules']) ? m['rules'].length : 1),
      0,
    ),
    withdrawn_models: envelope.withdrawn_models,
    next_page: envelope.next_page,
    upstream_digest: envelope.upstream_document_sha256,
  };
}

function cells(line: string): string[] {
  return line
    .trim()
    .slice(1, -1)
    .split('|')
    .map((x) => x.trim());
}
function money(cell: string): number | null {
  if (cell === '-') return null;
  const m = /^\$([0-9]+(?:\.[0-9]+)?)(?:\s*\/ MTok)?$/.exec(cell);
  if (!m) throw new Error('unsupported price table cell');
  return Number(m[1]);
}

/** Versioned, deliberately bounded coverage. Unsupported tables/models are never inferred. */
export function parsePublicPriceDocument(
  text: string,
  source: PriceSourceConfig,
  digest: string,
): SourcePage {
  const result = new Map<string, SourceModel>();
  if (source.adapter === 'openai-public-v1') {
    const section = text.split('Cyber models')[0]!;
    if (!section.includes('Short context: ≤272K input tokens. Long context: >272K input tokens.'))
      throw new Error('OpenAI context semantics changed');
    const supported = new Set(['gpt-6-astra', 'gpt-6.1-sol', 'gpt-6.1-terra', 'gpt-6-sol']);
    const tiers: Record<string, string> = {
      Standard: 'standard',
      Batch: 'batch',
      Flex: 'flex',
      Fast: 'priority',
      Ultrafast: 'ultrafast',
    };
    let tier: string | undefined;
    let table = false;
    for (const line of section.split('\n')) {
      const heading = /^(?:#{3,4} )?(Standard|Batch|Flex|Fast|Ultrafast)(?: pricing data)?$/.exec(
        line.trim(),
      );
      if (heading) {
        tier = tiers[heading[1]!];
        table = false;
      }
      if (/^\|\s*Model\s*\|\s*Short context input\s*\|/.test(line)) {
        if (
          cells(line).join('|') !==
          'Model|Short context input|Short context output|Short context cached input|Short context 30m write|Short context 12h write|Long context input|Long context output|Long context cached input|Long context 30m write|Long context 12h write'
        )
          throw new Error('OpenAI price table changed');
        table = true;
        continue;
      }
      if (!table || !line.startsWith('|')) continue;
      const row = cells(line);
      if (!supported.has(row[0]!)) continue;
      if (!tier || row.length !== 11) throw new Error('OpenAI price table changed');
      const model = result.get(row[0]!) ?? {
        model_id: row[0]!,
        profile_version: 'openai-v1',
        usage_profile: 'exclusive-v1',
        provider_effective_at: null,
        rules: [],
      };
      for (const [start, min, max] of [
        [1, 0, 272001],
        [6, 272001, null],
      ] as const) {
        const input = money(row[start]!),
          output = money(row[start + 1]!),
          read = money(row[start + 2]!);
        if (input === null || output === null) continue;
        // 30m/12h writes cannot be relabelled as the Engine's 5m/1h buckets.
        model.rules.push({
          conditions: { service_tier: tier, request_input_tokens: { min, max } },
          rates: {
            input_per_million_tokens: input,
            output_per_million_tokens: output,
            cache_read_per_million_tokens: read,
            cache_write_per_million_tokens: null,
            cache_write_1h_per_million_tokens: null,
          },
        });
      }
      result.set(model.model_id, model);
    }
    if ([...supported].some((id) => !result.has(id)))
      throw new Error('OpenAI supported model table missing');
  } else {
    if (
      !text.includes('All prices are in USD.') ||
      !text.includes('Claude 4.6 and later models (except Claude Haiku 5.5)') ||
      !text.includes('1M token context window')
    )
      throw new Error('Anthropic coverage semantics changed');
    const section = text.split('## Model pricing')[1]?.split('\n## ')[0];
    if (!section) throw new Error('Anthropic model table missing');
    const names: Record<string, string> = {
      'Claude Sonnet 4.6': 'claude-sonnet-4-6',
      'Claude Opus 4.6': 'claude-opus-4-6',
    };
    const header = section.split('\n').find((line) => line.startsWith('| Model '));
    if (
      !header ||
      cells(header).join('|') !==
        'Model|Base input tokens|5m cache writes|1h cache writes|Cache hits and refreshes|Output tokens'
    )
      throw new Error('Anthropic price table changed');
    for (const line of section.split('\n').filter((line) => line.trim().startsWith('|'))) {
      const row = cells(line),
        id = names[row[0]!];
      if (!id) continue;
      if (row.length !== 6 || result.has(id)) throw new Error('Anthropic price table changed');
      const [input, write, hour, read, output] = row.slice(1).map(money);
      if (
        input == null ||
        write == null ||
        hour == null ||
        output == null ||
        Math.abs(hour - write * 1.6) > 1e-9
      )
        throw new Error('Anthropic cache profile changed');
      result.set(id, {
        model_id: id,
        profile_version: 'anthropic-v1',
        usage_profile: 'anthropic-cache-v1',
        provider_effective_at: null,
        rules: [
          {
            conditions: {
              service_tier: 'standard',
              request_input_tokens: { min: 0, max: 1_000_001 },
            },
            rates: {
              input_per_million_tokens: input,
              output_per_million_tokens: output,
              cache_read_per_million_tokens: read!,
              cache_write_per_million_tokens: write,
              cache_write_1h_per_million_tokens: hour,
            },
          },
        ],
      });
    }
    if (Object.keys(names).length !== result.size)
      throw new Error('Anthropic supported model table missing');
  }
  return {
    models: [...result.values()].map((value) => ({ model_id: value.model_id, value })),
    rule_count: [...result.values()].reduce((sum, m) => sum + m.rules.length, 0),
    withdrawn_models: [],
    next_page: null,
    upstream_digest: digest,
  };
}

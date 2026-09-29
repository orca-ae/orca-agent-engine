// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * Outcome evaluation.
 *
 * A client defines a success criterion with
 * `user.define_outcome { description, rubric, max_iterations? }`.
 * After each turn the harness evaluates whether the conversation so far achieved
 * that criterion and surfaces the verdict via the `span.outcome_evaluation_*`
 * events; the registry projects the latest verdict onto the session `outcome`.
 *
 * The default evaluator is an LLM judge that calls the Anthropic Messages API
 * with the same credentials the harness already holds. The evaluator is
 * injectable (see `ClaudeHarnessOptions.evaluateOutcome`) so tests can supply a
 * deterministic verdict and the judge contract stays swappable.
 *
 * NOTE: the judge prompt + `{ achieved, reasoning }` verdict shape are the orca
 * interpretation of Claude's outcome-evaluation semantics (the public beta does
 * not publish the exact judge contract); they are intentionally isolated here.
 */

import type { ModelEffort, ModelSpeed } from '../agent-harness.js';

export interface OutcomeCriterion {
  /** Stable id derived from the defining `user.define_outcome` event. */
  id: string;
  /** What the agent should accomplish. */
  description: string;
  /** Public rubric input; can be inline text or a structured/file reference. */
  rubric: unknown;
  /** Maximum evaluation iterations requested by the client. */
  maxIterations: number;
  /** Zero-based next evaluation iteration. */
  iteration: number;
}

export interface OutcomeTranscriptTurn {
  role: 'user' | 'agent';
  text: string;
}

export interface OutcomeVerdict {
  achieved: boolean;
  reasoning: string;
}

export interface EvaluateOutcomeInput {
  criterion: OutcomeCriterion;
  transcript: OutcomeTranscriptTurn[];
}

export type OutcomeEvaluator = (input: EvaluateOutcomeInput) => Promise<OutcomeVerdict>;

export interface MessagesApiEvaluatorOptions {
  apiKey: string;
  baseURL?: string;
  /** Gateway auth for the separate Session's optional outcome judge request. */
  gatewayAuth?: { sessionId: string; getToken: () => Promise<string> };
  model: string;
  speed?: ModelSpeed;
  effort?: ModelEffort;
  /** Injectable for tests; defaults to global fetch. */
  fetchImpl?: typeof fetch;
}

const JUDGE_MAX_TOKENS = 512;
const DEFAULT_ANTHROPIC_BASE_URL = 'https://api.anthropic.com';
const ANTHROPIC_VERSION = '2023-06-01';
const FAST_MODE_BETA = 'fast-mode-2026-02-01';

/**
 * Build the default LLM-judge evaluator. Returns `{ achieved:false }` with a
 * diagnostic reasoning rather than throwing, so a judge outage never breaks the
 * agent turn — the span still closes with a well-formed (if pessimistic) verdict.
 */
export function createMessagesApiOutcomeEvaluator(
  opts: MessagesApiEvaluatorOptions,
): OutcomeEvaluator {
  const fetchImpl = opts.fetchImpl ?? fetch;
  const baseURL = (opts.baseURL ?? DEFAULT_ANTHROPIC_BASE_URL).replace(/\/+$/, '');
  return async ({ criterion, transcript }) => {
    const prompt = buildJudgePrompt(criterion, transcript);
    try {
      const gatewayToken = opts.gatewayAuth ? await opts.gatewayAuth.getToken() : undefined;
      if (opts.gatewayAuth && !gatewayToken) {
        throw new Error('empty LLM gateway Session JWT');
      }
      const res = await fetchImpl(`${baseURL}/v1/messages`, {
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          ...(opts.gatewayAuth
            ? {
                authorization: `Bearer ${gatewayToken}`,
                'X-Orca-Session-Id': opts.gatewayAuth.sessionId,
              }
            : { 'x-api-key': opts.apiKey }),
          'anthropic-version': ANTHROPIC_VERSION,
          ...(opts.speed === 'fast' ? { 'anthropic-beta': FAST_MODE_BETA } : {}),
        },
        body: JSON.stringify({
          model: opts.model,
          max_tokens: JUDGE_MAX_TOKENS,
          messages: [{ role: 'user', content: prompt }],
          ...(opts.speed === 'fast' ? { speed: 'fast' } : {}),
          ...(opts.effort !== undefined ? { output_config: { effort: opts.effort } } : {}),
        }),
      });
      if (!res.ok) {
        return { achieved: false, reasoning: `outcome judge HTTP ${res.status}` };
      }
      const body = (await res.json()) as {
        content?: Array<{ type?: string; text?: string }>;
        usage?: { speed?: unknown };
      };
      const speedError = outcomeJudgeSpeedError(opts.speed, body.usage?.speed);
      if (speedError) return { achieved: false, reasoning: speedError };
      const text = (body.content ?? [])
        .filter((b) => b.type === 'text' && typeof b.text === 'string')
        .map((b) => b.text as string)
        .join('');
      return parseVerdict(text);
    } catch (err) {
      return {
        achieved: false,
        reasoning: `outcome judge error: ${err instanceof Error ? err.message : String(err)}`,
      };
    }
  };
}

function outcomeJudgeSpeedError(
  requested: ModelSpeed | undefined,
  observed: unknown,
): string | null {
  if (requested === 'fast') {
    if (observed === undefined || observed === null) {
      return 'outcome judge omitted usage.speed for requested fast inference';
    }
    if (observed !== 'fast') {
      return `outcome judge reported usage.speed=${JSON.stringify(observed)} for requested fast inference`;
    }
    return null;
  }
  if (observed !== undefined && observed !== null && observed !== 'standard') {
    return `outcome judge reported usage.speed=${JSON.stringify(observed)} for requested standard inference`;
  }
  return null;
}

function buildJudgePrompt(
  criterion: OutcomeCriterion,
  transcript: OutcomeTranscriptTurn[],
): string {
  const convo = transcript
    .map((t) => `${t.role === 'user' ? 'User' : 'Agent'}: ${t.text}`)
    .join('\n');
  const rubricText =
    typeof criterion.rubric === 'string' ? criterion.rubric : JSON.stringify(criterion.rubric);
  return [
    'You are an impartial evaluator judging whether an AI agent achieved a defined outcome.',
    '',
    `Outcome description:\n${criterion.description}`,
    '',
    `Rubric:\n${rubricText}`,
    '',
    `Conversation transcript:\n${convo || '(no conversation yet)'}`,
    '',
    'Respond with ONLY a JSON object of the form {"achieved": boolean, "reasoning": string}.',
  ].join('\n');
}

/** Parse the judge's JSON verdict, tolerating surrounding prose / code fences. */
export function parseVerdict(text: string): OutcomeVerdict {
  const match = text.match(/\{[\s\S]*\}/);
  if (match) {
    try {
      const parsed = JSON.parse(match[0]) as { achieved?: unknown; reasoning?: unknown };
      return {
        achieved: parsed.achieved === true,
        reasoning: typeof parsed.reasoning === 'string' ? parsed.reasoning : '',
      };
    } catch {
      // fall through
    }
  }
  return { achieved: false, reasoning: text.trim().slice(0, 500) || 'unparseable judge response' };
}

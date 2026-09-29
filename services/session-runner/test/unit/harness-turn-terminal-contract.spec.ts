// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { CodexSdkHarness } from '../../src/harness/codex-sdk/index.js';
import { HARNESS_CATALOG } from '@orca/harness-catalog';
// THE HARNESS TURN-TERMINAL CONFORMANCE SUITE — one invariant, every provider, every
// fault shape.
//
// THE INVARIANT
//
//   Every path that ends a turn must put a TERMINAL EVENT on the consumer's stream: an
//   `agent.error` describing the fault when the turn faulted, followed by EXACTLY ONE
//   `agent.turn_completed` marker.
//
// It is load-bearing because the registry's owner pod defines "answered" as exactly an
// agent-produced `agent.turn_completed` after the user turn (see
// `registry-service-ts/src/tunnel/session-recovery.ts`). A turn that ends without a
// marker leaves that session PENDING FOREVER; a turn that ends with a BARE marker is
// worse — the durable transcript records a crashed turn as a successful, empty answer,
// and the client reasons on from a fabricated non-answer at HTTP 200.
//
// WHY A PARAMETERISED SUITE INSTEAD OF MORE PER-PROVIDER TESTS
//
// Two review rounds fixed this same defect roughly sixteen times, instance by instance,
// and round two still found four sites round one had never enumerated — because each
// fix was a test of ONE provider under ONE fault. This suite inverts that: it
// enumerates the CROSS PRODUCT (every registered provider × every fault shape the two
// rounds actually found) so a provider that is added, or a shape that is discovered,
// has a cell that must be filled in rather than a hole nobody looks at.
//
// The rows are the ten providers `defaultRegisterProviders` registers in `src/main.ts`
// plus the `multiagent` coordinator DECORATOR, which wraps a base harness and owes the
// consumer exactly the same contract. That sentence is ENFORCED, not asserted: the first
// spec below registers the real `defaultRegisterProviders` against a real
// `ProviderRegistry` and requires the registered keys to equal this matrix's rows. Without
// it `ROWS` was a hand-maintained literal — a new provider produced no new cell and no
// failure, so the hole this suite exists to close reappeared silently.
//
// The columns are the fault shapes:
//
//   1. submit-rejects        — the harness's `submit()` REJECTS.
//   2. stream-ends-mid-turn  — `events()` yields `done` before any terminal marker.
//   3. cli-launch-fails      — the CLI binary is missing / misnamed on the sandbox PATH.
//   4. cli-dies-mid-turn     — the CLI took the turn frame and then died (OOM / segfault).
//   5. child-mute            — the child stays ALIVE and answers nothing at all.
//   6. approval-gate-throws  — the tool-approval gate raises instead of deciding.
//   7. non-terminal-error-then-fault — the harness's last word before the turn is cut
//      short is an `agent.error` that is NOT about the turn ending (the SDK's
//      transcript-mirror data-loss diagnostic), so the loop still owes the truncation.
//   8. child-acks-then-goes-mute — the child ANSWERS the turn request and then never
//      notifies completion. Only codex can be measured on it (only codex has a request
//      bound distinct from the turn bound); it exists because the codex `child-mute` cell
//      satisfies the REQUEST deadline first, so codex's turn deadline could be deleted
//      whole and the suite stayed at exactly its baseline count.
//   9. deadline-then-next-turn — a SECOND turn is driven after the deadline abandoned the
//      first. No other column drives two turns, which is why the worst defect this suite
//      has yet found had no cell: a deadline that ends the turn but leaves the child ALIVE
//      lets the abandoned turn's answer be credited to the next one.
//
// A cell that does not apply to a provider is an EXPLICIT, NAMED skip carrying the
// reason — never a bare `return`, because a vacuous pass is the exact failure mode this
// suite exists to prevent.
//
// EVERYTHING IS DRIVEN THROUGH THE REAL `SessionLoop`, not through a harness directly:
// the invariant is about what the CONSUMER sees, and the consumer is the owner pod
// reading the loop's NDJSON turn stream. Faults are injected at each provider's OWN
// seam — a stubbed `NativeCliProcess` for the five native-CLI providers and for the SDK
// worker of `codex-sdk` and `pi-sdk`, a stubbed SDK `query` for the two in-process claude
// providers, a scripted base harness for the coordinator — so the provider's own fault
// path runs in MOST cells.
//
// ONE COLUMN IS AN EXCEPTION, and narrowing the claim is the point. The eleven
// `stream-ends-mid-turn` cells use `beforeTurn: stopFirst`, so the harness is already torn
// down when the turn is driven: `submit` returns on its terminated / no-child guard and NO
// turn code runs. Those cells pin the LOOP's truncation path — that a turn driven against a
// dead harness still ends with a fault and exactly one marker — not the harness's. Proved by
// mutation: making `ClaudeCodeCliHarness.submit` an unconditional no-op fails SEVEN cells
// across the claude-code and multiagent rows, and `stream-ends-mid-turn` is not one of them
// (nor is `cli-launch-fails`, whose fault the READ LOOP reports independently of `submit`).
// Aggregate coverage survives, because every other column drives the provider for real.

import { describe, it, expect } from 'vitest';
import { InMemorySandboxRuntime } from '@orca/sandbox-runtime';
import { ProviderRegistry } from '../../src/harness/provider.js';
import {
  SessionLoop,
  AGENT_ERROR_EVENT_KIND,
  TURN_COMPLETED_EVENT_KIND,
} from '../../src/session-loop.js';
import type {
  AgentHarness,
  SessionStartInput,
  ToolConfirmer,
} from '../../src/harness/agent-harness.js';
import type { SandboxHandle } from '../../src/sandbox/seam.js';
import type { NativeCliProcess } from '../../src/sandbox/native-cli-launcher.js';
import { ClaudeAgentSdkHarness, type ClaudeQuery } from '../../src/harness/claude/index.js';
import {
  ClaudePersistentSdkHarness,
  type PersistentClaudeQuery,
  type PersistentQueryHandle,
} from '../../src/harness/claude/persistent.js';
import { ClaudeAgentSdkAdapter } from '../../src/harness/claude/session-adapter.js';
import { InMemoryTranscriptStore } from '../../src/transcript/in-memory-transcript-store.js';
import { ClaudeCodeCliHarness } from '../../src/harness/claude-code/index.js';
import { CodexCliHarness } from '../../src/harness/codex/index.js';
import { CursorCliHarness } from '../../src/harness/cursor/index.js';
import { PiCliHarness } from '../../src/harness/pi/index.js';
import { CustomCliHarness } from '../../src/harness/custom/index.js';
import { MockAgentHarness } from '../../src/harness/mock/provider.js';
import { FakeAgentHarness } from './support/fake-agent-harness.js';
import { defaultRegisterProviders } from '../../src/main.js';
import type { RunnerConfig } from '../../src/config.js';
import {
  dyingNativeCli,
  failedNativeCli,
  muteNativeCli,
  scriptedNativeCli,
  scriptedNativeCliWithDeath,
} from './support/failed-native-cli.js';

const WS = 'ws_conformance';
const SES = 'ses_conformance';

/**
 * How long a driven turn may take before the suite calls it HUNG. Generous next to the
 * (stubbed, in-process) work each cell does, and the whole point of bounding it: a turn
 * that never terminates is precisely the failure the invariant forbids, so it must
 * surface as a named assertion failure rather than as a suite that hangs.
 */
const TURN_DEADLINE_MS = 4_000;

/** Per-cell vitest timeout — comfortably above {@link TURN_DEADLINE_MS}. */
const CELL_TIMEOUT_MS = 20_000;

/** The bounded JSON-RPC request deadline the codex cells run under. */
const CODEX_REQUEST_TIMEOUT_MS = 60;

/**
 * The bounded per-turn wall clock the `child-mute` cells run under.
 *
 * Production's bound is {@link DEFAULT_TURN_DEADLINE_MS} (30 minutes — above any legitimate
 * agentic turn); every native-CLI harness takes a `turnTimeoutMs` override for exactly this
 * reason, so the SAME code path is measured in milliseconds here.
 */
const CELL_TURN_TIMEOUT_MS = 60;

// ---------------------------------------------------------------------------
// The fault shapes
// ---------------------------------------------------------------------------

/** The fault shapes every provider is measured against. */
const SHAPES = [
  {
    id: 'submit-rejects',
    title: 'submit() rejects — the fault reaches the wire, then exactly one marker',
  },
  {
    id: 'stream-ends-mid-turn',
    title: 'the event stream ends mid-turn — the fault reaches the wire, then exactly one marker',
  },
  {
    id: 'cli-launch-fails',
    title: 'the CLI never launched — the launch failure reaches the wire, then exactly one marker',
  },
  {
    id: 'cli-dies-mid-turn',
    title: 'the CLI died mid-turn — the exit status reaches the wire, then exactly one marker',
  },
  {
    id: 'child-mute',
    title:
      'the child stays alive and mute — the deadline reaches the wire, then exactly one marker',
  },
  {
    id: 'approval-gate-throws',
    title: 'the tool-approval gate throws — the turn still ends with exactly one marker',
  },
  {
    id: 'non-terminal-error-then-fault',
    title:
      'a NON-terminal agent.error precedes the fault — the truncation still reaches the wire, ' +
      'then exactly one marker',
  },
  {
    id: 'child-acks-then-goes-mute',
    title:
      'the child ACKNOWLEDGES the turn and then goes mute — the per-turn wall clock (not a ' +
      'request deadline) reaches the wire, then exactly one marker',
  },
  {
    id: 'deadline-then-next-turn',
    title:
      'the turn AFTER a deadline is not answered by the abandoned one — it ends on its own ' +
      'fault, with exactly one marker',
  },
] as const;

/** One fault shape's identifier. */
type ShapeId = (typeof SHAPES)[number]['id'];

/**
 * What a cell asserts.
 *
 * - `turn-terminal`: the turn WAS accepted, so the consumer is owed the full terminal
 *   contract — an `agent.error` naming the cause, then exactly one marker.
 * - `snapshot-refused`: the fault is fatal at BOOT, so `applySnapshot` must reject
 *   (the handler acks non-2xx and the owner pod retries). No turn is ever accepted, so
 *   none is owed a marker — and the loop must still drive no turn against a
 *   half-configured harness.
 * - `diagnostic-then-turn-terminal`: the turn carried a MID-TURN `agent.error` that is not
 *   about the turn ending (the transcript-mirror data-loss diagnostic) and was THEN cut
 *   short. The consumer is owed BOTH — the diagnostic where it happened, and the
 *   truncation as the turn's terminal explanation, in that order. Two errors is the
 *   correct count here and exactly one everywhere else, which is why this is its own
 *   outcome rather than a looser version of `turn-terminal`.
 * - `deadline-then-next-turn`: TWO turns are driven. The first is abandoned by the turn
 *   deadline; the second must end on a fault of its OWN and must never be handed the
 *   abandoned turn's answer. It is a separate outcome because it is the only one whose
 *   subject is a turn BOUNDARY rather than a single turn — the corruption it pins is
 *   invisible to any assertion that looks at one turn in isolation.
 */
type CellOutcome =
  | { kind: 'turn-terminal'; cause: RegExp }
  | { kind: 'snapshot-refused'; cause: RegExp }
  | { kind: 'diagnostic-then-turn-terminal'; diagnostic: RegExp; cause: RegExp }
  | {
      kind: 'deadline-then-next-turn';
      /** How the FIRST turn ends — the deadline. */
      firstCause: RegExp;
      /** How the SECOND turn ends. It is owed a fault of its OWN, never the first's answer. */
      secondCause: RegExp;
      /** The abandoned turn's answer text, which must reach NEITHER turn's consumer. */
      strandedAnswer: string;
    };

/** A conformance cell: either a real assertion, or an explicit reason it does not apply. */
type Cell =
  | { kind: 'run'; build: () => Promise<ConformanceRun>; expect: CellOutcome }
  | { kind: 'skip'; reason: string };

/** Everything a cell needs the shared driver to wire up. */
interface ConformanceRun {
  /** The provider name the snapshot selects (the registry key the loop dispatches on). */
  provider: string;
  /** The harness the provider factory returns for this cell's fault. */
  harness: AgentHarness;
  /** Extra snapshot fields (e.g. a `multiagent` roster). */
  snapshot?: Record<string, unknown>;
  /** Run after the snapshot is applied and before the turn is driven. */
  beforeTurn?: (harness: AgentHarness) => Promise<void>;
  /**
   * Run BETWEEN the two turns of a `deadline-then-next-turn` cell — where a cell releases
   * the abandoned turn's late answer itself, rather than letting the next turn's frame shake
   * it loose. Unused by every other outcome (they drive one turn).
   */
  betweenTurns?: () => Promise<void>;
}

/** A conformance row: one provider and one cell per fault shape. */
interface ProviderRow {
  provider: string;
  cells: Record<ShapeId, Cell>;
}

// ---------------------------------------------------------------------------
// The shared driver + assertions
// ---------------------------------------------------------------------------

/** One parsed NDJSON line off the loop's turn stream. */
type TurnLine = Record<string, unknown>;

/** A snapshot body selecting `provider`, plus any extra fields the cell needs. */
function snapshotBody(provider: string, extra: Record<string, unknown> = {}): Uint8Array {
  return new TextEncoder().encode(
    `${JSON.stringify({
      model: { provider: 'anthropic', id: 'claude-sonnet-4' },
      provider,
      system: 'be helpful',
      allowed_tool_names: ['bash'],
      allowed_mcp_server_names: [],
      egress: { mode: 'gateway' },
      ...extra,
    })}\n`,
  );
}

/**
 * The user-turn body a cell drives. Parameterised because the `deadline-then-next-turn`
 * cells drive TWO turns and the assertions turn on telling them apart.
 */
function turnBody(text = 'do the thing', id = 'evt_conformance'): Uint8Array {
  return new TextEncoder().encode(
    JSON.stringify({ type: 'user.message', id, content: [{ type: 'text', text }] }),
  );
}

/**
 * Bound `work` by the turn deadline. A turn that never settles is the *worst* form of
 * the defect (the owner pod's streaming POST hangs and the session is pending forever),
 * so it is reported as a named failure rather than left to stall the suite.
 */
async function withinTurnDeadline<T>(work: Promise<T>, what: string): Promise<T> {
  let timer: NodeJS.Timeout | undefined;
  const deadline = new Promise<never>((_resolve, reject) => {
    timer = setTimeout(
      () =>
        reject(
          new Error(
            `${what} never terminated within ${TURN_DEADLINE_MS}ms — the consumer got NO ` +
              'agent.turn_completed, so the registry would treat this turn as pending forever',
          ),
        ),
      TURN_DEADLINE_MS,
    );
  });
  try {
    return await Promise.race([work, deadline]);
  } finally {
    clearTimeout(timer);
  }
}

/** Drain the loop's NDJSON turn stream into parsed lines. */
async function drainLines(stream: AsyncIterable<Uint8Array>): Promise<TurnLine[]> {
  const chunks: Uint8Array[] = [];
  for await (const chunk of stream) {
    chunks.push(chunk);
  }
  return Buffer.concat(chunks.map((c) => Buffer.from(c)))
    .toString('utf8')
    .split('\n')
    .filter((line) => line.trim().length > 0)
    .map((line) => JSON.parse(line) as TurnLine);
}

/** Whether an NDJSON line belongs to the PRIMARY thread (a coordinator's child rides a subpath). */
function isPrimary(line: TurnLine): boolean {
  const subpath = line['subpath'];
  return subpath === undefined || subpath === '';
}

/** Render the observed line kinds for an assertion message. */
function kindsOf(lines: TurnLine[]): string {
  return JSON.stringify(lines.map((l) => l['type']));
}

/**
 * The invariant, asserted on what the CONSUMER actually received.
 *
 * Four things, and each of them was a shipped bug at least once:
 *   - the turn ends with a marker at all (else: pending forever);
 *   - the marker is not bare — an `agent.error` precedes it (else: a crash recorded as a
 *     successful empty answer);
 *   - that error NAMES the real cause (else: the operator gets "something went wrong");
 *   - there is EXACTLY ONE of each (a doubled terminal error means the harness reported
 *     the cause and the loop then buried it under a generic one; two markers would end
 *     the turn twice).
 */
function assertTurnTerminalContract(lines: TurnLine[], cause: RegExp): void {
  const primary = lines.filter(isPrimary);
  const markers = primary.filter((l) => l['type'] === TURN_COMPLETED_EVENT_KIND);
  expect(markers, `expected exactly one turn-completed marker; got ${kindsOf(lines)}`).toHaveLength(
    1,
  );
  expect(lines[lines.length - 1]?.['type'], `the marker must be LAST; got ${kindsOf(lines)}`).toBe(
    TURN_COMPLETED_EVENT_KIND,
  );

  const errors = primary.filter((l) => l['type'] === AGENT_ERROR_EVENT_KIND);
  expect(
    errors,
    `expected exactly one terminal agent.error naming the fault; got ${kindsOf(lines)}`,
  ).toHaveLength(1);
  expect(String(errors[0]?.['message'] ?? '')).toMatch(cause);

  // The error must be the line IMMEDIATELY before the marker: it is the turn's terminal
  // explanation, not an aside buried mid-stream.
  expect(
    lines[lines.length - 2]?.['type'],
    `the agent.error must immediately precede the marker; got ${kindsOf(lines)}`,
  ).toBe(AGENT_ERROR_EVENT_KIND);
}

/**
 * The invariant for the `non-terminal-error-then-fault` shape: the mid-turn diagnostic
 * reaches the wire where it happened, and the TRUNCATION is still the turn's terminal
 * explanation — the last thing before the marker.
 *
 * The failure this pins is silent and plausible-looking: with the diagnostic mistaken for
 * the turn's last word, the consumer got `[diagnostic, marker]` — a turn that ended for a
 * reason nobody stated, described by an accurate message about something else entirely.
 */
function assertDiagnosticThenTerminal(lines: TurnLine[], diagnostic: RegExp, cause: RegExp): void {
  const primary = lines.filter(isPrimary);
  const markers = primary.filter((l) => l['type'] === TURN_COMPLETED_EVENT_KIND);
  expect(markers, `expected exactly one turn-completed marker; got ${kindsOf(lines)}`).toHaveLength(
    1,
  );
  expect(lines[lines.length - 1]?.['type'], `the marker must be LAST; got ${kindsOf(lines)}`).toBe(
    TURN_COMPLETED_EVENT_KIND,
  );

  const errors = primary.filter((l) => l['type'] === AGENT_ERROR_EVENT_KIND);
  expect(
    errors,
    'expected the mid-turn diagnostic AND the terminal truncation — a single error here ' +
      `means one of them was swallowed; got ${JSON.stringify(errors.map((e) => e['message']))}`,
  ).toHaveLength(2);
  expect(String(errors[0]?.['message'] ?? '')).toMatch(diagnostic);
  expect(
    String(errors[1]?.['message'] ?? ''),
    'the TRUNCATION must be the turn`s terminal explanation, not the diagnostic',
  ).toMatch(cause);
  expect(
    lines[lines.length - 2]?.['type'],
    `the terminal agent.error must immediately precede the marker; got ${kindsOf(lines)}`,
  ).toBe(AGENT_ERROR_EVENT_KIND);
}

/**
 * The invariant for `deadline-then-next-turn`: the turn the deadline ABANDONED must never be
 * the turn the next one ANSWERS.
 *
 * The deadline ends a turn; on its own it says nothing to the child. A child left alive still
 * owes an answer to the frame it took, and every one of these readers credits a terminal
 * frame to whatever turn is CURRENT — so that answer landed on the NEXT turn, at HTTP 200,
 * replying to a question the transcript had already recorded as FAILED, while the question
 * the consumer actually asked went unanswered for good.
 *
 * The two placements corrupt VISIBLY differently — an answer arriving while turn two is in
 * flight hands the consumer turn one's text; one arriving between turns can leave turn two
 * with a bare marker — so both are driven, across more than one provider. Both must end the
 * same way: the abandoned answer reaches nobody, and turn two ends loudly on its own fault.
 */
function assertAbandonedAnswerNeverDelivered(
  first: TurnLine[],
  second: TurnLine[],
  answer: string,
): void {
  const carries = (lines: TurnLine[]): boolean =>
    lines.some((line) => JSON.stringify(line).includes(answer));
  expect(
    carries(first),
    'the abandoned turn was reported FAILED, so its own answer cannot also be its content',
  ).toBe(false);
  expect(
    carries(second),
    'the NEXT turn was answered by the ABANDONED one — a confident, wrong answer at HTTP ' +
      `200 to a question the transcript already recorded as failed; got ${kindsOf(second)}`,
  ).toBe(false);
}

/** Build a loop whose single registered provider hands back `run.harness`. */
function loopFor(run: ConformanceRun): SessionLoop {
  const providers = new ProviderRegistry();
  providers.register(run.provider, () => run.harness);
  // A coordinator row registers the same factory for the roster member too — the loop
  // dispatches a subagent's snapshot back through this very registry.
  return new SessionLoop({ workspaceId: WS, providers });
}

/** Drive one cell end to end and assert its declared outcome. */
async function runCell(cell: Extract<Cell, { kind: 'run' }>): Promise<void> {
  const run = await cell.build();
  const loop = loopFor(run);
  try {
    if (cell.expect.kind === 'snapshot-refused') {
      await expect(
        withinTurnDeadline(
          loop.applySnapshot(SES, snapshotBody(run.provider, run.snapshot)),
          'the snapshot apply',
        ),
      ).rejects.toThrow(cell.expect.cause);
      // No harness was configured, so the loop must drive NOTHING (the handler surfaces a
      // 503 and the owner pod retries) rather than half-run a turn against a dead harness.
      const lines = await withinTurnDeadline(
        drainLines(loop.runTurn(SES, turnBody(), new AbortController().signal)),
        'the turn after a refused snapshot',
      );
      expect(lines, `a refused snapshot must drive no turn; got ${kindsOf(lines)}`).toEqual([]);
      return;
    }

    await withinTurnDeadline(
      loop.applySnapshot(SES, snapshotBody(run.provider, run.snapshot)),
      'the snapshot apply',
    );
    await run.beforeTurn?.(run.harness);
    if (cell.expect.kind === 'deadline-then-next-turn') {
      const first = await withinTurnDeadline(
        drainLines(
          loop.runTurn(SES, turnBody('question one', 'evt_turn_one'), new AbortController().signal),
        ),
        'the turn the deadline abandons',
      );
      assertTurnTerminalContract(first, cell.expect.firstCause);
      // Where a cell releases the abandoned turn's answer ITSELF — the between-turns
      // placement. The during-next-turn cells leave this unset and let turn two's own frame
      // reach the child instead.
      await run.betweenTurns?.();
      const second = await withinTurnDeadline(
        drainLines(
          loop.runTurn(SES, turnBody('question two', 'evt_turn_two'), new AbortController().signal),
        ),
        'the turn AFTER a deadline',
      );
      assertAbandonedAnswerNeverDelivered(first, second, cell.expect.strandedAnswer);
      assertTurnTerminalContract(second, cell.expect.secondCause);
      return;
    }
    const lines = await withinTurnDeadline(
      drainLines(loop.runTurn(SES, turnBody(), new AbortController().signal)),
      'the driven turn',
    );
    if (cell.expect.kind === 'diagnostic-then-turn-terminal') {
      assertDiagnosticThenTerminal(lines, cell.expect.diagnostic, cell.expect.cause);
      return;
    }
    assertTurnTerminalContract(lines, cell.expect.cause);
  } finally {
    await loop.stop('client.archived').catch(() => undefined);
  }
}

// ---------------------------------------------------------------------------
// Per-provider fault injection
// ---------------------------------------------------------------------------

/** A per-session sandbox for the native-CLI rows (they refuse to run unsandboxed). */
async function sandbox(): Promise<SandboxHandle> {
  return new InMemorySandboxRuntime().acquire({});
}

/** Stop a harness before the turn — its `events()` is then already `done` on the first pull. */
async function stopFirst(harness: AgentHarness): Promise<void> {
  await harness.stop('replica.shutting_down');
}

/** A gate that RAISES instead of deciding — the tool-approval fault shape. */
const THROWING_GATE: ToolConfirmer = () => {
  throw new Error('approval gate exploded');
};

/**
 * Swap a throwing gate onto the harness's boot context.
 *
 * The loop always binds its OWN `confirmTool`, so a gate fault is injected here — at the
 * provider seam, on the way into `start` — leaving every other wire unchanged.
 */
function withThrowingGate<H extends AgentHarness>(harness: H): H {
  const start = harness.start.bind(harness);
  harness.start = (input: SessionStartInput): Promise<void> =>
    start({ ...input, confirmTool: THROWING_GATE });
  return harness;
}

/**
 * The SDK frame behind the `non-terminal-error-then-fault` shape: `system/mirror_error` —
 * the transcript-mirror DATA-LOSS diagnostic the SDK emits when a `SessionStore.append()`
 * batch could not be persisted and was DROPPED (a store / Kafka outage).
 *
 * It is the one real NON-terminal `agent.error` this codebase produces: the mapper turns it
 * into a primary `agent.error` (see `sdk-message-mapper.ts` `mapSystem`) that explains a
 * dropped batch and says nothing about the turn ending. It exists here because a suppression
 * rule keyed on the last event's KIND treated it as the turn's terminal explanation, so a
 * turn cut short right after a dropped batch reported the DROP and never mentioned the
 * truncation — the accurate cause silently dropped in favor of an unrelated one.
 */
const MIRROR_ERROR_FRAME = {
  type: 'system' as const,
  subtype: 'mirror_error' as const,
  error: 'kafka append timed out',
  key: { projectKey: WS, sessionId: SES },
  uuid: '00000000-0000-4000-8000-000000000001' as const,
  session_id: SES,
};

/** The message {@link MIRROR_ERROR_FRAME} maps to (reused where a harness is scripted). */
const MIRROR_ERROR_MESSAGE =
  'transcript mirror append failed (batch dropped): kafka append timed out';

/**
 * The ABANDONED turn's answer — the text that must reach neither turn's consumer.
 *
 * Shouted so it is unmistakable in a failure diff: where this string appears in turn two's
 * lines, the consumer was handed a confident answer to a question the durable transcript had
 * already recorded as FAILED, and its own question went unanswered for good.
 */
const STRANDED_ANSWER = 'THIS IS THE ANSWER TO TURN ONE';

/** claude-code / cursor stream-json: the settled assistant text, then the turn's `result`. */
const SDK_STREAM_ANSWER = [
  JSON.stringify({
    type: 'assistant',
    message: { content: [{ type: 'text', text: STRANDED_ANSWER }] },
  }),
  JSON.stringify({ type: 'result', subtype: 'success' }),
];

/** pi RPC: the assistant `message_end`, then the terminal `agent_end`. */
const PI_ANSWER = [
  JSON.stringify({
    type: 'message_end',
    message: { role: 'assistant', content: [{ type: 'text', text: STRANDED_ANSWER }] },
  }),
  JSON.stringify({ type: 'agent_end' }),
];

/** The `custom` row's declared dialect: an `assistant` text line, then the `done` boundary. */
const CUSTOM_ANSWER = [
  JSON.stringify({ type: 'assistant', text: STRANDED_ANSWER }),
  JSON.stringify({ type: 'done' }),
];

/** codex notifications: the settled `agentMessage`, then `turn/completed`. */
const CODEX_ANSWER = [
  JSON.stringify({
    method: 'item/completed',
    params: { item: { type: 'agentMessage', text: STRANDED_ANSWER } },
  }),
  JSON.stringify({ method: 'turn/completed', params: {} }),
];

/**
 * The status a wedged child reports once the deadline KILLS it.
 *
 * The stub tracks `requested` itself, so a kill this harness issued is reported as expected
 * and produces no exit fault — which is what makes the SECOND turn's cause the stream ending
 * rather than a spurious crash report.
 */
const KILLED_EXIT = { code: null, signal: 'SIGTERM' as const };

/**
 * Let a late frame released BETWEEN turns reach the read loop before the next turn is driven.
 *
 * Without it the between-turns placement would prove nothing: the frame would still be
 * sitting in the stub's queue when turn two starts, and the cell would pass for a reason that
 * has nothing to do with the child being dead.
 */
async function settleReader(): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, 20));
}

/** The in-process claude providers' shared SDK history adapter. */
function claudeAdapter(): ClaudeAgentSdkAdapter {
  return new ClaudeAgentSdkAdapter(new InMemoryTranscriptStore(), WS);
}

/** A one-shot SDK `query` whose turn stream FAULTS with `err`. */
function faultingQuery(err: Error): ClaudeQuery {
  return () => ({
    // A turn stream that only FAULTS yields nothing — that is the fault shape.
    // eslint-disable-next-line require-yield
    async *[Symbol.asyncIterator]() {
      throw err;
    },
  });
}

/** A one-shot SDK `query` that gates a tool call through the harness's `canUseTool`. */
const GATED_QUERY: ClaudeQuery = ({ options }) => ({
  // The turn's whole content is the gated tool call, so the generator yields nothing.
  // eslint-disable-next-line require-yield
  async *[Symbol.asyncIterator]() {
    const canUseTool = options.canUseTool;
    if (canUseTool !== undefined) {
      await canUseTool('bash', { command: 'ls' }, {
        signal: new AbortController().signal,
        suggestions: undefined,
        toolUseID: 'toolu_conformance',
      } as unknown as Parameters<NonNullable<typeof options.canUseTool>>[2]);
    }
  },
});

/** The persistent SDK handle shape, driven by a scripted turn generator. */
function persistentQuery(script: (options: Record<string, unknown>) => AsyncGenerator): {
  query: PersistentClaudeQuery;
} {
  const query: PersistentClaudeQuery = ({ options }) => {
    const handle = {
      interrupt: async (): Promise<void> => undefined,
      setModel: async (): Promise<void> => undefined,
      close: (): void => undefined,
      [Symbol.asyncIterator]: () => script(options as unknown as Record<string, unknown>),
    };
    return handle as unknown as PersistentQueryHandle;
  };
  return { query };
}

/** Options shared by every native-CLI row (the stub `launch` replaces the real spawn). */
function nativeOptions(
  handle: SandboxHandle,
  cli: NativeCliProcess,
): {
  workspaceId: string;
  sessionId: string;
  sandbox: SandboxHandle;
  launch: () => NativeCliProcess;
} {
  return { workspaceId: WS, sessionId: SES, sandbox: handle, launch: () => cli };
}

/**
 * A stubbed `codex app-server`: it completes the `initialize` handshake and opens the
 * thread, then behaves as `onTurnStart` says. Getting past the handshake is what makes
 * the codex `turn/start` leg reachable at all — a stub that fails `initialize` never
 * gets there, which is why that leg went untested through two review rounds.
 */
function codexAppServerStub(opts: CodexStubOptions): NativeCliProcess {
  return codexAppServerPilot(opts).cli;
}

/** How a {@link codexAppServerPilot} behaves once the handshake is done. */
interface CodexStubOptions {
  /**
   * `'mute'` answers `turn/start` with NOTHING, so codex's REQUEST deadline is what ends the
   * park. `'ack-then-mute'` ANSWERS it and then never notifies completion — the only shape
   * that reaches codex's per-TURN wall clock, and the reason that bound could be deleted
   * whole while the suite stayed at its baseline count. `'die'` ends stdout on the frame.
   */
  onTurnStart: 'mute' | 'die' | 'ack-then-mute';
  exit?: { code: number | null; signal: NodeJS.Signals | null };
  /**
   * Notifications emitted alongside the SECOND `turn/start` ack — the FIRST turn's answer,
   * arriving only once the next turn's frame reaches the app-server.
   */
  lateAnswer?: string[];
}

/**
 * {@link codexAppServerStub} plus the `say` a test drives LATE notifications through — the
 * app-server speaking with no request to hang the output off, which is exactly what an
 * abandoned turn's answer is.
 */
function codexAppServerPilot(opts: CodexStubOptions): {
  cli: NativeCliProcess;
  say: (line: string) => void;
} {
  let turnStarts = 0;
  const piloted = scriptedNativeCliWithDeath({
    ...(opts.exit !== undefined ? { exit: opts.exit } : {}),
    respond: (frame) => {
      let msg: { id?: unknown; method?: unknown };
      try {
        msg = JSON.parse(frame) as { id?: unknown; method?: unknown };
      } catch {
        return undefined;
      }
      if (msg.method === 'initialize') {
        return [JSON.stringify({ id: msg.id, result: {} })];
      }
      if (msg.method === 'thread/start') {
        return [JSON.stringify({ id: msg.id, result: { thread: { id: 'th_conformance' } } })];
      }
      if (msg.method === 'turn/start') {
        if (opts.onTurnStart === 'die') {
          return 'end';
        }
        if (opts.onTurnStart === 'mute') {
          return undefined;
        }
        turnStarts += 1;
        const ack = JSON.stringify({ id: msg.id, result: { turn: { id: `turn_${turnStarts}` } } });
        return turnStarts === 2 && opts.lateAnswer !== undefined
          ? [ack, ...opts.lateAnswer]
          : [ack];
      }
      return undefined;
    },
  });
  return { cli: piloted.cli, say: piloted.say };
}

/** The operator CLI contract the `custom` row runs under (a JSON-line dialect). */
const CUSTOM_SPEC = {
  command: 'my-json-agent',
  argv: ['--bridge-cmd', '{bridgeCommand}', '--bridge-args', '{bridgeArgsJson}'],
  stdin: { template: { prompt: '{userText}' } },
  stdout: {
    mode: 'jsonLine',
    text: { type_equals: 'assistant', text_field: 'text' },
    turn_completed: { type_equals: 'done' },
    approval_request: {
      type_equals: 'approval',
      id_field: 'request_id',
      name_field: 'tool',
      input_field: 'args',
    },
  },
  approvals: {
    response: { type: 'approval_response', request_id: '{requestId}', decision: '{decision}' },
    allow_value: 'allow',
    deny_value: 'deny',
  },
};

/** The reason a native-CLI `submit` cannot be made to reject. */
const SUBMIT_NEVER_REJECTS =
  'a native-CLI `submit` writes a frame and parks on the CLI’s terminal notification; it ' +
  'has no awaited fallible leg, so its faults arrive as the cli-launch-fails / ' +
  'cli-dies-mid-turn / child-mute shapes instead.';

/** The reason a row has no child process at all. */
const NO_CHILD_PROCESS = 'this provider runs the model in-process and launches no CLI child.';

/**
 * The reason a row cannot be measured on the `non-terminal-error-then-fault` shape.
 *
 * The shape needs an `agent.error` the harness emits MID-TURN — one that is not the turn's
 * terminal explanation. These providers' stream normalizers emit only `agent.message` /
 * `agent.tool_use` / `agent.tool_result` / `agent.usage`; every `agent.error` they produce
 * comes from a terminal fault path (a failed launch, a crash exit status, the turn
 * deadline, a protocol failure) and is flagged {@link AgentEvent.terminal}. So there is no
 * non-terminal `agent.error` to drive, and the shape has nothing to measure.
 */
/**
 * The `multiagent` row's OWN reason for a genuinely inapplicable cell — it is a DECORATOR,
 * not a provider, so a shape can fail to apply for reasons no provider row shares.
 * (`NO_CHILD_PROCESS` is false here: the coordinator wraps whatever harness the registry
 * built, which can be any native-CLI provider.)
 */
const COORDINATOR_OWNS_NO_TOOLS =
  'the coordinator owns no tools of its own — it threads the gate onto each subagent, so a ' +
  'gate fault is the SUBAGENT provider’s row, not this decorator’s.';

/**
 * The reason only codex can be measured on `child-acks-then-goes-mute`.
 *
 * These protocols have no separate ACKNOWLEDGEMENT of a turn frame: the harness writes it and
 * the very next thing it can hear is the turn's terminal notification, so there is exactly
 * ONE bound and `child-mute` already drives it — an ack-then-mute child is byte-for-byte a
 * mute one here. codex alone answers `turn/start` as a JSON-RPC REQUEST and notifies
 * completion separately, so it has TWO distinct bounds and only its cell can tell them apart.
 * That mattered: the codex `child-mute` cell satisfies the REQUEST deadline first, so codex's
 * entire per-turn wall-clock leg could be deleted and the suite stayed at exactly its
 * baseline count — the same "the bound has no coverage" class the exit-status grace was
 * caught in one round earlier.
 */
const ONE_TURN_BOUND =
  'this protocol does not acknowledge a turn frame separately from completing it — there is ' +
  'ONE bound here and `child-mute` drives it, so an ack-then-mute child is byte-for-byte a ' +
  'mute one. Only codex has a request bound distinct from its turn bound.';

const NO_NON_TERMINAL_ERROR =
  'this provider’s stream normalizer emits no `agent.error` at all — every error it ' +
  'produces comes from a terminal fault path (failed launch / crash exit status / turn ' +
  'deadline / protocol failure) and is flagged terminal, so it has no MID-TURN error this ' +
  'shape could put before the fault.';

// ---------------------------------------------------------------------------
// The matrix
// ---------------------------------------------------------------------------

async function codexSdkRun(
  mode: 'die' | 'mute' | 'launch' | 'boot-mute' | 'empty',
  provider: 'codex-sdk' | 'pi-sdk' = 'codex-sdk',
): Promise<ConformanceRun> {
  const handle = await sandbox();
  const cli =
    mode === 'launch'
      ? failedNativeCli(new Error('missing SDK worker'))
      : scriptedNativeCli({
          respond: (line) => {
            const command = JSON.parse(line);
            if (command.type === 'start')
              return mode === 'boot-mute' ? [] : [JSON.stringify({ type: 'ready' })];
            if (command.type === 'submit')
              return mode === 'die'
                ? 'end'
                : mode === 'empty'
                  ? [JSON.stringify({ type: 'done' })]
                  : [];
            return [];
          },
        });
  return {
    provider,
    snapshot: { model: { provider: 'openai', id: 'gpt-5.4' } },
    harness: new CodexSdkHarness({
      provider,
      apiKey: 'test-key',
      sandbox: handle,
      launch: () => cli,
      timeoutMs: CELL_TURN_TIMEOUT_MS,
    }),
  };
}

const ROWS: ProviderRow[] = [
  ...(['codex-sdk', 'pi-sdk'] as const).map(
    (provider): ProviderRow => ({
      provider,
      cells: {
        'submit-rejects': {
          kind: 'run',
          build: async () => ({ ...(await codexSdkRun('empty', provider)), beforeTurn: stopFirst }),
          expect: { kind: 'turn-terminal', cause: /unavailable/ },
        },
        'stream-ends-mid-turn': {
          kind: 'run',
          build: async () => ({ ...(await codexSdkRun('empty', provider)), beforeTurn: stopFirst }),
          expect: { kind: 'turn-terminal', cause: /unavailable/ },
        },
        'cli-launch-fails': {
          kind: 'run',
          build: () => codexSdkRun('launch', provider),
          expect: { kind: 'snapshot-refused', cause: /exited before completion/ },
        },
        'cli-dies-mid-turn': {
          kind: 'run',
          build: () => codexSdkRun('die', provider),
          expect: { kind: 'turn-terminal', cause: /exited before completion/ },
        },
        'child-mute': {
          kind: 'run',
          build: () => codexSdkRun('boot-mute', provider),
          expect: { kind: 'snapshot-refused', cause: /timed out/ },
        },
        'approval-gate-throws': { kind: 'skip', reason: GATE_COVERED_BY_PROVIDER_SPEC(provider) },
        'non-terminal-error-then-fault': { kind: 'skip', reason: NO_NON_TERMINAL_ERROR },
        'child-acks-then-goes-mute': {
          kind: 'run',
          build: () => codexSdkRun('mute', provider),
          expect: { kind: 'turn-terminal', cause: /timed out/ },
        },
        'deadline-then-next-turn': {
          kind: 'run',
          build: () => codexSdkRun('mute', provider),
          expect: {
            kind: 'deadline-then-next-turn',
            firstCause: /timed out/,
            secondCause: /unavailable/,
            strandedAnswer: STRANDED_ANSWER,
          },
        },
      },
    }),
  ),
  {
    provider: 'claude',
    cells: {
      'submit-rejects': {
        kind: 'run',
        build: async () => ({
          provider: 'claude',
          harness: new ClaudeAgentSdkHarness({
            apiKey: '',
            modelDefault: 'claude-sonnet-4',
            adapter: claudeAdapter(),
            workspaceId: WS,
            sessionId: SES,
            query: faultingQuery(new Error('anthropic stream reset')),
          }),
        }),
        expect: { kind: 'turn-terminal', cause: /anthropic stream reset/ },
      },
      'stream-ends-mid-turn': {
        kind: 'run',
        build: async () => ({
          provider: 'claude',
          harness: new ClaudeAgentSdkHarness({
            apiKey: '',
            modelDefault: 'claude-sonnet-4',
            adapter: claudeAdapter(),
            workspaceId: WS,
            sessionId: SES,
            query: () => ({
              async *[Symbol.asyncIterator]() {
                // no messages — the turn drains against an already-ended stream.
              },
            }),
          }),
          beforeTurn: stopFirst,
        }),
        expect: { kind: 'turn-terminal', cause: /stream ended before the turn completed/ },
      },
      'cli-launch-fails': { kind: 'skip', reason: NO_CHILD_PROCESS },
      'cli-dies-mid-turn': { kind: 'skip', reason: NO_CHILD_PROCESS },
      'child-mute': { kind: 'skip', reason: NO_CHILD_PROCESS },
      'approval-gate-throws': {
        kind: 'run',
        build: async () => ({
          provider: 'claude',
          harness: withThrowingGate(
            new ClaudeAgentSdkHarness({
              apiKey: '',
              modelDefault: 'claude-sonnet-4',
              adapter: claudeAdapter(),
              workspaceId: WS,
              sessionId: SES,
              query: GATED_QUERY,
            }),
          ),
        }),
        expect: { kind: 'turn-terminal', cause: /approval gate exploded/ },
      },
      'non-terminal-error-then-fault': {
        kind: 'run',
        build: async () => {
          // The turn drops a transcript-mirror batch (the diagnostic) and is THEN cut
          // short: the runner tears the harness down mid-turn — `stop()` ends `events()`,
          // which is how every mid-turn truncation reaches the loop. The diagnostic is the
          // harness's LAST primary word, so it is exactly the event a kind-only suppression
          // rule mistook for the turn's terminal explanation.
          const harness: ClaudeAgentSdkHarness = new ClaudeAgentSdkHarness({
            apiKey: '',
            modelDefault: 'claude-sonnet-4',
            adapter: claudeAdapter(),
            workspaceId: WS,
            sessionId: SES,
            query: () => ({
              async *[Symbol.asyncIterator]() {
                yield MIRROR_ERROR_FRAME;
                await harness.stop('replica.shutting_down');
              },
            }),
          });
          return { provider: 'claude', harness };
        },
        expect: {
          kind: 'diagnostic-then-turn-terminal',
          diagnostic: /transcript mirror append failed \(batch dropped\)/,
          cause: /stream ended before the turn completed/,
        },
      },
      'child-acks-then-goes-mute': { kind: 'skip', reason: NO_CHILD_PROCESS },
      'deadline-then-next-turn': { kind: 'skip', reason: NO_CHILD_PROCESS },
    },
  },
  {
    provider: 'claude-sdk-persistent',
    cells: {
      'submit-rejects': {
        kind: 'run',
        build: async () => ({
          provider: 'claude-sdk-persistent',
          harness: new ClaudePersistentSdkHarness({
            apiKey: '',
            modelDefault: 'claude-sonnet-4',
            adapter: claudeAdapter(),
            workspaceId: WS,
            sessionId: SES,
            // A live session that only FAULTS yields nothing — that is the fault shape.
            // eslint-disable-next-line require-yield
            ...persistentQuery(async function* () {
              throw new Error('live session transport reset');
            }),
          }),
        }),
        expect: { kind: 'turn-terminal', cause: /live session transport reset/ },
      },
      'stream-ends-mid-turn': {
        kind: 'run',
        build: async () => ({
          provider: 'claude-sdk-persistent',
          harness: new ClaudePersistentSdkHarness({
            apiKey: '',
            modelDefault: 'claude-sonnet-4',
            adapter: claudeAdapter(),
            workspaceId: WS,
            sessionId: SES,
            ...persistentQuery(async function* () {
              // no frames — the live generator simply ends.
            }),
          }),
          beforeTurn: stopFirst,
        }),
        expect: { kind: 'turn-terminal', cause: /stream ended before the turn completed/ },
      },
      'cli-launch-fails': { kind: 'skip', reason: NO_CHILD_PROCESS },
      'cli-dies-mid-turn': { kind: 'skip', reason: NO_CHILD_PROCESS },
      'child-mute': { kind: 'skip', reason: NO_CHILD_PROCESS },
      'approval-gate-throws': {
        kind: 'run',
        build: async () => ({
          provider: 'claude-sdk-persistent',
          harness: withThrowingGate(
            new ClaudePersistentSdkHarness({
              apiKey: '',
              modelDefault: 'claude-sonnet-4',
              adapter: claudeAdapter(),
              workspaceId: WS,
              sessionId: SES,
              // The turn's whole content is the gated tool call, so it yields nothing.
              // eslint-disable-next-line require-yield
              ...persistentQuery(async function* (options) {
                const canUseTool = options['canUseTool'];
                if (typeof canUseTool === 'function') {
                  await (canUseTool as (...a: unknown[]) => Promise<unknown>)(
                    'bash',
                    { command: 'ls' },
                    { signal: new AbortController().signal, toolUseID: 'toolu_conformance' },
                  );
                }
              }),
            }),
          ),
        }),
        expect: { kind: 'turn-terminal', cause: /approval gate exploded/ },
      },
      'non-terminal-error-then-fault': {
        kind: 'run',
        build: async () => {
          // Same shape over the LIVE-session harness: the mirror diagnostic lands, then the
          // runner tears the harness down mid-turn and `events()` ends behind it.
          const harness: ClaudePersistentSdkHarness = new ClaudePersistentSdkHarness({
            apiKey: '',
            modelDefault: 'claude-sonnet-4',
            adapter: claudeAdapter(),
            workspaceId: WS,
            sessionId: SES,
            ...persistentQuery(async function* () {
              yield MIRROR_ERROR_FRAME;
              await harness.stop('replica.shutting_down');
            }),
          });
          return { provider: 'claude-sdk-persistent', harness };
        },
        expect: {
          kind: 'diagnostic-then-turn-terminal',
          diagnostic: /transcript mirror append failed \(batch dropped\)/,
          cause: /stream ended before the turn completed/,
        },
      },
      'child-acks-then-goes-mute': { kind: 'skip', reason: NO_CHILD_PROCESS },
      'deadline-then-next-turn': { kind: 'skip', reason: NO_CHILD_PROCESS },
    },
  },
  {
    provider: 'claude-code',
    cells: {
      'submit-rejects': { kind: 'skip', reason: SUBMIT_NEVER_REJECTS },
      'stream-ends-mid-turn': {
        kind: 'run',
        build: async () => ({
          provider: 'claude-code',
          harness: new ClaudeCodeCliHarness({
            modelDefault: 'claude-sonnet-4',
            ...nativeOptions(await sandbox(), scriptedNativeCli()),
          }),
          beforeTurn: stopFirst,
        }),
        expect: { kind: 'turn-terminal', cause: /stream ended before the turn completed/ },
      },
      'cli-launch-fails': {
        kind: 'run',
        build: async () => ({
          provider: 'claude-code',
          harness: new ClaudeCodeCliHarness({
            modelDefault: 'claude-sonnet-4',
            ...nativeOptions(await sandbox(), failedNativeCli(new Error('spawn claude ENOENT'))),
          }),
        }),
        expect: { kind: 'turn-terminal', cause: /failed to launch: spawn claude ENOENT/ },
      },
      'cli-dies-mid-turn': {
        kind: 'run',
        build: async () => ({
          provider: 'claude-code',
          harness: new ClaudeCodeCliHarness({
            modelDefault: 'claude-sonnet-4',
            ...nativeOptions(await sandbox(), dyingNativeCli({ code: null, signal: 'SIGKILL' })),
          }),
        }),
        expect: { kind: 'turn-terminal', cause: /killed by SIGKILL/ },
      },
      'child-mute': {
        kind: 'run',
        build: async () => ({
          provider: 'claude-code',
          harness: new ClaudeCodeCliHarness({
            modelDefault: 'claude-sonnet-4',
            turnTimeoutMs: CELL_TURN_TIMEOUT_MS,
            ...nativeOptions(await sandbox(), muteNativeCli()),
          }),
        }),
        expect: {
          kind: 'turn-terminal',
          cause: /claude-code CLI did not complete the turn within 60ms/,
        },
      },
      'approval-gate-throws': {
        kind: 'skip',
        reason: GATE_COVERED_BY_PROVIDER_SPEC('claude-code'),
      },
      'non-terminal-error-then-fault': {
        kind: 'run',
        build: async () => ({
          provider: 'claude-code',
          // claude-code's stream-json normalizer routes every non-control frame through the
          // SAME `SdkMessageMapper` the in-process providers use, so a `system/mirror_error`
          // line on the CLI's stdout becomes the same MID-TURN `agent.error`. The CLI emits
          // it and then EXITS CLEANLY on the turn frame — a clean exit is no fault of its
          // own (`exit 0` reports nothing), so the diagnostic is the last thing said before
          // the stream ends and the loop owes the truncation.
          harness: new ClaudeCodeCliHarness({
            modelDefault: 'claude-sonnet-4',
            ...nativeOptions(
              await sandbox(),
              scriptedNativeCli({
                initial: [JSON.stringify(MIRROR_ERROR_FRAME)],
                respond: () => 'end',
                exit: { code: 0, signal: null },
              }),
            ),
          }),
        }),
        expect: {
          kind: 'diagnostic-then-turn-terminal',
          diagnostic: /transcript mirror append failed \(batch dropped\)/,
          cause: /stream ended before the turn completed/,
        },
      },
      'child-acks-then-goes-mute': { kind: 'skip', reason: ONE_TURN_BOUND },
      'deadline-then-next-turn': {
        kind: 'run',
        build: async () => {
          // LATE-DURING-THE-NEXT-TURN: turn one's frame is taken and answered by NOTHING, and
          // turn TWO's frame is what finally shakes turn one's answer loose. That placement is
          // how the consumer came to receive turn one's text as turn two's answer.
          const { cli } = scriptedNativeCliWithDeath({
            respond: (_frame, ordinal) => (ordinal === 2 ? SDK_STREAM_ANSWER : undefined),
            exit: KILLED_EXIT,
          });
          return {
            provider: 'claude-code',
            harness: new ClaudeCodeCliHarness({
              modelDefault: 'claude-sonnet-4',
              turnTimeoutMs: CELL_TURN_TIMEOUT_MS,
              ...nativeOptions(await sandbox(), cli),
            }),
          };
        },
        expect: {
          kind: 'deadline-then-next-turn',
          firstCause: /claude-code CLI did not complete the turn within 60ms/,
          secondCause: /stream ended before the turn completed/,
          strandedAnswer: STRANDED_ANSWER,
        },
      },
    },
  },
  {
    provider: 'codex',
    cells: {
      'submit-rejects': { kind: 'skip', reason: SUBMIT_NEVER_REJECTS },
      'stream-ends-mid-turn': {
        kind: 'run',
        build: async () => ({
          provider: 'codex',
          harness: new CodexCliHarness({
            modelDefault: 'gpt-5-codex',
            requestTimeoutMs: CODEX_REQUEST_TIMEOUT_MS,
            ...nativeOptions(await sandbox(), codexAppServerStub({ onTurnStart: 'mute' })),
          }),
          beforeTurn: stopFirst,
        }),
        expect: { kind: 'turn-terminal', cause: /stream ended before the turn completed/ },
      },
      'cli-launch-fails': {
        kind: 'run',
        build: async () => ({
          provider: 'codex',
          harness: new CodexCliHarness({
            modelDefault: 'gpt-5-codex',
            requestTimeoutMs: CODEX_REQUEST_TIMEOUT_MS,
            ...nativeOptions(await sandbox(), failedNativeCli(new Error('spawn codex ENOENT'))),
          }),
        }),
        // codex blocks `start` on its `initialize` handshake, so a dead CLI is fatal at
        // BOOT: the snapshot must be refused rather than a session run whose every turn
        // silently no-ops.
        expect: { kind: 'snapshot-refused', cause: /codex initialize failed/ },
      },
      'cli-dies-mid-turn': {
        kind: 'run',
        build: async () => ({
          provider: 'codex',
          harness: new CodexCliHarness({
            modelDefault: 'gpt-5-codex',
            requestTimeoutMs: CODEX_REQUEST_TIMEOUT_MS,
            ...nativeOptions(
              await sandbox(),
              codexAppServerStub({
                onTurnStart: 'die',
                exit: { code: 137, signal: null },
              }),
            ),
          }),
        }),
        expect: { kind: 'turn-terminal', cause: /exited with code 137/ },
      },
      'child-mute': {
        kind: 'run',
        build: async () => ({
          provider: 'codex',
          harness: new CodexCliHarness({
            modelDefault: 'gpt-5-codex',
            requestTimeoutMs: CODEX_REQUEST_TIMEOUT_MS,
            ...nativeOptions(await sandbox(), codexAppServerStub({ onTurnStart: 'mute' })),
          }),
        }),
        expect: { kind: 'turn-terminal', cause: /turn\/start.*timed out after 60ms/ },
      },
      'approval-gate-throws': { kind: 'skip', reason: GATE_COVERED_BY_PROVIDER_SPEC('codex') },
      'non-terminal-error-then-fault': { kind: 'skip', reason: NO_NON_TERMINAL_ERROR },
      'child-acks-then-goes-mute': {
        kind: 'run',
        build: async () => ({
          provider: 'codex',
          harness: new CodexCliHarness({
            modelDefault: 'gpt-5-codex',
            // BOTH bounds, which is the entire point of this cell: `turn/start` is ANSWERED,
            // so the request deadline never fires and only the per-turn wall clock can end
            // the park. The `child-mute` cell sets `requestTimeoutMs` alone and therefore
            // measures the other bound.
            requestTimeoutMs: CODEX_REQUEST_TIMEOUT_MS,
            turnTimeoutMs: CELL_TURN_TIMEOUT_MS,
            ...nativeOptions(await sandbox(), codexAppServerStub({ onTurnStart: 'ack-then-mute' })),
          }),
        }),
        expect: {
          kind: 'turn-terminal',
          cause: /codex CLI did not complete the turn within 60ms/,
        },
      },
      'deadline-then-next-turn': {
        kind: 'run',
        build: async () => ({
          provider: 'codex',
          // LATE-DURING-THE-NEXT-TURN over JSON-RPC: the app-server acks turn one and says
          // nothing more, then answers turn one's question alongside turn two's `turn/start`.
          harness: new CodexCliHarness({
            modelDefault: 'gpt-5-codex',
            requestTimeoutMs: CODEX_REQUEST_TIMEOUT_MS,
            turnTimeoutMs: CELL_TURN_TIMEOUT_MS,
            ...nativeOptions(
              await sandbox(),
              codexAppServerStub({
                onTurnStart: 'ack-then-mute',
                lateAnswer: CODEX_ANSWER,
                exit: KILLED_EXIT,
              }),
            ),
          }),
        }),
        expect: {
          kind: 'deadline-then-next-turn',
          firstCause: /codex CLI did not complete the turn within 60ms/,
          secondCause: /stream ended before the turn completed/,
          strandedAnswer: STRANDED_ANSWER,
        },
      },
    },
  },
  {
    provider: 'cursor',
    cells: {
      'submit-rejects': { kind: 'skip', reason: SUBMIT_NEVER_REJECTS },
      'stream-ends-mid-turn': {
        kind: 'run',
        build: async () => ({
          provider: 'cursor',
          harness: new CursorCliHarness({
            modelDefault: 'auto',
            ...nativeOptions(await sandbox(), scriptedNativeCli()),
          }),
          beforeTurn: stopFirst,
        }),
        expect: { kind: 'turn-terminal', cause: /stream ended before the turn completed/ },
      },
      'cli-launch-fails': {
        kind: 'run',
        build: async () => ({
          provider: 'cursor',
          harness: new CursorCliHarness({
            modelDefault: 'auto',
            ...nativeOptions(
              await sandbox(),
              failedNativeCli(new Error('spawn cursor-agent ENOENT')),
            ),
          }),
        }),
        expect: { kind: 'turn-terminal', cause: /failed to launch: spawn cursor-agent ENOENT/ },
      },
      'cli-dies-mid-turn': {
        kind: 'run',
        build: async () => ({
          provider: 'cursor',
          harness: new CursorCliHarness({
            modelDefault: 'auto',
            ...nativeOptions(await sandbox(), dyingNativeCli({ code: 1, signal: null })),
          }),
        }),
        expect: { kind: 'turn-terminal', cause: /exited with code 1/ },
      },
      'child-mute': {
        kind: 'run',
        build: async () => ({
          provider: 'cursor',
          harness: new CursorCliHarness({
            modelDefault: 'auto',
            turnTimeoutMs: CELL_TURN_TIMEOUT_MS,
            ...nativeOptions(await sandbox(), muteNativeCli()),
          }),
        }),
        expect: {
          kind: 'turn-terminal',
          cause: /cursor CLI did not complete the turn within 60ms/,
        },
      },
      'approval-gate-throws': { kind: 'skip', reason: GATE_COVERED_BY_PROVIDER_SPEC('cursor') },
      'non-terminal-error-then-fault': { kind: 'skip', reason: NO_NON_TERMINAL_ERROR },
      'child-acks-then-goes-mute': { kind: 'skip', reason: ONE_TURN_BOUND },
      'deadline-then-next-turn': {
        kind: 'run',
        build: async () => {
          // LATE-BETWEEN-TURNS: nothing shakes the answer loose — the child simply speaks
          // while NO turn is in flight. It corrupts visibly differently (turn two ended with a
          // BARE marker, its own question never answered), so both placements are driven.
          const piloted = scriptedNativeCliWithDeath({ exit: KILLED_EXIT });
          return {
            provider: 'cursor',
            harness: new CursorCliHarness({
              modelDefault: 'auto',
              turnTimeoutMs: CELL_TURN_TIMEOUT_MS,
              ...nativeOptions(await sandbox(), piloted.cli),
            }),
            betweenTurns: async () => {
              for (const line of SDK_STREAM_ANSWER) {
                piloted.say(line);
              }
              await settleReader();
            },
          };
        },
        expect: {
          kind: 'deadline-then-next-turn',
          firstCause: /cursor CLI did not complete the turn within 60ms/,
          secondCause: /stream ended before the turn completed/,
          strandedAnswer: STRANDED_ANSWER,
        },
      },
    },
  },
  {
    provider: 'pi',
    cells: {
      'submit-rejects': { kind: 'skip', reason: SUBMIT_NEVER_REJECTS },
      'stream-ends-mid-turn': {
        kind: 'run',
        build: async () => ({
          provider: 'pi',
          harness: new PiCliHarness({
            modelDefault: 'claude-sonnet-4-5',
            ...nativeOptions(await sandbox(), scriptedNativeCli()),
          }),
          beforeTurn: stopFirst,
        }),
        expect: { kind: 'turn-terminal', cause: /stream ended before the turn completed/ },
      },
      'cli-launch-fails': {
        kind: 'run',
        build: async () => ({
          provider: 'pi',
          harness: new PiCliHarness({
            modelDefault: 'claude-sonnet-4-5',
            ...nativeOptions(await sandbox(), failedNativeCli(new Error('spawn pi ENOENT'))),
          }),
        }),
        expect: { kind: 'turn-terminal', cause: /failed to launch: spawn pi ENOENT/ },
      },
      'cli-dies-mid-turn': {
        kind: 'run',
        build: async () => ({
          provider: 'pi',
          harness: new PiCliHarness({
            modelDefault: 'claude-sonnet-4-5',
            ...nativeOptions(await sandbox(), dyingNativeCli({ code: null, signal: 'SIGSEGV' })),
          }),
        }),
        expect: { kind: 'turn-terminal', cause: /killed by SIGSEGV/ },
      },
      'child-mute': {
        kind: 'run',
        build: async () => ({
          provider: 'pi',
          harness: new PiCliHarness({
            modelDefault: 'claude-sonnet-4-5',
            turnTimeoutMs: CELL_TURN_TIMEOUT_MS,
            ...nativeOptions(await sandbox(), muteNativeCli()),
          }),
        }),
        expect: { kind: 'turn-terminal', cause: /pi CLI did not complete the turn within 60ms/ },
      },
      'approval-gate-throws': { kind: 'skip', reason: GATE_COVERED_BY_PROVIDER_SPEC('pi') },
      'non-terminal-error-then-fault': { kind: 'skip', reason: NO_NON_TERMINAL_ERROR },
      'child-acks-then-goes-mute': { kind: 'skip', reason: ONE_TURN_BOUND },
      'deadline-then-next-turn': {
        kind: 'run',
        build: async () => {
          // LATE-BETWEEN-TURNS over pi's RPC dialect.
          const piloted = scriptedNativeCliWithDeath({ exit: KILLED_EXIT });
          return {
            provider: 'pi',
            harness: new PiCliHarness({
              modelDefault: 'claude-sonnet-4-5',
              turnTimeoutMs: CELL_TURN_TIMEOUT_MS,
              ...nativeOptions(await sandbox(), piloted.cli),
            }),
            betweenTurns: async () => {
              for (const line of PI_ANSWER) {
                piloted.say(line);
              }
              await settleReader();
            },
          };
        },
        expect: {
          kind: 'deadline-then-next-turn',
          firstCause: /pi CLI did not complete the turn within 60ms/,
          secondCause: /stream ended before the turn completed/,
          strandedAnswer: STRANDED_ANSWER,
        },
      },
    },
  },
  {
    provider: 'custom',
    cells: {
      'submit-rejects': { kind: 'skip', reason: SUBMIT_NEVER_REJECTS },
      'stream-ends-mid-turn': {
        kind: 'run',
        build: async () => ({
          provider: 'custom',
          harness: new CustomCliHarness({
            spec: CUSTOM_SPEC,
            modelDefault: 'my-model',
            ...nativeOptions(await sandbox(), scriptedNativeCli()),
          }),
          beforeTurn: stopFirst,
        }),
        expect: { kind: 'turn-terminal', cause: /stream ended before the turn completed/ },
      },
      'cli-launch-fails': {
        kind: 'run',
        build: async () => ({
          provider: 'custom',
          harness: new CustomCliHarness({
            spec: CUSTOM_SPEC,
            modelDefault: 'my-model',
            ...nativeOptions(
              await sandbox(),
              failedNativeCli(new Error('spawn my-json-agent ENOENT')),
            ),
          }),
        }),
        expect: { kind: 'turn-terminal', cause: /failed to launch: spawn my-json-agent ENOENT/ },
      },
      'cli-dies-mid-turn': {
        kind: 'run',
        build: async () => ({
          provider: 'custom',
          harness: new CustomCliHarness({
            spec: CUSTOM_SPEC,
            modelDefault: 'my-model',
            ...nativeOptions(await sandbox(), dyingNativeCli({ code: 2, signal: null })),
          }),
        }),
        expect: { kind: 'turn-terminal', cause: /exited with code 2/ },
      },
      'child-mute': {
        kind: 'run',
        build: async () => ({
          provider: 'custom',
          harness: new CustomCliHarness({
            spec: CUSTOM_SPEC,
            modelDefault: 'my-model',
            turnTimeoutMs: CELL_TURN_TIMEOUT_MS,
            ...nativeOptions(await sandbox(), muteNativeCli()),
          }),
        }),
        expect: {
          kind: 'turn-terminal',
          cause: /custom CLI did not complete the turn within 60ms/,
        },
      },
      'approval-gate-throws': { kind: 'skip', reason: GATE_COVERED_BY_PROVIDER_SPEC('custom') },
      'non-terminal-error-then-fault': { kind: 'skip', reason: NO_NON_TERMINAL_ERROR },
      'child-acks-then-goes-mute': { kind: 'skip', reason: ONE_TURN_BOUND },
      'deadline-then-next-turn': {
        kind: 'run',
        build: async () => {
          // LATE-DURING-THE-NEXT-TURN over an operator-declared dialect.
          const { cli } = scriptedNativeCliWithDeath({
            respond: (_frame, ordinal) => (ordinal === 2 ? CUSTOM_ANSWER : undefined),
            exit: KILLED_EXIT,
          });
          return {
            provider: 'custom',
            harness: new CustomCliHarness({
              spec: CUSTOM_SPEC,
              modelDefault: 'my-model',
              turnTimeoutMs: CELL_TURN_TIMEOUT_MS,
              ...nativeOptions(await sandbox(), cli),
            }),
          };
        },
        expect: {
          kind: 'deadline-then-next-turn',
          firstCause: /custom CLI did not complete the turn within 60ms/,
          secondCause: /stream ended before the turn completed/,
          strandedAnswer: STRANDED_ANSWER,
        },
      },
    },
  },
  {
    provider: 'mock',
    cells: {
      'submit-rejects': {
        kind: 'skip',
        reason:
          'the LLM-free mock harness answers from the user message alone — it awaits nothing ' +
          'fallible, so its `submit` has no rejection path to drive.',
      },
      'stream-ends-mid-turn': {
        kind: 'run',
        build: async () => ({
          provider: 'mock',
          harness: new MockAgentHarness(),
          beforeTurn: stopFirst,
        }),
        expect: { kind: 'turn-terminal', cause: /stream ended before the turn completed/ },
      },
      'cli-launch-fails': { kind: 'skip', reason: NO_CHILD_PROCESS },
      'cli-dies-mid-turn': { kind: 'skip', reason: NO_CHILD_PROCESS },
      'child-mute': { kind: 'skip', reason: NO_CHILD_PROCESS },
      'approval-gate-throws': {
        kind: 'skip',
        reason: 'the mock harness has no tools, so it never reaches the approval gate.',
      },
      'non-terminal-error-then-fault': { kind: 'skip', reason: NO_NON_TERMINAL_ERROR },
      'child-acks-then-goes-mute': { kind: 'skip', reason: NO_CHILD_PROCESS },
      'deadline-then-next-turn': { kind: 'skip', reason: NO_CHILD_PROCESS },
    },
  },
  {
    provider: 'multiagent',
    cells: {
      'submit-rejects': {
        kind: 'run',
        build: async () => ({
          provider: 'coordinator-base',
          snapshot: MULTIAGENT_SNAPSHOT,
          harness: new FakeAgentHarness([
            { events: [], fail: new Error('coordinator base model transport reset') },
          ]),
        }),
        expect: { kind: 'turn-terminal', cause: /coordinator base model transport reset/ },
      },
      'stream-ends-mid-turn': {
        kind: 'run',
        build: async () => ({
          provider: 'coordinator-base',
          snapshot: MULTIAGENT_SNAPSHOT,
          harness: new FakeAgentHarness([{ events: [], endStream: true, hold: never() }]),
          beforeTurn: stopFirst,
        }),
        expect: { kind: 'turn-terminal', cause: /stream ended before the turn completed/ },
      },
      // NOT `NO_CHILD_PROCESS`: `maybeWrapCoordinator` wraps whatever harness the registry
      // built, so a coordinator's BASE can be any native-CLI provider — a claude-code child
      // here. These three drive that base's real fault paths through the coordinator's
      // MERGED-stream path, which is the path this suite's own commit had to fix.
      'cli-launch-fails': {
        kind: 'run',
        build: async () => ({
          provider: 'coordinator-base',
          snapshot: MULTIAGENT_SNAPSHOT,
          harness: new ClaudeCodeCliHarness({
            modelDefault: 'claude-sonnet-4',
            ...nativeOptions(await sandbox(), failedNativeCli(new Error('spawn claude ENOENT'))),
          }),
        }),
        expect: { kind: 'turn-terminal', cause: /failed to launch: spawn claude ENOENT/ },
      },
      'cli-dies-mid-turn': {
        kind: 'run',
        build: async () => ({
          provider: 'coordinator-base',
          snapshot: MULTIAGENT_SNAPSHOT,
          harness: new ClaudeCodeCliHarness({
            modelDefault: 'claude-sonnet-4',
            ...nativeOptions(await sandbox(), dyingNativeCli({ code: null, signal: 'SIGKILL' })),
          }),
        }),
        expect: { kind: 'turn-terminal', cause: /killed by SIGKILL/ },
      },
      'child-mute': {
        kind: 'run',
        build: async () => ({
          provider: 'coordinator-base',
          snapshot: MULTIAGENT_SNAPSHOT,
          harness: new ClaudeCodeCliHarness({
            modelDefault: 'claude-sonnet-4',
            turnTimeoutMs: CELL_TURN_TIMEOUT_MS,
            ...nativeOptions(await sandbox(), muteNativeCli()),
          }),
        }),
        expect: {
          kind: 'turn-terminal',
          cause: /claude-code CLI did not complete the turn within 60ms/,
        },
      },
      'approval-gate-throws': { kind: 'skip', reason: COORDINATOR_OWNS_NO_TOOLS },
      'non-terminal-error-then-fault': {
        kind: 'run',
        build: async () => ({
          provider: 'coordinator-base',
          snapshot: MULTIAGENT_SNAPSHOT,
          // A base whose last word is a MID-TURN error (no `terminal` flag) and whose stream
          // then ends. The coordinator merges the diagnostic onto the primary thread
          // untouched and — because the base did NOT end the turn with a terminal error of
          // its own — reports the truncation itself, since the merged stream is still open
          // and the loop cannot see that the base's ended.
          harness: new FakeAgentHarness([
            {
              events: [
                { kind: AGENT_ERROR_EVENT_KIND, payload: { message: MIRROR_ERROR_MESSAGE } },
              ],
              endStream: true,
            },
          ]),
        }),
        expect: {
          kind: 'diagnostic-then-turn-terminal',
          diagnostic: /transcript mirror append failed \(batch dropped\)/,
          cause: /stream ended before the turn completed/,
        },
      },
      'child-acks-then-goes-mute': { kind: 'skip', reason: ONE_TURN_BOUND },
      'deadline-then-next-turn': {
        kind: 'run',
        build: async () => {
          // The same corruption THROUGH the coordinator's merged stream, over a real
          // claude-code base — the path whose own fault handling this suite's commit had to
          // fix once already.
          const piloted = scriptedNativeCliWithDeath({ exit: KILLED_EXIT });
          return {
            provider: 'coordinator-base',
            snapshot: MULTIAGENT_SNAPSHOT,
            harness: new ClaudeCodeCliHarness({
              modelDefault: 'claude-sonnet-4',
              turnTimeoutMs: CELL_TURN_TIMEOUT_MS,
              ...nativeOptions(await sandbox(), piloted.cli),
            }),
            betweenTurns: async () => {
              for (const line of SDK_STREAM_ANSWER) {
                piloted.say(line);
              }
              await settleReader();
            },
          };
        },
        expect: {
          kind: 'deadline-then-next-turn',
          firstCause: /claude-code CLI did not complete the turn within 60ms/,
          secondCause: /stream ended before the turn completed/,
          strandedAnswer: STRANDED_ANSWER,
        },
      },
    },
  },
];

/**
 * The reason a native-CLI gate cell is not re-measured here.
 *
 * The shape is real but produces NO turn fault on these rows, so there is no terminal
 * contract to assert: every native-CLI approval handler seeds a fail-closed deny, catches
 * the gate's throw, and answers the CLI from a `finally`, so the turn continues and ends
 * normally. That behavior is pinned per provider against its REAL protocol — including
 * that the gate's reason is not discarded, which is the part a fail-closed deny could
 * silently lose.
 *
 * Note the difference in SHAPE from the pinned provider specs' own gate assertions: those
 * drive a gate that REJECTS (an async throw the handler awaits), while
 * {@link THROWING_GATE} here throws SYNCHRONOUSLY. Both are caught by the same `try`, and
 * both are named accurately rather than being conflated.
 */
function GATE_COVERED_BY_PROVIDER_SPEC(provider: string): string {
  return (
    `every native-CLI provider contains a gate fault by construction (its approval handler ` +
    `seeds a fail-closed deny, binds the cause, and answers the CLI from a \`finally\`), so ` +
    `the turn continues and ends normally rather than faulting — there is no terminal ` +
    `contract for this shape to assert. Pinned against the real ${provider} protocol in ` +
    `test/unit/${provider}-provider.spec.ts (“a THROWING gate is answered as a DENY rather ` +
    `than wedging the CLI”, plus the companion case asserting the gate’s REASON is ` +
    `carried rather than discarded).`
  );
}

/** A promise that never settles — holds a scripted turn boundary open. */
function never(): Promise<void> {
  return new Promise<void>(() => undefined);
}

/**
 * The one row that is NOT a registry key: `multiagent` is a DECORATOR the loop wraps a
 * registered provider in, so the row-vs-registry guard above excludes it by name.
 */
const MULTIAGENT_DECORATOR_ROW = 'multiagent';

/** The minimal runner config `defaultRegisterProviders` needs for the row guard. */
const CONFORMANCE_CONFIG: RunnerConfig = {
  bindingToken: 'binding-token',
  registryRunnerUrl: 'ws://registry:8081/runner',
  workspace: '/var/run/orca/ws',
  workspaceId: WS,
  idleTimeoutS: 0,
  provider: { modelDefault: 'claude-sonnet-4' },
};

/** The coordinator roster the `multiagent` row's snapshot carries. */
const MULTIAGENT_SNAPSHOT: Record<string, unknown> = {
  multiagent: {
    type: 'coordinator',
    primary_thread_id: 'sth_primary',
    agents: [
      {
        agent_name: 'researcher',
        snapshot: {
          model: { provider: 'anthropic', id: 'claude-haiku-4' },
          provider: 'coordinator-base',
          system: 'research',
          allowed_tool_names: [],
          allowed_mcp_server_names: [],
          egress: { mode: 'gateway' },
        },
      },
    ],
  },
};

// ---------------------------------------------------------------------------
// The suite
// ---------------------------------------------------------------------------

/**
 * The rows are the providers the runner actually registers — ENFORCED, not asserted.
 *
 * The suite's premise is that "a provider that is added has a cell that must be filled in
 * rather than a hole nobody looks at". Nothing made that true: {@link ROWS} was a
 * hand-maintained literal that imported no production wiring, so a new provider produced
 * no new cell, no skip, and no failure — the hole reappeared silently and this suite would
 * have gone on claiming full coverage.
 *
 * So the real {@link defaultRegisterProviders} runs against a real {@link ProviderRegistry}
 * and its registered keys must EQUAL this matrix's provider rows. `multiagent` is excluded
 * from the comparison because it is a DECORATOR (`maybeWrapCoordinator` wraps a registered
 * provider), not a registry key — the one row that is deliberately not a provider.
 */
describe('the matrix rows are the registered providers', () => {
  it('every provider defaultRegisterProviders registers has a row, and vice versa', () => {
    const registry = new ProviderRegistry();
    defaultRegisterProviders(registry, CONFORMANCE_CONFIG, new InMemoryTranscriptStore());
    const registered = [...registry.providerNames()].sort();
    expect(
      Object.values(HARNESS_CATALOG)
        .map((entry) => entry.provider)
        .sort(),
    ).toEqual(registered);
    const rows = ROWS.map((r) => r.provider)
      .filter((p) => p !== MULTIAGENT_DECORATOR_ROW)
      .sort();
    expect(
      rows,
      'a provider was registered with no row in this matrix (or a row names a provider that ' +
        'is no longer registered) — every provider owes the consumer this contract, so add ' +
        'its row and fill every cell rather than leaving a hole nobody looks at',
    ).toEqual(registered);
  });
});

describe('harness turn-terminal conformance — every provider × every fault shape', () => {
  for (const row of ROWS) {
    describe(row.provider, () => {
      for (const shape of SHAPES) {
        const cell = row.cells[shape.id];
        if (cell.kind === 'skip') {
          it.skip(`${shape.title} [NOT APPLICABLE: ${cell.reason}]`, () => {
            expect(cell.reason.length).toBeGreaterThan(0);
          });
          continue;
        }
        it(
          shape.title,
          async () => {
            await runCell(cell);
          },
          CELL_TIMEOUT_MS,
        );
      }
    });
  }
});

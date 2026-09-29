# `@orca/harness-catalog`

> Library: the single source of truth for which harnesses exist, which
> topologies each supports, what sandbox image and port a `colocated`
> harness boots with, and which controls each Claude model accepts. Pure data
> plus pure functions — no I/O, no dependencies. Source:
> `packages/harness-catalog/`.

## Why it is a library and not a constant in one service

Two components need the same answer and would otherwise drift apart:

- **`registry-service-ts`** validates `metadata.harness` / `metadata.mode` on
  agent create and update, rejecting an unknown harness or an unsupported
  (harness, mode) pair before it reaches storage.
- **`harness-server`** routes on the resolved value and looks up the sandbox
  image and port for `colocated`.

Both import this package; neither restates the table.

**What it validates, and what it does not.** The catalog validates the annotation,
managed SDK model and feature policies, and the guardrail capabilities a harness
declares. It resolves execution ownership from the Environment target and pinned
harness/mode. It does **not** load providers or probe a deployed runtime. A
declared image and port do not establish an implementation in that image: the
`codex` native CLI uses the Registry runner, while cloud `claude_code` uses the
HTTP bridge. Each execution owner validates provider registration when selecting
the Session runtime.

## The annotation

A harness is selected per agent through `metadata.harness` + `metadata.mode` —
free-form JSONB on the Agent record. Carrying it in `metadata` rather than a
dedicated contract field is what keeps the wire Anthropic Managed
Agents-compatible: a client that knows nothing about harnesses sends nothing,
and gets the platform default.

```ts
import { resolveHarnessAnnotation } from '@orca/harness-catalog';

resolveHarnessAnnotation(agent.metadata);
// both keys absent  → { harness: 'claude_agent_sdk', mode: 'separate' }
// { harness: 'claude_code' }
//                   → { harness: 'claude_code', mode: 'colocated' }
// { harness: 'claude_code', mode: 'separate' }
//                   → { error: "harness 'claude_code' does not support mode 'separate' (allowed: colocated)" }
```

Resolution rules:

- Both keys absent → `DEFAULT_HARNESS` / `DEFAULT_MODE`.
- `harness` set, `mode` absent → that harness's first supported mode.
- Unknown harness, invalid mode, or an unsupported pair → `{ error }`.

The function never mutates `metadata`; the caller persists it as received. That
matters because the annotation round-trips through the public API — a client
reading an agent back sees exactly what it sent.

## The catalog

`src/catalog.ts` declares every harness:

| Harness                       | Modes                   | Default image                                        | Entrypoint                            | Port   |
| ----------------------------- | ----------------------- | ---------------------------------------------------- | ------------------------------------- | ------ |
| `claude_agent_sdk`            | `separate`              | — (runs inside `harness-server`)                     | —                                     | —      |
| `claude_agent_sdk_persistent` | `separate`              | — (live SDK session, no image)                       | —                                     | —      |
| `claude_code`                 | `colocated`             | `ghcr.io/orca-ae/sandbox-harness-claude-code:latest` | `/usr/local/bin/orca-sandbox-harness` | `4096` |
| `pi_sdk`                      | `separate`, `colocated` | `ghcr.io/orca-ae/sandbox-harness-claude-code:latest` | `/usr/local/bin/orca-sandbox-harness` | `4096` |
| `codex_sdk`                   | `separate`, `colocated` | `ghcr.io/orca-ae/sandbox-harness-claude-code:latest` | `/usr/local/bin/orca-sandbox-harness` | `4096` |
| `codex`                       | `colocated`             | `ghcr.io/orca-ae/sandbox-harness-codex:latest`       | `/usr/local/bin/orca-sandbox-harness` | `4096` |
| `cursor`                      | `colocated`             | `ghcr.io/orca-ae/sandbox-harness-cursor:latest`      | — (image default command)             | `4096` |
| `pi`                          | `colocated`             | `ghcr.io/orca-ae/sandbox-harness-pi:latest`          | — (image default command)             | `4096` |
| `custom`                      | `colocated`             | `ghcr.io/orca-ae/sandbox-harness-custom:latest`      | — (image default command)             | `4096` |
| `mock`                        | `colocated`             | — (in-process, no image)                             | —                                     | —      |

Each entry also carries a `provider` — the runner provider id the harness maps
to, which `harnessToProvider` exposes as the single source of that mapping.

`supportedModes[0]` is the default when `metadata.mode` is omitted.
`defaultImage` is `null` for a harness with no dedicated image to boot: the two
Claude SDK harnesses and `mock`, which runs in-process. `entrypoint` is
`null` wherever the image's own default command is left in charge — every harness
except the image entries for `claude_code`, `pi_sdk`, `codex_sdk`, and `codex`, so a `colocated` harness with a `defaultImage` can still have a `null`
entrypoint. No other code declares an image, an entrypoint, or a port.

`codex_sdk` and `pi_sdk` default to `separate`, matching the Claude SDK harness.
`cloud` plus `codex_sdk` or `pi_sdk` belongs to harness-server in both modes;
colocated shares the Claude sandbox image and HTTP bridge. A self-hosted Codex
Session requires the explicit colocated mode.

## Execution capabilities

`src/capabilities.ts` declares default-mode capabilities and deployment-specific
overrides. Consumers with a pinned selection call
`resolveHarnessCapabilities(selection)`; reading the default alone is insufficient
for ownership or tool setup. Codex's default separate capabilities enable the
host-side Sandbox toolset, MCP rewrite, and managed Skills. Its colocated override
keeps that host-side setup and managed Skills, adds the `self_hosted` target, and
narrows guardrail phases to `request`. Both modes retain the same model catalog
and reject multiagent rosters.

`validateHarnessDeployment(target, selection)` checks the resolved capability's
supported Environment targets. Registry applies it when creating Sessions,
changing an Environment target, preparing execution, and resolving a runner
snapshot. These checks use the pinned Agent version, so a latest-version mode
edit cannot change an existing Session's deployment.

`validateHarnessGuardrails(selection, guardrails)` returns an admission error for
any unsupported phase, stateful rule, or subagent scope. Codex separate supports
stateless `request`, `tool_call`, and `tool_result` phases, with stateful rules
limited by `statefulPhases: ["request"]`; colocated supports only the `request`
phase, for stateless and stateful rules. A null policy preserves another provider's existing
runtime enforcement contract. The catalog performs no guardrail evaluation.

## Model controls

`src/model-controls.ts` owns the Claude fast-mode allowlist
(`CLAUDE_FAST_MODE_MODEL_IDS`) and the per-model effort levels and defaults
(`CLAUDE_MODEL_EFFORT_LEVELS`, `getClaudeModelEffortCapability`). Registry and
both Claude runtimes validate against the same data, so their fail-closed
decisions stay aligned when the supported model catalog changes — the same
reason the harness table lives here rather than in one service.

## No I/O

The package is pure data and pure functions: no HTTP server, database client,
object store, or agent SDK. Its consumers own all persistence and transport.
That constraint is what makes the guarantee hold: what one boundary validates
cannot drift from what another boundary enforces, because both read the same
in-process data.

## Adding a harness

1. Add the entry to `HARNESS_CATALOG` with its supported modes, image, and port.
2. Declare capabilities, including execution ownership and any mode overrides.
3. Register the provider in its execution owner. A cloud HTTP bridge provider
   also needs an implementation in `services/sandbox-harness/` and an image.
4. Validate registration, selection, and admission against the shared catalog.

## Tests

```bash
pnpm -F @orca/harness-catalog test
```

No integration suite: the package performs no I/O.

## See also

- [`../harness-modes.md`](../harness-modes.md) — what the two topologies mean at
  runtime, the bridge, and the transport abstraction.
- [`../services/sandbox-harness.md`](../services/sandbox-harness.md) — the
  in-sandbox server the `colocated` entries point at.

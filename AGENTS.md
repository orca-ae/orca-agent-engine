# Repository Guidelines

Orca Agent Engine — a self-hosted alternative to Anthropic's Managed Agents API. A public
Anthropic-compatible registry service, an internal harness that runs the agent loop, pluggable
sandboxes, and a vault-aware egress gateway. See [`README.md`](README.md) for the architecture
diagram.

This file is for coding agents. People start with [`CONTRIBUTING.md`](CONTRIBUTING.md). Agents
follow the [AI policy](AI_POLICY.md): only a human adds `Signed-off-by:`, and a human approves every
push, pull request, issue and comment before an agent makes it.

Each `CLAUDE.md` in this repository is a symlink to the `AGENTS.md` next to it. Edit `AGENTS.md`,
never `CLAUDE.md`: a write that replaces the file turns the symlink into a copy.

## Repo map

| Path                               | What it is                                                                                                        |
| ---------------------------------- | ----------------------------------------------------------------------------------------------------------------- |
| `services/registry-service-ts/`    | Public Anthropic-compatible HTTP API + control plane                                                              |
| `services/harness-server/`         | Internal-only session runner; owns sandboxes, mounts, and harness routing                                         |
| `services/observability-exporter/` | Exports session transcripts from Kafka as OTLP traces; Docker image and an optional Helm workload, off by default |
| `services/sandbox-harness/`        | `@orca/sandbox-harness` — HTTP/SSE server baked into `colocated` harness images                                   |
| `services/session-runner/`         | Self-hosted per-session runner; dials the registry tunnel outbound and serves it                                  |
| `services/environment-worker/`     | Per-Environment worker; dials the registry worker tunnel and spawns those runners                                 |
| `services/environment-image/`      | Sandbox image baking the worker + runner binaries at known paths                                                  |
| `services/proto/`                  | Shared `.proto` definitions + buf workspace                                                                       |
| `services/dev/`                    | docker-compose + bring-up scripts (infra + ai-gateway image)                                                      |
| `packages/`                        | Store + support libraries, imported in-process via `workspace:*`                                                  |
| `packages/oeadm/`                  | The `oeadm` binary — `run` / `attach` / `env` / `worker`; a client, not a library                                 |
| `charts/`                          | Helm charts: the engine chart (registry + harness + ai-gateway against external infra) and `opensandbox-patches`  |
| `docs/managed-agents/`             | Design docs — the contract this code implements                                                                   |
| `proposals/`                       | Orca Improvement Proposals (OIPs): the design records behind changes others build on                              |

The AI gateway (MCP egress, and model egress when routed through it) is not built from this
repository. It ships as the public image `ghcr.io/orca-ae/orca-ai-gateway` and the Helm chart
`oci://ghcr.io/orca-ae/charts/orca-ai-gateway`; [`docs/compatibility.md`](docs/compatibility.md)
lists the version each release is tested with.

## Commands

```bash
pnpm install --frozen-lockfile   # install workspace deps
pnpm -r build                    # build every package
pnpm test                        # every package's tests except @orca/e2e-tests (what CI runs)
pnpm -r test                     # also the e2e suites, which fail unless `make stack-up` is running
pnpm lint                        # ESLint over every package and scripts/ (what CI runs)
pnpm format:check                # the Prettier gate; `pnpm format` rewrites the same paths
pnpm proto:gen                   # regenerate gRPC stubs after editing .proto
make stack-up / stack-down       # full local stack; see docs/managed-agents/local-stack.md

COVERAGE=1 pnpm test && pnpm coverage:report    # coverage for the unit suites
```

TypeScript: Node 22, ES modules, two-space indent, 100-column lines where Prettier applies (the
paths `pnpm format:check` covers). `camelCase` values, `PascalCase` types/classes. Unit tests in
`test/unit/`, integration in `test/integration/`.
`.proto`: file and message names `PascalCase`, fields `snake_case`.

## Hard rules

1. **Docs describe what exists; `roadmap.md` describes what doesn't.**
   Every doc under `docs/` states current behavior in the present tense. Anything not built — gaps,
   deferrals, triggers, "we will add…" — belongs in
   [`docs/managed-agents/roadmap.md`](docs/managed-agents/roadmap.md) and nowhere else. A PR that
   changes behavior updates the owning doc in the same commit; a PR that defers something adds a
   roadmap entry with its trigger instead of a note in the design doc. `docs/` is for developers and
   users — implementation trackers do not live there. Design reasoning (why a change was made, what
   was rejected) lives in an OIP under [`proposals/`](proposals/README.md): OIPs are design records,
   `roadmap.md` is the status register, and `docs/` is the reference.

   **Reviewing a docs change, ask:** does any sentence promise future work outside `roadmap.md`?
   Does any statement contradict the code it describes?

2. **API changes must stay Anthropic-compatible.** Check request/response shapes, beta headers,
   error envelope, streaming behavior, and versioning against
   <https://platform.claude.com/docs/en/api/beta>.
3. **Never commit secrets.** Prefer environment variables; `.env` is gitignored.
4. **Commit subjects are imperative and component-scoped** — for example
   `registry-service-ts: tighten vault binding`. Bodies explain _why_ and call out follow-ups.
   Sign off every commit (DCO, `git commit -s`); the human author adds the sign-off, never an agent.
   Disclose AI help with one `Assisted-by:` trailer. Never add AI `Co-Authored-By:` lines, session
   links, or references to non-public issues.
5. **Validate before review:**
   `pnpm install --frozen-lockfile && pnpm -r build && pnpm test && pnpm lint && pnpm format:check`,
   plus `pnpm docs:env-check`, `pnpm license:check` (`--fix` adds missing license headers) and
   `pnpm test:chart:render`, and
   `pnpm openapi:gen && pnpm conformance:gen` after an API change. CI (`test-ts.yml`,
   `test-proto.yml`) is the merge gate; integration suites are gated on local infra and run
   separately. The e2e suites are not part of that gate — see
   [CI runs only the suites a change can affect](#ci-runs-only-the-suites-a-change-can-affect).
6. **Files and commit messages are public.** Never write internal hostnames, private repository
   names, customer names, personal paths, credentials or AI session links into them.
7. **Changes others build on need an accepted OIP first.** A public API contract change, a new
   component or backend, a cross-cutting or breaking change, or a new harness or sandbox runtime
   starts as an [Orca Improvement Proposal](proposals/README.md). Its code lands after the OIP is
   accepted, and implementation PRs link it.

Pull requests follow [the template](.github/pull_request_template.md): explain rationale, link the
relevant `docs/managed-agents/...` section or open question, and call out any docs / CI / template
changes.

### CI retry and tool-path knobs

The helper scripts under `.github/scripts/` are tuned by environment, so a slow
or flaky runner can be adjusted without editing a workflow. All have defaults;
none needs to be set.

| Var                                   | Default   | Effect                                                                        |
| ------------------------------------- | --------- | ----------------------------------------------------------------------------- |
| `PNPM_INSTALL_ATTEMPTS`               | `3`       | Install retries in `pnpm-install-with-retry.sh`                               |
| `PNPM_INSTALL_RETRY_DELAY_SECONDS`    | `10`      | Wait between those attempts                                                   |
| `KIND_IMAGE_LOAD_MAX_ATTEMPTS`        | script    | Image-load retries in `load-kind-image-with-retry.sh`                         |
| `KIND_IMAGE_LOAD_RETRY_DELAY_SECONDS` | script    | Wait between those attempts                                                   |
| `KIND_IMAGE_LOAD_STREAM`              | `0`       | Stream exports to Docker-backed overlayfs kind nodes, avoiding a host archive |
| `KIND_IMAGE_LOAD_TIMEOUT`             | script    | Per-attempt timeout passed to `timeout`                                       |
| `PORT_FORWARD_RESTART_DELAY_SECONDS`  | script    | Restart delay in `supervise-kubectl-port-forward.sh`                          |
| `DOCKER_BIN`, `KIND_BIN`              | on `PATH` | Override the tool binaries the kind loader invokes                            |
| `KUBECTL_BIN`, `TIMEOUT_BIN`          | on `PATH` | Same, for the port-forward supervisor and the timeout wrapper                 |

### CI runs only the suites a change can affect

`.github/scripts/changed-areas.sh` classifies the diff and emits three booleans —
`ts`, `stack`, `kind` — that gate the jobs in `test-ts.yml`, `e2e-stack.yml` and
`e2e-kind-helm.yml`. A change to Markdown outside `services/` and `packages/`
skips all three (`pnpm format:check` covers Markdown under `services/`, so that
sets `ts`); a change to the engine chart under `charts/` runs `ts` and `kind`
but not `stack`. The script's
header holds the full mapping; edit it there, not in the workflows.

Two rules constrain any change to this:

- **Gate with job-level `if:`, never a `paths:` filter on `on:`.** `test`,
  `integration` and `lint-and-breaking` are required checks on `main`. GitHub
  reports a job skipped by an `if:` as _success_, but a workflow skipped by a
  trigger filter never reports at all — the check sits Pending and the PR can
  never merge. The e2e workflows follow the same rule, so that making one of
  them a required check can never wedge every open PR.
- **Anything unclassifiable runs everything.** An unrecognised path, an
  unresolvable diff range (force-push, shallow clone, new branch) and an empty
  diff all fail open. A wasted run costs minutes; a wrongly skipped one ships the
  break.

### The e2e suites run after merge, not before

`e2e-stack.yml` and `e2e-kind-helm.yml` are the two expensive workflows —
`e2e-stack` pays for one Claude leg (Kafka), one Pi SDK / Claude Sonnet leg
(Postgres), and one Codex SDK leg (Pulsar) in Layer B. They run:

| Trigger             | When                                                 |
| ------------------- | ---------------------------------------------------- |
| `push` to `main`    | every merged commit                                  |
| `schedule`          | 03:00 UTC (`e2e-stack`), 04:00 UTC (`e2e-kind-helm`) |
| `pull_request`      | **only** when the PR carries the `run-e2e` label     |
| `workflow_dispatch` | any ref, on demand                                   |

So a stack regression is caught after it lands, not before. **Label a PR
`run-e2e` when you are changing something the e2e suites cover** — the harness,
the sandbox runtimes, the gateway wiring, the charts — and the full suite runs
on the PR. Adding a label needs triage or write access, so contributors ask a
maintainer. The label cannot widen coverage past the filter above: a labelled
prose-only PR still skips.

The script reads `GITHUB_EVENT_NAME` and `GITHUB_BASE_REF` to pick a diff range,
`BEFORE_SHA` (set from `github.event.before` by each workflow) for the push case,
and writes its booleans to `GITHUB_OUTPUT`. The first three and the last are
provided by the Actions runtime; only `BEFORE_SHA` is ours to set.

The `e2e-stack` aggregator does not accept `skipped` or `success` per job. It
derives **one** expectation for the whole run and every suite must match it:

- a `pull_request` without the `run-e2e` label → all three must be `skipped`
- the filter said `stack=false` → all three must be `skipped`
- anything else, a broken `changes` job included → all three must be `success`

Per-job "success or skipped" looks equivalent and is not: a job that times out
records as `cancelled`, and a cancelled need cascades its dependents to
`skipped`, which under the lenient rule reported green over a run that proved
nothing. Any change
to a suite's `if:` must be mirrored in that block, or the verdict describes a
run that did not happen.

`test-proto.yml` and `claude-code-review.yml` are deliberately unfiltered: the
first is a required check that finishes in seconds, and the second is useful on
docs PRs.

## Go deeper

Per-area guidance loads automatically when you work in that directory:

- [`services/registry-service-ts/AGENTS.md`](services/registry-service-ts/AGENTS.md) — contracts,
  idempotency, headers, listeners.
- [`services/harness-server/AGENTS.md`](services/harness-server/AGENTS.md) — sandbox runtimes,
  harness modes, mount strategies.
- [`services/sandbox-harness/AGENTS.md`](services/sandbox-harness/AGENTS.md) — the in-sandbox bridge.
- [`services/session-runner/AGENTS.md`](services/session-runner/AGENTS.md) — the tunnel wire, the
  provider seam, the sandbox seam.
- [`services/environment-worker/AGENTS.md`](services/environment-worker/AGENTS.md) — the worker
  tunnel, the runner-env boundary, the two auth paths.
- [`packages/AGENTS.md`](packages/AGENTS.md) — library rules shared by every `packages/*` entry.
- [`packages/oeadm/AGENTS.md`](packages/oeadm/AGENTS.md) — the `oeadm` binary: its seams, its
  exit codes, and the tool-approval gate.

Design docs, in reading order:

- [`docs/managed-agents/README.md`](docs/managed-agents/README.md) — index.
- [`docs/managed-agents/overview.md`](docs/managed-agents/overview.md) — context, decisions,
  engineering principles.
- [`docs/managed-agents/architecture.md`](docs/managed-agents/architecture.md) — runtime topology.
- [`docs/managed-agents/services/`](docs/managed-agents/services/) — one doc per component.
- [`docs/managed-agents/roadmap.md`](docs/managed-agents/roadmap.md) — the only forward-looking
  doc: gaps, deferrals, and the trigger for each.

Operations:

- [`docs/operation/internal-traffic-auth.md`](docs/operation/internal-traffic-auth.md) — internal
  cluster traffic is plain HTTP/gRPC; registry internal calls use ServiceAccount JWT + TokenReview in
  Kubernetes or a shared service token elsewhere. Istio remains recommended for transport encryption.
- Kafka cluster operation (retention, tiered storage) is the platform's responsibility —
  application code does not configure it.

## Code coverage

Coverage is off by default so ordinary runs stay fast, and opt-in via `COVERAGE=1`:

```bash
pnpm coverage:clean                           # drop stale reports first
COVERAGE=1 pnpm test                          # unit -> <pkg>/coverage/unit/
make dev-up && COVERAGE=1 pnpm -r test:integration   # integration -> <pkg>/coverage/integration/
pnpm coverage:report                          # merge + print, no enforcement
```

Start with `coverage:clean`: unit and integration write to sibling directories and
each run only clears its own, so a report left over from an earlier commit would
otherwise be merged in and inflate the result. CI checks out fresh, so this only
bites locally.

Settings live in `vitest.shared.mjs` (plain ESM, because `scripts/merge-coverage.mjs`
imports it too). `all: true` is deliberate: without it v8 reports only files a test
imported, so never-loaded source would be invisible instead of scoring 0%.

`scripts/merge-coverage.mjs` merges the reports **workspace-wide**, not per package.
That is required, not stylistic — `harness-server` integration specs import
`registry-service-ts/src/server.ts` directly, so one package's run emits coverage for
another's source. Attribution comes from each covered file's own path, and paths are
normalized to repo-relative so a local checkout and a CI runner compare equal.

Capturing that cross-package coverage needs **`allowExternal: true`**, because Vitest
otherwise scopes coverage to the package directory and silently drops it. It is paired
with a `**/`-anchored `include`, and the two must change together: enabling
`allowExternal` switches the provider's globs to match absolute paths, so a
root-relative `src/**/*.ts` matches nothing and coverage drops to **zero entries with
no error**. `all: true` still globs from the package root, so a package enumerates only
its own files as 0% — an external file appears solely when a test actually loads it.

Every PR gets a **sticky comment** with the table, a delta column against the pinned
baseline, and a pass/fail verdict — one comment per PR, edited in place on each push.
It is the same markdown the script writes to `coverage/merged/summary.md`, so
`pnpm coverage:report` shows locally exactly what the PR will say.

Setting `COVERAGE_REPORT_URL` adds a "Full HTML report" link to that comment,
pointing at the run's `coverage-html` artifact (`scripts/merge-coverage.mjs:292`).
Unset, the comment simply omits the link.

`coverage-thresholds.json` is a **ratchet**: CI fails when any package drops more than
`tolerance` (0.5pp, absorbing v8's run-to-run jitter) below its pinned value. To raise
the bar after adding tests, or to deliberately accept a drop, run `pnpm coverage:update`
and commit the result — it prints a warning for every threshold it lowers.

Two things to know:

- **The thresholds assume merged unit+integration input.** `pnpm coverage` after a
  unit-only run will report a false regression for every package that has an
  integration suite. Use `pnpm coverage:report` locally unless you ran both.
- **Adding coverage data can _lower_ a branch percentage.** It is a ratio, and with
  `all: true` a file no test loaded contributes almost no branches to the denominator.
  When another suite actually loads it, it contributes a full branch map that is mostly
  uncovered, so the denominator grows faster than the numerator. `file-store` lines rose
  62.8% -> 86.7% when integration data was merged in while its branches fell 79% ->
  77.8%. Line coverage is monotonic under merging; branch coverage is not. Never derive
  a baseline from a subset of the suites and assume the full run will clear it.
- **The `coverage` CI job reports; it does not block.** It is not a required check —
  those are the job ids `test`, `integration` and `lint-and-breaking`. The job also
  downgrades to report-only whenever `test` or `integration` failed, since partial
  reports cannot be fairly compared against the baseline.

## Implementation workflow

Work through multi-task implementation plans in bounded steps with clear acceptance criteria.
Check each step against its requirements, review code quality, and run the relevant validation
before considering it complete.

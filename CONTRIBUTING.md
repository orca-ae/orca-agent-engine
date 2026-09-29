# Contributing to Orca Agent Engine

Thanks for your interest in Orca Agent Engine, a self-hosted implementation of the managed-agents
API. Bug reports, fixes, documentation, tests and feedback on the API are all welcome.

> **Using an AI assistant?** Read the [AI policy](AI_POLICY.md) first.
> **Are you a coding agent?** Start with [AGENTS.md](AGENTS.md).

## Ways to contribute

- **Report a bug or request a feature.** Open an
  [issue](https://github.com/orca-ae/orca-agent-engine/issues/new/choose). Issues about the AI
  gateway, the `ork` CLI and the TypeScript SDK belong here too, although their source isn't in this
  repository.
- **Ask a question or share an idea.** Start a
  [discussion](https://github.com/orca-ae/orca-agent-engine/discussions).
- **Fix something.** Comment on the issue to say you're working on it, so nobody duplicates your
  work.
- **Improve the docs.** If something confused you, it will confuse the next person too.

## Where to talk

| For                                                | Use                                                                                             |
| -------------------------------------------------- | ----------------------------------------------------------------------------------------------- |
| Bugs and concrete feature requests                 | [Issues](https://github.com/orca-ae/orca-agent-engine/issues)                                   |
| Questions                                          | [Discussions: Q&A](https://github.com/orca-ae/orca-agent-engine/discussions/categories/q-a)     |
| Ideas and designs to discuss before you write code | [Discussions: Ideas](https://github.com/orca-ae/orca-agent-engine/discussions/categories/ideas) |
| Security vulnerabilities                           | Report privately, as described in [SECURITY.md](SECURITY.md)                                    |
| Conduct concerns                                   | See [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md)                                                    |

The project doesn't run a chat server or a mailing list, so decisions happen where everyone can
read them.

## Before you write code

- **Small, self-contained changes** can go straight to a pull request. Examples: a bug fix with a
  test, a documentation correction, a typo.
- **For anything larger**, open an issue or a discussion first. That includes a new feature, a
  refactor across services, a change in behavior, and any change to a public contract:
  - the Anthropic-compatible `/v1` API
  - an extension group under `/apis/<group>/<version>`
  - the runner and worker tunnels, or the `.proto` wire format
  - the database schema and its migrations
  - environment variables and other configuration
  - Helm chart values

  Agreeing on the approach first saves you from writing code that has to be redone.

- **Changes other people build on need an Orca Improvement Proposal (OIP).** That covers a public
  API contract change, a new component or backend, a cross-cutting or breaking change, and a new
  harness or sandbox runtime. Start with a thread in
  [Discussions → Ideas](https://github.com/orca-ae/orca-agent-engine/discussions/categories/ideas),
  then open a pull request that adds the OIP under [`proposals/`](proposals/README.md). The code
  lands after the OIP is accepted; [`proposals/README.md`](proposals/README.md) has the process.

- **Changes to the `/v1` API must stay compatible with Anthropic's.** Check request and response
  shapes, beta headers, the error envelope, streaming behavior and versioning against
  [Anthropic's API reference](https://platform.claude.com/docs/en/api/beta).

## Build and test

You need Node.js 22 (22.21 or later) or Node.js 24.9 or later, pnpm 9, and Docker with the Compose
plugin. `pnpm install` builds a native Pulsar binding, so you also need a C++ toolchain and
Python 3: the Xcode Command Line Tools on macOS, or `python3 make g++ binutils xz-utils` on Debian
and Ubuntu. Editing `.proto` files needs [`buf`](https://buf.build/), and the chart tests need
[`helm`](https://helm.sh/).

```bash
git clone https://github.com/orca-ae/orca-agent-engine.git
cd orca-agent-engine
pnpm install --frozen-lockfile
```

Before you open a pull request, run the checks that CI's `test` job runs:

```bash
pnpm -r build            # build every package
pnpm test                # every package's tests, except the end-to-end suites
pnpm lint                # ESLint over every package and scripts/
pnpm format:check        # Prettier; `pnpm format` fixes what it reports
pnpm docs:env-check      # environment variables the code reads match the ones the docs describe
pnpm license:check       # every source file has the license header; `pnpm license:check --fix` adds it
pnpm test:chart:render   # renders the Helm chart and checks the output; needs helm
```

Some changes need generated files regenerated and committed. CI fails when they are out of date:

```bash
pnpm openapi:gen && pnpm conformance:gen   # after an API change: the OpenAPI spec and the conformance matrix
pnpm proto:gen                             # after editing a .proto file: the generated stubs
```

Use `pnpm test` and `pnpm lint`, not `pnpm -r test` and `pnpm -r lint`. `pnpm -r test` also runs
`@orca/e2e-tests`, which fails unless the local stack is running, and `pnpm -r lint` skips the lint
of `scripts/`.

Integration suites need local infrastructure. `make dev-up` starts Postgres, RustFS and the
transcript broker in Docker, and `pnpm -F <package> test:integration` runs one package's suite.

The end-to-end suites need a running stack. After `make stack-up`, `pnpm e2e:wire` checks the API
without a model key, and `pnpm e2e:agent` runs real agents; the model provider bills those calls.
After `make self-hosted-up`, `pnpm e2e:self-hosted` runs a session on this machine without a model
key. [`docs/managed-agents/local-stack.md`](docs/managed-agents/local-stack.md) describes every
suite.

### What CI runs

- `test-ts.yml` runs `test` (the checks above), `integration` (the integration suites against
  Postgres, Kafka and RustFS) and `coverage`. `coverage` posts a report and doesn't block.
- `test-proto.yml` runs `lint-and-breaking`: `buf lint`, and on pull requests `buf breaking`, which
  reports without failing.
- `test`, `integration` and `lint-and-breaking` are required. The `test-ts.yml` jobs skip, and
  report success, when [`.github/scripts/changed-areas.sh`](.github/scripts/changed-areas.sh) finds
  that the change can't affect them. A change to Markdown outside `services/` and `packages/` skips
  them.
- The end-to-end workflows, `e2e-stack.yml` and `e2e-kind-helm.yml`, run on every push to `main`,
  daily, and on a pull request that carries the `run-e2e` label. Adding a label needs triage or write
  access to the repository, so ask a maintainer to add `run-e2e` when your change touches the
  harness, the sandbox runtimes, the gateway wiring or the charts.
- A pull request can get an automated review from Claude Code, pinned to `claude-sonnet-5`.
  Its comments are advisory; a maintainer approves every merge.

## How the code is organized

| Path                            | What it is                                                                   |
| ------------------------------- | ---------------------------------------------------------------------------- |
| `services/registry-service-ts/` | The public Anthropic-compatible API and control plane                        |
| `services/harness-server/`      | Runs the agent loop for cloud sessions                                       |
| `services/session-runner/`      | Per-session runner that dials the registry                                   |
| `services/environment-worker/`  | One per self-hosted environment; spawns session runners                      |
| `services/proto/`               | Shared `.proto` definitions                                                  |
| `services/dev/`                 | The local stack                                                              |
| `packages/`                     | Libraries imported in-process, the `oeadm` client, and the end-to-end suites |
| `charts/`                       | Helm charts                                                                  |
| `docs/managed-agents/`          | Design docs: the contract this code implements                               |

[AGENTS.md](AGENTS.md) has the full map, and each service and `packages/` has its own `AGENTS.md`
with the rules for working there. A few rules apply everywhere:

- `packages/` holds libraries, not services. The services import them through `workspace:*`.
- Every tenant key and query carries workspace scope. That is the isolation boundary between
  workspaces.
- [`packages/harness-catalog/src/catalog.ts`](packages/harness-catalog/src/catalog.ts) is the single
  source of truth for which harnesses exist and which modes each supports.

## Code style

- TypeScript on Node.js, as ES modules, with two-space indentation. ESLint checks every package,
  and Prettier formats the paths that `pnpm format:check` lists.
- `camelCase` for values, `PascalCase` for types and classes.
- In `.proto` files, file and message names are `PascalCase` and fields are `snake_case`.
- Unit tests live in `test/unit/`, and integration tests in `test/integration/`.
- Never commit secrets. Read them from environment variables; `.env` files are ignored by git.
- Files and commit messages are public. Don't write internal hostnames, private repository names,
  customer names, personal paths, credentials or AI session links into them.

## Documentation

Docs describe what exists, and [`docs/managed-agents/roadmap.md`](docs/managed-agents/roadmap.md)
describes what doesn't:

- Every doc under `docs/` states current behavior in the present tense.
- A pull request that changes behavior updates the doc that owns it, in the same pull request.
- A pull request that defers something adds a roadmap entry with the condition that would justify
  building it, instead of a note in a design doc.

`pnpm docs:env-check` fails when a doc names an environment variable that nothing reads, or when
the code reads one that no doc describes.

## Commits

### Sign your commits (DCO)

Every commit needs a Developer Certificate of Origin sign-off:

```bash
git commit -s -m "registry-service-ts: tighten vault binding"
```

The `-s` flag adds a line such as `Signed-off-by: Your Name <you@example.com>`. The line certifies
that you wrote the change, or otherwise have the right to submit it under the project's license. The
full text is at [developercertificate.org](https://developercertificate.org/).

If you forgot to sign off, fix the last commit with `git commit --amend -s --no-edit`, or a series
with `git rebase --signoff origin/main`, and then force-push your branch.

We don't use a CLA. The DCO sign-off is all we ask.

### Write useful messages

Start the subject with the component you changed, then a short summary in the imperative mood:
`registry-service-ts: tighten vault binding`. In the body, explain why the change is needed and call
out any follow-up work.

Pull requests are squash-merged. The pull request title becomes the commit subject on `main`, so
title your pull request the same way. The squashed commit keeps the `Signed-off-by:` and
`Assisted-by:` trailers of the commits it replaces.

### Say when AI helped

If an AI tool helped meaningfully, add one `Assisted-by:` trailer that names the tool, such as
`Assisted-by: Claude Code`. Don't credit a tool with `Co-authored-by:`, which is for people, and
don't add session links or other trailers that a tool generates. The [AI policy](AI_POLICY.md)
explains what counts.

## Pull requests

1. Fork the repository on GitHub, and add your fork as a remote:
   `git remote add fork https://github.com/<your-username>/orca-agent-engine.git`. Create a branch
   for your change, and push it to `fork`.
2. Keep each pull request to one logical change. Smaller pull requests get reviewed sooner.
3. Fill in the [pull request template](.github/pull_request_template.md): what changed and why,
   compatibility, how you tested it, and AI assistance.
4. Update the documentation in the same pull request when you change behavior or a contract, as
   described in [Documentation](#documentation).
5. Make sure CI passes.
6. A code owner for the files you touched reviews and approves the change. Code owners are listed in
   [CODEOWNERS](.github/CODEOWNERS). These docs call them maintainers; see
   [MAINTAINERS.md](MAINTAINERS.md).

If your pull request has been quiet for a while, @-mention one of the maintainers.

A large change can land as a stack of pull requests, each reviewable on its own. Maintainers stack
them on branches in this repository, base each pull request on the one before it, and say "Stack
n/N, depends on #X" in its description; when the parent merges, the next one is retargeted to
`main`. Contributors working from a fork send the pieces as sequential pull requests instead.

## Security issues

Don't report a vulnerability in a public issue, pull request or discussion. Follow
[SECURITY.md](SECURITY.md) instead.

## License

Orca Agent Engine is licensed under the [Apache License 2.0](LICENSE), and so is your contribution.
If you copy code from another project, keep its license header in the file and add the project to
[NOTICE](NOTICE) in the same pull request.

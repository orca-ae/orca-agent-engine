# Orca Improvement Proposals (OIPs)

An OIP is the design record for a change that other people build on or operate against. It says what
problem the change solves, what was decided, which alternatives were rejected and why, and how the
change stays compatible.

OIPs sit next to two other kinds of document:

- **`docs/`** describes what the code does today, in the present tense.
- **[`docs/managed-agents/roadmap.md`](../docs/managed-agents/roadmap.md)** lists work that isn't
  done, with the condition that would make us do it.
- **An OIP** records why and how a change was designed. When its work lands, the owning document
  under `docs/` is updated in the same pull request, and the OIP keeps the reasoning.

## When you need an OIP

You need an accepted OIP before the code lands for:

- **A public API contract change.** Anything on the Anthropic-compatible `/v1` surface or an
  `/apis/<group>/<version>` extension group: request and response shapes, headers, errors,
  pagination and streaming behavior.
- **A new component, backend or deployable.** A new service, library or storage backend, such as a
  transcript store or file store, or a new workload in the Helm chart.
- **A cross-cutting or breaking change.** The runner or worker tunnel protocol, gRPC and proto
  definitions, the database schema, configuration and environment variables, Helm values, or
  anything that breaks an upgrade or a rollback.
- **A new harness or sandbox runtime.** Adding an agent harness, or a runtime behind the sandbox
  seam.

You don't need an OIP for bug fixes, internal refactoring, performance work that keeps behavior and
formats unchanged, tests or documentation. If you're not sure, ask in
[Discussions](https://github.com/orca-ae/orca-agent-engine/discussions/categories/ideas).

## How it works

1. **Start a discussion.** Open a thread in Discussions, under Ideas, that describes the problem:
   what you're trying to do, and what gets in the way today. The first step is agreeing that the
   problem is worth solving.
2. **Write the OIP.** Copy [`TEMPLATE.md`](TEMPLATE.md) to `proposals/OIP-NNN-short-title.md`, using
   the highest existing number plus one. If two open pull requests pick the same number, the one
   merged second renumbers. Add a row to the index below.
3. **Open a pull request** that adds the file with the status *Proposed*, and link it from the
   discussion. The design review happens on the pull request, so the document and its review stay
   together. The pull request carries the `oip` label.
4. **Review.** The maintainers review it, together with the owner of each component it changes.
   Expect questions about compatibility, rollback, alternatives and testing.
5. **Acceptance.** An OIP is accepted with approvals from two maintainers, at least one of whom owns
   an affected area, and no unresolved objection from a maintainer. Merging the pull request accepts
   it. An OIP that isn't accepted is closed, with the reasons recorded on the pull request.
6. **Keep the status current.** Implementation pull requests link the OIP. As the work lands, update
   the OIP's header and its row in the index in the same pull request.

## Statuses

| Status | Meaning |
|---|---|
| Proposed | Under review in a pull request |
| Accepted | Merged; implementation can start |
| Implemented | The code has landed on `main` |
| Released | Shipped in a release; the header records the version |
| Superseded | Replaced by a later OIP, which the header links |

## Design records for features that predate this process

Features built before this repository was public have OIPs too. Each one records the design as it
shipped, including alternatives that were considered, reversed or withdrawn. They carry the status
*Released*, cite v0.5.0 as the first public release that contains them, and keep the date the design
was first proposed.

## Writing a good OIP

- Lead with the problem, not the solution.
- Say what changes in each component, and in what order the changes ship.
- Be explicit about upgrade and rollback. Call a breaking change breaking.
- Keep it short. Link to `docs/` rather than repeating it.
- Record the alternatives you rejected and why. The next person to propose one of them will want to
  know.
- After the change ships, use *Status notes* to record where the code diverged from the design.

## Index

| OIP | Title | Components | Status |
|---|---|---|---|
| [001](OIP-001-transcript-store-backends.md) | Pluggable transcript store backends | transcript-store, registry-service-ts, harness-server, observability-exporter, Helm chart | Released in v0.5.0 |
| [002](OIP-002-agent-harnesses-and-execution-modes.md) | Agent harnesses and execution modes | harness-server, sandbox-harness, harness libraries, Helm chart | Released in v0.5.0 |
| [003](OIP-003-egress-boundary.md) | The egress boundary: AI gateway, MCP routing and LLM egress | registry-service-ts, harness-server, sandbox-harness, Helm chart | Released in v0.5.0 |
| [004](OIP-004-sandbox-runtimes.md) | Sandbox runtimes behind one seam | sandbox-runtime, cloud-sandbox, harness-server, Helm charts | Released in v0.5.0 |
| [005](OIP-005-conformance-decision-register.md) | Conformance as a decision register | registry-service-ts, CI | Released in v0.5.0 |
| [006](OIP-006-session-event-semantics.md) | Session event acceptance and completion | registry-service-ts, harness-server, transcript-store | Released in v0.5.0 |
| [007](OIP-007-tenancy.md) | Tenancy: workspace isolation, admin planes and OIDC audiences | registry-service-ts, harness-server, file-store, memory-store, transcript-store, Helm chart | Released in v0.5.0 |
| [008](OIP-008-skills.md) | Skills: immutable bundles and progressive disclosure | registry-service-ts, harness-server, skill-store, sandbox-runtime, session-runner | Released in v0.5.0 |
| [009](OIP-009-core-and-extension-api-groups.md) | Core and extension API groups | registry-service-ts | Released in v0.5.0 |
| [010](OIP-010-guardrails-pricing-and-spend.md) | Guardrails, model pricing and spend limits | registry-service-ts, harness-server, session-runner, guardrails, harness-catalog, Helm chart | Released in v0.5.0 |
| [011](OIP-011-self-hosted-session-runner.md) | Self-hosted session runner and environments | session-runner, environment-worker, registry-service-ts, oeadm, harness-tunnel, environment-image | Released in v0.5.0 |
| [012](OIP-012-agent-observability.md) | Agent observability | observability-exporter, registry-service-ts, agent-event-contract, Helm chart | Released in v0.5.0 |
| [013](OIP-013-registry-api-read-performance.md) | Registry API read performance | registry-service-ts, memory-store | Released in v0.5.0 |
| [014](OIP-014-exporter-state-and-startup.md) | Exporter state and startup recovery | observability-exporter, Helm chart | Released in v0.5.0 |

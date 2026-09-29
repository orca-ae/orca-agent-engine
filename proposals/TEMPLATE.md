# OIP-NNN: Title of the proposal

<!--
Copy this file to proposals/OIP-NNN-short-title.md and replace every placeholder.
Delete any section that doesn't apply, and write "None" where a category has no changes.
See README.md in this directory for the process.
-->

- *Author(s)*: Your Name (@your-github-handle)
- *Status*: Proposed
- *Proposal time*: YYYY-MM-DD
- *Components*: the components this changes, such as registry-service-ts, harness-server,
  session-runner, environment-worker, observability-exporter, a library under `packages/`, or the
  Helm chart
- *Discussion*: link to the discussion or issue where the proposal started
- *Implementation*: links to the pull requests, once they exist
- *Released in*: the version, once released, such as "v0.6.0"

## TL;DR

Two or three sentences: the problem, the change, and who it affects.

## Background

What a reader needs to know to follow the proposal. Link to the documents under `docs/` rather than
repeating them.

## Motivation

The problem, concretely. What are people trying to do, and what gets in the way today?

## Goals

### In scope

### Out of scope

## Design

### High-level design

### Detailed design

## Changes by component

What changes in each component. Delete the components that don't change.

- **registry-service-ts** (the public API and control plane):
- **harness-server** (the session runtime):
- **session-runner** and **environment-worker** (self-hosted execution):
- **observability-exporter**:
- **Libraries** (`packages/*`):
- **Helm chart** (`charts/orca-managed-agents`):

## Public-facing changes

List everything that a user, an operator or an implementer can see change.

### API

Changes to the Anthropic-compatible `/v1` surface or to an `/apis/<group>/<version>` extension
group: request and response shapes, headers, errors and pagination. Say how the change stays
compatible with the managed-agents API it implements.

### Events and streaming

Changes to session events, their ordering or their delivery over SSE.

### Wire protocols

Changes to the runner and worker tunnels, gRPC or proto definitions, or the in-sandbox harness
protocol.

### Storage

Changes to the database schema and migrations, the transcript format, or object-store layouts.

### Configuration

New, changed or removed environment variables and Helm values, with their defaults, and the
component that reads each one.

### Metrics, logs and traces

## Compatibility

### Upgrade

What happens when a deployment upgrades? Do existing data, configuration and running sessions keep
working?

### Rollback

What happens if an operator rolls back to the previous release after this change has written data?
If rolling back isn't possible, say so plainly.

### Version skew

If the change spans components, which one has to be upgraded first, and what happens while they run
different versions, for example during a rolling upgrade?

## Security considerations

New inputs, credentials, network access or permissions, and how they are protected.

## Testing

How the change is tested: unit tests, integration tests against the local stack, and the end-to-end
suites.

## Alternatives

What else you considered, and why you didn't choose it.

## Open questions

What the review has to settle before the OIP can be accepted. Delete this section once every
question is answered.

## Status notes

After the change ships, record here where the implementation diverged from this design, and why.

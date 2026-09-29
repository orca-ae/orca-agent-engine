# OIP-008: Skills: immutable bundles and progressive disclosure

- *Author(s)*: @jiangpengcheng
- *Status*: Released
- *Proposal time*: 2026-07-29
- *Components*: registry-service-ts, harness-server, `packages/skill-store` (`@orca/skill-store`),
  `packages/sandbox-runtime` (shared bundle validators and catalog), session-runner (the
  `POST /v1/runner/skills` route)
- *Discussion*: None (predates the public repository)
- *Implementation*: `packages/skill-store/`, `packages/sandbox-runtime/src/skills-materialize.ts`;
  registry-service-ts `src/api/{skills.routes,skill-version-archive}.ts`,
  `src/domain/{session-skill-bindings,prepare-execution,skill-bundle-deletion-outbox}.ts`,
  `src/tunnel/session-skills-delivery.ts`, migrations `0035` to `0040`; harness-server
  `src/sandbox/skills/materialize.ts`, `src/runner/dispatcher.ts`; session-runner
  `src/skills-materialize.ts`, `src/session-loop.ts`, `src/resources.ts`
- *Released in*: v0.5.0

## TL;DR

An earlier design treated a Skill as a prompt fragment: each SkillVersion's `system_prompt` was
appended to the Agent's system prompt and its `tool_allowlist` intersected with the Agent's tools,
so every Skill cost context on every turn and could silently remove tools. A SkillVersion is now an
immutable file bundle with a `SKILL.md` entrypoint, addressed by SHA-256 in object storage, pinned
when a Session is created and copied read-only to `/workspace/skills/<name>/`. The model sees each
Skill's name, description and entrypoint path and reads the rest on demand with the Agent's own
`read` tool. Skill and Agent authors and harness-server and runner operators are affected.

## Background

[`skills.md`](../docs/managed-agents/skills.md) owns the behavior; see also
[`libraries/skill-store.md`](../docs/managed-agents/libraries/skill-store.md), the Skills sections
of [registry-service](../docs/managed-agents/services/registry-service.md#skills) and
[session-runner](../docs/managed-agents/services/session-runner.md#skills), and the paged `read` in
[harness-server](../docs/managed-agents/services/harness-server.md#bounded-server-side-read).
Related: [OIP-002](OIP-002-agent-harnesses-and-execution-modes.md) (topologies),
[OIP-004](OIP-004-sandbox-runtimes.md) (setup), [OIP-010](OIP-010-guardrails-pricing-and-spend.md)
(`block_skills`), [OIP-011](OIP-011-self-hosted-session-runner.md) (runner tunnel).

## Motivation

- **Context grew with every Skill.** The rubric appended every Skill's `system_prompt` to the
  Agent's prompt at session start, so a Session paid for all of them on every turn.
- **Skills changed the tool surface.** Runtime tools were the Agent's tools intersected with each
  Skill's `tool_allowlist` (`[]` meant none), so a Skill could remove a tool the Agent declared,
  Skills interacted through the intersection, and permission had two owners.
- **The shape diverged from Anthropic's**: directories with a `SKILL.md` entrypoint, immutable
  versions with timestamp identifiers, `{type, skill_id, version}` references that allow `latest`,
  and progressive disclosure. Stored prompts did not map onto that, uploaded files sat in a JSONB
  column with no content address or integrity check, and scripts had nowhere to run.
- **Reproducibility sat at the wrong boundary.** References resolved when the Agent was written,
  freezing `latest` into the AgentVersion, so a Skill fix meant re-versioning every Agent; yet a
  running Session must keep its bytes even after a version is deleted.

## Goals

### In scope

- Immutable bundles named by the SHA-256 of their stored bytes, verified at every open, with only
  metadata in Postgres; Anthropic's `/v1/skills` lifecycle, `latest` included.
- `latest` pinned, fail-closed, at Session creation for the primary agent and its direct roster.
- One catalog for `separate` and `colocated`, and the same pinned bytes for runner-owned Sessions.
- A Skill adds a catalog row and read-only files; tools and permission policies stay with the
  AgentVersion, `block_skills` applies where Skills are staged, and only exact pins populate the
  reserved root.

### Out of scope

- Anthropic pre-built Skills: Agents accept `type: "anthropic"` references, but Session creation
  resolves them only against locally stored `anthropic`-source Skills, and no route creates one.
- Skill discovery by an SDK's own loader on harness-server; guardrails (OIP-010), runner providers
  (OIP-011), harness selection (OIP-002); Environment images for `colocated`, a limitation listed
  on [`roadmap.md`](../docs/managed-agents/roadmap.md#known-limitations).

## Design

### High-level design

```text
 upload ─► Registry: validate, SkillStore.put ─► …/skill-versions/<id>/bundles/<sha256>
 Session create ─► session_skill_bindings: agent, version, ordinal, id, bundle_sha256
 harness-server:    descriptors ─► open exact pin, verify ─► /workspace/skills/<name>/
 Registry + runner: open exact pin, verify ─► POST /v1/runner/skills ─► runner tree
 prompt:    Agent text + <available_skills>, one row each: name, description, path
 on demand: read SKILL.md ─► read referenced files ─► scripts via Agent-enabled tools
```

### Detailed design

**The bundle.** `@orca/skill-store` is an in-process library: a `SkillStore` interface with
`S3SkillStore` and `InMemorySkillStore`. `put` (`codec.ts`) normalizes paths, keeps only
executable intent (`0755` or `0644`), rejects paths that collide exactly or after NFC plus Unicode
full case folding, and serializes a canonical, path-sorted `orca.skill-bundle.v1` JSON envelope of
each file's mode, media type, SHA-256 and base64 content. The SHA-256 of those bytes, at most
64 MiB, is the identity; the object is written once under
`workspaces/{workspace}/skill-versions/{id}/bundles/{sha256}/` with `If-None-Match: *`. `open`
re-hashes, requires canonical JSON and base64, and checks every file digest. Postgres keeps the
digest, the size and a content-free manifest.

**Upload.** Both upload routes take `multipart/form-data` only (`skill-version-archive.ts`): one
ZIP or several file parts, at most 128 files and 30 MiB uncompressed; a ZIP inflates in 16 KiB
chunks against per-file and total limits, so a compression bomb is a `413`. A single top-level
directory holds a `SKILL.md` whose frontmatter meets the
[Storage model](../docs/managed-agents/skills.md#storage-model) rules; the workspace comes from
authentication. The public `version` is milliseconds × 1000 plus the ordinal, kept increasing.

**References and pinning.** An Agent holds up to 500 references, `{type: "custom", skill_id,
version}` (`version` defaults to `latest`) or `{type: "anthropic", …}`. Agent writes reject a
missing version, one SkillVersion twice, or one name with two digests (`validateSkillRefs`); an
update that omits `skills` tolerates references that stopped resolving. The Session is the
reproducibility boundary: in its creation transaction, `resolveSessionSkillBindings` reads the
primary AgentVersion (or `agent_with_overrides.skills`) and each direct roster member under
`FOR SHARE`, rejects nested coordinators and over 500 bindings, resolves `latest`, allows one digest
per name across the graph, and inserts a `session_skill_bindings` row per reference. A failure is
a `400`; a cron Trigger fire that cannot bind pauses its Trigger. Session reads show identifiers.

**What the model sees.** Prepared execution returns each agent's own ordered descriptors, joined
from the bindings on version id and digest. `composeSkillsCatalog` (`@orca/sandbox-runtime`)
appends an `<available_skills>` block to the Agent's text: one fixed instruction to read a relevant
Skill's `SKILL.md` before relying on it, then only what it references, and one JSON line per Skill
with `name`, `description`, `path` (`/workspace/skills/<name>/SKILL.md`) and `package_sha256`. No
body enters the prompt, and each roster agent sees only its own Skills. The model pages files with
`read` and runs scripts only through tools its AgentVersion enables; nothing adds `read` or `bash`.
A catalog without a sandbox, a SkillStore or an enabled `read` fails setup, as does `colocated`
without `read` at `always_allow`, since the in-sandbox bridge has no confirmation round trip. The
failure is `session.error` (`setup_failed`, `phase: "skill_setup"`) and an idle `retries_exhausted`.

**Materialization on harness-server.** Every harness-server topology prepares its sandbox in
order: `prepareFilesystemRoots` proves the output root, each mount path and `/workspace/skills`
canonical and mount-free; resources activate, each target checked against the reserved root
lexically and through symlinks; `materializeSkills` runs; the write policy is sealed.
`materializeSkills` always deletes the tree first, image-supplied files included; opens exact pins
host-side one at a time; checks digest, size, manifest, paths, file digests and entrypoint; writes
the roster's union once, files `0444` or `0555` and directories `0555`; and removes a partial tree
on failure. The policy lists the root read-only only for a non-empty catalog. Both modes share tree
and prompt: the `separate` Claude SDK runs with `settingSources: []` and no plugin loader, and the
Codex and Pi SDKs take the catalog as developer instructions.

**Delivery to a runner.** A runner holds no SkillStore and no object-store credentials. On each
(re)connect the owning Registry sends managed resources, Skills, Git capabilities, then the
snapshot. `SessionSkillsDelivery` resolves the Session's union, keeps one bundle per name, checks
each with the shared `validateSkillBundle`, and sends one NDJSON request: a `skills_manifest` line,
then one `skill_file` line per file. The runner re-validates names and paths, then:

- **Managed `codex_sdk` and `pi_sdk`**: delivered descriptors must equal the snapshot's pins as a
  multiset; `block_skills` applies; `/workspace/skills` in the model-tool root is swapped atomically
  while no tool runs; the same catalog is composed; a missing acknowledgement fails preparation.
- **Other providers**: a read-only Claude Code plugin (`.claude-plugin/plugin.json` plus
  `skills/<name>/…`) under the runner workspace, which `claude_code` loads with `--plugin-dir`.

**Native resume.** Codex and Pi checkpoints store `instructionsSha256`, a digest of the developer
instructions, catalog included. harness-server fails native resume closed on a changed catalog. A
managed runner rewrites the rollout's developer message for a `block_skills` change: both catalogs
exact subsets of the same verified pins, Agent text unchanged (`isPinnedSkillsCatalog`).

**Deletion.** Deleting a version sets `deleted_at` and moves `latest_version` to the newest
remaining version or `null`; deleting a Skill is a `400` while any version is public. Rows and
bundles are retained and prepared execution ignores `deleted_at`, so Sessions keep their pins.
`skill_bundle_deletion_outbox` only removes a bundle whose metadata transaction failed, under an
advisory lock shared with the writer, and never while an exact SkillVersion owns it
([Deletion lifecycle](../docs/managed-agents/skills.md#deletion-lifecycle)).

## Changes by component

- **registry-service-ts**: uploads, the Skill routes, reference validation, Session binding,
  per-agent descriptors, reserved mount paths (`resource-mount-path.ts`), outbox, the runner push.
- **harness-server**: materialization, the per-agent catalog, `block_skills` filtering, setup order.
- **session-runner**: the skills route, the managed Skill root and the Claude Code plugin layout.
- **Libraries**: new `@orca/skill-store`; validators and catalog in `@orca/sandbox-runtime`;
  instruction digests in `@orca/codex-harness` and `@orca/pi-harness`. **Helm chart**: none.

## Public-facing changes

### API

| Operation | Behavior |
| --- | --- |
| `POST /v1/skills` | multipart; creates the Skill and its first version; `400`, `409`, `413` |
| `GET /v1/skills` | `limit` 1 to 100 (default 20), opaque `page` cursor, `source` filter |
| `GET`, `DELETE /v1/skills/{id}` | delete is a `400` while versions remain |
| `POST /v1/skills/{id}/versions` | multipart; `display_title` is a `400` |
| `GET /v1/skills/{id}/versions` | `limit` 1 to 1000 (default 20); `page` is the last identifier |
| `GET`, `DELETE /v1/skills/{id}/versions/{version}` | delete returns `skill_version_deleted` |
| `GET /v1/skills/{id}/versions/{version}/content` | the bundle as a ZIP under `<directory>/` |

`display_title` (create only) is unique among live custom Skills (`409`). Agents and
`agent_with_overrides` carry `skills` references; Sessions show pinned identifiers. The SDK's
`anthropic-beta: skills-2025-10-02` is ignored, and the remaining differences are classified in
[`conformance-matrix.md`](../docs/managed-agents/conformance-matrix.md).

### Events and streaming

None added; Skill reads are ordinary `read` tool events.

### Wire protocols

Prepared execution (`schema_version: 2`) carries per-agent descriptors. The runner snapshot carries
the Session's union in `skills`; for plugin providers the runner adds its `skills_plugin_dir`.

### Storage

`0035_dazzling_iron_monger.sql` makes `display_title`, not the upload directory, unique.
`0036_adorable_dreaming_celestial.sql` adds the package columns to `skill_versions` and creates
`session_skill_bindings`, whose foreign key to `(workspace_id, id, package_sha256)` restricts.
`0037` adds the outbox, `0038` to `0040` the identifier CHECK and page indexes, and `0058`
`deleted_at`.

### Configuration

None added. SkillStore reuses the object-storage settings Registry and harness-server already read
(`S3_BUCKET`, `S3_KEY_PREFIX`, `S3_ENDPOINT`, `S3_REGION`, credentials), in a namespace disjoint
from Files and Memory. The backend must honor `If-None-Match: *` and read-after-write per key.

### Metrics, logs and traces

`registry_service_skills_delivered_total{result}` counts runner pushes.

## Compatibility

### Upgrade

This was a breaking change made before the first public release, with no compatibility path.
`0036_adorable_dreaming_celestial.sql` empties `agents.skills`, strips `skills` from AgentVersion
snapshots and Session `agent_overrides`, deletes every Skill and SkillVersion, and drops
`system_prompt`, `tool_allowlist`, `examples`, `metadata` and `content_files`: a migration cannot
turn rows into SkillStore objects. JSON Skill creation is a `400`, `skl_`/`sklv_` ids became
Anthropic's `skill_`/`skillver_`, and custom references accept only `latest` or an identifier.
The change also tightened every managed sandbox, Skills or not: `colocated` refuses an Environment
`image`, managed executions refuse non-empty Environment `packages`, `edit` refuses files over
100,000 UTF-8 bytes, and `separate` E2B and OpenSandbox images must meet the read prerequisites
in [Harness materialization](../docs/managed-agents/skills.md#harness-materialization). These fail
at Session setup, not at Environment write. v0.5.0 is the first public release with this design.

### Rollback

No earlier public release exists to return to; the migrations are forward-only.

### Version skew

Registry, harness-server and the runner share the descriptor, the catalog text and the push format,
and ship together. A harness-server without SkillStore fails Sessions with Skills at `skill_setup`;
a managed runner whose delivered descriptors differ from the snapshot's pins refuses the snapshot.

## Security considerations

- **Integrity.** Registry's download, harness-server's materialization and the runner push each
  re-verify envelope, manifest and file digests, and a binding names version and digest, so a
  re-upload cannot stand in for a pin.
- **Where files land.** Only the reserved root, read-only, rebuilt from exact pins after tenant
  resources and before the write policy is sealed. Registry refuses mount paths at, below or above
  it; harness-server re-checks lexically, through symlinks and with the probes in
  [`skills.md`](../docs/managed-agents/skills.md#pre-release-execution-boundary-changes); refusing
  `colocated` Environment images and package installers keeps other processes out of that window.
- **Credentials.** The sandbox gets bytes, never a credential for the Skill namespace; scoped
  per-session credentials for output and memory mounts name only those prefixes
  (`auth/sts-creds.ts`), and the runner receives pushed bytes.
- **Host and prompt.** Skills are read and run only through sandbox tools, and no harness-server
  SDK discovers them itself. Catalog rows are JSON-encoded and descriptions exclude XML tags, so a
  description cannot close the `<available_skills>` block.

## Testing

- **Unit**: the store codec and backends; the shared validators; harness-server
  `skill-materialize` (image-supplied trees, collisions, digest drift, cleanup) and
  `dispatcher-event-source` (preflight, missing sandbox or `read`); Registry references, bindings,
  delivery and mount paths; the runner's materializer, loop, resources and Codex worker.
- **Integration**: the store against RustFS (concurrent and pre-existing writes); Registry
  `skills.spec.ts` on Postgres (monotonic identifiers, cursors, retention, the unknown-COMMIT race).
- **End to end**: `real-agent-loop.spec.ts` (`separate`) and `sandbox-harness-agent.spec.ts`
  (`colocated`) hide a marker in `references/marker.txt` and require reads of `SKILL.md`, then the
  reference, then the marker in the reply; they run after merge or with the `run-e2e` label.

## Alternatives

- **Prompt concatenation with intersected allowlists (an earlier design, reversed).** Simple and
  deterministic, but context grew with every Skill, Skills removed tools and interacted, and scripts
  had nowhere to run. The catalog is now the only prompt a Skill contributes.
- **Resolving references when the Agent is written (the same earlier design).** Reproducible, but
  it froze `latest` into the AgentVersion, so every Skill fix re-versioned Agents; Session creation
  now resolves and pins the primary agent, roster and overrides in one transaction.
- **File content in Postgres.** The first version API kept files in JSONB and synthesized a
  `SKILL.md` for JSON-created versions: no content address, nothing to verify on the way in.
- **An SDK's own Skill loader on harness-server.** In `separate` mode the SDK runs on the host,
  where discovery could read or run host files; catalog plus sandbox `read` is one boundary.
- **Converting legacy rows** was rejected: a migration cannot write SkillStore objects.

## Status notes

- **Deletion became retention.** As first built, deleting an unbound version dropped its row and
  queued its bundle through the outbox, a bound one became a tombstone, and deleting a Skill that
  still protected a Session pin was a `409`. Registry-wide soft deletion replaced this: deleted rows
  and bundles are kept, and the outbox only removes bundles of failed uploads.
- **Runner coverage is partial.** Registry attaches the Session's union to the top-level snapshot
  only. `claude_code` and managed `codex_sdk` and `pi_sdk` surface it; the in-process Claude
  providers and the Codex, Cursor, Pi and custom CLIs get no catalog and do not load the plugin
  directory, although Registry admits Skills for them. Only managed preparation fails when Skills
  are not acknowledged; for other providers a failed delivery is logged and the snapshot follows.
- **Two of three writers share the validators.** Registry's push and the runner use
  `@orca/sandbox-runtime`; harness-server keeps an equivalent copy, with its own `SKILLS_ROOT`.
- **Rubric residue.** `composeSystemPrompt` and `intersectToolAllowlists` in `@orca/harness-catalog`
  still run in the runner snapshot builder, always with an empty Skill list.

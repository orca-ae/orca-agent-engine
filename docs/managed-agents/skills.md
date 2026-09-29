# Skills Progressive Disclosure

**Status:** Resolved. This decision replaced the eager prompt-composition
rubric before the first release, so there is no compatibility path for the
old `system_prompt`, `tool_allowlist`, `examples`, `metadata`, or inline
`content_files` representation.

## Decision

A SkillVersion is an immutable filesystem bundle whose entrypoint is
`SKILL.md`. Skills are disclosed in three levels:

1. The model always receives only the ordered catalog: skill name,
   description, entrypoint path, and package digest.
2. When a skill appears relevant, the model uses the normal sandbox `read`
   tool to open its `SKILL.md`.
3. The model reads referenced files or runs bundled scripts only when the
   instructions require them.

Skill instructions are never concatenated into the agent system prompt. A
SkillVersion does not grant or restrict tools; tools and permission policies
belong exclusively to the AgentVersion.

Cloud `codex_sdk` Sessions in the default `separate` mode use the same managed
catalog and verified, read-only `/workspace/skills` tree as `claude_agent_sdk`.
The Codex worker receives the catalog as developer instructions and reads each
entrypoint through Orca's sandbox `read` tool with the Agent's permission policy.
`block_skills` excludes matching bundles before catalog composition and
materialization. Managed Skills use the catalog supplied by Orca.

Codex checkpoints pin the effective developer instructions, including this
catalog. A policy update that changes the catalog causes native resume to fail
closed with an instruction-compatibility error, preserving the stored history.

## Storage model

Registry stores SkillVersion metadata in Postgres and bundle bytes in the
private `@orca/skill-store` library backed by S3/MinIO:

```text
skill_versions
  id, workspace_id, skill_id, version, version_identifier
  name, description, directory, entrypoint
  package_sha256, package_size_bytes, package_manifest
  archived_at, created_at
```

`package_manifest` contains paths, modes, sizes, content digests, and media
types, but never file contents. Bundle objects are addressed by
`(workspace_id, skill_version_id, package_sha256)`. Registry is the only public
writer; Harness reads an exact object directly. Sandbox processes receive no
object keys for Skill bundles. With an STS role configured, the credentials
Harness mints for a sandbox's output and memory mounts are scoped to those
prefixes; the development-only static-key fallback
(`ALLOW_INSECURE_STATIC_S3_CREDS`) gives sandboxes unscoped keys for the bucket.

Uploads must contain a root `SKILL.md` with `name` and `description`
frontmatter. Paths are normalized and may not be absolute, escape the bundle,
or collide after NFC normalization plus Unicode default full case folding.
This check applies to both complete file paths and file-versus-directory
ancestors, so bundles remain unambiguous on case-insensitive filesystems.
Bundle serialization is deterministic, and the stored SHA-256 covers the exact
serialized bytes. A SkillVersion is immutable: changing any file creates a new
version.

The upload surface follows the Claude Skills constraints: the common
top-level directory must match `name` after case and underscore/hyphen
normalization; `name` is at most 64 lowercase alphanumeric/hyphen characters
and may not contain the reserved `anthropic` or `claude` segments;
`description` is non-empty, at most 1024 characters, and contains no XML tags;
and total uncompressed upload content is capped at 30 MiB.

## Session pinning

AgentVersions retain the ordered skill references supplied by the caller.
For a custom Skill, the optional public `version` selector is either `latest`
or the decimal timestamp string returned as `version_identifier`; omitted or
explicit `null` selectors normalize to `latest`. Internal numeric ordinals and
`skillver_...` row ids are never accepted as version selectors on the wire.
An AgentVersion may declare at most 500 skill references. Agent creation and
updates that supply `skills` resolve every locally stored custom reference and
reject a missing version, a repeated SkillVersion, or an effective name mapped
to different package digests. Updates that omit `skills` retain unresolved
references so deletion or archival does not block unrelated Agent changes, but
still reject conflicts among references that currently resolve. Identical
Anthropic catalog references are rejected immediately, while other Anthropic
references remain unresolved until Session creation.
Creating a Session walks the pinned primary agent and its direct coordinator
roster and resolves every reference to a concrete SkillVersion. Nested
coordinators are rejected by the Agent API and again at this boundary because
the Harness supports one roster level. The result is persisted in
`session_skill_bindings`:

```text
session_id, agent_id, agent_version, ordinal,
skill_version_id, bundle_sha256
```

This is the reproducibility boundary. Publishing a newer SkillVersion cannot
change an existing Session, while a newly created Session may resolve `latest`
to the newer version. Session resolution repeats the validation across the
whole agent graph and remains authoritative for catalog resolution, changes to
`latest`, Session overrides, and cross-Agent name collisions.

Resolution is fail-closed:

- all referenced skills and versions must be active and in the Session
  workspace;
- a `type: "anthropic"` reference resolves only to a locally stored immutable
  bundle, and no operator catalog maps Anthropic Skills to one, so an
  unresolved reference is rejected;
- at most 500 bindings may exist across the primary and direct coordinator
  roster;
- a repeated effective name may share one materialization only when its bundle
  digest is identical; the same name with different digests is rejected;
- one AgentVersion may not repeat the same resolved SkillVersion; ordering is
  otherwise preserved exactly by binding ordinal;
- deleting package bytes is forbidden while any Session binding references the
  SkillVersion.

## Deletion lifecycle

`DELETE /v1/skills/{skill_id}/versions/{version}` removes that version from
the public API, including when it is the final version. The parent Skill then
reports `latest_version: null` until another version is uploaded.

Deletion sets `deleted_at` on the SkillVersion. Both bound and unbound versions
retain their metadata and immutable bundles. Public reads and new bindings exclude
deleted versions; prepared execution for an existing Session uses its retained
`session_skill_bindings`. Deleting a Session retains those bindings and bundles.

If an upload reaches SkillStore but the following metadata transaction fails,
Registry records the bundle in the Skill bundle deletion outbox before returning
the failure. The metadata writer and reconciler take the same transaction-scoped
PostgreSQL advisory lock keyed by `(workspace_id, skill_version_id)`. A reconciler racing
an unknown COMMIT therefore waits for that transaction to commit or roll back
before checking ownership. It clears a cleanup row without deleting its object
whenever an exact `(workspace_id, skill_version_id, package_sha256)`
SkillVersion still exists. If Postgres cannot durably determine ownership,
Registry leaves the object in place rather than risk deleting a live bundle.
Operators inventory the content-addressed prefix for this rare orphan window.

`DELETE /v1/skills/{skill_id}` returns `400` while any public version remains,
matching the Claude lifecycle that requires deleting versions first. After all
public versions are gone, the parent can be deleted. Parent deletion also sets `deleted_at`; Session-pinned metadata and bundles remain
available to existing Sessions.

## Prepared execution contract

Registry returns each agent's ordered catalog alongside that agent snapshot.
It does not flatten all skills into one global list:

```json
{
  "skills": [
    {
      "id": "skillver_...",
      "skill_id": "skill_...",
      "source": "custom",
      "version_identifier": "1759178010641129",
      "name": "pdf-analysis",
      "description": "Analyze and extract structured data from PDFs.",
      "entrypoint": "SKILL.md",
      "package_sha256": "...",
      "package_size_bytes": 12345
    }
  ]
}
```

The primary agent and every subagent receive only their own catalog in their
runtime prompt. Catalog order is the order of the agent's declared references.

## Harness materialization

After acquiring the sandbox and before starting the runner, Harness:

1. preflights every planned resource, output, and Skill root before the first
   sandbox-side write or mount;
2. creates the output root and activates every Session resource;
3. deletes the reserved `/workspace/skills` tree, including any files supplied
   by the sandbox image;
4. opens every exact pinned bundle from SkillStore;
5. verifies its package digest, manifest, paths, and per-file digests;
6. writes it to `/workspace/skills/<name>/`;
7. restores only safe executable bits declared by the bundle;
8. makes the entire skill tree read-only.

Skill materialization is deliberately the final filesystem setup step before
the write policy is sealed. This closes resource-path aliases through
image-provided symlinks: the trusted delete-and-rebuild either leaves
`/workspace/skills` containing only exact Session pins or fails startup.
Because `realpath` cannot reveal bind-mount or hard-link aliases, production
Linux runtimes first require every planned root to resolve to its exact lexical
path and reject a pre-existing mount point at or below it. After trusted mounts
are active, they additionally compare device/inode identities between every
writable root and both ancestor chains of `/workspace/skills`. Any overlap
fails startup before the model receives a turn. The final boundary probe also
rejects mount points strictly below every readable or writable session root.
Local development uses a fresh runtime-owned workdir with no external image or
FUSE mounts, so it runs the portable SRT policy probe rather than the Linux-only
`/proc` mount-identity probe.
`/workspace/skills` is included in the read-only sandbox policy only when the
prepared execution has a non-empty Skill catalog.

### Pre-release execution-boundary changes

Progressive Skill disclosure tightens the trust boundary for every managed
sandbox execution, including Sessions with no configured Skills. Remote
providers expose no provider-owned sealed mount namespace that can exclude image
background processes from the setup window, so `colocated` rejects every
Environment-supplied custom image and uses only the operator-owned harness
catalog image (see [`roadmap.md`](./roadmap.md#known-limitations)). Managed
sandbox execution also rejects non-empty Environment package installer
declarations, because installer hooks could leave a concurrent process behind.
In `separate` mode an Environment
image is not used; the operator-selected runtime image or template remains the
trust boundary. An Environment that sets those fields fails these checks; remove
the fields or select an operator-controlled catalog/runtime image.

Session resources cannot mount at `/workspace/skills`, below it, or at an
ancestor that could replace the tree. Registry enforces this on Session create,
resource attach, and resource update; Harness repeats it against every prepared
execution before materialization. Immediately before any output, file, memory,
or repository write/mount, Harness also resolves existing sandbox symlinks for
both the target and the reserved root and fails closed if their canonical paths
overlap.

The deterministic prompt suffix is an `<available_skills>` block: one
instruction line, then one compact JSON line per skill with its name,
description, `/workspace/skills/<name>/SKILL.md` path, and `package_sha256`.
The instruction tells the model to read the entrypoint only when a Skill is
relevant and to load further resources on demand. It does not contain the
`SKILL.md` body.

Any non-empty skill catalog requires a sandbox and an enabled `read` tool.
Harness rejects the execution if either condition is false. It does not
implicitly add `read` or `bash`; the AgentVersion must declare the capabilities
its skills need.

Both `separate` and `colocated` modes use the same sandbox files and the same
progressive-disclosure prompt. The separate-mode Claude SDK's native
plugin/Skill loader is intentionally not used: that SDK process lives on the
Harness host, so native discovery could read or execute host files and would
not enforce the managed sandbox boundary.

The `read` pagination contract accepts a non-negative `offset` and a requested
`limit` from `1` through `100,000`. A path-only call starts at byte offset `0`.
The server-side `separate` adapters use a `4096`-byte default page and cap the
effective limit at `8192` bytes. They reduce the window further when needed to
keep the complete serialized tool result within `16384` bytes, including
metadata and escaping; the requested limit is an upper bound, not an exact
page size. See [Bounded server-side Read](services/harness-server.md#bounded-server-side-read).
Offsets and limits are UTF-8 byte positions, and a page boundary is adjusted
by at most three bytes so it never splits a valid code point. Read metadata
reports the effective `limit`, `offset_unit: "utf8_bytes"`, `truncation`, and `next_offset`.
While `truncation` is true, the model follows `next_offset` to read the next
bounded page. This makes the end of a large `SKILL.md` reachable without ever
returning an unbounded file in one tool result.

`edit` uses the same bounded read primitive before applying its literal
replace-all operation. It rejects files larger than `100,000` UTF-8 bytes
instead of loading them without a bound. This limit applies to both harness
modes and to every sandboxed Session, including Sessions with no configured
Skills.

`read` is restricted to the exact Session resource, output, and Skill roots
encoded in the sandbox policy. Both the requested path and its resolved target
must remain within one of those roots; `/proc`, `/sys`, device files, and image
filesystem paths are not readable through the tool. The in-sandbox
implementation opens a stable file descriptor, verifies the descriptor target
and filesystem, and reads only the requested page rather than buffering the
whole file. Registering a child-only `read` handler also does not expose it to
the primary model: the Claude SDK is launched with a synthetic primary
AgentDefinition whose tool list is independent of the child roster.

Separate-mode E2B and OpenSandbox images must provide `/usr/bin/env`, a Node.js
executable in the root-owned `/usr/local/bin` or `/usr/bin` path, Linux
`/proc/self/fd`, and `statfs`. Sandbox setup probes those exact prerequisites
and fails closed when any is unavailable. A read then runs one bounded helper
command in a cleared environment: it opens the declared resource root, opens
the target through that root descriptor, rejects non-regular files, hard
links, pseudo-filesystems, and descriptor targets outside the root, and
`pread`s at most `limit + 3` bytes from the checked descriptor. Directory-root
authorization uses descriptor-resolved path containment rather than matching
`st_dev`, because gVisor assigns different synthetic device ids to ordinary
parents and children. The policy-sealing probe separately rejects nested
mounts, and the agent has no mount capability afterward. There is no
canonicalize-then-download or whole-file fallback.

## Example

Given:

```text
agent.system = "You are a careful analyst."
agent.tools  = [{
  type: "agent_toolset_20260401",
  configs: [{
    name: "read",
    enabled: true,
    permission_policy: { type: "always_allow" }
  }]
}]
agent.skills = [{ type: "custom", skill_id: "skill_pdf", version: "latest" }]
```

the Session may pin `skill_pdf` version `7`, and Harness materializes:

```text
/workspace/skills/pdf-analysis/
  SKILL.md
  references/schema.md
  scripts/extract.py
```

The runtime system prompt contains the agent system text plus a compact catalog
entry pointing at `SKILL.md`; it contains none of those files' contents.
The Agent tool configuration remains unchanged. If the task concerns PDFs, the
model reads `SKILL.md` and then only the referenced material it needs.

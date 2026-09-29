# @orca/harness-catalog

> Library: the single source of truth for which harnesses exist, which
> topologies each supports, and what sandbox image and port a `colocated`
> harness boots with. Pure data plus one resolver — no I/O, no dependencies.

Both consumers import it in-process: `registry-service-ts` **validates** an
agent's harness annotation on create/update, and `harness-server` **routes** on
the same resolved value and looks up the in-sandbox image. Never duplicate this
table in either service.

## The annotation

A harness is selected per agent through `metadata.harness` + `metadata.mode` —
free-form JSONB on the Agent record, so the wire stays Anthropic Managed
Agents-compatible with no dedicated contract field.

```ts
import { resolveHarnessAnnotation } from '@orca/harness-catalog';

resolveHarnessAnnotation(agent.metadata);
// → { harness: 'claude_agent_sdk', mode: 'separate' }        (both keys absent)
// → { harness: 'claude_code',      mode: 'colocated' }       (mode defaulted)
// → { error: "harness 'claude_code' does not support mode 'separate' (allowed: colocated)" }
```

Resolution rules:

- Both keys absent → the platform default (`DEFAULT_HARNESS` / `DEFAULT_MODE`).
- `harness` set, `mode` absent → that harness's first supported mode.
- Unknown harness, invalid mode, or an unsupported pair → `{ error }`.

`resolveHarnessAnnotation` never mutates `metadata`; the caller persists it as-is.

## The catalog

| Harness                       | Modes       | Default image                                        | Port   |
| ----------------------------- | ----------- | ---------------------------------------------------- | ------ |
| `claude_agent_sdk`            | `separate`  | — (runs in `harness-server`)                         | —      |
| `claude_agent_sdk_persistent` | `separate`  | — (live SDK session, no image)                       | —      |
| `claude_code`                 | `colocated` | `ghcr.io/orca-ae/sandbox-harness-claude-code:latest` | `4096` |
| `codex`                       | `colocated` | `ghcr.io/orca-ae/sandbox-harness-codex:latest`       | `4096` |
| `cursor`                      | `colocated` | `ghcr.io/orca-ae/sandbox-harness-cursor:latest`      | `4096` |
| `pi`                          | `colocated` | `ghcr.io/orca-ae/sandbox-harness-pi:latest`          | `4096` |
| `custom`                      | `colocated` | `ghcr.io/orca-ae/sandbox-harness-custom:latest`      | `4096` |
| `mock`                        | `colocated` | — (in-process, no image)                             | —      |

`supportedModes[0]` is the default when `metadata.mode` is omitted. Each entry
also carries a `provider` — the runner provider id the harness maps to, read by
`harnessToProvider` — and an `entrypoint`, the container command to set when
booting the harness image (`null` leaves the image's own default command in
charge; it is set only for `claude_code` and `codex`).

> `codex` is registered here and has an image tag, but
> `@orca/sandbox-harness`'s provider registry currently ships **only** the
> `claude` provider — selecting `agent=codex` fails with `unsupported agent:
codex` until a Codex provider lands. Keep both places in sync when that
> changes.

## Tests

```bash
pnpm -F @orca/harness-catalog test
```

No integration suite — the package has no I/O.

## Design docs

[`harness-modes.md`](../../docs/managed-agents/harness-modes.md) — the
annotation, the `separate` vs. `colocated` topologies, the bridge, and the
transport abstraction.

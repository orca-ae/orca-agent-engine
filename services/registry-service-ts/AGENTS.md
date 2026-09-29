# registry-service-ts — agent guidelines

The public, Anthropic-compatible control plane. Owns Postgres metadata, idempotency, and every
`/v1/*` route. Read [`docs/managed-agents/services/registry-service.md`](../../docs/managed-agents/services/registry-service.md)
before changing route shapes.

That doc describes current behavior only; unimplemented surface belongs in
[`roadmap.md`](../../docs/managed-agents/roadmap.md), which is also the reference the conformance
register points `not-implemented` operations at.

## Contracts are the route source of truth

`src/contracts/*.contract.ts` holds split ts-rest contracts. A route's request/response shape is
defined there, not in the handler. Change the contract first, then `src/api/*.routes.ts`.

- `toolset-aliasing.ts` — accept Anthropic's dated names (`agent_toolset_20260401`) on the wire;
  use the vendor-neutral `agent_toolset` internally and in our own surface.
- `model-wire.ts`, `environment-wire.ts` — wire-shape adapters where the Orca and Claude
  representations diverge.
- `id-prefix.ts` — resource id prefixes. Do not mint ids ad hoc.

## Three listeners, three trust levels

`src/main.ts` binds all three; never merge them.

| Listener | Config             | Auth                                                                     | Carries                                                     |
| -------- | ------------------ | ------------------------------------------------------------------------ | ----------------------------------------------------------- |
| public   | `httpPort`         | `x-api-key` or OIDC/JWT                                                  | `/v1/*` — the Anthropic-compatible surface                  |
| internal | `internalHttpPort` | ServiceAccount JWT + TokenReview, or a shared token (`InternalAuthMode`) | `/internal/*` — harness, ai-gateway, observability-exporter |
| admin    | `adminHttpPort`    | admin credential                                                         | workspace lifecycle, API-key provisioning                   |

Session-scoped internal routes carry the workspace and session in the path
(`/internal/v1/workspaces/{ws}/sessions/{id}/...`); do not add one that takes the workspace from a
body field or header. The environment routes (`/internal/environments/...`) carry no workspace,
and `GET /internal/v1/guardrails/effective` takes its session scope from query parameters, which it
checks against the session row.

## Non-negotiables in this service

- **`orca-beta`, not `anthropic-beta`.** The SDK auto-injects the `anthropic-beta` header with
  `managed-agents-2026-04-01`; we silently ignore it. Version-gated behavior keys off `orca-beta`.
- **Idempotency on every write path.** `src/middleware/idempotency.ts` replays the cached response
  within TTL for any `Idempotency-Key`. `POST /v1/sessions/{id}/events` additionally honors a
  request-level `request_id` body field for batch dedup. New write endpoints inherit this — add a test.
- **Conformance is generated, not asserted.** `pnpm openapi:gen` emits
  `openapi/managed-agents.yaml` from the contracts; `pnpm conformance:gen` diffs it against
  Anthropic's vendored spec into
  [`conformance-matrix.md`](../../docs/managed-agents/conformance-matrix.md), which demands an
  explicit decision per difference. Both artifacts are checked in and CI fails if regenerating
  produces a diff — **edit the contracts, never the artifacts.** See
  [`conformance.md`](../../docs/managed-agents/conformance.md) for the decision register and
  [`orca-extensions.md`](../../docs/managed-agents/orca-extensions.md) for the Orca-only surface.
- **SSE terminates here.** `src/streaming/sse.ts` bridges `TranscriptStore.tail` to the client with
  heartbeats and a `Last-Event-ID` reconnect cursor. Registry never proxies SSE to harness.

## Tests

```bash
pnpm -F @orca/registry-service-ts test                                  # unit
pnpm -F @orca/registry-service-ts exec vitest run -c vitest.integration.config.ts
```

Integration specs need the dev compose stack (`make dev-up`).

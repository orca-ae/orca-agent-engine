# Vendored Anthropic OpenAPI description

This directory vendors Anthropic's published OpenAPI description of the Claude API, unmodified.

- `openapi.json` holds the bytes exactly as Anthropic serves them: the document named by
  `openapi_spec_url` in the `.stats.yml` of Anthropic's TypeScript SDK repository
  (`anthropics/anthropic-sdk-typescript`).
- `PINNED.json` pins them: it records the exact source URL, where that URL was discovered, and a
  sha256 computed locally over `openapi.json`, which the conformance tooling re-checks before using
  the file.

The description is used only to measure this API's conformance with Anthropic's: it feeds the
conformance matrix, the Orca-extension tagging in `openapi/managed-agents.yaml`, and the conformance
test suites. No service reads it at runtime.

Refresh both files with `pnpm anthropic:sync`; never edit them by hand. Formatters skip this
directory so the bytes stay identical to what upstream serves.

`openapi.json` is Anthropic's material, redistributed here unmodified; Anthropic's terms apply to
it.

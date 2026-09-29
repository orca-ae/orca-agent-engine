The model-catalog.json data is derived from OpenAI Codex, Apache License 2.0:
https://github.com/openai/codex/blob/rust-v0.154.0/codex-rs/models-manager/models.json

Copyright OpenAI. License: https://github.com/openai/codex/blob/rust-v0.154.0/LICENSE

Orca modifications disable shell_type, apply_patch_tool_type and experimental
native tools and deferred tool search so tool execution uses the Orca relay and its approval policy.
SDK version, this model catalog, and packages/harness-catalog/src/codex-models.json
are updated together.

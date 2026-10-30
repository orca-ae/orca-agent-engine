# Public price extractor fixtures

Recorded on 2026-10-29 from the official URLs below with `Accept: text/markdown`.
The files retain the supported table rows, headings and context statements; they
are fixture excerpts, not complete provider catalogs or deployable seed prices.

- [OpenAI pricing](https://developers.openai.com/api/docs/pricing): four exact
  flagship model IDs, standard/batch/flex/fast and available ultrafast rows,
  including both sides of the 272,000 input-token boundary. Cache-write lifetimes
  in these tables are not the Engine's legacy 5m/1h buckets.
- [Anthropic pricing](https://platform.claude.com/docs/en/about-claude/pricing):
  Claude Sonnet 4.6 and Opus 4.6 standard-tier rows, with the documented full
  one-million-token context coverage and explicit cache buckets.

The normalized feed schema is tested with illustrative prices. Cross-language
resolution vectors are in
[`conditional-pricing-v1.json`](../../../../../packages/harness-catalog/test/fixtures/conditional-pricing-v1.json).
Its nano-USD expectations are decimal strings so Rust and TypeScript retain exact
integer values. Tests exercise these vectors directly; no inference is used.

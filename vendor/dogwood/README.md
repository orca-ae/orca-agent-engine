# Pinned Dogwood artifacts

This directory owns the actual shared Dogwood core used by Engine. The archives are local
file dependencies; installing Engine requires no sibling repository or unpublished package.

- Source commit: `df6624b5226fde0fd2f0e49af01f5513be1bd067`.
- Node artifact: `runorca-dogwood-0.1.0-df6624b.tgz`, SHA-256
  `81bda1a0a05ad2edcc964a80c3607ffa9e617431b734fe419629eff64ea8dfad`.
- Matching source: `orca-dogwood-source-0.1.0.tgz`, SHA-256
  `855e047801b3be3e812b6bd7a691217f40ab6f3b118e0952992ee6cd09fc65d3`.
- Semantic pins: `orca-dogwood` 0.1.0, `dogwood-language` 1.1.0, Cedar 4.11.0.

`provenance.json` records upstream origin, the Rust 1.94 compatibility backport, source,
WASM and native-helper hashes. Archives retain their license and notice files. The Node
archive contains a Darwin arm64 clock helper. The Harness Dockerfile compiles its Linux
helper from the matching source with Rust 1.94.0. Before extraction/build,
`scripts/verify-dogwood-artifacts.mjs` checks both archives against the committed provenance,
every source-manifest file, package/source pins, the WASM payload and packaged helper hashes.
CI runs the verifier's actual-archive and corruption regressions.

The Rust builder emits a helper checksum. `scripts/install-dogwood-clock.mjs` verifies it
and the deployed package against the selected provenance before installation, records the
source archive/manifest/commit and helper hashes, and checks an actual native reading.
These checks establish input and transfer integrity plus a consumer build attestation;
they do not establish reproducible source-to-binary equivalence. Unsupported/missing
helpers stay closed.

See [Dogwood behavior](../../docs/managed-agents/dogwood.md) and the
[qualification register](../../docs/managed-agents/roadmap.md).

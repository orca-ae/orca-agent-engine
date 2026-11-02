# Native DSH profile packaging

This example packages the four native configuration texts into the Files input
described in the [profile reference](../../docs/managed-agents/harness-modes.md#agent-bound-native-profiles). It uploads
no plugin source or `node_modules`. Operator approval covers the locked package
code as well as its configuration.

Use the pinned DSH `0.2.1-alpha.2` and pnpm `9.15.9`. Start with a fresh profile
containing the three files in [template](template/), then use the native CLI to
add an exact version of a reviewed, prebuilt public npm bundle:

```sh
export DSH_HOME="$(mktemp -d)"
mkdir -p "$DSH_HOME/profiles/orca-managed"
cp examples/dsh-profile/template/* "$DSH_HOME/profiles/orca-managed/"
dsh plugin --profile orca-managed add --save-exact BUNDLE@VERSION \
  --ignore-scripts --registry=https://registry.npmjs.org \
  --config.auto-install-peers=false --config.node-linker=hoisted
node examples/dsh-profile/package-profile.mjs \
  "$DSH_HOME/profiles/orca-managed" > profile.json
```

Replace `BUNDLE@VERSION` with the selected package and its exact version. Keep
`cordis.patch.yml` as business data only; the image supplies the trusted managed
patch. Shared Cordis/tools belong in the bundle's peer dependencies. Bundles that
need install scripts, private registries or separate copies of shared runtime
packages are outside this profile format.

The packaging command prints `config_sha256=...` to stderr and writes the JSON
envelope to stdout. It preserves native texts, rejects invalid UTF-8 and bounds
the envelope/text bytes; Registry validates the native configuration at ingestion.
Do not substitute the SHA-256 of `profile.json` for this effective configuration
digest: envelope formatting is not part of the approval identity.

Configure the exact workspace and digest on Registry through Helm:

```yaml
registry:
  extraEnv:
    - name: DSH_APPROVED_PROFILES
      value: '{"WORKSPACE_ID":["CONFIG_SHA256"]}'
```

Replace both placeholders and roll Registry out with the new setting. The default
approval map is empty. Upload `profile.json` through Files, then set the returned
File ID on the Agent's `metadata.dsh_profile`, alongside `harness=dsh_sdk` and
`mode=colocated`. The Environment uses cloud execution and unrestricted networking.
Declare agent_toolset to expose plugin tools. They inherit its standard enabled/permission
defaults; use default_config and per-tool configs to disable tools or require confirmation.

Run the packaging helper check with:

```sh
node --test examples/dsh-profile/package-profile.test.mjs
```

The native tool/recovery smoke uses the built product image and scripts only the
loopback model HTTP responses:

```sh
docker build -f services/sandbox-harness/Dockerfile \
  --build-arg DEFAULT_AGENT=dsh-sdk -t sandbox-harness-dsh:local .
ORCA_DSH_PROFILE_IMAGE=sandbox-harness-dsh:local \
  pnpm -F @orca/dsh-harness exec vitest run test/unit/custom-profile-native.test.ts
ORCA_DSH_NAMESPACE_IMAGE=sandbox-harness-dsh:local \
  pnpm -F @orca/sandbox-harness exec vitest run test/dsh-entrypoint-namespace.spec.ts
```

The first smoke mounts a preinstalled business fixture and exercises real bridge
execution, explicit denial, frozen schemas and cold recovery; it does not test npm
installation. The namespace smoke requires Docker mount-namespace privileges. Neither
smoke provisions OpenSandbox or calls a production model.

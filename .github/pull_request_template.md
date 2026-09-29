<!--
Thanks for contributing! Please read CONTRIBUTING.md before opening a pull request.
Keep each pull request to one logical change, and title it the way you would write a commit
subject, for example "registry-service-ts: tighten vault binding".
-->

### What changes and why

<!-- Link the issue or discussion this addresses (for example, "Fixes #123"), and the
     docs/managed-agents section it implements, if any. Call out changes to docs, CI or templates.
     If this change needs an Orca Improvement Proposal (see proposals/README.md), link it here. -->

### Compatibility

- [ ] No change to the Anthropic-compatible `/v1` API, an `/apis/<group>/<version>` extension group, the runner or worker tunnel or the `.proto` wire format, the database schema or migrations, environment variables or configuration, or Helm values
- [ ] Changes one of the above. The change is described in _What changes and why_, with its OIP if one is required.

### How I tested it

<!-- The tests you added or changed, and the commands you ran. Say which integration or
     end-to-end suites you ran, if any. -->

### AI assistance

<!-- Required when an AI tool generated or substantially rewrote code, tests, documentation or a design in
     this pull request. Autocomplete, spelling and grammar fixes, formatting and mechanical renames don't
     count. See AI_POLICY.md. Choose one: -->

- [ ] No AI assistance
- [ ] AI-assisted. Tool(s): ___ . What it did: ___ . How I verified the result: ___ .

### Checklist

- [ ] Every commit is signed off (`git commit -s`), as described in CONTRIBUTING.md
- [ ] Commits with meaningful AI assistance carry one `Assisted-by:` trailer
- [ ] `pnpm -r build`, `pnpm test`, `pnpm lint`, `pnpm format:check`, `pnpm docs:env-check`, `pnpm license:check` and `pnpm test:chart:render` pass locally, and generated files are regenerated and committed if their inputs changed
- [ ] Documentation is updated if behavior or a contract changed, and anything deferred has an entry in `docs/managed-agents/roadmap.md`

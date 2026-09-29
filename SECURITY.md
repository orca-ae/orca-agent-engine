# Security policy

## Supported versions

We fix security issues in the latest minor release line, currently 0.5.x. Fixes land on `main` and
ship in the next release of that line. Older lines don't receive patches, so upgrade to the latest
release to pick up a fix.

## Reporting a vulnerability

**Please don't report security problems in a public issue, pull request or discussion.**

Report them privately, in either of these ways:

1. **GitHub private vulnerability reporting.** Open a
   [private report](https://github.com/orca-ae/orca-agent-engine/security/advisories/new) from the
   Security tab of this repository. We prefer this channel: it's private, it keeps the conversation
   in one thread, and it stays attached to the repository.
2. **Email `security@runorca.ai`**, with the repository name in the subject line.

As much as you have of the following helps us act quickly:

- the affected version, image tag or commit
- the component, such as the registry API, harness-server, a sandbox runtime or the Helm chart
- what an attacker could do, and under which configuration
- steps to reproduce the problem
- anything you already know about the impact

A rough report sent early is better than a polished one sent late.

## What happens next

We'll acknowledge your report, investigate it, and keep you updated as we go. We coordinate
disclosure with you. By default we aim to publish within 90 days of the report, and sooner once a fix
is available.

When the fix is released, we publish a security advisory in this repository and credit you in it,
unless you ask us not to.

We don't run a bug bounty program.

## Scope

This policy covers the code in this repository. It also covers the AI gateway, the `ork` CLI and
the TypeScript SDK, which aren't built from this repository but are available as public images and
packages under the Apache License 2.0. Report vulnerabilities in them here too.

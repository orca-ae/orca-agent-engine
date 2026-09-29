// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { BUILTIN_EVALUATORS } from '../../src/builtins/index.js';
import type { EvaluatorContext } from '../../src/engine.js';
import type { GuardrailEvent } from '../../src/types.js';

function ctx(
  params: Record<string, unknown>,
  event: Partial<GuardrailEvent> = {},
): EvaluatorContext {
  return {
    params,
    state: {},
    event: {
      phase: 'tool_call',
      sessionId: 'ses_1',
      tool: { name: 'mcp__github__get_file_contents', input: {} },
      ...event,
    },
    guardrail: {
      id: 'grd_1',
      name: 'test',
      enabled: true,
      phases: ['tool_call'],
      scope: 'explicit',
      rule: { kind: 'builtin', builtin: 'github_policy', params },
    },
  };
}

function run(params: Record<string, unknown>, event?: Partial<GuardrailEvent>) {
  const evaluator = BUILTIN_EVALUATORS.get('github_policy');
  if (!evaluator) throw new Error('no evaluator registered for github_policy');
  return evaluator(ctx(params, event));
}

const tool = (name: string, input: Record<string, unknown>): Partial<GuardrailEvent> => ({
  tool: { name, input },
});

const shell = (command: string, name = 'Bash'): Partial<GuardrailEvent> => ({
  tool: { name, input: { command } },
});

describe('github_policy over the integration tools', () => {
  it('abstains on a read while reads are unrestricted', () => {
    const out = run({}, tool('mcp__github__get_file_contents', { owner: 'acme', repo: 'other' }));
    expect(out).toBeUndefined();
  });

  it('recognizes consolidated tools whose read verb is last', () => {
    for (const name of ['mcp__github__pull_request_read', 'mcp__github__issue_read']) {
      expect(run({}, tool(name, { owner: 'acme', repo: 'other' })), name).toBeUndefined();
      expect(
        run(
          { read_all: false, read_repos: ['acme/app'] },
          tool(name, { owner: 'acme', repo: 'other' }),
        )?.verdict,
        name,
      ).toBe('deny');
    }
  });

  it('denies a read of a repository outside the readable list', () => {
    const out = run(
      { read_all: false, read_repos: ['acme/app'] },
      tool('mcp__github__get_file_contents', { owner: 'acme', repo: 'other', path: 'README.md' }),
    );
    expect(out?.verdict).toBe('deny');
  });

  it('allows a read of a listed repository', () => {
    const out = run(
      { read_all: false, read_repos: ['acme/app'] },
      tool('mcp__github__get_file_contents', { owner: 'acme', repo: 'app', path: 'README.md' }),
    );
    expect(out).toBeUndefined();
  });

  it('matches a repository case-insensitively, as the host does', () => {
    const out = run(
      { read_all: false, read_repos: ['acme/app'] },
      tool('mcp__github__get_file_contents', { owner: 'Acme', repo: 'App' }),
    );
    expect(out).toBeUndefined();
  });

  it('asks when a restricted read names no repository it can check', () => {
    const out = run(
      { read_all: false, read_repos: ['acme/app'] },
      tool('mcp__github__search_code', { q: 'password' }),
    );
    expect(out?.verdict).toBe('ask');
  });

  it('denies every write by default, because no repository is writable', () => {
    const out = run(
      {},
      tool('mcp__github__create_or_update_file', { owner: 'acme', repo: 'app', branch: 'main' }),
    );
    expect(out?.verdict).toBe('deny');
  });

  it('denies a write to a repository outside the writable list', () => {
    const out = run(
      { write_repos: ['acme/app'] },
      tool('mcp__github__create_or_update_file', { owner: 'acme', repo: 'other', branch: 'main' }),
    );
    expect(out?.verdict).toBe('deny');
  });

  it('allows a write to a writable repository when branches are unrestricted', () => {
    const out = run(
      { write_repos: ['acme/app'] },
      tool('mcp__github__create_or_update_file', { owner: 'acme', repo: 'app', branch: 'main' }),
    );
    expect(out).toBeUndefined();
  });

  it('denies a write to a branch outside the writable branches', () => {
    const out = run(
      { write_repos: ['acme/app'], write_branches: ['feature/*'] },
      tool('mcp__github__create_or_update_file', { owner: 'acme', repo: 'app', branch: 'main' }),
    );
    expect(out?.verdict).toBe('deny');
  });

  it('allows a write to a branch the pattern covers', () => {
    const out = run(
      { write_repos: ['acme/app'], write_branches: ['feature/*'] },
      tool('mcp__github__create_or_update_file', {
        owner: 'acme',
        repo: 'app',
        branch: 'feature/login',
      }),
    );
    expect(out).toBeUndefined();
  });

  it('reads a fully qualified ref as the branch it names', () => {
    const out = run(
      { write_repos: ['acme/app'], write_branches: ['feature/*'] },
      tool('mcp__github__push_files', { repository: 'acme/app', ref: 'refs/heads/main' }),
    );
    expect(out?.verdict).toBe('deny');
  });

  it('asks when a write names no branch and branches are restricted', () => {
    // A write with no branch lands on the default branch, which this evaluator
    // cannot see. Allowing it would be the silent hole the guardrail exists for.
    const out = run(
      { write_repos: ['acme/app'], write_branches: ['feature/*'] },
      tool('mcp__github__create_issue', { owner: 'acme', repo: 'app', title: 'bug' }),
    );
    expect(out?.verdict).toBe('ask');
  });

  it('asks when a write names no repository it can check', () => {
    const out = run(
      { write_repos: ['acme/*'] },
      tool('mcp__github__create_repository', { name: 'fresh' }),
    );
    expect(out?.verdict).toBe('ask');
  });

  it('treats an unrecognised verb as a write rather than a read', () => {
    const out = run(
      { write_repos: ['acme/app'] },
      tool('mcp__github__transfer_repository', { owner: 'acme', repo: 'other' }),
    );
    expect(out?.verdict).toBe('deny');
  });

  it('reads the repository from a URL argument', () => {
    const out = run(
      { read_all: false, read_repos: ['acme/app'] },
      tool('mcp__github__get_pull_request', { url: 'https://github.com/acme/other/pull/7' }),
    );
    expect(out?.verdict).toBe('deny');
  });

  it('abstains on another integration entirely', () => {
    expect(
      run({ write_repos: [] }, tool('mcp__slack__send_message', { text: 'hi' })),
    ).toBeUndefined();
  });
});

describe('github_policy over the shell', () => {
  it('denies a push to a branch outside the writable branches', () => {
    const out = run(
      { write_repos: ['acme/app'], write_branches: ['feature/*'] },
      shell('git push origin main'),
    );
    expect(out?.verdict).toBe('deny');
  });

  it('reads the destination of a refspec rather than its source', () => {
    const out = run(
      { write_repos: ['acme/app'], write_branches: ['feature/*'] },
      shell('git push origin HEAD:refs/heads/main'),
    );
    expect(out?.verdict).toBe('deny');
  });

  it('allows a push whose repository and branch are both permitted', () => {
    const out = run(
      { write_repos: ['acme/app'], write_branches: ['feature/*'] },
      shell('git push https://github.com/acme/app.git feature/login'),
    );
    expect(out).toBeUndefined();
  });

  it('denies a push to a repository outside the writable list', () => {
    const out = run(
      { write_repos: ['acme/app'] },
      shell('git push git@github.com:acme/other.git main'),
    );
    expect(out?.verdict).toBe('deny');
  });

  it('asks when a remote name hides which repository is being written', () => {
    const out = run({ write_repos: ['acme/app'] }, shell('git push origin feature/login'));
    expect(out?.verdict).toBe('ask');
  });

  it('sees through a global option placed before the subcommand', () => {
    const out = run(
      { write_repos: ['acme/app'], write_branches: ['feature/*'] },
      shell('git -C /tmp/checkout push origin main'),
    );
    expect(out?.verdict).toBe('deny');
  });

  it('resolves an inline Git alias before classifying the subcommand', () => {
    const out = run(
      { write_repos: ['allowed/repo'], write_branches: ['feature/*'] },
      shell('git -c alias.p=push p https://github.com/victim/secret main'),
    );
    expect(out?.verdict).toBe('deny');
  });

  it('resolves an inline Git alias chain before classifying the subcommand', () => {
    const out = run(
      { write_repos: ['allowed/repo'], write_branches: ['feature/*'] },
      shell('git -c alias.co=push -c alias.c=co c https://github.com/victim/secret main'),
    );
    expect(out?.verdict).toBe('deny');
  });

  it('covers a push chained after another command', () => {
    const out = run(
      { write_repos: ['acme/app'], write_branches: ['feature/*'] },
      shell('npm test && git push origin main'),
    );
    expect(out?.verdict).toBe('deny');
  });

  it('denies a clone of a repository outside the readable list', () => {
    const out = run(
      { read_all: false, read_repos: ['acme/app'] },
      shell('git clone https://github.com/acme/other.git'),
    );
    expect(out?.verdict).toBe('deny');
  });

  it('preserves every GitLab subgroup segment in a remote repository', () => {
    const params = { read_all: false, read_repos: ['group/subgroup'] };
    expect(
      run(params, shell('git clone https://gitlab.com/group/subgroup/secret.git'))?.verdict,
    ).toBe('deny');
    expect(run(params, shell('git clone git@gitlab.com:group/subgroup/secret.git'))?.verdict).toBe(
      'deny',
    );
    expect(
      run(
        { read_all: false, read_repos: ['group/subgroup/secret'] },
        shell('git clone https://gitlab.com/group/subgroup/secret.git'),
      ),
    ).toBeUndefined();
  });

  it('does not mistake a clone option value for the remote', () => {
    const out = run(
      { read_all: false, read_repos: ['allowed/repo'] },
      shell(
        'git clone --reference-if-able https://github.com/allowed/repo https://github.com/victim/secret',
      ),
    );
    expect(out?.verdict).toBe('deny');
  });

  it('preserves the remote after a valueless negated clone option', () => {
    const out = run(
      { read_all: false, read_repos: ['allowed/repo'] },
      shell('git clone --no-reference https://github.com/victim/secret'),
    );
    expect(out?.verdict).toBe('deny');
  });

  it('skips value-taking options before each Git read remote', () => {
    for (const command of [
      'git fetch -o https://github.com/allowed/repo https://github.com/victim/secret',
      'git pull --server-option https://github.com/allowed/repo https://github.com/victim/secret',
      'git ls-remote -o https://github.com/allowed/repo https://github.com/victim/secret',
    ]) {
      const out = run({ read_all: false, read_repos: ['allowed/repo'] }, shell(command));
      expect(out?.verdict, command).toBe('deny');
    }
  });

  it('skips git pull jobs values before selecting the remote', () => {
    for (const command of [
      'git pull -j 4 https://github.com/victim/secret',
      'git pull --jobs 4 https://github.com/victim/secret',
    ]) {
      const out = run({ read_all: false, read_repos: ['allowed/repo'] }, shell(command));
      expect(out?.verdict, command).toBe('deny');
    }
  });

  it('denies a command-line write to a repository outside the list', () => {
    const out = run(
      { write_repos: ['acme/app'] },
      shell('gh pr create --repo acme/other --title x'),
    );
    expect(out?.verdict).toBe('deny');
  });

  it('allows a command-line read of a permitted repository', () => {
    const out = run(
      { read_all: false, read_repos: ['acme/app'] },
      shell('gh pr list --repo acme/app'),
    );
    expect(out).toBeUndefined();
  });

  it('reads the repository out of an API path', () => {
    const out = run(
      { write_repos: ['acme/app'] },
      shell('gh api repos/acme/other/issues -f title=x'),
    );
    expect(out?.verdict).toBe('deny');
  });

  it('sees a push through a wrapper and through a nested shell', () => {
    const params = { write_repos: ['acme/app'], write_branches: ['feature/*'] };
    expect(run(params, shell('sudo git push origin main'))?.verdict).toBe('deny');
    expect(run(params, shell('bash -c "git push origin main"'))?.verdict).toBe('deny');
  });

  it('reads the branch past a flag that takes no value', () => {
    const out = run(
      { write_repos: ['acme/app'], write_branches: ['feature/*'] },
      shell('git push --force -u origin main'),
    );
    expect(out?.verdict).toBe('deny');
  });

  it('takes the strictest verdict when one string runs several invocations', () => {
    // The permitted push does not make the forbidden one acceptable.
    const out = run(
      { write_repos: ['acme/app'], write_branches: ['feature/*'] },
      shell('git push https://github.com/acme/app.git feature/login; git push origin main'),
    );
    expect(out?.verdict).toBe('deny');
  });

  it('checks a branch named on the command line', () => {
    const out = run(
      { write_repos: ['acme/app'], write_branches: ['feature/*'] },
      shell('gh pr create --repo acme/app --title x --base main'),
    );
    expect(out?.verdict).toBe('deny');
  });

  it('leaves the groups that reach no repository alone', () => {
    expect(run({ write_repos: [] }, shell('gh auth login'))).toBeUndefined();
  });

  it('leaves shell commands that touch no repository alone', () => {
    expect(run({ write_repos: [] }, shell('ls -la /tmp'))).toBeUndefined();
    expect(run({ write_repos: [] }, shell('git status'))).toBeUndefined();
  });

  it('covers the server-qualified spelling of the shell tool', () => {
    const out = run(
      { write_repos: ['acme/app'], write_branches: ['feature/*'] },
      shell('git push origin main', 'mcp__orca__bash'),
    );
    expect(out?.verdict).toBe('deny');
  });

  it('denies a push whose second refspec targets a forbidden branch', () => {
    // Only the first refspec used to be read, so a forbidden branch tucked
    // behind a permitted one pushed through.
    const out = run(
      { write_repos: ['acme/app'], write_branches: ['feature/*'] },
      shell('git push https://github.com/acme/app.git feature/login main'),
    );
    expect(out?.verdict).toBe('deny');
  });

  it('allows a push when every refspec targets a permitted branch', () => {
    const out = run(
      { write_repos: ['acme/app'], write_branches: ['feature/*'] },
      shell('git push https://github.com/acme/app.git feature/login feature/logout'),
    );
    expect(out).toBeUndefined();
  });
});

describe('github_policy gh subcommand classification', () => {
  it('treats a create whose resource is named like a read verb as a write', () => {
    for (const command of [
      'gh label create status --repo acme/other',
      'gh release create view --repo acme/other',
      'gh workflow run --repo acme/other',
    ]) {
      expect(run({ write_repos: ['acme/app'] }, shell(command))?.verdict, command).toBe('deny');
    }
  });

  it('does not read-classify a repo create just because a positional reads as a verb', () => {
    // No repository is determinable, but it must be treated as a write and so
    // ask rather than pass silently as an unrestricted read.
    const out = run({ write_repos: ['acme/app'] }, shell('gh repo create status --private'));
    expect(out?.verdict).toBe('ask');
  });

  it('still reads a genuine read subcommand', () => {
    const out = run(
      { read_all: false, read_repos: ['acme/app'] },
      shell('gh issue view 42 --repo acme/app'),
    );
    expect(out).toBeUndefined();
  });
});

describe('github_policy recognition by tool name', () => {
  it('covers a git-server MCP tool the github substring would miss', () => {
    const out = run({ write_repos: [] }, tool('mcp__git__push', {}));
    expect(out?.verdict).toBe('deny');
  });

  it('covers a unified-server tool whose own name carries the family', () => {
    const out = run(
      { write_repos: ['acme/app'] },
      tool('mcp__devtools__github_create_issue', { owner: 'acme', repo: 'other' }),
    );
    expect(out?.verdict).toBe('deny');
  });

  it('does not sweep in an unrelated tool whose word merely resembles one', () => {
    // `report` contains `repo`; whole-word matching keeps it out of the family.
    expect(run({ write_repos: [] }, tool('mcp__analytics__get_report', {}))).toBeUndefined();
  });

  it('does not treat a server that merely contains the letters g-i-t as git', () => {
    // `digit`, `legit` contain `git` as a substring but are not git servers.
    expect(run({ write_repos: [] }, tool('mcp__digitalocean__create_droplet', {}))).toBeUndefined();
    expect(run({ write_repos: [] }, tool('mcp__legit_notes__create_note', {}))).toBeUndefined();
  });
});

describe('github_policy branch flags', () => {
  it('does not read a gh `-b` body string as a target branch', () => {
    // `-b` is `--body` on `gh issue create`; reading it as a branch would deny
    // the wrong thing. With no branch resolved and the repo permitted, the write
    // asks about the unnamed branch rather than denying a bogus one.
    const out = run(
      { write_repos: ['acme/app'], write_branches: ['release/*'] },
      shell('gh issue create -R acme/app -b "fixes main branch"'),
    );
    expect(out?.verdict).not.toBe('deny');
  });

  it('still reads the long --base flag as the target branch', () => {
    const out = run(
      { write_repos: ['acme/app'], write_branches: ['release/*'] },
      shell('gh pr create -R acme/app --base main'),
    );
    expect(out?.verdict).toBe('deny');
  });
});

describe('github_policy fails closed on a command nested past the reader', () => {
  it('does not let a deeply nested git push slip the repo policy', () => {
    // The shell reader gives up past its nesting bound and marks the command
    // unresolved; the repo policy must fail closed on it rather than pass it
    // through because its argv is empty.
    const nested = `${'eval '.repeat(9)}git push origin main`;
    const out = run({ write_repos: [] }, shell(nested));
    expect(out?.verdict).toBe('ask');
  });
});

describe('github_policy repo decoy', () => {
  it('denies a write whose forbidden url sits beside an allowlisted full_name', () => {
    const out = run(
      { write_repos: ['acme/allowed'] },
      tool('mcp__github__create_or_update_file', {
        full_name: 'acme/allowed',
        url: 'https://github.com/victim/secret',
      }),
    );
    expect(out?.verdict).toBe('deny');
  });
});

describe('github_policy owner/name and url decoys', () => {
  it('denies a write whose forbidden repository hides in a secondary name key', () => {
    const out = run(
      { write_repos: ['acme/allowed'] },
      tool('mcp__github__create_or_update_file', {
        owner: 'acme',
        repo: 'allowed',
        repository: 'victim/secret',
      }),
    );
    expect(out?.verdict).toBe('deny');
  });

  it('crosses every owner with every bare name', () => {
    const out = run(
      { write_repos: ['acme/allowed'] },
      tool('mcp__github__fork_repository', {
        owner: 'acme',
        organization: 'attacker',
        repo: 'allowed',
      }),
    );
    expect(out?.verdict).toBe('deny');
  });

  it('does not silently drop an unparseable over-long url', () => {
    const longUrl = `https://github.com/${'a'.repeat(3000)}/x`;
    const out = run(
      { write_repos: ['acme/allowed'] },
      tool('mcp__github__create_or_update_file', { full_name: 'acme/allowed', url: longUrl }),
    );
    expect(out?.verdict).toBe('ask');
  });
});

describe('github_policy branch decoy', () => {
  const params = { write_repos: ['acme/app'], write_branches: ['feature/*'] };

  it('denies when the real destination hides behind an allowlisted decoy branch', () => {
    // A decoy `branch` on the allowlist beside the real target in a later key must
    // not launder the write: every branch key the call names has to pass.
    for (const input of [
      { full_name: 'acme/app', base: 'main', branch: 'feature/x' },
      { full_name: 'acme/app', ref: 'main', branch: 'feature/x' },
      { full_name: 'acme/app', ref: 'refs/heads/main', branch: 'feature/x' },
      { full_name: 'acme/app', target_branch: 'main', branch: 'feature/x' },
      { full_name: 'acme/app', base_branch: 'main', branch: 'feature/x' },
    ]) {
      const out = run(params, tool('mcp__github__create_pull_request', input));
      expect(out?.verdict, JSON.stringify(input)).toBe('deny');
    }
  });

  it('still allows when every named branch is on the allowlist', () => {
    const out = run(
      params,
      tool('mcp__github__create_pull_request', {
        full_name: 'acme/app',
        base: 'feature/main',
        branch: 'feature/x',
      }),
    );
    expect(out?.verdict).toBeUndefined();
  });

  it('catches a decoy across gh --branch/--base flags too', () => {
    const out = run(
      params,
      shell('gh pr create --repo acme/app --branch feature/x --base main', 'Bash'),
    );
    expect(out?.verdict).toBe('deny');
  });
});

describe('github_policy gh api path decoy', () => {
  it('treats an ordinary gh api GET as a read', () => {
    expect(run({}, shell('gh api repos/acme/app/issues', 'Bash'))).toBeUndefined();
    expect(
      run(
        { read_all: false, read_repos: ['acme/other'] },
        shell('gh api repos/acme/app/issues', 'Bash'),
      )?.verdict,
    ).toBe('deny');
  });

  it('uses POST when fields are supplied unless an explicit method overrides it', () => {
    expect(run({}, shell('gh api repos/acme/app/issues -f title=x', 'Bash'))?.verdict).toBe('deny');
    expect(
      run({}, shell('gh api repos/acme/app/issues -f state=open --method GET', 'Bash')),
    ).toBeUndefined();
  });

  it('classifies an explicit non-read method as a write', () => {
    expect(
      run({}, shell('gh api repos/acme/app/issues/1 -X PATCH -f state=closed', 'Bash'))?.verdict,
    ).toBe('deny');
  });

  it('preserves the endpoint after a valueless api flag', () => {
    const out = run(
      { write_repos: ['allowed/ok'] },
      shell('gh api --method DELETE --verbose repos/victim/secret -f repo=allowed/ok', 'Bash'),
    );
    expect(out?.verdict).toBe('deny');
  });

  it('denies a gh api write whose path repo differs from the --repo decoy', () => {
    // `gh api repos/owner/name/...` targets the repo in the path; an allowlisted
    // `--repo` beside it must not launder the real destination.
    const out = run(
      { write_repos: ['acme/app'] },
      shell('gh api repos/victim/secret/issues --method POST -f title=x --repo acme/app', 'Bash'),
    );
    expect(out?.verdict).toBe('deny');
  });

  it('allows a gh api write whose path repo is on the allowlist', () => {
    const out = run(
      { write_repos: ['acme/app'] },
      shell('gh api repos/acme/app/issues --method POST -f title=x', 'Bash'),
    );
    expect(out).toBeUndefined();
  });

  it('reads a repo api path hidden in a -f field value', () => {
    const out = run(
      { write_repos: ['acme/app'] },
      shell('gh api graphql -f path=repos/victim/secret --repo acme/app', 'Bash'),
    );
    expect(out?.verdict).toBe('deny');
  });

  it('does not mistake a git ref in a field for a repo', () => {
    // `-f ref=heads/main` is a git ref, not a `repos/owner/name` api path.
    const out = run(
      { write_repos: ['acme/app'] },
      shell('gh api repos/acme/app/git/refs --method POST -f ref=heads/main', 'Bash'),
    );
    expect(out).toBeUndefined();
  });

  it('reads a bare repo slug from a repo-keyed field', () => {
    // `-f repo=victim/secret` names a repository even without the `repos/` prefix.
    const out = run(
      { write_repos: ['acme/app'] },
      shell('gh api graphql -f repo=victim/secret --repo acme/app', 'Bash'),
    );
    expect(out?.verdict).toBe('deny');
  });

  it('asks when a gh api write carries an opaque --input body', () => {
    // The body can name the real target repo and cannot be inspected, so an
    // allowlisted --repo beside it does not clear the call.
    const out = run(
      { write_repos: ['acme/app'] },
      shell('gh api graphql --input - --repo acme/app', 'Bash'),
    );
    expect(out?.verdict).toBe('ask');
  });

  it('asks for the attached --input=<file> spelling too', () => {
    const out = run(
      { write_repos: ['acme/app'] },
      shell('gh api graphql --input=payload.json --repo acme/app', 'Bash'),
    );
    expect(out?.verdict).toBe('ask');
  });

  it('crosses an owner field with a bare repo/name field', () => {
    // `-f owner=victim -f repo=secret` names victim/secret across two fields, the
    // same laundering the MCP resolver already crosses.
    for (const command of [
      'gh api graphql -f owner=victim -f repo=secret --repo acme/app',
      'gh api graphql -f owner=victim -f name=secret --repo acme/app',
    ]) {
      expect(run({ write_repos: ['acme/app'] }, shell(command, 'Bash'))?.verdict, command).toBe(
        'deny',
      );
    }
    // The allowlisted pair split the same way still passes.
    expect(
      run({ write_repos: ['acme/app'] }, shell('gh api graphql -f owner=acme -f repo=app', 'Bash')),
    ).toBeUndefined();
  });
});

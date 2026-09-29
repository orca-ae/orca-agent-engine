// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import simpleGit, { type SimpleGit } from 'simple-git';

export interface CloneInput {
  url: string;
  /** Branch name or commit SHA, or undefined for the default branch. */
  ref?: string;
  /** Pre-resolved PAT — embedded in the clone URL one-shot. After the clone
   * succeeds, GitWorker resets the remote URL to the bare form so the PAT
   * doesn't sit in `.git/config`. */
  pat?: string;
  /** Absolute path. The directory is created if missing; if it exists with
   * content, simple-git's clone fails. */
  dest: string;
  /** Default true. Pass false for tests against file:// remotes — some git
   * builds don't support `--filter=blob:none` against local file:// remotes. */
  filterBlobs?: boolean;
  /** Default true. Pass false for full-history clones (e.g., when checkout
   * targets a commit SHA that's not the tip). */
  shallow?: boolean;
}

export interface GitWorker {
  cloneInto(input: CloneInput): Promise<{ commit: string }>;
  /** Recursively list the working tree's tracked + untracked-but-not-ignored
   * files relative to the clone root. Excludes anything under `.git/`. */
  listWorkingTree(dest: string): Promise<string[]>;
}

/**
 * Embeds the PAT into an HTTPS clone URL:
 * `https://x-access-token:<PAT>@host/path`. Non-HTTPS URLs are rejected before
 * `git` starts so credentials cannot cross a plaintext transport.
 * The URL is on the git subprocess's argv (visible to `ps` only on the harness
 * host — never reaches the sandbox), but NOT in the resulting `.git/config`
 * because GitWorker.cloneInto resets the remote URL after clone.
 */
function injectPat(url: string, pat: string): string {
  const u = new URL(url);
  if (u.protocol !== 'https:') {
    throw new Error('refusing to send Git credentials over a non-HTTPS repository URL');
  }
  u.username = 'x-access-token';
  u.password = pat;
  return u.toString();
}

export function makeGitWorker(): GitWorker {
  return {
    async cloneInto(input: CloneInput): Promise<{ commit: string }> {
      const args: string[] = [];
      if (input.shallow !== false) args.push('--depth=1');
      if (input.filterBlobs !== false) args.push('--filter=blob:none');
      if (input.ref) args.push('--branch', input.ref);

      const cloneUrl = input.pat ? injectPat(input.url, input.pat) : input.url;
      await simpleGit().clone(cloneUrl, input.dest, args);

      const repo: SimpleGit = simpleGit(input.dest);
      // Reset the remote URL so the PAT doesn't linger in .git/config.
      await repo.remote(['set-url', 'origin', input.url]);

      const log = await repo.log({ maxCount: 1 });
      return { commit: log.latest?.hash ?? '' };
    },

    async listWorkingTree(dest: string): Promise<string[]> {
      const repo = simpleGit(dest);
      // ls-files lists tracked files; --others --exclude-standard adds
      // untracked-but-not-ignored entries. For a fresh clone there's no
      // untracked work; we add the flags for correctness against any caller
      // that runs listWorkingTree on a dirty work dir.
      const result = await repo.raw(['ls-files', '--cached', '--others', '--exclude-standard']);
      return result
        .split('\n')
        .map((s) => s.trim())
        .filter((s) => s.length > 0 && !s.startsWith('.git/'));
    },
  };
}

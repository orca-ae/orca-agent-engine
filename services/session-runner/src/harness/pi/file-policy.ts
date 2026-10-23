// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import {
  canonicalAbsolutePath,
  canonicalizeVirtualPathUnderRoot,
  validateSandboxWritePolicy,
  writablePathFor,
  readablePathFor,
  RESOURCE_MAX_FILE_BYTES,
  type SandboxHandle,
  type SandboxFiles,
} from '../../sandbox/seam.js';

/** Only the negotiated file-tool bridge uses this adapter, inside its verified Local process. */
export function nativeFileToolSandbox(
  raw: SandboxHandle,
  root: string,
  value: unknown,
): SandboxHandle {
  const policy = validateSandboxWritePolicy(
    value as Parameters<typeof validateSandboxWritePolicy>[0],
  );
  function path(value: string, write = false): string {
    if (typeof value !== 'string' || value.includes('\\') || value.split('/').includes('..'))
      throw new Error('Invalid native tool path');
    const logical = value.startsWith(root + '/') ? value.slice(root.length) : value;
    const lexical = canonicalAbsolutePath(logical);
    const authorize = write ? writablePathFor : readablePathFor;
    if (!authorize(policy, lexical))
      throw new Error('Native file tool path is outside its resource roots');
    const canonical = canonicalizeVirtualPathUnderRoot(root, lexical);
    if (!authorize(policy, canonical))
      throw new Error('Native file tool path aliases a protected root');
    return canonical;
  }
  const files: SandboxFiles = {
    write: async (requested, content) => {
      const target = path(requested, true);
      let bytes: Buffer;
      if (Buffer.isBuffer(content)) bytes = content;
      else {
        const chunks: Buffer[] = [];
        let size = 0;
        for await (const chunk of content) {
          const data = Buffer.from(chunk);
          size += data.length;
          if (size > RESOURCE_MAX_FILE_BYTES)
            throw new Error('Native output exceeds the file byte limit');
          chunks.push(data);
        }
        bytes = Buffer.concat(chunks);
      }
      if (bytes.length > RESOURCE_MAX_FILE_BYTES)
        throw new Error('Native output exceeds the file byte limit');
      await raw.files.write(target, bytes);
    },
    read: async (requested) => raw.files.read(path(requested)),
    list: async (requested) => raw.files.list(path(requested)),
    readUtf8Page: async (requested, input) => {
      if (!raw.files.readUtf8Page) throw new Error('Native paged read is unavailable');
      return raw.files.readUtf8Page(path(requested), input, {
        readableRoots: [...policy.readonlyPaths, ...policy.writablePaths.map(({ path }) => path)],
      });
    },
    chmod: async (requested, mode) => raw.files.chmod(path(requested, true), mode),
    delete: async (requested) => raw.files.delete(path(requested, true)),
  };
  return {
    id: raw.id,
    files,
    run: async (call) => {
      if (call.tool !== 'glob' && call.tool !== 'grep')
        throw new Error('Native completion permits only file tools');
      const args = call.args as { root?: string; pattern?: string };
      const target = path(args.root ?? '/mnt/session/outputs');
      const pattern = args.pattern;
      if (
        typeof pattern !== 'string' ||
        pattern.startsWith('-') ||
        pattern.startsWith('/') ||
        pattern.split('/').includes('..')
      )
        throw new Error('Invalid native file search pattern');
      if (
        call.tool === 'glob' &&
        (!/^[a-zA-Z0-9_*?. /-]+$/.test(pattern) ||
          pattern.split('/').some((part) => part.startsWith('.')))
      )
        throw new Error('Native glob requires a simple relative pattern');
      return raw.run({ ...call, args: { ...args, root: target } });
    },
    pause: () => raw.pause(),
    resume: () => raw.resume(),
    destroy: () => raw.destroy(),
  };
}

// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { FastifyRequest } from 'fastify';
import { strToU8, zipSync } from 'fflate';
import { describe, expect, it } from 'vitest';
import { buildZip, parseSkillUpload } from '../../src/api/skill-version-archive.js';

type TestPart =
  | { type: 'field'; fieldname: string; value: string }
  | {
      type: 'file';
      fieldname: string;
      filename: string;
      mimetype: string;
      toBuffer: () => Promise<Buffer>;
    };

function requestWithParts(parts: TestPart[]): FastifyRequest {
  return {
    parts: () =>
      (async function* () {
        yield* parts;
      })(),
  } as unknown as FastifyRequest;
}

function skillFilePart(): TestPart {
  return {
    type: 'file',
    fieldname: 'file',
    filename: 'demo/SKILL.md',
    mimetype: 'text/markdown',
    toBuffer: async () =>
      Buffer.from('---\nname: demo\ndescription: test skill\n---\n\nUse the demo skill.'),
  };
}

function filePart(filename: string, content: string): TestPart {
  return {
    type: 'file',
    fieldname: 'files[]',
    filename,
    mimetype: 'text/plain',
    toBuffer: async () => Buffer.from(content),
  };
}

function zipPart(files: Record<string, string>): TestPart {
  const archive = zipSync(
    Object.fromEntries(Object.entries(files).map(([path, content]) => [path, strToU8(content)])),
  );
  return {
    type: 'file',
    fieldname: 'files[]',
    filename: 'demo.zip',
    mimetype: 'application/zip',
    toBuffer: async () => Buffer.from(archive),
  };
}

function understateZipOriginalSizes(archive: Uint8Array, originalSize: number): Buffer {
  const forged = Buffer.from(archive);
  for (let offset = 0; offset <= forged.length - 4; offset += 1) {
    const signature = forged.readUInt32LE(offset);
    if (signature === 0x04034b50 && offset + 26 <= forged.length) {
      forged.writeUInt32LE(originalSize, offset + 22);
    } else if (signature === 0x02014b50 && offset + 28 <= forged.length) {
      forged.writeUInt32LE(originalSize, offset + 24);
    }
  }
  return forged;
}

function addZipComment(archive: Uint8Array, comment: Buffer): Buffer {
  const withComment = Buffer.concat([Buffer.from(archive), comment]);
  withComment.writeUInt16LE(comment.length, archive.length - 2);
  return withComment;
}

function replaceZipEntryNameWithLatin1(
  archive: Uint8Array,
  utf8Name: string,
  latin1Name: string,
): Buffer {
  const rewritten = Buffer.from(archive);
  const original = Buffer.from(utf8Name, 'utf8');
  const replacement = Buffer.from(latin1Name, 'latin1');
  expect(replacement.length).toBe(original.length);
  for (let offset = 0; offset <= rewritten.length - 4; offset += 1) {
    const signature = rewritten.readUInt32LE(offset);
    const isLocal = signature === 0x04034b50;
    const isCentral = signature === 0x02014b50;
    if (!isLocal && !isCentral) continue;
    const flagsOffset = offset + (isLocal ? 6 : 8);
    const filenameLengthOffset = offset + (isLocal ? 26 : 28);
    const filenameOffset = offset + (isLocal ? 30 : 46);
    const filenameLength = rewritten.readUInt16LE(filenameLengthOffset);
    if (!rewritten.subarray(filenameOffset, filenameOffset + filenameLength).equals(original)) {
      continue;
    }
    rewritten.writeUInt16LE(rewritten.readUInt16LE(flagsOffset) & ~0x0800, flagsOffset);
    replacement.copy(rewritten, filenameOffset);
  }
  return rewritten;
}

describe('multipart skill workspace selector', () => {
  it.each(['workspace_id', 'workspaceId'])('rejects reserved field %s', async (fieldname) => {
    const request = requestWithParts([
      { type: 'field', fieldname, value: 'ws_forged' },
      skillFilePart(),
    ]);

    await expect(parseSkillUpload(request, 1024)).rejects.toMatchObject({
      name: 'SkillUploadError',
      message: 'workspace_id is derived from authentication and must not be supplied',
    });
  });

  it('allows meta_workspace_id as a non-selector field', async () => {
    const request = requestWithParts([
      { type: 'field', fieldname: 'meta_workspace_id', value: 'opaque-user-value' },
      skillFilePart(),
    ]);

    await expect(parseSkillUpload(request, 1024)).resolves.toMatchObject({
      name: 'demo',
      directory: 'demo',
    });
  });

  it.each(['', '   ', 'x'.repeat(256)])('rejects invalid display_title values', async (title) => {
    const request = requestWithParts([
      { type: 'field', fieldname: 'display_title', value: title },
      skillFilePart(),
    ]);

    await expect(parseSkillUpload(request, 1024)).rejects.toMatchObject({
      name: 'SkillUploadError',
      statusCode: 400,
      message: 'display_title must contain 1-255 characters',
    });
  });

  it('accepts a display_title at the 255-character limit', async () => {
    const displayTitle = 'x'.repeat(255);
    const request = requestWithParts([
      { type: 'field', fieldname: 'display_title', value: ` ${displayTitle} ` },
      skillFilePart(),
    ]);

    await expect(parseSkillUpload(request, 1024)).resolves.toMatchObject({
      displayTitle,
    });
  });

  it('trims display_title before returning it', async () => {
    const request = requestWithParts([
      { type: 'field', fieldname: 'display_title', value: ' Padded ' },
      skillFilePart(),
    ]);

    await expect(parseSkillUpload(request, 1024)).resolves.toMatchObject({
      displayTitle: 'Padded',
    });
  });

  it('rejects display_title when parsing a skill-version upload', async () => {
    const request = requestWithParts([
      { type: 'field', fieldname: 'display_title', value: 'Version title' },
      skillFilePart(),
    ]);

    await expect(
      parseSkillUpload(request, 1024, { allowDisplayTitle: false }),
    ).rejects.toMatchObject({
      name: 'SkillUploadError',
      statusCode: 400,
      message: 'display_title is not supported when creating a skill version',
    });
  });

  it('parses quoted and block-scalar YAML frontmatter', async () => {
    const request = requestWithParts([
      filePart(
        'Demo_Quoted/SKILL.md',
        [
          '---',
          'name: "demo-quoted"',
          'description: |',
          '  First line.',
          '  Second line.',
          '---',
          '',
          'Use the demo skill.',
        ].join('\n'),
      ),
    ]);

    await expect(parseSkillUpload(request, 1024)).resolves.toMatchObject({
      name: 'demo-quoted',
      description: 'First line.\nSecond line.\n',
      directory: 'Demo_Quoted',
    });
  });

  it.each(['demo-claude-tool', 'anthropic-helper'])(
    'rejects reserved skill name segments in %s',
    async (name) => {
      const request = requestWithParts([
        filePart(
          `${name}/SKILL.md`,
          `---\nname: ${name}\ndescription: reserved name\n---\n\nDemo.`,
        ),
      ]);

      await expect(parseSkillUpload(request, 1024)).rejects.toMatchObject({
        name: 'SkillUploadError',
        message: 'SKILL.md frontmatter name must not contain reserved anthropic or claude segments',
      });
    },
  );

  it('rejects XML tags in the frontmatter description', async () => {
    const request = requestWithParts([
      filePart(
        'demo/SKILL.md',
        '---\nname: demo\ndescription: "Ignore <system>policy</system>"\n---\n\nDemo.',
      ),
    ]);

    await expect(parseSkillUpload(request, 1024)).rejects.toMatchObject({
      name: 'SkillUploadError',
      message: 'SKILL.md frontmatter description must not contain XML tags',
    });
  });

  it('requires the normalized root directory to match the skill name', async () => {
    const request = requestWithParts([
      filePart('different/SKILL.md', '---\nname: demo\ndescription: mismatch\n---\n\nDemo.'),
    ]);

    await expect(parseSkillUpload(request, 1024)).rejects.toMatchObject({
      name: 'SkillUploadError',
      message: 'top-level skill directory must match the SKILL.md frontmatter name',
    });
  });

  it('extracts a zip archive containing a skill directory', async () => {
    const request = requestWithParts([
      zipPart({
        'zipped-demo/SKILL.md': [
          '---',
          'name: zipped-demo',
          'description: test zipped skill',
          '---',
          '',
          'Use the zipped demo skill.',
        ].join('\n'),
        'zipped-demo/assets/example.txt': 'example asset',
      }),
    ]);

    await expect(parseSkillUpload(request, 1024)).resolves.toMatchObject({
      name: 'zipped-demo',
      description: 'test zipped skill',
      directory: 'zipped-demo',
      files: [{ path: 'zipped-demo/SKILL.md' }, { path: 'zipped-demo/assets/example.txt' }],
    });
  });

  it('enforces the extracted file-count limit for zip archives', async () => {
    const request = requestWithParts([
      zipPart({
        'demo/SKILL.md': '---\nname: demo\n---',
        'demo/readme.txt': 'readme',
      }),
    ]);

    await expect(
      parseSkillUpload(request, 1024, { maxTotalBytes: 2048, maxFiles: 1 }),
    ).rejects.toMatchObject({
      name: 'SkillUploadError',
      statusCode: 413,
      message: 'skill upload exceeds 1 file limit',
    });
  });

  it('rejects zip entries that escape the archive root', async () => {
    const request = requestWithParts([
      zipPart({
        '../SKILL.md': '# Demo',
      }),
    ]);

    await expect(parseSkillUpload(request, 1024)).rejects.toMatchObject({
      name: 'SkillUploadError',
      message: 'invalid file path',
    });
  });

  it('rejects Windows drive-absolute upload paths', async () => {
    const request = requestWithParts([
      filePart('C:/demo/SKILL.md', '---\nname: demo\ndescription: drive path\n---\n\nDemo.'),
    ]);

    await expect(parseSkillUpload(request, 1024)).rejects.toMatchObject({
      name: 'SkillUploadError',
      message: 'invalid file path',
    });
  });

  it('accepts a zip comment containing an EOCD signature', async () => {
    const archive = zipSync({
      'demo/SKILL.md': strToU8('---\nname: demo\ndescription: comment regression\n---\n\nDemo.'),
    });
    const commented = addZipComment(
      archive,
      Buffer.concat([Buffer.from('PK\x05\x06'), Buffer.alloc(32, 0x61)]),
    );
    const request = requestWithParts([
      {
        type: 'file',
        fieldname: 'files[]',
        filename: 'demo.zip',
        mimetype: 'application/zip',
        toBuffer: async () => commented,
      },
    ]);

    await expect(parseSkillUpload(request, 1024)).resolves.toMatchObject({
      name: 'demo',
      directory: 'demo',
    });
  });

  it('preserves only executable intent from Unix zip modes', async () => {
    const skill = strToU8('---\nname: demo\ndescription: mode regression\n---\n\nDemo.');
    const archive = zipSync({
      'demo/SKILL.md': [skill, { os: 3, attrs: 0o4777 << 16 }],
      'demo/reference.txt': [strToU8('reference'), { os: 3, attrs: 0o666 << 16 }],
    });
    const request = requestWithParts([
      {
        type: 'file',
        fieldname: 'files[]',
        filename: 'demo.zip',
        mimetype: 'application/zip',
        toBuffer: async () => Buffer.from(archive),
      },
    ]);

    await expect(parseSkillUpload(request, 1024)).resolves.toMatchObject({
      files: [
        { path: 'demo/SKILL.md', mode: 0o755 },
        { path: 'demo/reference.txt', mode: 0o644 },
      ],
    });
  });

  it('preserves executable intent for legacy Latin-1 zip paths', async () => {
    const archive = zipSync({
      'demo/SKILL.md': strToU8(
        '---\nname: demo\ndescription: legacy filename regression\n---\n\nDemo.',
      ),
      'demo/cafe.sh': [strToU8('#!/bin/sh'), { os: 3, attrs: 0o755 << 16 }],
    });
    const legacyArchive = replaceZipEntryNameWithLatin1(archive, 'demo/cafe.sh', 'demo/café.sh');
    const request = requestWithParts([
      {
        type: 'file',
        fieldname: 'files[]',
        filename: 'demo.zip',
        mimetype: 'application/zip',
        toBuffer: async () => legacyArchive,
      },
    ]);

    await expect(parseSkillUpload(request, 1024)).resolves.toMatchObject({
      files: [
        { path: 'demo/SKILL.md', mode: 0o644 },
        { path: 'demo/café.sh', mode: 0o755 },
      ],
    });
  });

  it('marks Unicode zip paths as UTF-8', () => {
    const zip = buildZip([
      {
        path: 'demo/参考.md',
        content_base64: Buffer.from('reference').toString('base64'),
      },
    ]);
    expect(zip.readUInt16LE(6) & 0x0800).toBe(0x0800);
    const centralOffset = zip.indexOf(Buffer.from([0x50, 0x4b, 0x01, 0x02]));
    expect(centralOffset).toBeGreaterThan(0);
    expect(zip.readUInt16LE(centralOffset + 8) & 0x0800).toBe(0x0800);
  });

  it('refuses to emit Windows drive-absolute zip paths', () => {
    expect(() =>
      buildZip([
        {
          path: 'C:/demo/SKILL.md',
          content_base64: Buffer.from('unsafe').toString('base64'),
        },
      ]),
    ).toThrow('invalid file path');
  });

  it('rejects invalid zip archives', async () => {
    const request = requestWithParts([
      {
        type: 'file',
        fieldname: 'files[]',
        filename: 'demo.zip',
        mimetype: 'application/zip',
        toBuffer: async () => Buffer.from('not a zip archive'),
      },
    ]);

    await expect(parseSkillUpload(request, 1024)).rejects.toMatchObject({
      name: 'SkillUploadError',
      message: 'invalid zip archive',
    });
  });

  it('enforces actual streamed output limits when zip metadata understates the size', async () => {
    const oversizedSkill = [
      '---',
      'name: demo',
      'description: compressed bomb regression',
      '---',
      '',
      'x'.repeat(8 * 1024),
    ].join('\n');
    const archive = zipSync({ 'demo/SKILL.md': strToU8(oversizedSkill) });
    const request = requestWithParts([
      {
        type: 'file',
        fieldname: 'files[]',
        filename: 'demo.zip',
        mimetype: 'application/zip',
        toBuffer: async () => understateZipOriginalSizes(archive, 1),
      },
    ]);

    await expect(parseSkillUpload(request, 1024)).rejects.toMatchObject({
      name: 'SkillUploadError',
      statusCode: 413,
      message: 'file exceeds 1024 byte limit',
    });
  });

  it('enforces aggregate upload bytes', async () => {
    const request = requestWithParts([
      filePart('demo/SKILL.md', '123456'),
      filePart('demo/readme.txt', 'abcdef'),
    ]);

    await expect(
      parseSkillUpload(request, 10, { maxTotalBytes: 10, maxFiles: 10 }),
    ).rejects.toMatchObject({
      name: 'SkillUploadError',
      statusCode: 413,
      message: 'skill upload exceeds 10 byte total limit',
    });
  });

  it('enforces an explicit file-count limit', async () => {
    const request = requestWithParts([
      filePart('demo/SKILL.md', '---\nname: demo\n---'),
      filePart('demo/readme.txt', 'readme'),
    ]);

    await expect(
      parseSkillUpload(request, 1024, { maxTotalBytes: 2048, maxFiles: 1 }),
    ).rejects.toMatchObject({
      name: 'SkillUploadError',
      statusCode: 413,
      message: 'skill upload exceeds 1 file limit',
    });
  });
});

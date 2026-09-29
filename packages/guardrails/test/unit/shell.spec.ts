// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { commandName, parseShellCommands } from '../../src/shell.js';

/** Every parsed command's argv, in order. */
function argvs(command: string): string[][] {
  return parseShellCommands(command).map((c) => c.argv);
}

/** True when some parsed command starts with `name` and contains every argument. */
function reports(command: string, name: string, ...args: string[]): boolean {
  return parseShellCommands(command).some(
    (c) => commandName(c.argv) === name && args.every((a) => c.argv.includes(a)),
  );
}

describe('parseShellCommands: chaining and grouping', () => {
  it('returns a single command for a single invocation', () => {
    expect(argvs('rm -rf build')).toEqual([['rm', '-rf', 'build']]);
  });

  it('splits on a semicolon', () => {
    expect(argvs('echo one; echo two')).toEqual([
      ['echo', 'one'],
      ['echo', 'two'],
    ]);
  });

  it('splits on and-if', () => {
    expect(argvs('echo one && echo two')).toEqual([
      ['echo', 'one'],
      ['echo', 'two'],
    ]);
  });

  it('splits on or-if', () => {
    expect(argvs('echo one || echo two')).toEqual([
      ['echo', 'one'],
      ['echo', 'two'],
    ]);
  });

  it('splits on a pipe and keeps both stages in one pipeline', () => {
    const parsed = parseShellCommands('cat file | grep needle');
    expect(parsed.map((c) => c.argv)).toEqual([
      ['cat', 'file'],
      ['grep', 'needle'],
    ]);
    expect(parsed[0]?.pipeline).toBe(parsed[1]?.pipeline);
  });

  it('gives sequenced commands distinct pipelines', () => {
    const parsed = parseShellCommands('echo one; echo two');
    expect(parsed[0]?.pipeline).not.toBe(parsed[1]?.pipeline);
  });

  it('splits on a newline', () => {
    expect(argvs('echo one\necho two')).toEqual([
      ['echo', 'one'],
      ['echo', 'two'],
    ]);
  });

  it('splits on a background operator', () => {
    expect(argvs('echo one & echo two')).toEqual([
      ['echo', 'one'],
      ['echo', 'two'],
    ]);
  });

  it('sees into a parenthesised subshell', () => {
    expect(argvs('(cd /tmp && rm -rf junk)')).toEqual([
      ['cd', '/tmp'],
      ['rm', '-rf', 'junk'],
    ]);
  });

  it('sees into a brace group', () => {
    expect(argvs('{ rm -rf junk; }')).toEqual([['rm', '-rf', 'junk']]);
  });

  it('drops shell keywords so the guarded command is argv[0]', () => {
    expect(argvs('if true; then rm -rf junk; fi')).toEqual([['true'], ['rm', '-rf', 'junk']]);
  });

  it('joins a line continuation', () => {
    expect(argvs('rm \\\n  -rf junk')).toEqual([['rm', '-rf', 'junk']]);
  });

  it('ignores a comment', () => {
    expect(argvs('echo one # rm -rf /')).toEqual([['echo', 'one']]);
  });

  it('returns nothing for an empty command', () => {
    expect(argvs('')).toEqual([]);
    expect(argvs('   \n  ')).toEqual([]);
  });
});

describe('parseShellCommands: quoting', () => {
  it('does not split on an operator inside double quotes', () => {
    expect(argvs('echo "one; two && three"')).toEqual([['echo', 'one; two && three']]);
  });

  it('does not split on an operator inside single quotes', () => {
    expect(argvs("echo 'one | two'")).toEqual([['echo', 'one | two']]);
  });

  it('keeps a dangerous-looking string as one argument rather than a command', () => {
    // The whole point of parsing: `rm` here is data, not an invocation.
    expect(argvs('git commit -m "rm -rf /"')).toEqual([['git', 'commit', '-m', 'rm -rf /']]);
  });

  it('does not throw on an unbalanced double quote', () => {
    expect(() => parseShellCommands('echo "unterminated')).not.toThrow();
    expect(argvs('echo "unterminated')).toEqual([['echo', 'unterminated']]);
  });

  it('does not throw on an unbalanced single quote', () => {
    expect(() => parseShellCommands("rm -rf 'unterminated")).not.toThrow();
    expect(argvs("rm -rf 'unterminated")).toEqual([['rm', '-rf', 'unterminated']]);
  });

  it('keeps an escaped separator out of the split', () => {
    // The `\;` terminating find's action is a word, not a command border.
    expect(argvs('find . -exec rm {} \\;')[0]).toEqual(['find', '.', '-exec', 'rm', '{}', ';']);
  });

  it('strips the `$` from ANSI-C quoting so the command reaches argv[0]', () => {
    // `$'rm'` is `rm`, not `$rm`; leaving the `$` on hides it from every rule.
    expect(argvs("$'rm' -rf /")).toEqual([['rm', '-rf', '/']]);
  });

  it("decodes hex escapes in ANSI-C quoting, so `$'\\x72\\x6d'` is `rm`", () => {
    expect(argvs("$'\\x72\\x6d' -rf /")).toEqual([['rm', '-rf', '/']]);
  });

  it('decodes octal escapes in ANSI-C quoting', () => {
    // \162\155 is `rm` in octal.
    expect(argvs("$'\\162\\155' -rf /")).toEqual([['rm', '-rf', '/']]);
  });

  it('decodes a mix of literal and escaped characters', () => {
    // d then \x64 (d), so `$'d\x64'` is `dd`.
    expect(argvs("$'d\\x64' if=/dev/zero of=/dev/sda")).toEqual([
      ['dd', 'if=/dev/zero', 'of=/dev/sda'],
    ]);
  });

  it('strips the `$` from locale-prefixed double quoting', () => {
    expect(argvs('$"rm" -rf /')).toEqual([['rm', '-rf', '/']]);
  });
});

describe('parseShellCommands: redirections', () => {
  it('separates a redirection and its target from the command words', () => {
    const [cmd] = parseShellCommands('echo hi > /etc/motd');
    expect(cmd?.argv).toEqual(['echo', 'hi']);
    expect(cmd?.redirections).toEqual([{ op: '>', target: '/etc/motd' }]);
  });

  it('separates a redirection written without spaces', () => {
    const [cmd] = parseShellCommands('echo hi>/etc/motd');
    expect(cmd?.argv).toEqual(['echo', 'hi']);
    expect(cmd?.redirections).toEqual([{ op: '>', target: '/etc/motd' }]);
  });

  it('captures append and file-descriptor-duplication forms distinctly', () => {
    const [cmd] = parseShellCommands('echo hi >> log 2>&1');
    expect(cmd?.argv).toEqual(['echo', 'hi']);
    expect(cmd?.redirections).toEqual([
      { op: '>>', target: 'log' },
      { op: '2>&', target: '1' },
    ]);
  });

  it('keeps a quoted redirection-looking token as an ordinary operand', () => {
    // The regression this refactor fixes: a quoted `>` is a filename, not an
    // operator, so the path after it must not be swallowed as a redirect target.
    const [cmd] = parseShellCommands("rm keep '>' /etc/passwd");
    expect(cmd?.argv).toEqual(['rm', 'keep', '>', '/etc/passwd']);
    expect(cmd?.redirections).toEqual([]);
  });

  it('puts a leading redirection in redirections, not at argv[0]', () => {
    const [cmd] = parseShellCommands('2>/dev/null rm -rf /');
    expect(cmd?.argv).toEqual(['rm', '-rf', '/']);
    expect(commandName(cmd?.argv ?? [])).toBe('rm');
  });
});

describe('parseShellCommands: wrappers', () => {
  it('unwraps sudo', () => {
    expect(argvs('sudo rm -rf junk')).toEqual([['rm', '-rf', 'junk']]);
  });

  it('unwraps sudo with its own options', () => {
    expect(argvs('sudo -u root -n rm -rf junk')).toEqual([['rm', '-rf', 'junk']]);
  });

  it('unwraps sudo directory and other value-taking options', () => {
    expect(argvs('sudo -D /tmp rm -rf junk')).toEqual([['rm', '-rf', 'junk']]);
    expect(argvs('sudo -R /sandbox --group staff rm -rf junk')).toEqual([['rm', '-rf', 'junk']]);
    expect(argvs('sudo -nD /tmp rm -rf junk')).toEqual([['rm', '-rf', 'junk']]);
    expect(argvs('sudo --chdir=/tmp rm -rf junk')).toEqual([['rm', '-rf', 'junk']]);
  });

  it('unwraps env with assignments', () => {
    expect(argvs('env FOO=1 BAR=2 rm -rf junk')).toEqual([['rm', '-rf', 'junk']]);
  });

  it('unwraps a bare assignment prefix', () => {
    expect(argvs('FOO=1 rm -rf junk')).toEqual([['rm', '-rf', 'junk']]);
  });

  it('unwraps nohup', () => {
    expect(argvs('nohup rm -rf junk')).toEqual([['rm', '-rf', 'junk']]);
  });

  it('unwraps a path-qualified keyword wrapper', () => {
    expect(argvs('/usr/bin/nohup rm -rf junk')).toEqual([['rm', '-rf', 'junk']]);
  });

  it('unwraps time', () => {
    expect(argvs('time rm -rf junk')).toEqual([['rm', '-rf', 'junk']]);
  });

  it('unwraps xargs, including its own options', () => {
    expect(argvs('cat list | xargs -n1 -I {} rm -rf junk')).toEqual([
      ['cat', 'list'],
      ['rm', '-rf', 'junk'],
    ]);
  });

  it('skips every value-taking long xargs option before the child command', () => {
    for (const option of [
      '--arg-file list',
      '--delimiter ,',
      '--max-args 1',
      '--max-chars 1024',
      '--max-procs 2',
      '--process-slot-var SLOT',
    ]) {
      expect(reports(`xargs ${option} sh -c 'rm -rf /'`, 'rm', '-rf', '/'), option).toBe(true);
    }
  });

  it('does not swallow the child after an xargs option with an optional argument', () => {
    for (const option of ['-i', '--replace', '--max-lines', '--eof']) {
      expect(reports(`xargs ${option} sh -c 'rm -rf /'`, 'rm', '-rf', '/'), option).toBe(true);
    }
  });

  it('unwraps chroot past its options and new-root operand', () => {
    expect(reports("chroot / sh -c 'rm -rf /etc'", 'rm', '-rf', '/etc')).toBe(true);
    expect(
      reports(
        "chroot --groups staff --userspec user:group / sh -c 'rm -rf /etc'",
        'rm',
        '-rf',
        '/etc',
      ),
    ).toBe(true);
  });

  it('unwraps timeout and its duration', () => {
    expect(argvs('timeout 30s rm -rf junk')).toEqual([['rm', '-rf', 'junk']]);
  });

  it('leaves a wrapper with nothing to wrap out of the results', () => {
    expect(argvs('sudo')).toEqual([]);
  });

  it('unwraps command past its options', () => {
    expect(argvs('command -p rm -rf /')).toEqual([['rm', '-rf', '/']]);
    expect(argvs('command rm -rf /')).toEqual([['rm', '-rf', '/']]);
  });

  it('unwraps builtin', () => {
    expect(argvs('builtin cd /tmp')).toEqual([['cd', '/tmp']]);
  });

  it('unwraps exec, including its -a name option', () => {
    expect(argvs('exec -a title rm -rf /')).toEqual([['rm', '-rf', '/']]);
    expect(argvs('exec rm -rf /')).toEqual([['rm', '-rf', '/']]);
  });

  it('expands an inline Git alias before reporting the effective subcommand', () => {
    expect(argvs('git -c alias.p="push --force" p https://github.com/victim/secret main')).toEqual([
      [
        'git',
        '-c',
        'alias.p=push --force',
        'push',
        '--force',
        'https://github.com/victim/secret',
        'main',
      ],
    ]);
  });

  it('recursively expands an inline Git alias chain', () => {
    expect(
      argvs('git -c alias.co=push -c alias.c=co c https://github.com/victim/secret main'),
    ).toEqual([
      [
        'git',
        '-c',
        'alias.co=push',
        '-c',
        'alias.c=co',
        'push',
        'https://github.com/victim/secret',
        'main',
      ],
    ]);
  });

  it('marks a cyclic inline Git alias chain as unresolved', () => {
    const [command] = parseShellCommands('git -c alias.a=b -c alias.b=a a');
    expect(command?.unresolved).toBe(true);
  });

  it('marks an inline Git shell alias as unresolved', () => {
    const [command] = parseShellCommands("git -c alias.p='!rm -rf /' p");
    expect(command?.unresolved).toBe(true);
  });

  it('unwraps an applet multiplexer to its applet', () => {
    expect(argvs('busybox rm -rf /')).toEqual([['rm', '-rf', '/']]);
    expect(argvs('toybox rm -rf /')).toEqual([['rm', '-rf', '/']]);
  });
});

describe('parseShellCommands: nested shells', () => {
  it('reports the payload of bash -c with a double-quoted argument', () => {
    expect(reports('bash -c "rm -rf junk"', 'rm', '-rf', 'junk')).toBe(true);
  });

  it('reports the payload of sh -c with a single-quoted argument', () => {
    expect(reports("sh -c 'rm -rf junk'", 'rm', '-rf', 'junk')).toBe(true);
  });

  it('reports every command inside a nested payload', () => {
    expect(reports('bash -c "cd /tmp && rm -rf junk"', 'cd', '/tmp')).toBe(true);
    expect(reports('bash -c "cd /tmp && rm -rf junk"', 'rm', '-rf', 'junk')).toBe(true);
  });

  it('reports the payload of eval', () => {
    expect(reports('eval "rm -rf junk"', 'rm', '-rf', 'junk')).toBe(true);
  });

  it('reports the payload of eval given as separate words', () => {
    expect(reports('eval rm -rf junk', 'rm', '-rf', 'junk')).toBe(true);
  });

  it('keeps the wrapping invocation as well as its payload', () => {
    // Reporting the outer shell too costs nothing and leaves a rule that wants
    // to gate `bash -c` itself something to match on.
    expect(reports('bash -c "rm -rf junk"', 'bash', '-c')).toBe(true);
  });

  it('sees through two layers of wrapping', () => {
    expect(reports('sudo bash -c "rm -rf /"', 'rm', '-rf', '/')).toBe(true);
  });

  it('sees through a wrapper nested inside a nested shell', () => {
    expect(reports(`bash -c 'sudo sh -c "rm -rf /"'`, 'rm', '-rf', '/')).toBe(true);
  });

  it('sees through a shell invoked by path', () => {
    expect(reports('/bin/sh -c "rm -rf /"', 'rm', '-rf', '/')).toBe(true);
  });

  it('sees through a pipe into a shell', () => {
    expect(reports('echo whatever | xargs bash -c "rm -rf /"', 'rm', '-rf', '/')).toBe(true);
  });

  it('reports the command find runs for each match', () => {
    expect(reports('find . -name "*.tmp" -exec rm -rf {} \\;', 'rm', '-rf')).toBe(true);
    expect(reports('find . -exec rm -rf {} +', 'rm', '-rf')).toBe(true);
    expect(reports('find . -execdir rm -rf {} \\;', 'rm', '-rf')).toBe(true);
  });

  it('reports every command from multiple find actions', () => {
    const command = "find . -exec echo {} ';' -exec rm -rf / ';'";
    expect(reports(command, 'echo', '{}')).toBe(true);
    expect(reports(command, 'rm', '-rf', '/')).toBe(true);
  });

  it('sees through a wrapper inside a find action', () => {
    expect(reports('find . -exec sudo rm -rf {} \\;', 'rm', '-rf')).toBe(true);
  });

  it('reports the payload of a shell with c bundled before another flag', () => {
    expect(reports("bash -cx 'rm -rf /'", 'rm', '-rf', '/')).toBe(true);
    expect(reports("bash -cl 'rm -rf /'", 'rm', '-rf', '/')).toBe(true);
    expect(reports("sh -xc 'rm -rf /'", 'rm', '-rf', '/')).toBe(true);
  });

  it('reports the payload su runs through a shell', () => {
    expect(reports("su -c 'rm -rf /'", 'rm', '-rf', '/')).toBe(true);
    expect(reports("su root -c 'rm -rf /'", 'rm', '-rf', '/')).toBe(true);
  });

  it('sees through an applet multiplexer invoking a shell', () => {
    expect(reports("busybox sh -c 'rm -rf /'", 'rm', '-rf', '/')).toBe(true);
  });

  it('stops recursing rather than following unbounded nesting', () => {
    const deep = 'bash -c "'.repeat(20) + 'rm -rf /' + '"'.repeat(20);
    expect(() => parseShellCommands(deep)).not.toThrow();
  });
});

describe('parseShellCommands: command substitution', () => {
  it('reports the commands inside a substitution', () => {
    expect(reports('echo $(rm -rf junk)', 'rm', '-rf', 'junk')).toBe(true);
  });

  it('reports the commands inside a backquoted substitution', () => {
    expect(reports('echo `rm -rf junk`', 'rm', '-rf', 'junk')).toBe(true);
  });

  it('reports a substitution nested inside double quotes', () => {
    expect(reports('echo "result: $(rm -rf junk)"', 'rm', '-rf', 'junk')).toBe(true);
  });

  it('does not treat arithmetic expansion as a command', () => {
    expect(argvs('echo $((1 + 2))')).toEqual([['echo', '$((1 + 2))']]);
  });

  it('reports command substitutions nested inside arithmetic expansion', () => {
    expect(reports('echo $(( $(rm -rf /) + 1 ))', 'rm', '-rf', '/')).toBe(true);
    expect(reports('echo $(( $(rm -rf /)))', 'rm', '-rf', '/')).toBe(true);
  });

  it('does not throw on an unbalanced substitution', () => {
    expect(() => parseShellCommands('echo $(rm -rf junk')).not.toThrow();
  });

  it('reads past a paren quoted inside a substitution', () => {
    // The `)` in `')'` is literal; closing the substitution on it would hide the
    // command that follows.
    expect(reports("echo $(x=')'; rm -rf /)", 'rm', '-rf', '/')).toBe(true);
  });
});

describe('parseShellCommands: depth cap', () => {
  it('marks a payload nested past the cap unresolved rather than dropping it', () => {
    // `eval ×8 rm -rf /` still surfaces `rm`; one layer deeper the payload was
    // silently dropped, so every name-based rule abstained. It is now flagged.
    const commands = parseShellCommands('eval '.repeat(9) + 'rm -rf /');
    expect(commands.some((c) => c.unresolved)).toBe(true);
  });

  it('marks a deeply nested command substitution unresolved', () => {
    const deep = 'echo ' + '$('.repeat(9) + 'rm -rf /' + ')'.repeat(9);
    expect(parseShellCommands(deep).some((c) => c.unresolved)).toBe(true);
  });

  it('still resolves a chain that stops just short of the cap', () => {
    const command = 'eval '.repeat(8) + 'rm -rf /';
    expect(parseShellCommands(command).some((c) => c.unresolved)).toBe(false);
    expect(reports(command, 'rm', '-rf', '/')).toBe(true);
  });
});

describe('commandName', () => {
  it('is the argv[0] of a bare command', () => {
    expect(commandName(['rm', '-rf', '/'])).toBe('rm');
  });

  it('strips a leading path, which is otherwise a one-character bypass', () => {
    expect(commandName(['/bin/rm', '-rf', '/'])).toBe('rm');
    expect(commandName(['../../usr/bin/rm'])).toBe('rm');
  });

  it('is empty for an empty argv', () => {
    expect(commandName([])).toBe('');
  });
});

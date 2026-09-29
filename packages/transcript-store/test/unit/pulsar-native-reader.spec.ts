// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { execFileSync } from 'node:child_process';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { expect, it } from 'vitest';

it('does not take ownership of a Reader output pointer after a native read error', () => {
  const require = createRequire(import.meta.url);
  const root = dirname(require.resolve('pulsar-client/package.json'));
  const source = readFileSync(join(root, 'src/Reader.cc'), 'utf8');
  const worker = source.slice(source.indexOf('class ReaderReadNextWorker'));
  const execute = worker.slice(worker.indexOf('  void Execute()'), worker.indexOf('  void OnOK()'));
  expect(execute).toContain('pulsar_reader_read_next_with_timeout');
  const directory = mkdtempSync(join(tmpdir(), 'orca-pulsar-reader-ownership-'));
  try {
    const file = join(directory, 'reader.cc');
    const binary = join(directory, 'reader');
    // Compile the installed binding's actual Execute body against a C-API fault
    // double. Only ResultOk guarantees ownership of the output pointer; poison
    // on failure makes the otherwise allocator-dependent native bug deterministic.
    writeFileSync(
      file,
      `
#include <memory>
#include <string>
#include <iostream>
struct pulsar_reader_t {};
struct pulsar_message_t {};
enum pulsar_result { pulsar_result_Ok, pulsar_result_Timeout, pulsar_result_AlreadyClosed };
static pulsar_result nextResult;
static int invalidFrees = 0, validFrees = 0;
const char* pulsar_result_str(pulsar_result) { return "injected read error"; }
void pulsar_message_free(pulsar_message_t* value) {
  if (value == reinterpret_cast<pulsar_message_t*>(1)) ++invalidFrees;
  else { ++validFrees; delete value; }
}
pulsar_result pulsar_reader_read_next(pulsar_reader_t*, pulsar_message_t** output) {
  *output = nextResult == pulsar_result_Ok ? new pulsar_message_t : reinterpret_cast<pulsar_message_t*>(1);
  return nextResult;
}
pulsar_result pulsar_reader_read_next_with_timeout(pulsar_reader_t* reader, pulsar_message_t** output, long) {
  return pulsar_reader_read_next(reader, output);
}
struct Worker {
  std::shared_ptr<pulsar_reader_t> cReader = std::make_shared<pulsar_reader_t>();
  std::shared_ptr<pulsar_message_t> cMessage;
  long timeout = 1;
  bool errored = false;
  void SetError(const std::string&) { errored = true; }
  ${execute}
};
int main() {
  for (auto timeout : {1, -1}) {
    for (auto result : {pulsar_result_Ok, pulsar_result_Timeout, pulsar_result_AlreadyClosed}) {
      nextResult = result;
      { Worker worker; worker.timeout = timeout; worker.Execute();
        if (worker.errored != (result != pulsar_result_Ok)) return 2; }
    }
  }
  if (invalidFrees != 0 || validFrees != 2) {
    std::cerr << "invalid output ownership: " << invalidFrees << " invalid frees" << std::endl;
    return 1;
  }
}
`,
    );
    execFileSync(process.env['CXX'] ?? 'c++', ['-std=c++17', file, '-o', binary], {
      timeout: 30_000,
    });
    expect(() => execFileSync(binary, [], { timeout: 5_000, stdio: 'pipe' })).not.toThrow();
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
}, 40_000);

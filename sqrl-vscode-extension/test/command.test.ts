///
/// Copyright © 2021 DataSQRL (contact@datasqrl.com)
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///     http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
///

import * as assert from 'node:assert/strict';
import * as path from 'node:path';
import { describe, it } from 'node:test';
import * as command from '../src/command';

const options: command.ServerOptions = {
  launcher: 'docker',
  docker: 'docker',
  dockerImage: 'datasqrl/cmd:latest',
  jar: '',
  java: '',
  command: [],
};

describe('server', () => {
  it('givenDocker_whenServer_thenMountsRootsAtTheirPathsWithTemporaryWorkspace', () => {
    const server = command.server(options, ['/repo', '/repo/shop', '/other'], {});

    assert.equal(server.command, 'docker');
    assert.deepEqual(server.args, [
      'run',
      '--interactive',
      '--rm',
      '--tmpfs',
      '/workspace',
      '--volume',
      '/repo:/repo',
      '--volume',
      '/other:/other',
      'datasqrl/cmd:latest',
      'lsp',
    ]);
  });

  it('givenJarAndJavaHome_whenServer_thenRunsJarWithJvmOptions', () => {
    const server = command.server({ ...options, launcher: 'jar', jar: '/cli/sqrl-cli.jar' }, [], {
      JAVA_HOME: '/jdk',
    });

    assert.equal(server.command, path.join('/jdk', 'bin', 'java'));
    assert.deepEqual(server.args, [...command.JVM_OPTIONS, '-jar', '/cli/sqrl-cli.jar', 'lsp']);
  });

  it('givenJarWithoutPath_whenServer_thenFails', () => {
    assert.throws(() => command.server({ ...options, launcher: 'jar' }, [], {}), /sqrl.server.jar/);
  });

  it('givenCommand_whenServer_thenRunsCommand', () => {
    const server = command.server({ ...options, launcher: 'command', command: ['sqrl', 'lsp'] }, [], {});

    assert.deepEqual(server, { command: 'sqrl', args: ['lsp'] });
  });
});

describe('test', () => {
  it('givenProjectInRoot_whenTest_thenMountsRootAsWorkspace', () => {
    const test = command.test(options, '/repo', ['/repo/shop/package.json', '/repo/shop/package-test.json']);

    assert.equal(test.cwd, '/repo');
    assert.deepEqual(test.args, [
      'run',
      '--rm',
      '--interactive',
      '--tty',
      '--volume',
      '/repo:/workspace',
      'datasqrl/cmd:latest',
      'test',
      'shop/package.json',
      'shop/package-test.json',
    ]);
  });

  it('givenProjectOutsideRoot_whenTest_thenMountsProjectDirectory', () => {
    const test = command.test(options, '/repo', ['/elsewhere/package.json']);

    assert.equal(test.cwd, '/elsewhere');
    assert.ok(test.args.includes('/elsewhere:/workspace'));
  });

  it('givenJar_whenTest_thenRunsJarInImage', () => {
    const test = command.test({ ...options, launcher: 'jar', jar: '/cli/sqrl-cli.jar' }, '/repo', [
      '/repo/package.json',
    ]);

    assert.ok(test.args.includes('/cli/sqrl-cli.jar:/opt/sqrl/sqrl-cli.jar'));
  });

  it('givenCommand_whenTest_thenReplacesLspWithTest', () => {
    const test = command.test({ ...options, launcher: 'command', command: ['sqrl', 'lsp'] }, '/repo', [
      '/repo/shop/package.json',
    ]);

    assert.deepEqual(test, { command: 'sqrl', args: ['test', 'package.json'], cwd: '/repo/shop' });
  });
});

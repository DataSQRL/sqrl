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

import * as path from 'node:path';

/** The JVM options that the CLI needs, as in `SQRL_JVM_TOOL_OPTS` of its Docker image */
export const JVM_OPTIONS = [
  '--add-exports=java.base/sun.net.util=ALL-UNNAMED',
  '--add-exports=java.rmi/sun.rmi.registry=ALL-UNNAMED',
  '--add-exports=jdk.compiler/com.sun.tools.javac.api=ALL-UNNAMED',
  '--add-exports=jdk.compiler/com.sun.tools.javac.file=ALL-UNNAMED',
  '--add-exports=jdk.compiler/com.sun.tools.javac.parser=ALL-UNNAMED',
  '--add-exports=jdk.compiler/com.sun.tools.javac.tree=ALL-UNNAMED',
  '--add-exports=jdk.compiler/com.sun.tools.javac.util=ALL-UNNAMED',
  '--add-exports=java.security.jgss/sun.security.krb5=ALL-UNNAMED',
  '--add-opens=java.base/java.lang=ALL-UNNAMED',
  '--add-opens=java.base/java.net=ALL-UNNAMED',
  '--add-opens=java.base/java.io=ALL-UNNAMED',
  '--add-opens=java.base/java.nio=ALL-UNNAMED',
  '--add-opens=java.base/sun.nio.ch=ALL-UNNAMED',
  '--add-opens=java.base/java.lang.reflect=ALL-UNNAMED',
  '--add-opens=java.base/java.text=ALL-UNNAMED',
  '--add-opens=java.base/java.time=ALL-UNNAMED',
  '--add-opens=java.base/java.util=ALL-UNNAMED',
  '--add-opens=java.base/java.util.concurrent=ALL-UNNAMED',
  '--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED',
  '--add-opens=java.base/java.util.concurrent.locks=ALL-UNNAMED',
];

/** Where the Docker image of the CLI starts, which it requires to exist */
export const WORKSPACE = '/workspace';

/** Where the Docker image of the CLI has the jar of the CLI */
export const IMAGE_JAR = '/opt/sqrl/sqrl-cli.jar';

export type Launcher = 'docker' | 'jar' | 'command';

export interface ServerOptions {
  launcher: Launcher;
  docker: string;
  dockerImage: string;
  jar: string;
  java: string;
  command: string[];
}

export interface ProcessCommand {
  command: string;
  args: string[];
}

export function java(options: ServerOptions, env: NodeJS.ProcessEnv): string {
  if (options.java) {
    return options.java;
  }
  const home = env.JAVA_HOME?.trim();
  return home ? path.join(home, 'bin', 'java') : 'java';
}

/**
 * The directories that are mounted into the Docker container at their own paths, since the editor
 * refers to files by their paths: the roots without the ones inside others.
 */
export function mounts(roots: string[]): string[] {
  const unique = [...new Set(roots.map((root) => path.resolve(root)))];
  return unique.filter(
    (root) => !unique.some((other) => other !== root && isInside(other, root)),
  );
}

function isInside(dir: string, file: string): boolean {
  const relative = path.relative(dir, file);
  return relative !== '' && !relative.startsWith('..') && !path.isAbsolute(relative);
}

/**
 * The command that starts the server. The Docker image starts in its workspace, which it requires,
 * so that is an empty temporary directory: files that checks create at relative paths, e.g. a local
 * Iceberg warehouse, stay out of the projects.
 */
export function server(
  options: ServerOptions,
  roots: string[],
  env: NodeJS.ProcessEnv,
): ProcessCommand {
  switch (options.launcher) {
    case 'command':
      if (options.command.length === 0) {
        throw new Error('Set the command that starts the language server in sqrl.server.command.');
      }
      return { command: options.command[0], args: options.command.slice(1) };
    case 'jar':
      if (!options.jar) {
        throw new Error('Set the path of sqrl-cli.jar in sqrl.server.jar.');
      }
      return { command: java(options, env), args: [...JVM_OPTIONS, '-jar', options.jar, 'lsp'] };
    case 'docker':
      return {
        command: options.docker,
        args: [
          'run',
          '--interactive',
          '--rm',
          '--tmpfs',
          WORKSPACE,
          ...mounts(roots).flatMap((root) => ['--volume', `${root}:${root}`]),
          options.dockerImage,
          'lsp',
        ],
      };
  }
}

/**
 * The command that runs the tests of a project, `sqrl test` with its package files. In Docker, the
 * root is the workspace of the image, so that the packages that the project includes are there too,
 * and the image has the services that tests need, e.g. Kafka and Postgres. With the jar, that jar
 * runs in the image.
 *
 * @param root the directory that contains the project, e.g. its workspace folder
 * @param packageFiles the paths of the package files of the project
 */
export function test(
  options: ServerOptions,
  root: string | undefined,
  packageFiles: string[],
): ProcessCommand & { cwd: string } {
  const projectDir = path.dirname(packageFiles[0]);
  const base =
    root && packageFiles.every((file) => isInside(root, file)) ? path.resolve(root) : projectDir;
  const relative = (dir: string) =>
    packageFiles.map((file) => path.relative(dir, file).split(path.sep).join('/'));
  if (options.launcher === 'command') {
    const command = [...options.command];
    if (command[command.length - 1] === 'lsp') {
      command.pop();
    }
    return {
      command: command[0],
      args: [...command.slice(1), 'test', ...relative(projectDir)],
      cwd: projectDir,
    };
  }
  const args = ['run', '--rm', '--interactive', '--tty', '--volume', `${base}:${WORKSPACE}`];
  if (options.launcher === 'jar' && options.jar) {
    args.push('--volume', `${options.jar}:${IMAGE_JAR}`);
  }
  args.push(options.dockerImage, 'test', ...relative(base));
  return { command: options.docker, args, cwd: base };
}

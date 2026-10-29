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

import * as fs from 'node:fs';
import * as path from 'node:path';

/** `engines` is left out, since the package.json of npm has it as well */
const DATASQRL_SECTIONS = ['script', 'enabled-engines', 'connectors', 'compiler', 'test-runner'];

const NPM_SECTIONS = ['dependencies', 'devDependencies', 'peerDependencies', 'scripts', 'workspaces'];

function hasSection(text: string, sections: string[]): boolean {
  return sections.some((section) => new RegExp(`"${section}"\\s*:`).test(text));
}

export function isPackageName(file: string): boolean {
  const name = path.basename(file).toLowerCase();
  return name.endsWith('.json') && name.includes('package');
}

function hasScripts(dir: string): boolean {
  try {
    return fs.readdirSync(dir).some((name) => name.endsWith('.sqrl'));
  } catch {
    return false;
  }
}

/**
 * Whether a file is a DataSQRL package file: one with sections of DataSQRL, or one next to scripts
 * without sections of npm, e.g. a new one.
 */
export function isDataSqrlPackage(file: string, text: string): boolean {
  if (!isPackageName(file)) {
    return false;
  }
  if (hasSection(text, DATASQRL_SECTIONS)) {
    return true;
  }
  return !hasSection(text, NPM_SECTIONS) && hasScripts(path.dirname(file));
}

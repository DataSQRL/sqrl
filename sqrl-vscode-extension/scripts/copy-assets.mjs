/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// The grammar and package schema are shared with the IntelliJ plugin and the compiler
import { copyFileSync, mkdirSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const extension = join(dirname(fileURLToPath(import.meta.url)), '..');
const repository = join(extension, '..');
const textmate = join(repository, 'sqrl-intellij-plugin/src/main/textmate/sqrl');

const assets = {
  'syntaxes/sqrl.tmLanguage.json': join(textmate, 'syntaxes/sqrl.tmLanguage.json'),
  'language-configuration.json': join(textmate, 'language-configuration.json'),
  'schemas/packageSchema.json': join(
    repository,
    'sqrl-planner/src/main/resources/jsonSchema/packageSchema.json',
  ),
  LICENSE: join(repository, 'LICENSE'),
};

for (const [target, source] of Object.entries(assets)) {
  mkdirSync(dirname(join(extension, target)), { recursive: true });
  copyFileSync(source, join(extension, target));
}

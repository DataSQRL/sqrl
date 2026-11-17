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

// Checks that every docs page and blog post sets a `title` and a `description` in its front matter,
// and that no two docs pages or blog posts share a title. Docusaurus otherwise derives the meta
// description from the first paragraph of the page, which is often a fragment or a heading.
//
// Usage: node scripts/check-front-matter.mjs
// Run it after `npm run generate-docs` so that the generated function pages are checked too.

import fs from 'fs';
import path from 'path';
import {fileURLToPath} from 'url';
import {load as loadYaml} from 'js-yaml';

const ROOT = path.join(path.dirname(fileURLToPath(import.meta.url)), '..');

// The stdlib-docs submodule holds Flink SQL Runner repository files, not doc pages (see the docs
// `exclude` in docusaurus.config.ts)
const SECTIONS = [
  {name: 'docs', dir: path.join(ROOT, 'docs'), skip: ['stdlib-docs']},
  {name: 'blog', dir: path.join(ROOT, 'blog'), skip: []},
];

function* markdownFiles(dir, skip) {
  for (const entry of fs.readdirSync(dir, {withFileTypes: true})) {
    // Docusaurus ignores files and directories starting with an underscore
    if (entry.name.startsWith('_') || skip.includes(entry.name)) continue;
    const file = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      yield* markdownFiles(file, []);
    } else if (/\.mdx?$/.test(entry.name)) {
      yield file;
    }
  }
}

function frontMatter(file) {
  const match = fs.readFileSync(file, 'utf8').match(/^---\r?\n([\s\S]*?)\r?\n---\r?\n/);
  return match ? (loadYaml(match[1]) ?? {}) : null;
}

const errors = [];
for (const section of SECTIONS) {
  const titles = new Map();
  for (const file of markdownFiles(section.dir, section.skip)) {
    const relative = path.relative(ROOT, file);
    const data = frontMatter(file);
    if (data === null) {
      errors.push(`${relative}: missing front matter with a title and a description`);
      continue;
    }
    for (const key of ['title', 'description']) {
      if (typeof data[key] !== 'string' || data[key].trim() === '') {
        errors.push(`${relative}: missing '${key}' in front matter`);
      }
    }
    if (typeof data.title === 'string') {
      const other = titles.get(data.title);
      if (other) {
        errors.push(`${relative}: title "${data.title}" is also used by ${other}`);
      } else {
        titles.set(data.title, relative);
      }
    }
  }
}

if (errors.length > 0) {
  console.error(`Front matter check failed:\n${errors.map((error) => `  ${error}`).join('\n')}`);
  process.exit(1);
}
console.log('Front matter check passed: every docs page and blog post has a unique title and a description.');

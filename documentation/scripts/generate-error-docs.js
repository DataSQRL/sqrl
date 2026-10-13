#!/usr/bin/env node
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

/**
 * Generates the Error Reference page from the error code descriptions the compiler prints.
 * The compiler links each error code to its section of this page (see ErrorPrinter).
 */

const fs = require('fs');
const path = require('path');

const ERROR_CODES_DIR = path.join(
  __dirname,
  '../../sqrl-planner/src/main/resources/com/datasqrl/error/errorCodes',
);
const OUTPUT = path.join(__dirname, '../docs/errors-generated.md');

const header = `---
slug: /errors
title: Error Reference
---

# Error Reference

The DataSQRL compiler reports errors with an error code, e.g. \`[FATAL] [MISSING_TABLE] ...\`, and links to the
matching section below. Each section describes the error, shows an example that causes it, and how to fix it.

Errors without a code come from the underlying engines (e.g. the Flink SQL parser), and their message
describes the problem. Set the \`SQRL_DEBUG\` environment variable (e.g. \`docker run -e SQRL_DEBUG=1 ...\`)
to also print the stack trace of an error.
`;

function main() {
  const sections = fs
    .readdirSync(ERROR_CODES_DIR)
    .filter((file) => file.endsWith('.md'))
    .sort()
    .map((file) => {
      const code = path.basename(file, '.md').toUpperCase();
      const description = fs.readFileSync(path.join(ERROR_CODES_DIR, file), 'utf8').trim();
      return `## ${code}\n\n${description}\n`;
    });

  fs.writeFileSync(OUTPUT, [header, ...sections].join('\n'));
  console.log(`✅ Error reference generated with ${sections.length} error codes`);
}

main();

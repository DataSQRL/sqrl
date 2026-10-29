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
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { describe, it } from 'node:test';
import { isDataSqrlPackage, isPackageName } from '../src/packages';

describe('packages', () => {
  it('givenFileNames_whenIsPackageName_thenMatchesJsonWithPackage', () => {
    assert.equal(isPackageName('/p/orders_test_Package.json'), true);
    assert.equal(isPackageName('/p/package.yaml'), false);
    assert.equal(isPackageName('/p/schema.json'), false);
  });

  it('givenDataSqrlSections_whenIsDataSqrlPackage_thenTrue', () => {
    assert.equal(isDataSqrlPackage('/p/package.json', '{ "script": { "main": "a.sqrl" } }'), true);
  });

  it('givenNpmPackage_whenIsDataSqrlPackage_thenFalse', () => {
    const text = '{ "dependencies": {}, "engines": { "node": ">=22" } }';

    assert.equal(isDataSqrlPackage('/p/package.json', text), false);
  });

  it('givenEmptyPackageNextToScripts_whenIsDataSqrlPackage_thenTrue', () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'sqrl-'));
    fs.writeFileSync(path.join(dir, 'shop.sqrl'), '');

    assert.equal(isDataSqrlPackage(path.join(dir, 'package.json'), '{}'), true);
    assert.equal(isDataSqrlPackage(path.join(os.tmpdir(), 'package.json'), '{}'), false);
  });
});

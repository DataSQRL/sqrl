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

// Generates a resized WebP version of every PNG and JPEG image in static/img into
// static-generated/img (same path, .webp extension), which docusaurus.config.ts serves as a second
// static directory. The originals stay in static/ as the masters, and for social cards (og:image),
// which not every platform accepts as WebP. The remark plugin src/remark/optimized-images.mjs makes
// docs and blog posts reference the WebP versions; React pages reference them directly.
//
// Images are only regenerated when the original is newer than its WebP version.
//
// Usage: node scripts/optimize-images.mjs

import fs from 'fs';
import path from 'path';
import {fileURLToPath} from 'url';
import sharp from 'sharp';

const ROOT = path.join(path.dirname(fileURLToPath(import.meta.url)), '..');
const SOURCE_DIR = path.join(ROOT, 'static');
const OUTPUT_DIR = path.join(ROOT, 'static-generated');
const IMAGE_DIR = 'img';

// Twice the widest content column, so images stay sharp on high-density displays
const MAX_WIDTH = 1600;
// Images displayed much smaller than MAX_WIDTH, keyed by their path under static/
const MAX_WIDTH_OVERRIDES = {
  // Home page, displayed 250px high
  'img/screenshots/open_source_technologies.png': 500,
};
const WEBP_OPTIONS = {quality: 80, effort: 6};
const RASTER = /\.(png|jpe?g)$/i;

export function webpPath(imagePath) {
  return imagePath.replace(RASTER, '.webp');
}

function* rasterImages(dir) {
  for (const entry of fs.readdirSync(dir, {withFileTypes: true})) {
    const file = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      yield* rasterImages(file);
    } else if (RASTER.test(entry.name)) {
      yield file;
    }
  }
}

function* generatedImages(dir) {
  if (!fs.existsSync(dir)) return;
  for (const entry of fs.readdirSync(dir, {withFileTypes: true})) {
    const file = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      yield* generatedImages(file);
    } else {
      yield file;
    }
  }
}

async function optimize(source) {
  const relative = path.relative(SOURCE_DIR, source).split(path.sep).join('/');
  const target = path.join(OUTPUT_DIR, webpPath(relative));
  if (fs.existsSync(target) && fs.statSync(target).mtimeMs >= fs.statSync(source).mtimeMs) {
    return {relative, target, generated: false};
  }
  fs.mkdirSync(path.dirname(target), {recursive: true});
  await sharp(source)
    .rotate()
    .resize({width: MAX_WIDTH_OVERRIDES[relative] ?? MAX_WIDTH, withoutEnlargement: true})
    .webp(WEBP_OPTIONS)
    .toFile(target);
  return {relative, target, generated: true};
}

async function main() {
  const sources = [...rasterImages(path.join(SOURCE_DIR, IMAGE_DIR))];
  const results = await Promise.all(sources.map(optimize));

  // Remove WebP versions whose original was deleted or renamed
  const expected = new Set(results.map((result) => result.target));
  for (const file of generatedImages(OUTPUT_DIR)) {
    if (!expected.has(file)) fs.rmSync(file);
  }

  const generated = results.filter((result) => result.generated);
  const before = generated.reduce((sum, r) => sum + fs.statSync(path.join(SOURCE_DIR, r.relative)).size, 0);
  const after = generated.reduce((sum, r) => sum + fs.statSync(r.target).size, 0);
  const mb = (bytes) => (bytes / 1024 / 1024).toFixed(1);
  console.log(
    generated.length > 0
      ? `Optimized ${generated.length} images to WebP: ${mb(before)} MB -> ${mb(after)} MB`
      : `All ${results.length} WebP images are up to date`,
  );
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  await main();
}

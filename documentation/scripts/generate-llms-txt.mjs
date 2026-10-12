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

// Generates agent-friendly documentation (https://llmstxt.org) for each docs version from the docs
// sources and sidebars, in sidebar order:
//   <version path>/llms.txt       index of the docs pages, linking to their Markdown versions
//   <version path>/llms-full.txt  all docs pages in a single Markdown file
//   <version path>/<page>.md      each docs page as plain Markdown, next to its HTML page
// The root /llms.txt is the one of the default docs version (the latest release, or main when the
// docs are unversioned).
//
// Usage: node scripts/generate-llms-txt.mjs <out-dir> [--current-path <path>]
//   <out-dir>                the Docusaurus build directory, or any directory to write into
//   --current-path <path>    docs path of the current (main) docs, defaults to `main` when the docs
//                            are versioned and to none otherwise (as in docusaurus.config.ts)

import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath, pathToFileURL} from 'node:url';

const SITE_URL = 'https://docs.datasqrl.com';
const ROOT_DIR = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const INTRO = fs.readFileSync(path.join(ROOT_DIR, 'scripts', 'llms-intro.md'), 'utf8').trim();
// Path of the `current` docs version in docusaurus.config.ts when the docs are versioned
const VERSIONED_CURRENT_PATH = 'main';

async function loadVersions(currentPath) {
  const versionsFile = path.join(ROOT_DIR, 'versions.json');
  const releases = fs.existsSync(versionsFile)
    ? JSON.parse(fs.readFileSync(versionsFile, 'utf8'))
    : [];
  const sidebarsUrl = pathToFileURL(path.join(ROOT_DIR, 'sidebars.ts')).href;
  const current = {
    name: 'main',
    docsDir: path.join(ROOT_DIR, 'docs'),
    sidebars: (await import(sidebarsUrl)).default,
    path: currentPath ?? (releases.length > 0 ? VERSIONED_CURRENT_PATH : ''),
  };
  const released = releases.map((name, index) => ({
    name,
    docsDir: path.join(ROOT_DIR, 'versioned_docs', `version-${name}`),
    sidebars: JSON.parse(
      fs.readFileSync(
        path.join(ROOT_DIR, 'versioned_sidebars', `version-${name}-sidebars.json`),
        'utf8',
      ),
    ),
    // The latest release is the default version served at /docs/, the others at their name with
    // dots replaced by dashes (as in docusaurus.config.ts)
    path: index === 0 ? '' : name.replaceAll('.', '-'),
  }));
  // The default version comes first
  return [...released, current].map((version) => ({
    ...version,
    basePath: version.path ? `/docs/${version.path}` : '/docs',
  }));
}

// Sidebar labels start with an emoji
const stripEmoji = (label) => label.replace(/^[\p{Extended_Pictographic}️‍\s]+/u, '');

function docIds(item) {
  if (typeof item === 'string') return [item];
  switch (item.type) {
    case 'doc':
      return [item.id];
    case 'category':
      return [...(item.link?.type === 'doc' ? [item.link.id] : []), ...item.items.flatMap(docIds)];
    case 'link':
    case 'html':
      return [];
    default:
      throw new Error(`Unsupported sidebar item type '${item.type}'`);
  }
}

// One section per top-level sidebar item, containing its docs in sidebar order
function sidebarSections(sidebars) {
  return Object.values(sidebars)
    .flat()
    .map((item) => ({label: typeof item === 'string' ? null : item.label, ids: docIds(item)}))
    .filter((section) => section.ids.length > 0);
}

function loadDoc(version, id, label) {
  const file = ['.md', '.mdx']
    .map((ext) => path.join(version.docsDir, id + ext))
    .find((candidate) => fs.existsSync(candidate));
  if (!file) {
    throw new Error(`Docs page '${id}' of version ${version.name} not found in ${version.docsDir}`);
  }
  const {frontMatter, body} = splitFrontMatter(fs.readFileSync(file, 'utf8'));
  // index pages are served at the path of their directory
  const slug = id === 'index' ? '' : id.replace(/\/index$/, '');
  const pageUrl = `${SITE_URL}${version.basePath}${slug ? `/${slug}` : ''}`;
  return {
    id,
    body,
    title: frontMatter.title ?? heading(body) ?? stripEmoji(label ?? id),
    description: frontMatter.description ?? firstSentence(body),
    pageUrl,
    markdownPath: `${version.basePath}/${slug || 'index'}.md`,
    markdownUrl: `${SITE_URL}${version.basePath}/${slug || 'index'}.md`,
  };
}

function splitFrontMatter(content) {
  const match = content.match(/^---\r?\n([\s\S]*?)\r?\n---\r?\n/);
  if (!match) return {frontMatter: {}, body: content};
  const frontMatter = Object.fromEntries(
    match[1]
      .split('\n')
      .map((line) => line.match(/^(\w+):\s*(.*)$/))
      .filter(Boolean)
      .map(([, key, value]) => [key, value.replace(/^(['"])(.*)\1$/, '$2')]),
  );
  return {frontMatter, body: content.slice(match[0].length)};
}

// Lines of Markdown outside of fenced code blocks
function* proseLines(body) {
  let fence = null;
  for (const line of body.split('\n')) {
    const marker = line.match(/^\s*(`{3,}|~{3,})/)?.[1];
    if (fence) {
      if (marker && marker[0] === fence[0] && marker.length >= fence.length && !line.trim().slice(marker.length)) {
        fence = null;
      }
    } else if (marker) {
      fence = marker;
    } else {
      yield line;
    }
  }
}

function heading(body) {
  for (const line of proseLines(body)) {
    const match = line.match(/^#\s+(.+?)\s*#*\s*$/);
    if (match) return plainText(match[1]);
  }
  return null;
}

// The first sentence of the first paragraph after the title
function firstSentence(body) {
  const paragraph = [];
  for (const line of proseLines(body)) {
    const text = line.trim();
    if (!text || /^(#|<|:::|!\[|\||>|import\s|export\s|---)/.test(text)) {
      if (paragraph.length > 0) break;
      continue;
    }
    paragraph.push(text);
  }
  if (paragraph.length === 0) return null;
  const text = plainText(paragraph.join(' '));
  const sentence = (text.match(/^.+?[.!?](?=\s|$)/)?.[0] ?? text).replace(/:$/, '');
  return sentence.length > 200 ? `${sentence.slice(0, 197).trimEnd()}...` : sentence;
}

const plainText = (markdown) =>
  markdown
    .replace(/!?\[([^\]]*)\]\([^)]*\)/g, '$1')
    .replace(/(\*\*|__)(.+?)\1/g, '$2')
    .replace(/<[^>]+>/g, '')
    .trim();

// Converts a docs page to plain Markdown: removes MDX syntax, turns admonitions into block quotes
// and HTML images into Markdown images, and makes all links absolute, pointing to the Markdown
// versions of the docs pages
function toMarkdown(doc, version, docsById) {
  const out = [];
  let fence = null;
  let inAdmonition = false;
  const emit = (line) => out.push(inAdmonition ? `> ${line}`.trimEnd() : line);
  for (const line of doc.body.split('\n')) {
    const marker = line.match(/^\s*(`{3,}|~{3,})/)?.[1];
    if (fence) {
      if (marker && marker[0] === fence[0] && marker.length >= fence.length && !line.trim().slice(marker.length)) {
        fence = null;
      }
      emit(line);
      continue;
    }
    if (marker) {
      fence = marker;
      emit(line);
      continue;
    }
    const admonition = line.match(/^\s*:::(\w+)(?:\s+(.*?))?\s*$/);
    if (admonition && !inAdmonition) {
      const type = admonition[1][0].toUpperCase() + admonition[1].slice(1);
      inAdmonition = true;
      emit(`**${admonition[2] ? `${type}: ${admonition[2]}` : type}**`);
      emit('');
      continue;
    }
    if (inAdmonition && /^\s*:::\s*$/.test(line)) {
      inAdmonition = false;
      continue;
    }
    // MDX imports and exports
    if (/^(import\s.*\sfrom\s|import\s+['"]|export\s+(default|const|let|function)\b)/.test(line)) {
      continue;
    }
    // Tabs: keep the content of every tab, introduced by its label
    if (/^\s*(<Tabs\b[^>]*>|<\/Tabs>|<\/TabItem>)\s*$/.test(line)) {
      continue;
    }
    const tabItem = line.match(/^\s*<TabItem\b([^>]*)>\s*$/);
    if (tabItem) {
      const attribute = (name) => tabItem[1].match(new RegExp(`\\s${name}=["']([^"']*)["']`))?.[1];
      emit(`**${attribute('label') ?? attribute('value')}:**`);
      continue;
    }
    emit(transformInline(line, doc, version, docsById));
  }
  return `${out.join('\n').replace(/<!--[\s\S]*?-->/g, '').replace(/\n{3,}/g, '\n\n').trim()}\n`;
}

function transformInline(line, doc, version, docsById) {
  // Leave inline code untouched
  const code = [];
  const protectedLine = line.replace(/`[^`]*`/g, (span) => `\u0000${code.push(span) - 1}\u0000`);
  const transformed = protectedLine
    .replace(/\{\/\*.*?\*\/\}/g, '')
    .replace(/<img\s[^>]*>/gi, (tag) => {
      const src = tag.match(/\ssrc=["']([^"']+)["']/i)?.[1] ?? '';
      const alt = tag.match(/\salt=["']([^"']*)["']/i)?.[1] ?? '';
      return `![${alt}](${resolveUrl(src, doc)})`;
    })
    .replace(/(!?)\[([^\]]*)\]\(([^)\s]+)((?:\s+"[^"]*")?)\)/g, (_, bang, text, target, title) => {
      const url = bang ? resolveUrl(target, doc) : resolveLink(target, doc, version, docsById);
      return `${bang}[${text}](${url}${title})`;
    });
  // Fail on MDX components that are not converted, rather than publishing them as Markdown
  const component = transformed.match(/<\/?[A-Z][\w.]*(\s[^>]*)?\/?>/);
  if (component) {
    throw new Error(`Unsupported MDX component ${component[0]} in docs page '${doc.id}'`);
  }
  return transformed.replace(/\u0000(\d+)\u0000/g, (_, index) => code[Number(index)]);
}

const isAbsoluteUrl = (target) => /^[a-z][a-z0-9+.-]*:/i.test(target) || target.startsWith('#');

function resolveUrl(target, doc) {
  if (isAbsoluteUrl(target)) return target;
  return target.startsWith('/') ? SITE_URL + target : new URL(target, doc.pageUrl).href;
}

function resolveLink(target, doc, version, docsById) {
  if (isAbsoluteUrl(target)) return target;
  const hashIndex = target.indexOf('#');
  const [linkPath, hash] =
    hashIndex < 0 ? [target, ''] : [target.slice(0, hashIndex), target.slice(hashIndex)];
  const lookup = (candidate) => {
    const id = candidate.replace(/\/$/, '').replace(/\.mdx?$/, '');
    return docsById.get(id) ?? docsById.get(`${id}/index`);
  };
  // Links to /docs/ point to the same docs version, relative links are resolved against the file
  const linked = linkPath.startsWith('/')
    ? linkPath.startsWith('/docs/') && lookup(linkPath.slice('/docs/'.length))
    : lookup(path.posix.normalize(path.posix.join(path.posix.dirname(doc.id), linkPath)));
  return linked ? linked.markdownUrl + hash : resolveUrl(target, doc);
}

function versionNotice(version, versions) {
  const latest = `The documentation of the latest release is at ${SITE_URL}/llms.txt.`;
  if (version.name === 'main') {
    // The current docs are those of main on the site, but of a release when built from its branch
    if (version.path === VERSIONED_CURRENT_PATH) {
      return `This is the documentation of the unreleased \`main\` branch of DataSQRL. ${latest}`;
    }
    return version.path
      ? `This is the documentation of DataSQRL ${version.path}. ${latest}`
      : 'This is the documentation of the latest DataSQRL release.';
  }
  return version === versions[0]
    ? `This is the documentation of DataSQRL ${version.name}, the latest release.`
    : `This is the documentation of DataSQRL ${version.name}, an older release. ${latest}`;
}

function llmsTxt(version, versions, sections) {
  const lines = ['# DataSQRL', '', INTRO, '', versionNotice(version, versions), ''];
  for (const section of sections) {
    lines.push(`## ${stripEmoji(section.label ?? section.docs[0].title)}`, '');
    for (const doc of section.docs) {
      lines.push(`- [${doc.title}](${doc.markdownUrl})${doc.description ? `: ${doc.description}` : ''}`);
    }
    lines.push('');
  }
  lines.push(
    '## Optional',
    '',
    `- [Full documentation](${SITE_URL}${version.basePath}/llms-full.txt): all of the above pages in a single file`,
    `- [Blog](${SITE_URL}/blog): articles on the design of DataSQRL and its harness for data engineering agents`,
  );
  for (const other of versions.filter((other) => other !== version)) {
    const label = other.name === 'main' ? 'Unreleased `main` documentation' : `DataSQRL ${other.name} documentation`;
    lines.push(`- [${label}](${SITE_URL}${other.basePath}/llms.txt)`);
  }
  return `${lines.join('\n')}\n`;
}

function llmsFullTxt(version, versions, docs, markdownById) {
  const pages = docs.map((doc) => {
    // State the source of each page right below its title
    const markdown = markdownById.get(doc.id);
    const title = markdown.match(/^#\s.*$/m);
    return title && title.index === 0
      ? `${title[0]}\n\nSource: ${doc.pageUrl}${markdown.slice(title[0].length)}`
      : `# ${doc.title}\n\nSource: ${doc.pageUrl}\n\n${markdown}`;
  });
  const header = ['# DataSQRL Documentation', '', INTRO, '', versionNotice(version, versions)].join('\n');
  return `${[header, ...pages].join('\n\n---\n\n').trimEnd()}\n`;
}

function write(outDir, relativePath, content) {
  const file = path.join(outDir, relativePath);
  fs.mkdirSync(path.dirname(file), {recursive: true});
  fs.writeFileSync(file, content);
}

async function main() {
  const args = process.argv.slice(2);
  const currentPathIndex = args.indexOf('--current-path');
  const currentPath = currentPathIndex < 0 ? undefined : args.splice(currentPathIndex, 2)[1];
  if (args.length !== 1 || currentPath === undefined && currentPathIndex >= 0) {
    console.error('Usage: node scripts/generate-llms-txt.mjs <out-dir> [--current-path <path>]');
    process.exit(1);
  }
  const outDir = path.resolve(args[0]);
  const versions = await loadVersions(currentPath);

  for (const version of versions) {
    const sections = sidebarSections(version.sidebars).map((section) => ({
      label: section.label,
      docs: section.ids.map((id) => loadDoc(version, id, section.label)),
    }));
    const docs = sections.flatMap((section) => section.docs);
    const docsById = new Map(docs.map((doc) => [doc.id, doc]));
    const markdownById = new Map(docs.map((doc) => [doc.id, toMarkdown(doc, version, docsById)]));

    for (const doc of docs) write(outDir, doc.markdownPath, markdownById.get(doc.id));
    const index = llmsTxt(version, versions, sections);
    write(outDir, `${version.basePath}/llms.txt`, index);
    write(outDir, `${version.basePath}/llms-full.txt`, llmsFullTxt(version, versions, docs, markdownById));
    if (version === versions[0]) write(outDir, 'llms.txt', index);
    console.log(`Generated llms.txt for docs version ${version.name} at ${version.basePath} (${docs.length} pages)`);
  }
}

await main();

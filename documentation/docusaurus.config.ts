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

import fs from 'fs';
import path from 'path';
import {themes as prismThemes} from 'prism-react-renderer';
import type {Config} from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';

// This runs in Node.js - Don't use client-side code here (browser APIs, JSX...)

// Versioning: only the docs are versioned. `scripts/prepare-versioned-docs.sh` snapshots the docs of
// the latest release branch of each major version into versioned_docs/ and lists them in
// versions.json (newest first), or of each minor version before 1.0 (e.g. `v0.11`). The latest
// release is the default version at /docs/, `main` is served at /docs/main/ and older releases at
// /docs/vX/ (dots replaced by dashes, e.g. /docs/v0-11/). Without versions.json (e.g. for local
// development and PR checks), the docs are unversioned.
const versionsFile = path.join(__dirname, 'versions.json');
const releaseVersions: string[] = fs.existsSync(versionsFile)
  ? JSON.parse(fs.readFileSync(versionsFile, 'utf8'))
  : [];
const docsVersioning =
  releaseVersions.length > 0
    ? {
        lastVersion: releaseVersions[0],
        versions: {
          current: {label: 'main', path: 'main', banner: 'unreleased' as const},
          ...Object.fromEntries(
            releaseVersions.map((version, index) => [
              version,
              index === 0
                ? {label: version, banner: 'none' as const}
                : {label: version, path: version.replaceAll('.', '-'), banner: 'unmaintained' as const},
            ]),
          ),
        },
      }
    : {};

const config: Config = {
  title: 'DataSQRL',
  tagline: 'Data Engineering Harness',
  favicon: 'img/favicon.ico',

  // Set the production url of your site here
  url: 'https://docs.datasqrl.com',
  // Set the /<baseUrl>/ pathname under which your site is served
  // For GitHub pages deployment, it is often '/<projectName>/'
  baseUrl: '/',

  // GitHub pages deployment config.
  // If you aren't using GitHub pages, you don't need these.
  organizationName: 'datasqrl', // Usually your GitHub org/user name.
  projectName: 'sqrl', // Usually your repo name.

  onBrokenLinks: 'throw',
  markdown: {
    mermaid: true,
    hooks: {
      onBrokenMarkdownLinks: 'warn',
    },
  },

  // Even if you don't use internationalization, you can use this field to set
  // useful metadata like html lang. For example, if your site is Chinese, you
  // may want to replace "en" with "zh-Hans".
  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  presets: [
    [
      'classic',
      {
        docs: {
          sidebarPath: './sidebars.ts',
          // Docusaurus defaults, plus the root files of the stdlib-docs submodule (README, CLAUDE,
          // CONTRIBUTING, etc.), which are flink-sql-runner repository files rather than doc pages
          exclude: [
            '**/_*.{js,jsx,ts,tsx,md,mdx}',
            '**/_*/**',
            '**/*.test.{js,jsx,ts,tsx}',
            '**/__tests__/**',
            'stdlib-docs/*.md',
          ],
          ...docsVersioning,
        },
        blog: {
          showReadingTime: true,
          feedOptions: {
            type: ['rss', 'atom'],
            xslt: true,
          },
          // Useful options to enforce blogging best practices
          onInlineTags: 'warn',
          onInlineAuthors: 'warn',
          onUntruncatedBlogPosts: 'warn',
        },
        theme: {
          customCss: './src/css/custom.css',
        },
        gtag: {
          trackingID: 'G-Y4XLW4QZYX',
          anonymizeIP: false,
        },
      } satisfies Preset.Options,
    ],
  ],

  themes: [
    [
      "@easyops-cn/docusaurus-search-local",
      /** @type {import("@easyops-cn/docusaurus-search-local").PluginOptions} */
      ({
        hashed: true,
        language: ["en"],
        highlightSearchTermsOnTargetPage: true,
        explicitSearchResultPath: true,
        indexPages: true,
        searchResultLimits: 10,
        searchResultContextMaxLength: 50
      }),
    ],
    '@docusaurus/theme-mermaid',
  ],

  themeConfig: {
    // Replace with your project's social card
    image: 'img/datasqrl-social-card.jpg',
    navbar: {
      title: 'DataSQRL',
      logo: {
        alt: 'DataSQRL Logo',
        src: 'img/logo.svg',
      },
      items: [
        {
          type: 'docSidebar',
          sidebarId: 'tutorialSidebar',
          position: 'left',
          label: 'Documentation',
        },
        {to: '/blog', label: 'Blog', position: 'left'},
        {to: '/community', label: 'Community', position: 'left'},
        {
          href: 'https://github.com/DataSQRL/sqrl',
          label: 'GitHub',
          position: 'right',
        },
      ],
    },
    footer: {
      style: 'dark',
      links: [
        {
          title: 'Docs',
          items: [
            {
              label: 'Getting Started',
              to: '/docs/intro/getting-started',
            },
            {
              label: 'User Documentation',
              to: '/docs/intro',
            }
          ],
        },
        {
          title: 'Community',
          items: [
            {
              label: 'GitHub',
              href: 'https://github.com/DataSQRL/sqrl/discussions',
            },
            {
              label: 'Blog',
              href: '/blog',
            }
          ],
        },
        {
          title: 'More',
          items: [
            {
              label: 'Implementation',
              href: 'https://github.com/DataSQRL/sqrl',
            },
            {
              label: 'File an Issue',
              href: 'https://github.com/DataSQRL/sqrl/issues/new',
            },
          ],
        },
      ],
      copyright: `Copyright © ${new Date().getFullYear()} DataSQRL, Inc.`,
    },
    prism: {
      theme: prismThemes.github,
      darkTheme: prismThemes.dracula,
    },
    colorMode: {
      defaultMode: 'dark',
      disableSwitch: false,
      respectPrefersColorScheme: false,
    },
    metadata: [
      {name: 'keywords', content: 'data engineering harness, data engineering agent, coding agent, SQRL, DataSQRL, data pipeline, data product, data API, streaming, real-time analytics'},
      {name: 'description', content: 'DataSQRL is an open-source data engineering harness for building data engineering agents designed around human control, correctness, and safety.'},
      {name: 'twitter:card', content: 'summary'},
      {name: 'twitter:site', content: '@DataSQRL'}
    ],
  } satisfies Preset.ThemeConfig,
};

export default config;

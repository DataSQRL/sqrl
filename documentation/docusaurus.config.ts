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

import {themes as prismThemes} from 'prism-react-renderer';
import type {Config} from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';

// This runs in Node.js - Don't use client-side code here (browser APIs, JSX...)

const siteUrl = 'https://docs.datasqrl.com';

// Versioning: the latest major version is served at the root, `main` under `/main/` and older major
// versions under `/vX/`. Release versions are built from the latest `release-X.Y` branch of their
// major. These variables are set by `scripts/build-versioned-site.sh`; without them a plain
// unversioned site is built (e.g. for local development and PR checks).
const baseUrl = process.env.DOCS_BASE_URL ?? '/';
const versionLabel = process.env.DOCS_VERSION_LABEL;
// One of `latest`, `main` or `older`
const versionKind = process.env.DOCS_VERSION_KIND;
const latestLabel = process.env.DOCS_LATEST_LABEL;
const versions: {label: string; path: string}[] = JSON.parse(process.env.DOCS_VERSIONS ?? '[]');
// The blog is published with the version served at the root, the other versions link to it
const blogLink = baseUrl === '/' ? {to: '/blog'} : {href: `${siteUrl}/blog`, target: '_self'};

const latestLink = `<a href="/">Go to the latest release (${latestLabel})</a>.`;
const announcementContent = {
  main: `You are viewing the documentation of the unreleased <code>main</code> branch, which may describe features that are not released yet. ${latestLink}`,
  older: `You are viewing the documentation of DataSQRL ${versionLabel}, which is not the latest release. ${latestLink}`,
}[versionKind ?? ''];

const versionDropdown =
  versions.length > 1
    ? [
        {
          type: 'dropdown',
          label: versionLabel ?? 'Version',
          position: 'right',
          items: versions.map((v) => ({
            // Raw HTML links, so the paths are not prefixed with this build's baseUrl
            type: 'html',
            value: `<a class="dropdown__link" href="${v.path}">${v.label}</a>`,
          })),
        },
      ]
    : [];

const config: Config = {
  title: 'DataSQRL',
  tagline: 'Data Engineering Harness',
  favicon: 'img/favicon.ico',

  // Set the production url of your site here
  url: siteUrl,
  // Set the /<baseUrl>/ pathname under which your site is served
  baseUrl,

  // GitHub pages deployment config.
  // If you aren't using GitHub pages, you don't need these.
  organizationName: 'datasqrl', // Usually your GitHub org/user name.
  projectName: 'sqrl', // Usually your repo name.

  onBrokenLinks: 'warn',
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
          // Docusaurus defaults, plus the stdlib-docs submodule README whose relative links
          // point into the flink-sql-runner repository rather than to doc pages
          exclude: [
            '**/_*.{js,jsx,ts,tsx,md,mdx}',
            '**/_*/**',
            '**/*.test.{js,jsx,ts,tsx}',
            '**/__tests__/**',
            'stdlib-docs/README.md',
          ],
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
        {...blogLink, label: 'Releases & Updates', position: 'left'},
        {to: '/community', label: 'Community', position: 'left'},
        ...versionDropdown,
        {
          href: 'https://github.com/DataSQRL/sqrl',
          label: 'GitHub',
          position: 'right',
        },
      ],
    },
    ...(announcementContent && {
      announcementBar: {
        id: `version-${versionLabel}`,
        content: announcementContent,
        // Theme aware colors, defined in custom.css
        backgroundColor: 'var(--docs-version-banner-background)',
        textColor: 'var(--docs-version-banner-color)',
        isCloseable: false,
      },
    }),
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
              ...blogLink,
              label: 'Updates',
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

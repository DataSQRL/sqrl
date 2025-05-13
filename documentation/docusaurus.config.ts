import {themes as prismThemes} from 'prism-react-renderer';
import type {Config} from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';

// This runs in Node.js - Don't use client-side code here (browser APIs, JSX...)

const config: Config = {
  title: 'DataSQRL',
  tagline: 'Data Streaming Framework',
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

  onBrokenLinks: 'warn',
  onBrokenMarkdownLinks: 'warn',

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
        {to: '/blog', label: 'Releases & Updates', position: 'left'},
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
              to: '/docs/getting-started',
            },
            {
              label: 'User Documentation',
              to: '/docs/intro',
            },
            {
              label: 'Developer Documentation',
              to: '/docs/developer',
            },
          ],
        },
        {
          title: 'Community',
          items: [
            {
              label: 'GitHub',
              href: 'https://github.com/DataSQRL/sqrl',
            },
            {
              label: 'Slack',
              href: 'https://join.slack.com/t/datasqrlcommunity/shared_invite/zt-2l3rl1g6o-im6YXYCqU7t55CNaHqz_Kg',
            },
            {
              label: 'Updates',
              href: '/blog',
            }
          ],
        },
        {
          title: 'More',
          items: [
            {
              label: 'Talk to Us',
              to: 'https://join.slack.com/t/datasqrlcommunity/shared_invite/zt-2l3rl1g6o-im6YXYCqU7t55CNaHqz_Kg',
            },
            {
              label: 'Support',
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
      respectPrefersColorScheme: true,
    },
    metadata: [
      {name: 'keywords', content: 'data, API, SQRL, DataSQRL, data product, data pipeline, database, streaming, real-time analytics'},
      {name: 'description', content: 'DataSQRL is a compiler for building robust streaming data pipelines in minutes.'},
      {name: 'twitter:card', content: 'summary'},
      {name: 'twitter:site', content: '@DataSQRL'}
    ],
  } satisfies Preset.ThemeConfig,
};

export default config;

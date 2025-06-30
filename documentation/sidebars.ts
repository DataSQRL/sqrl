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

import type {SidebarsConfig} from '@docusaurus/plugin-content-docs';

// This runs in Node.js - Don't use client-side code here (browser APIs, JSX...)

/**
 * Creating a sidebar enables you to:
 - create an ordered group of docs
 - render a sidebar for each doc of that group
 - provide next/previous navigation

 The sidebars can be generated from the filesystem, or explicitly defined here.

 Create as many sidebars as you want.
 */

const sidebars = {
  tutorialSidebar: [
    {
      type: 'doc',
      id: 'intro',
      label: '📖 Overview',
    },
    {
      type: 'doc',
      id: 'getting-started',
      label: '🚀 Getting Started',
    },
    {
      type: 'category',
      label: '🧱 Core Concepts',
      collapsed: false,
      items: [
        {
          type: 'doc',
          id: 'sqrl-language',
          label: '📚 SQRL Language',
        },
        {
          type: 'doc',
          id: 'compiler',
          label: '🛠️ Compiler',
        },
        {
          type: 'doc',
          id: 'connectors',
          label: '🔌 Source & Sink Connectors',
        },
        {
          type: 'doc',
          id: 'configuration',
          label: '⚙️ Configuration',
        },
        {
          type: 'category',
          label: '🔢 Functions',
          link: {
            type: 'doc',
            id: 'functions',
          },
          items: [
            {
              type: 'doc',
              id: 'stdlib-docs/stdlib-docs/system-functions',
              label: 'System Functions',
            },
            {
              type: 'doc',
              id: 'stdlib-docs/stdlib-docs/library-functions',
              label: 'Library Functions',
            },
          ],
        },
        {
          type: 'doc',
          id: 'concepts',
          label: '🧠 Streaming Concepts',
        }
      ],
    },
    {
      type: 'category',
      label: '📘 How To',
      items: [
        {
          type: 'doc',
          id: 'tutorials',
          label: '🎓 Tutorials',
        },
        {
          type: 'doc',
          id: 'howto',
          label: '🧩 How To Guides',
        },
      ],
    },
    {
      type: 'category',
      label: '🧪 Advanced',
      items: [
        {
          type: 'doc',
          id: 'deepdive',
          label: '👩‍💻 How DataSQRL Works',
        },
        {
          type: 'doc',
          id: 'compatibility',
          label: '🔄 Compatibility',
        },
      ],
    },
  ],
};

module.exports = sidebars;

export default sidebars;

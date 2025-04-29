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

const sidebars: SidebarsConfig = {
  tutorialSidebar: [
    {
      type: 'doc',
      id: 'getting-started',
      label: '🚀 Getting Started',
    },
    {
      type: 'doc',
      id: 'intro',
      label: '📖 Introduction',
    },
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
      id: 'configuration',
      label: '⚙️ Configuration',
    },
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
    {
      type: 'doc',
      id: 'connectors',
      label: '🔌 Source & Sink Connectors',
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
          id: 'functions-docs/function-docs/system-functions',
          label: 'System Functions',
        },
        {
          type: 'doc',
          id: 'functions-docs/function-docs/library-functions',
          label: 'Library Functions',
        },
      ],
    },
    {
      type: 'doc',
      id: 'concepts',
      label: '🧠 Streaming Concepts',
    },
    {
      type: 'doc',
      id: 'developer',
      label: '👩‍💻 Developer Guide',
    },
  ],
};

export default sidebars;

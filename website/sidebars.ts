import type {SidebarsConfig} from '@docusaurus/plugin-content-docs';

const sidebars: SidebarsConfig = {
  docsSidebar: [
    {
      type: 'doc',
      id: 'intro',
      label: 'Introduction',
    },
    {
      type: 'category',
      label: 'Getting Started',
      collapsed: false,
      items: [
        'getting-started/installation',
        'getting-started/quick-start',
        'getting-started/loading-data',
      ],
    },
    {
      type: 'category',
      label: 'Core Concepts',
      items: [
        'concepts/architecture',
        'concepts/connection-api',
        'concepts/query-execution',
      ],
    },
    {
      type: 'category',
      label: 'Guides',
      items: [
        'guides/working-with-files',
        'guides/window-functions',
        'guides/prepared-statements',
        'guides/python-integration',
        'guides/memory-management',
        'guides/production-best-practices',
      ],
    },
    {
      type: 'category',
      label: 'Reference',
      items: [
        'reference/sql',
        'reference/api',
        'reference/configuration',
        'reference/error-handling',
      ],
    },
    {
      type: 'category',
      label: 'Advanced',
      items: [
        'advanced/custom-storage-providers',
        'advanced/query-optimization',
        'advanced/extending-blaze',
      ],
    },
    {
      type: 'doc',
      id: 'comparison',
      label: 'Why Blaze?',
    },
    {
      type: 'doc',
      id: 'benchmarks',
      label: 'Benchmarks',
    },
    {
      type: 'doc',
      id: 'limitations',
      label: 'Limitations',
    },
    {
      type: 'doc',
      id: 'faq',
      label: 'FAQ',
    },
    {
      type: 'doc',
      id: 'troubleshooting',
      label: 'Troubleshooting',
    },
    {
      type: 'doc',
      id: 'contributing',
      label: 'Contributing',
    },
    {
      type: 'doc',
      id: 'changelog',
      label: 'Changelog',
    },
  ],
};

export default sidebars;

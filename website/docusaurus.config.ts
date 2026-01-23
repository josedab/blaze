import {themes as prismThemes} from 'prism-react-renderer';
import type {Config} from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';

const config: Config = {
  markdown: {
    mermaid: true,
  },
  title: 'Blaze',
  tagline: 'High-performance embedded OLAP query engine for Rust',
  favicon: 'img/logo.svg',

  future: {
    v4: true,
  },

  url: 'https://blaze-db.github.io',
  baseUrl: '/blaze/',

  organizationName: 'blaze-db',
  projectName: 'blaze',

  onBrokenLinks: 'throw',
  onBrokenMarkdownLinks: 'warn',

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
          editUrl: 'https://github.com/blaze-db/blaze/tree/main/website/',
          showLastUpdateTime: true,
        },
        blog: false,
        theme: {
          customCss: './src/css/custom.css',
        },
      } satisfies Preset.Options,
    ],
  ],

  themes: [
    '@docusaurus/theme-mermaid',
    [
      require.resolve('@easyops-cn/docusaurus-search-local'),
      {
        hashed: true,
        language: ['en'],
        highlightSearchTermsOnTargetPage: true,
        explicitSearchResultPath: true,
      },
    ],
  ],

  themeConfig: {
    image: 'img/blaze-social-card.svg',
    colorMode: {
      defaultMode: 'dark',
      disableSwitch: false,
      respectPrefersColorScheme: true,
    },
    announcementBar: {
      id: 'star_us',
      content:
        'If you like Blaze, give us a <a target="_blank" rel="noopener noreferrer" href="https://github.com/blaze-db/blaze">star on GitHub</a>!',
      backgroundColor: '#ff6b35',
      textColor: '#ffffff',
      isCloseable: true,
    },
    navbar: {
      title: 'Blaze',
      logo: {
        alt: 'Blaze Logo',
        src: 'img/logo.svg',
      },
      items: [
        {
          type: 'docSidebar',
          sidebarId: 'docsSidebar',
          position: 'left',
          label: 'Documentation',
        },
        {
          to: '/docs/reference/sql',
          label: 'SQL Reference',
          position: 'left',
        },
        {
          to: '/docs/reference/api',
          label: 'API',
          position: 'left',
        },
        {
          href: 'https://crates.io/crates/blaze',
          label: 'Crates.io',
          position: 'right',
        },
        {
          href: 'https://github.com/blaze-db/blaze',
          label: 'GitHub',
          position: 'right',
        },
      ],
    },
    footer: {
      style: 'dark',
      links: [
        {
          title: 'Learn',
          items: [
            {
              label: 'Getting Started',
              to: '/docs/getting-started/installation',
            },
            {
              label: 'Core Concepts',
              to: '/docs/concepts/architecture',
            },
            {
              label: 'Guides',
              to: '/docs/guides/working-with-files',
            },
          ],
        },
        {
          title: 'Reference',
          items: [
            {
              label: 'SQL Reference',
              to: '/docs/reference/sql',
            },
            {
              label: 'API Reference',
              to: '/docs/reference/api',
            },
            {
              label: 'Configuration',
              to: '/docs/reference/configuration',
            },
          ],
        },
        {
          title: 'Community',
          items: [
            {
              label: 'GitHub',
              href: 'https://github.com/blaze-db/blaze',
            },
            {
              label: 'GitHub Discussions',
              href: 'https://github.com/blaze-db/blaze/discussions',
            },
            {
              label: 'Issues',
              href: 'https://github.com/blaze-db/blaze/issues',
            },
          ],
        },
        {
          title: 'More',
          items: [
            {
              label: 'Changelog',
              to: '/docs/changelog',
            },
            {
              label: 'Contributing',
              to: '/docs/contributing',
            },
            {
              label: 'Crates.io',
              href: 'https://crates.io/crates/blaze',
            },
          ],
        },
      ],
      copyright: `Copyright © ${new Date().getFullYear()} Blaze Contributors. Built with Docusaurus.`,
    },
    prism: {
      theme: prismThemes.github,
      darkTheme: prismThemes.dracula,
      additionalLanguages: ['rust', 'toml', 'bash', 'sql', 'python'],
    },
  } satisfies Preset.ThemeConfig,
};

export default config;

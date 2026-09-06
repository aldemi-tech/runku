import {themes as prismThemes} from "prism-react-renderer";
import type {Config} from "@docusaurus/types";
import type * as Preset from "@docusaurus/preset-classic";

const siteUrl = process.env.RUNKU_DOCS_URL ?? "http://localhost";
const baseUrl = process.env.RUNKU_DOCS_BASE_URL ?? "/";

const config: Config = {
  title: "Runku Documentation",
  tagline: "Build and operate Runku Self-Hosted",
  favicon: "brand/runku-favicon-32.png",
  url: siteUrl,
  baseUrl,
  organizationName: "aldemi-tech",
  projectName: "runku",
  trailingSlash: false,
  onBrokenLinks: "throw",
  future: {
    v4: true,
  },
  i18n: {
    defaultLocale: "en",
    locales: ["en"],
  },
  markdown: {
    mermaid: true,
  },
  themes: ["@docusaurus/theme-mermaid"],
  staticDirectories: ["../docs/assets"],
  presets: [
    [
      "classic",
      {
        docs: {
          path: "..",
          routeBasePath: "/",
          sidebarPath: "./sidebars.ts",
          include: [
            "docs/**/*.md",
            "deployments/docker/README.md"
          ],
          exclude: [
            "docs/brand.md",
            "docs/internals/**",
            "docs/maintainers/**",
            "docs/development/evolving-runku.md"
          ],
          editUrl: ({docPath}) =>
            `https://github.com/aldemi-tech/runku/edit/main/${docPath}`,
          showLastUpdateAuthor: true,
          showLastUpdateTime: true,
        },
        blog: false,
        theme: {
          customCss: "./src/css/custom.css",
        },
        sitemap: {
          changefreq: "weekly",
          priority: 0.5,
        },
      } satisfies Preset.Options,
    ],
  ],
  themeConfig: {
    image: "brand/runku-logo-horizontal.png",
    metadata: [
      {
        name: "description",
        content:
          "Authoritative developer and operator documentation for Runku Self-Hosted.",
      },
    ],
    colorMode: {
      respectPrefersColorScheme: true,
    },
    navbar: {
      title: "Runku",
      hideOnScroll: true,
      logo: {
        alt: "Runku",
        src: "brand/runku-mark.png",
      },
      items: [
        {
          type: "docSidebar",
          sidebarId: "docsSidebar",
          position: "left",
          label: "Documentation",
        },
        {
          to: "/docs/self-hosting/overview",
          label: "Self-host",
          position: "left",
        },
        {
          to: "/docs/functions/schema-and-types",
          label: "Build apps",
          position: "left",
        },
        {
          to: "/docs/cli/overview",
          label: "CLI",
          position: "left",
        },
        {
          to: "/docs/operations/operator-handbook",
          label: "Operate",
          position: "left",
        },
        {
          to: "/docs/getting-started/saas-validation",
          label: "Validate in SaaS",
          position: "right",
        },
        {
          href: "https://github.com/aldemi-tech/runku",
          label: "GitHub",
          position: "right",
        },
      ],
    },
    footer: {
      style: "dark",
      links: [
        {
          title: "Build",
          items: [
            {label: "Application tutorial", to: "/docs/getting-started/application-tutorial"},
            {label: "Schema and types", to: "/docs/functions/schema-and-types"},
            {label: "Function API", to: "/docs/reference/function-api"},
            {label: "TypeScript client", to: "/docs/reference/typescript-client"},
          ],
        },
        {
          title: "Operate",
          items: [
            {label: "Self-hosting", to: "/docs/self-hosting/overview"},
            {label: "Operator handbook", to: "/docs/operations/operator-handbook"},
            {label: "Storage configuration", to: "/docs/self-hosting/storage-configuration"},
            {label: "Security model", to: "/docs/security/security-model"},
          ],
        },
        {
          title: "Reference",
          items: [
            {label: "CLI", to: "/docs/reference/cli"},
            {label: "HTTP without SDK", to: "/docs/reference/public-api"},
            {label: "Management API", to: "/docs/reference/management-api"},
            {label: "Compatibility", to: "/docs/reference/compatibility"},
          ],
        },
        {
          title: "Project",
          items: [
            {label: "GitHub", href: "https://github.com/aldemi-tech/runku"},
            {label: "Support status", to: "/docs/concepts/capability-status"},
            {label: "Validate in SaaS", to: "/docs/getting-started/saas-validation"},
          ],
        },
      ],
      copyright:
        "Copyright © Aldemi. Runku is available under the Apache License 2.0.",
    },
    prism: {
      theme: prismThemes.github,
      darkTheme: prismThemes.dracula,
      additionalLanguages: ["bash", "json", "toml"],
    },
  } satisfies Preset.ThemeConfig,
};

export default config;

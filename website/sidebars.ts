import type {SidebarsConfig} from "@docusaurus/plugin-content-docs";

const sidebars: SidebarsConfig = {
  docsSidebar: [
    "docs/README",
    {
      type: "category",
      label: "Start building",
      collapsed: false,
      items: [
        "docs/getting-started/application-tutorial",
        "docs/getting-started/local-development",
        "docs/getting-started/saas-validation",
      ],
    },
    {
      type: "category",
      label: "Build applications",
      collapsed: false,
      items: [
        "docs/functions/schema-and-types",
        "docs/functions/query-mutation-action",
        "docs/reference/function-api",
        "docs/data/documents-and-indexes",
        "docs/data/data-and-realtime",
        "docs/functions/functions-and-runtimes",
        "docs/concepts/environment-configuration",
      ],
    },
    {
      type: "category",
      label: "Call your application",
      items: [
        "docs/reference/typescript-client",
        "docs/reference/react-client",
        "docs/reference/public-api",
      ],
    },
    {
      type: "category",
      label: "Use application storage",
      items: [
        "docs/functions/file-storage",
        "docs/concepts/object-storage",
      ],
    },
    {
      type: "category",
      label: "Use the CLI",
      collapsed: false,
      items: [
        "docs/cli/overview",
        "docs/reference/cli",
        "docs/operations/remote-lifecycle",
      ],
    },
    {
      type: "category",
      label: "Deliver releases",
      items: [
        "docs/functions/development-workflow",
        "docs/development/releases-and-workspaces",
        "docs/concepts/serving-policy",
      ],
    },
    {
      type: "category",
      label: "Install Self-Hosted",
      collapsed: false,
      items: [
        "docs/self-hosting/overview",
        "docs/self-hosting/deployment-guide",
        "deployments/docker/README",
        "docs/self-hosting/server-configuration",
        "docs/self-hosting/storage-configuration",
        "docs/self-hosting/product-postgresql",
        "docs/self-hosting/production-readiness",
      ],
    },
    {
      type: "category",
      label: "Operate Self-Hosted",
      collapsed: false,
      items: [
        "docs/operations/operator-handbook",
        "docs/operations/administration",
        "docs/operations/observability",
        "docs/operations/operational-logs",
        "docs/operations/backup-and-recovery",
        "docs/operations/upgrades",
        "docs/operations/capacity-planning",
        "docs/reference/troubleshooting",
      ],
    },
    {
      type: "category",
      label: "Identity and security",
      items: [
        "docs/auth/identity-map",
        "docs/auth/application-identity",
        "docs/auth/platform-identity",
        "docs/security/security-model",
        "docs/security/hardening-checklist",
      ],
    },
    {
      type: "category",
      label: "Administer by API",
      items: [
        "docs/reference/management-api",
        "docs/concepts/environment-lifecycle",
      ],
    },
    {
      type: "category",
      label: "Concepts and limits",
      items: [
        "docs/concepts/platform-model",
        "docs/concepts/capability-status",
        "docs/reference/compatibility",
      ],
    },
  ],
};

export default sidebars;

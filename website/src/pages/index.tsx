import type {ReactNode} from "react";
import clsx from "clsx";
import Link from "@docusaurus/Link";
import Layout from "@theme/Layout";
import Heading from "@theme/Heading";
import styles from "./index.module.css";

type Journey = Readonly<{
  eyebrow: string;
  title: string;
  description: string;
  to: string;
  link: string;
}>;

const journeys: readonly Journey[] = [
  {
    eyebrow: "Application development",
    title: "Build with Functions and data",
    description:
      "Define schema, Queries, Mutations, Actions, Realtime subscriptions, files, and schedules in TypeScript.",
    to: "/docs/getting-started/application-tutorial",
    link: "Start the application tutorial",
  },
  {
    eyebrow: "Command line",
    title: "Develop and deliver with the CLI",
    description:
      "Run the local loop, connect an Environment, publish immutable code, promote Channels, roll back, and diagnose safely.",
    to: "/docs/cli/overview",
    link: "Use the Runku CLI",
  },
  {
    eyebrow: "Platform operations",
    title: "Install and administer Self-Hosted",
    description:
      "Install the supported profile; configure identity, storage, TLS, and limits; then operate health, recovery, upgrades, and incidents.",
    to: "/docs/self-hosting/deployment-guide",
    link: "Deploy and administer",
  },
];

function JourneyCard({journey}: {journey: Journey}): ReactNode {
  return (
    <article className={styles.card}>
      <span className={styles.eyebrow}>{journey.eyebrow}</span>
      <Heading as="h2">{journey.title}</Heading>
      <p>{journey.description}</p>
      <Link className={styles.cardLink} to={journey.to}>
        {journey.link} <span aria-hidden="true">→</span>
      </Link>
    </article>
  );
}

const quickLinks = [
  ["Schema and value types", "/docs/functions/schema-and-types"],
  ["Function API", "/docs/reference/function-api"],
  ["HTTP API without SDK", "/docs/reference/public-api"],
  ["Runku Object Storage", "/docs/concepts/object-storage"],
  ["Server configuration", "/docs/self-hosting/server-configuration"],
] as const;

export default function Home(): ReactNode {
  return (
    <Layout
      title="Documentation"
      description="Build, self-host, and operate Runku with authoritative developer and operator documentation."
    >
      <main className={styles.page}>
        <header className={styles.docsHeader}>
          <div className={styles.headerGrid}>
            <div className={styles.introduction}>
              <div className={styles.contextLine}>
                <span>Runku Docs</span>
                <span>Self-Hosted first</span>
                <span>Product 0.4.x</span>
              </div>
              <Heading as="h1">Build and operate Runku</Heading>
              <p className={styles.lead}>
                Task-oriented documentation for application developers, CLI users, and Self-Hosted
                operators. Choose a responsibility below to see only its APIs, permissions, limits,
                procedures, and failure behavior.
              </p>
              <div className={styles.actions}>
                <Link className={clsx("button button--lg", styles.primary)} to="/docs/getting-started/application-tutorial">
                  Start building <span aria-hidden="true">→</span>
                </Link>
                <Link className={clsx("button button--lg", styles.secondary)} to="/docs/self-hosting/deployment-guide">
                  Install Self-Hosted
                </Link>
              </div>
            </div>

            <aside className={styles.quickReference} aria-label="Quick reference">
              <div className={styles.quickHeader}>
                <strong>Quick reference</strong>
                <span>Frequently used contracts</span>
              </div>
              {quickLinks.map(([label, to]) => (
                <Link key={to} to={to}>{label}<span aria-hidden="true">→</span></Link>
              ))}
            </aside>
          </div>
          <div className={styles.pathHeading}>
            <span>Documentation paths</span>
            <p>Application code, command-line workflows, and platform administration remain separate.</p>
          </div>
          <div className={styles.cards}>
            {journeys.map((journey) => <JourneyCard key={journey.title} journey={journey} />)}
          </div>
        </header>

        <section className={clsx(styles.section, styles.support)}>
          <div>
            <span className={styles.eyebrow}>Current supported distribution</span>
            <Heading as="h2">Compact by design.<br />Explicit about its limits.</Heading>
            <p>
              Tagged releases provide the CLI, TypeScript SDKs, a non-root Linux
              <code> runku-server </code> image, and a Docker Compose package for one initialized
              Safe V8 Product Environment. Distributed adapters and Kubernetes assets are
              conformance evidence, not a supported general-purpose cluster package.
            </p>
          </div>
          <div className={styles.supportLinks}>
            <Link to="/docs/self-hosting/overview">Understand the topology <span>→</span></Link>
            <Link to="/docs/self-hosting/production-readiness">Run the readiness review <span>→</span></Link>
            <Link to="/docs/getting-started/saas-validation">Validate the model in SaaS <span>→</span></Link>
          </div>
        </section>
      </main>
    </Layout>
  );
}

import type {ReactNode} from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Layout from '@theme/Layout';
import HomepageFeatures from '@site/src/components/HomepageFeatures';
import Heading from '@theme/Heading';
import CodeBlock from '@theme/CodeBlock';

import styles from './index.module.css';

function BadgesSection() {
  return (
    <div className={styles.badges}>
      <a href="https://crates.io/crates/blaze" target="_blank" rel="noopener noreferrer">
        <img src="https://img.shields.io/crates/v/blaze.svg?style=flat-square&color=ff6b35" alt="Crates.io" />
      </a>
      <a href="https://docs.rs/blaze" target="_blank" rel="noopener noreferrer">
        <img src="https://img.shields.io/docsrs/blaze?style=flat-square&color=ff6b35" alt="docs.rs" />
      </a>
      <a href="https://github.com/blaze-db/blaze/actions" target="_blank" rel="noopener noreferrer">
        <img src="https://img.shields.io/github/actions/workflow/status/blaze-db/blaze/ci.yml?style=flat-square" alt="CI" />
      </a>
      <a href="https://github.com/blaze-db/blaze/blob/main/LICENSE" target="_blank" rel="noopener noreferrer">
        <img src="https://img.shields.io/badge/license-MIT-blue.svg?style=flat-square" alt="License" />
      </a>
      <a href="https://github.com/blaze-db/blaze" target="_blank" rel="noopener noreferrer">
        <img src="https://img.shields.io/github/stars/blaze-db/blaze?style=flat-square&color=yellow" alt="GitHub stars" />
      </a>
    </div>
  );
}

function HomepageHeader() {
  const {siteConfig} = useDocusaurusContext();
  return (
    <header className={clsx('hero hero--primary', styles.heroBanner)}>
      <div className="container">
        <Heading as="h1" className="hero__title">
          {siteConfig.title}
        </Heading>
        <p className="hero__subtitle">
          High-performance embedded OLAP query engine written in Rust.
          <br />
          SQL:2016 compliant with native Apache Arrow and Parquet support.
        </p>
        <BadgesSection />
        <div className={styles.buttons}>
          <Link
            className="button button--secondary button--lg"
            to="/docs/getting-started/quick-start">
            Get Started
          </Link>
          <Link
            className="button button--outline button--lg"
            style={{marginLeft: '1rem', color: 'white', borderColor: 'white'}}
            href="https://github.com/blaze-db/blaze">
            View on GitHub
          </Link>
        </div>
        <div className={styles.installCommand}>
          <code>cargo add blaze</code>
        </div>
      </div>
    </header>
  );
}

function CodeExample() {
  const code = `use blaze::{Connection, Result};

fn main() -> Result<()> {
    let conn = Connection::in_memory()?;

    // Register a Parquet file as a table
    conn.register_parquet("sales", "data/sales.parquet")?;

    // Run analytical queries with full SQL support
    let results = conn.query("
        SELECT region, SUM(amount) as total
        FROM sales
        WHERE year = 2024
        GROUP BY region
        ORDER BY total DESC
    ")?;

    Ok(())
}`;

  return (
    <section className={styles.codeExample}>
      <div className="container">
        <div className="row">
          <div className="col col--6">
            <Heading as="h2">Query Data in 5 Lines</Heading>
            <p>
              Blaze makes analytical queries simple. No server setup, no network
              overhead. Just add it to your Cargo.toml and start querying.
            </p>
            <ul className={styles.featureList}>
              <li>Full SQL:2016 support with JOINs, CTEs, and window functions</li>
              <li>Native Apache Arrow for zero-copy data processing</li>
              <li>Read Parquet, CSV, and Delta Lake files directly</li>
              <li>Python bindings for seamless integration</li>
            </ul>
          </div>
          <div className="col col--6">
            <CodeBlock language="rust" title="main.rs">
              {code}
            </CodeBlock>
          </div>
        </div>
      </div>
    </section>
  );
}

function ArchitectureSection() {
  return (
    <section className={styles.architecture}>
      <div className="container">
        <Heading as="h2" className="text--center">How It Works</Heading>
        <p className="text--center" style={{marginBottom: '2rem', maxWidth: '600px', margin: '0 auto 2rem'}}>
          Blaze processes SQL queries through a modern, vectorized execution pipeline optimized for analytical workloads.
        </p>
        <div className={styles.architectureDiagram}>
          <div className={styles.pipelineContainer}>
            <div className={styles.pipelineStep}>
              <div className={styles.stepIcon}>
                <svg width="32" height="32" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                  <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/>
                  <polyline points="14 2 14 8 20 8"/>
                  <line x1="16" y1="13" x2="8" y2="13"/>
                  <line x1="16" y1="17" x2="8" y2="17"/>
                </svg>
              </div>
              <span className={styles.stepLabel}>SQL Query</span>
            </div>
            <div className={styles.arrow}>→</div>
            <div className={styles.pipelineStep}>
              <div className={styles.stepIcon}>
                <svg width="32" height="32" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                  <polyline points="4 17 10 11 4 5"/>
                  <line x1="12" y1="19" x2="20" y2="19"/>
                </svg>
              </div>
              <span className={styles.stepLabel}>Parser</span>
            </div>
            <div className={styles.arrow}>→</div>
            <div className={styles.pipelineStep}>
              <div className={styles.stepIcon}>
                <svg width="32" height="32" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                  <circle cx="12" cy="12" r="10"/>
                  <path d="M9.09 9a3 3 0 0 1 5.83 1c0 2-3 3-3 3"/>
                  <line x1="12" y1="17" x2="12.01" y2="17"/>
                </svg>
              </div>
              <span className={styles.stepLabel}>Analyzer</span>
            </div>
            <div className={styles.arrow}>→</div>
            <div className={styles.pipelineStep}>
              <div className={styles.stepIcon}>
                <svg width="32" height="32" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                  <polygon points="12 2 2 7 12 12 22 7 12 2"/>
                  <polyline points="2 17 12 22 22 17"/>
                  <polyline points="2 12 12 17 22 12"/>
                </svg>
              </div>
              <span className={styles.stepLabel}>Optimizer</span>
            </div>
            <div className={styles.arrow}>→</div>
            <div className={styles.pipelineStep}>
              <div className={styles.stepIcon}>
                <svg width="32" height="32" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                  <polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/>
                </svg>
              </div>
              <span className={styles.stepLabel}>Executor</span>
            </div>
            <div className={styles.arrow}>→</div>
            <div className={styles.pipelineStep}>
              <div className={styles.stepIcon}>
                <svg width="32" height="32" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                  <rect x="3" y="3" width="18" height="18" rx="2" ry="2"/>
                  <line x1="3" y1="9" x2="21" y2="9"/>
                  <line x1="9" y1="21" x2="9" y2="9"/>
                </svg>
              </div>
              <span className={styles.stepLabel}>Arrow Results</span>
            </div>
          </div>
        </div>
        <div className={styles.architectureFeatures}>
          <div className={styles.archFeature}>
            <strong>Vectorized Execution</strong>
            <span>Processes 8,192 rows at a time for cache efficiency</span>
          </div>
          <div className={styles.archFeature}>
            <strong>Zero-Copy</strong>
            <span>Arrow format eliminates serialization overhead</span>
          </div>
          <div className={styles.archFeature}>
            <strong>Predicate Pushdown</strong>
            <span>Filters data at the storage layer</span>
          </div>
        </div>
      </div>
    </section>
  );
}

function UseCases() {
  return (
    <section className={styles.useCases}>
      <div className="container">
        <Heading as="h2" className="text--center">Built For</Heading>
        <div className="row">
          <div className="col col--4">
            <div className={styles.useCase}>
              <h3>Data Applications</h3>
              <p>
                Embed analytical capabilities directly in your application.
                No external database server required.
              </p>
            </div>
          </div>
          <div className="col col--4">
            <div className={styles.useCase}>
              <h3>ETL Pipelines</h3>
              <p>
                Process Parquet and CSV files with SQL. Transform data
                efficiently with vectorized execution.
              </p>
            </div>
          </div>
          <div className="col col--4">
            <div className={styles.useCase}>
              <h3>Data Science</h3>
              <p>
                Use Python bindings for fast local analytics. Works with
                PyArrow, Pandas, and Polars.
              </p>
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}

function TrustSection() {
  return (
    <section className={styles.trustSection}>
      <div className="container">
        <div className={styles.trustGrid}>
          <div className={styles.trustItem}>
            <div className={styles.trustIcon}>
              <svg width="40" height="40" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                <path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/>
              </svg>
            </div>
            <div className={styles.trustText}>
              <strong>Memory Safe</strong>
              <span>Written in Rust with zero unsafe code in the core</span>
            </div>
          </div>
          <div className={styles.trustItem}>
            <div className={styles.trustIcon}>
              <svg width="40" height="40" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                <path d="M21 16V8a2 2 0 0 0-1-1.73l-7-4a2 2 0 0 0-2 0l-7 4A2 2 0 0 0 3 8v8a2 2 0 0 0 1 1.73l7 4a2 2 0 0 0 2 0l7-4A2 2 0 0 0 21 16z"/>
              </svg>
            </div>
            <div className={styles.trustText}>
              <strong>No Dependencies</strong>
              <span>Pure Rust with no C/C++ required</span>
            </div>
          </div>
          <div className={styles.trustItem}>
            <div className={styles.trustIcon}>
              <svg width="40" height="40" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                <circle cx="12" cy="12" r="10"/>
                <polyline points="12 6 12 12 16 14"/>
              </svg>
            </div>
            <div className={styles.trustText}>
              <strong>Battle-Tested</strong>
              <span>Built on Apache Arrow, the standard for columnar data</span>
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}

function CommunitySection() {
  return (
    <section className={styles.communitySection}>
      <div className="container">
        <Heading as="h2" className="text--center">Open Source Community</Heading>
        <p className="text--center" style={{marginBottom: '2rem', maxWidth: '600px', margin: '0 auto 2rem'}}>
          Blaze is open source and community-driven. Join us in building the future of embedded analytics.
        </p>
        <div className={styles.communityGrid}>
          <a href="https://github.com/blaze-db/blaze" target="_blank" rel="noopener noreferrer" className={styles.communityCard}>
            <div className={styles.communityIcon}>
              <svg width="32" height="32" viewBox="0 0 24 24" fill="currentColor">
                <path d="M12 0c-6.626 0-12 5.373-12 12 0 5.302 3.438 9.8 8.207 11.387.599.111.793-.261.793-.577v-2.234c-3.338.726-4.033-1.416-4.033-1.416-.546-1.387-1.333-1.756-1.333-1.756-1.089-.745.083-.729.083-.729 1.205.084 1.839 1.237 1.839 1.237 1.07 1.834 2.807 1.304 3.492.997.107-.775.418-1.305.762-1.604-2.665-.305-5.467-1.334-5.467-5.931 0-1.311.469-2.381 1.236-3.221-.124-.303-.535-1.524.117-3.176 0 0 1.008-.322 3.301 1.23.957-.266 1.983-.399 3.003-.404 1.02.005 2.047.138 3.006.404 2.291-1.552 3.297-1.23 3.297-1.23.653 1.653.242 2.874.118 3.176.77.84 1.235 1.911 1.235 3.221 0 4.609-2.807 5.624-5.479 5.921.43.372.823 1.102.823 2.222v3.293c0 .319.192.694.801.576 4.765-1.589 8.199-6.086 8.199-11.386 0-6.627-5.373-12-12-12z"/>
              </svg>
            </div>
            <div className={styles.communityText}>
              <strong>GitHub</strong>
              <span>Star, fork, and contribute</span>
            </div>
          </a>
          <a href="https://github.com/blaze-db/blaze/discussions" target="_blank" rel="noopener noreferrer" className={styles.communityCard}>
            <div className={styles.communityIcon}>
              <svg width="32" height="32" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                <path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/>
              </svg>
            </div>
            <div className={styles.communityText}>
              <strong>Discussions</strong>
              <span>Ask questions, share ideas</span>
            </div>
          </a>
          <a href="https://github.com/blaze-db/blaze/issues/new" target="_blank" rel="noopener noreferrer" className={styles.communityCard}>
            <div className={styles.communityIcon}>
              <svg width="32" height="32" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                <circle cx="12" cy="12" r="10"/>
                <line x1="12" y1="8" x2="12" y2="12"/>
                <line x1="12" y1="16" x2="12.01" y2="16"/>
              </svg>
            </div>
            <div className={styles.communityText}>
              <strong>Issues</strong>
              <span>Report bugs, request features</span>
            </div>
          </a>
          <Link to="/docs/contributing" className={styles.communityCard}>
            <div className={styles.communityIcon}>
              <svg width="32" height="32" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                <path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/>
                <circle cx="9" cy="7" r="4"/>
                <path d="M23 21v-2a4 4 0 0 0-3-3.87"/>
                <path d="M16 3.13a4 4 0 0 1 0 7.75"/>
              </svg>
            </div>
            <div className={styles.communityText}>
              <strong>Contributing</strong>
              <span>Help improve Blaze</span>
            </div>
          </Link>
        </div>
      </div>
    </section>
  );
}

function CTASection() {
  return (
    <section className={styles.ctaSection}>
      <div className="container">
        <Heading as="h2">Ready to Get Started?</Heading>
        <p>Add Blaze to your project and run your first query in under 5 minutes.</p>
        <div className={styles.ctaButtons}>
          <Link
            className="button button--primary button--lg"
            to="/docs/getting-started/quick-start">
            Read the Docs
          </Link>
          <Link
            className="button button--secondary button--lg"
            to="/docs/comparison">
            Why Blaze?
          </Link>
        </div>
      </div>
    </section>
  );
}

export default function Home(): ReactNode {
  const {siteConfig} = useDocusaurusContext();
  return (
    <Layout
      title="Embedded OLAP Query Engine"
      description="High-performance embedded OLAP query engine written in Rust with SQL:2016 compliance and native Apache Arrow/Parquet support.">
      <HomepageHeader />
      <main>
        <HomepageFeatures />
        <CodeExample />
        <ArchitectureSection />
        <UseCases />
        <TrustSection />
        <CommunitySection />
        <CTASection />
      </main>
    </Layout>
  );
}

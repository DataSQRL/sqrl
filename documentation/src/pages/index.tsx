import React from 'react';
import clsx from 'clsx';
import Layout from '@theme/Layout';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import styles from './index.module.css';
import useBaseUrl from "@docusaurus/useBaseUrl";

import HomepageFeatures from '../components/HomepageFeatures';
import HomepageHeader, {HomepageHeaderProps} from '../components/HomepageHeader';
import CodeBlock from "@theme/CodeBlock";



const header: HomepageHeaderProps = {
  title: 'DataSQRL - Data Automation Framework',
  tagLine: 'Data Automation Framework',
  text: (
      <>
        DataSQRL is a world-model and simulator to automate data pipeline development and operation with AI.<br />
        Build safe and reliable data products, data streams, or data APIs.
      </>
  ),
  buttonLink: 'docs/intro/getting-started',
  buttonText: 'Automate Your Data',
  image: "/img/diagrams/world_model_architecture.png"
};

export default function Home() {
  const {siteConfig} = useDocusaurusContext();

  return (
      <Layout title={header.title} description={header.tagLine}>
        <HomepageHeader {...header} />
        <main>
          <section className={styles.content}>

            <div className="container">
              <div className="row margin-bottom--xl margin-top--lg">
                <div className="col col--6">
                  <CodeBlock language="sql">
                    {`-- Ingest data from connected systems
IMPORT banking_data.*;

-- Enrich debit transactions with creditor information using time-consistent join
SpendingTransactions :=
    SELECT
        t.*,
        h.name AS creditor_name,
        h.type AS creditor_type
    FROM Transactions t
             JOIN Accounts FOR SYSTEM_TIME AS OF t.tx_time a
                  ON t.credit_account_id = a.account_id
             JOIN AccountHolders FOR SYSTEM_TIME AS OF t.tx_time h
                  ON a.holder_id = h.holder_id;

-- Create secure MCP tooling endpoint with description for agentic retrieval
/** Retrieve spending transactions within the given time-range.
  from_time (inclusive) and to_time (exclusive) must be RFC-3339 compliant date time.
*/
SpendingTransactionsByTime(
  account_id STRING NOT NULL METADATA FROM 'auth.accountId',
  from_time TIMESTAMP NOT NULL,
  to_time TIMESTAMP NOT NULL
) :=
    SELECT * FROM SpendingTransactions
    WHERE debit_account_id = :account_id
      AND :from_time <= tx_time
      AND :to_time > tx_time
    ORDER BY tx_time DESC;`}
                  </CodeBlock>
                </div>
                <div className="col col--5 text--left">
                  <h2>Data Pipeline in a Single SQL Script</h2>
                  <p className="hero__subtitle">
                    DataSQRL extends SQL to a comprehensive world model for data platforms.
                    SQL offers a mathematical foundation (relational algebra), deep introspection (declarative language),
                    deterministic validation and optimization, is humanly readable, and well-supported
                    by most LLMs.

                  </p>
                  <p className="hero__subtitle">
                    Define the entire data product in a single SQL script. <br />
                    DataSQRL automates the data plumbing.
                  </p>
                  <p className="text--center">
                    <Link className="button button--primary button--lg" to="https://github.com/DataSQRL/datasqrl-examples">
                      See more Examples
                    </Link>
                  </p>
                </div>
              </div>
              <div className="row margin-bottom--lg margin-top--lg">
                <div className="col col--6 text--center">
                  <img src={useBaseUrl("/img/diagrams/automation_overview.png")}
                       alt="DataSQRL compiles consistent data pipelines"/>
                </div>
                <div className="col col--5 text--left">
                  <h2>End-to-End Automation with Understanding</h2>
                  <p className="hero__subtitle">
                    DataSQRL compiles SQL into production-ready data pipelines that run on open-source technologies.
                    Run locally, on Kubernetes, or cloud services with full visibility.
                  </p>
                  <p className="hero__subtitle">
                    DataSQRL provides compile-time and runtime feedback to generative AI and users for consistent, high-quality results.
                  </p>
                </div>
              </div>
              <div className="row margin-bottom--xl margin-top--lg">
                <div className="col col--6">
                  <CodeBlock language="sql">
                    {`/*+test */
EnrichedTransactionsTest := 
    SELECT debit_holder_name, 
           COUNT(*) AS debit_tx_count, 
           SUM(amount) AS total_debit_amount
    FROM EnrichedTransactions
    GROUP BY debit_holder_name ORDER BY debit_holder_name ASC;`}
                  </CodeBlock>
                </div>
                <div className="col col--5 text--left">
                  <h2>Ensure Correctness</h2>
                  <p className="hero__subtitle">
                    Snapshot tests and assertions ensure correctness, spot regressions in CI/CD, and
                    provide real-world feedback to AI. </p>
                  <p className="hero__subtitle">
                    DataSQRL can simulate complex data flows with 100% reproducibility.
                  </p>
                </div>
              </div>
              <div className="row margin-bottom--xl margin-top--lg">
                <div className="col col--6 text--center">
                  <img src={useBaseUrl("/img/screenshots/banking_dag.png")}
                       alt="DataSQRL builds the processing DAG"/>
                </div>
                <div className="col col--5 text--left">
                  <h2>Pipeline Generation and Optimization</h2>
                  <p className="hero__subtitle">
                    DataSQRL optimizes the allocation of pipeline steps to execution engines and
                    generates the integration code with data mappings and schema alignment.
                  </p>
                  <p className="hero__subtitle">
                    Generate glue code deterministically to avoid subtle bugs and maintenance costs.
                  </p>
                </div>
              </div>
              <div className="row margin-bottom--xl margin-top--lg">
                <div className="col col--6">
                  <CodeBlock language="sql">
                    {`IMPORT stdlib.openai.*;

ContentEmbedding := 
    SELECT 
      vector_embedd(text, 'text-embedding-3-small') AS embedding,
      completions(concat('Summarize:', text), 'gpt-4o') AS summary
    FROM Content;
`}
                  </CodeBlock>
                </div>
                <div className="col col--5 text--left">
                  <h2>AI-Native Features</h2>
                  <p className="hero__subtitle">
                    Built-in support for vector embeddings, LLM invocation, and ML model inference.
                  </p>
                  <p className="hero__subtitle">
                    Use AI features for advanced data processing.
                  </p>
                </div>
              </div>
              <div className="row margin-bottom--xl margin-top--lg">
                <div className="col col--6 text--center">
                  <img src={useBaseUrl("/img/diagrams/architecture_proven_oss.png")}
                       alt="DataSQRL uses proven open-source technologies"/>
                </div>
                <div className="col col--5 text--left">
                  <h2>Robust and Scalable</h2>
                  <p className="hero__subtitle">
                    DataSQRL handles partitioning and compiles to proven open-source technologies
                    for runtime execution like Apache Flink, Kafka, and Iceberg that support HA
                    and scale out as needed.
                  </p>
                  <p className="hero__subtitle">
                    Get operational peace-of-mind and scale when you need to.
                  </p>
                </div>
              </div>
              <div className="row margin-top--lg">
                <div className="col col--6">
                  <CodeBlock language="sql">
                    {`-- Create a relationship between holder and accounts
AccountHolders.accounts(status STRING) := 
    SELECT * FROM Accounts a 
    WHERE a.holder_id = this.holder_id AND a.status = :status 
    ORDER BY a.account_type ASC;

-- Link accounts with spending transactions
Accounts.spendingTransactions(since TIMESTAMP NOT NULL) :=
    SELECT * FROM SpendingTransactions t
    WHERE t.debit_account_id = this.account_id AND :since <= tx_time
    ORDER BY tx_time DESC;`}
                  </CodeBlock>
                </div>
                <div className="col col--5 text--left">
                  <h2>Flexible API Design</h2>
                  <p className="hero__subtitle">
                    Support for table functions and relationships in SQL enable flexible
                  API design for GraphQL, REST, and MCP.</p>
                  <p className="hero__subtitle">
                    Define data access in SQL and refine the API in GraphQL schema.
                  </p>
                </div>
              </div>
              <div className="row margin-bottom--xl margin-top--lg">
                <div className="col col--6">
                  <CodeBlock language="sql">
                    {`Transactions(account_id STRING METADATA FROM 'auth.acct_id') :=
    SELECT * FROM SpendingTransactions 
    WHERE debit_account_id = :account_id
    ORDER BY tx_time DESC;`}
                  </CodeBlock>
                </div>
                <div className="col col--5 text--left">
                  <h2>Secure</h2>
                  <p className="hero__subtitle">
                    Use JWT for authentication and fine-grained authorization of data access.
                  </p>
                  <p className="hero__subtitle">
                    DataSQRL defends against injection attacks.
                  </p>
                </div>
              </div>
              <div className="row margin-bottom--xl margin-top--lg">
                <div className="col col--6 text--center">
                  <img src={useBaseUrl("/img/screenshots/banking_dag_expanded.png")}
                       alt="DataSQRL uses proven open-source technologies"/>
                </div>
                <div className="col col--5 text--left">
                  <h2>Data Lineage</h2>
                  <p className="hero__subtitle">
                    DataSQRL analyzes the data pipeline and tracks data lineage for full visibility.
                    Provides at-least-once or exactly-once processing guarantees.
                  </p>
                  <p className="hero__subtitle">
                    Know where the data is coming from and where it's going.
                  </p>
                </div>
              </div>
              <div className="row margin-bottom--xl">
                <div className="col col--6">
                  <CodeBlock language="sh">
                    {`# Run the entire pipeline locally for quick iteration
docker run -it --rm -p 8888:8888 -v $PWD:/build \\
             datasqrl/cmd run usertokens.sqrl;
# Run test cases locally or in CI/CD             
docker run --rm -v $PWD:/build \\
             datasqrl/cmd test usertokens.sqrl;
# Compile deployment assets to deploy in K8s or cloud
docker run --rm -v $PWD:/build \\
             datasqrl/cmd compile usertokens.sqrl;
# See compiled plan, schemas, indexes, etc
(cd build/deploy/plan; ls)`}
                  </CodeBlock>
                </div>
                <div className="col col--5 text--left">
                  <h2>Developer Tooling</h2>
                  <p className="hero__subtitle">
                    Local development, automated tests, CI/CD support, pipeline optimization,
                    introspection, debugging.<br />
                    DataSQRL automates data pipelines within your workflow and is easy to validate.
                  </p>
                </div>
              </div>
              <div className="row margin-bottom--xl margin-top--lg">
                <div className="col col--6 text--center">
                  <img src={useBaseUrl("/img/screenshots/deployment_options.png")}
                       alt="DataSQRL uses proven open-source technologies"
                       style={{ height: '300px' }} />
                </div>
                <div className="col col--5 text--left">
                  <h2>Flexible Deployment</h2>
                  <p className="hero__subtitle">
                    Run locally, in containerized environments (Docker, Kubernetes) and using
                    cloud-managed services.
                  </p>
                  <p className="hero__subtitle">
                    DataSQRL uses your existing data infrastructure.
                  </p>
                </div>
              </div>
              <div className="row margin-bottom--xl margin-top--lg">
                <div className="col col--6">
                  <CodeBlock language="sql">
                    {`CREATE TABLE Transactions (
  \`timestamp\` TIMESTAMP_LTZ(3) NOT NULL METADATA FROM 'timestamp',
  WATERMARK FOR \`timestamp\` AS \`timestamp\`
) WITH (
  'connector' = 'kafka',
  'topic' = 'indicators',
  'properties.bootstrap.servers' = '\${BOOTSTRAP_SERVERS}',
  'properties.group.id' = 'mygroup',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'flexible-json'
);
`}
                  </CodeBlock>
                </div>
                <div className="col col--5 text--left">
                  <h2>Connect to Your Data Systems</h2>
                  <p className="hero__subtitle">
                    Connect directly to existing data systems like streaming platforms,
                    databases, or data lakes. Or ingest data through APIs.
                  </p>
                  <p className="hero__subtitle">
                    DataSQRL simplifies data integration through connector configuration.
                  </p>
                </div>
              </div>
              <div className="row margin-bottom--xl margin-top--lg">
                <div className="col col--6 text--center">
                  <img src={useBaseUrl("/img/screenshots/open_source_technologies.png")}
                       alt="DataSQRL uses proven open-source technologies"
                       style={{ height: '250px' }} />
                </div>
                <div className="col col--5 text--left">
                  <h2>Open Source</h2>
                  <p className="hero__subtitle">
                    DataSQRL compiles to popular, proven open-source technologies.
                  </p>
                  <p className="hero__subtitle">
                    Build your data platform automation on top of community-driven innovation.
                  </p>
                </div>
              </div>
              <div className="row margin-bottom--xl margin-top--lg">
                <div className="col col--6">
                  <CodeBlock language="sql">
                    {`-- Deduplicate an update stream to a stateful table
Accounts := DISTINCT AccountsCDC ON account_id ORDER BY update_time DESC;

-- Join transactions with accounts at the time of the transaction consistently
SpendingTransactions := 
    SELECT t.*, 
           h.name AS creditor_name,
    FROM Transactions t JOIN Accounts FOR SYSTEM_TIME AS OF t.tx_time a 
                        ON t.credit_account_id=a.account_id;

-- Aggregate over tumbling time windows
SpendingByWeek := SELECT 
      account_id, 
      type, 
      window_start AS week,
      SUM(amount) AS total_spending
   FROM TABLE(TUMBLE(
                TABLE SpendingTransactions, 
                DESCRIPTOR(tx_time),
                INTERVAL '1' DAY
              ))
   GROUP BY debit_account_id, type, window_start, window_end;`}
                  </CodeBlock>
                </div>
                <div className="col col--5 text--left">
                  <h2>Focus on what Matters</h2>
                  <p className="hero__subtitle">
                    DataSQRL supports time-window aggregations, deduplication, time-consistent joins, and other complex transformations
                  succinctly in SQL. <br />
                    DataSQRL handles low-level optimizations.</p>
                  <p className="hero__subtitle">
                    Import custom functions when SQL alone is not enough.
                  </p>
                </div>
              </div>
              <div className="row margin-bottom--xl margin-top--lg">
                <div className="col col--6">
                  <CodeBlock language="sql">
                    {`-- Compute enriched transaction to Iceberg with partition
/*+engine(iceberg), partition_key(credit_holder_type) */
EnrichedTransactions := SELECT 
      t.*, 
      hc.name AS credit_holder_name,                                 
    FROM Transactions t JOIN AccountHolders hc 
                        ON t.credit_holder_id = hc.holder_id;`}
                  </CodeBlock>
                </div>
                <div className="col col--5 text--left">
                  <h2>Full Control</h2>
                  <p className="hero__subtitle">
                    Direct the DataSQRL compiler with hints and configuration for control over
                    pipeline design and execution.
                  </p>
                </div>
              </div>
              <div className="row margin-bottom--xl margin-top--lg">
                <div className="col col--6 text--center">
                  <div style={{ position: 'relative', paddingBottom: '56.25%', height: 0, overflow: 'hidden' }}>
                    <iframe
                        style={{ position: 'absolute', top: 0, left: 0, width: '100%', height: '100%' }}
                        src="https://www.youtube.com/embed/RfMzdrtrEqQ"
                        title="DataSQRL Developer Workflow"
                        frameBorder="0"
                        allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture"
                        allowFullScreen
                    ></iframe>
                  </div>
                </div>
                <div className="col col--5 text--left">
                  <h2>Developer Workflow</h2>
                  <p className="hero__subtitle">
                    DataSQRL integrates into your developer workflow and enables
                    quick iterations. It works with most LLMs and coding agents. <br/>
                    {/*Watch the video to see for yourself.*/}
                  </p>
                  <Link className="button button--primary button--lg"
                        to="/docs/intro/getting-started">Get Started</Link>
                  <Link className="button button--primary button--lg"
                        to="/docs/intro">Learn More</Link>
                </div>
              </div>
            </div>
          </section>

        </main>
      </Layout>
  );
}

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
  title: 'DataSQRL - Data Pipeline Framework',
  tagLine: 'Data Pipeline Framework',
  text: (
      <>
        Build consistent and reliable data pipelines with SQL that
        process, store, and serve data as MCP, RAG, GraphQL, REST, or data product.
      </>
  ),
  buttonLink: 'docs/getting-started',
  buttonText: 'Build Robust Data Pipelines in 10 min',
  image: "/img/diagrams/architecture_overview.png"
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
IMPORT banking-data.*;

-- Enrich transactions with account and holder information
SpendingTransactions := SELECT t.*, h.name, h.type
    FROM Transactions t JOIN Accounts FOR SYSTEM_TIME AS OF t.tx_time a ON t.credit_account_id=a.account_id
                        JOIN AccountHolders FOR SYSTEM_TIME AS OF t.tx_time h ON a.holder_id = h.holder_id;

-- Create MCP tooling endpoint with description for agentic retrieval
/** Retrieve spending transactions within the given time-range.
  fromTime (inclusive) and toTime (exclusive) must be RFC-3339 compliant.*/
SpendingTransactionsByTime(account_id STRING NOT NULL,
    fromTime TIMESTAMP NOT NULL, toTime TIMESTAMP NOT NULL) :=
        SELECT * FROM SpendingTransactions WHERE account_id = :account_id
        AND :fromTime <= tx_time AND :toTime > tx_time ORDER BY tx_time DESC;`}
                  </CodeBlock>
                </div>
                <div className="col col--5 text--left">
                  <h2>Build MCP Servers in One SQL Script</h2>
                  <p className="hero__subtitle">
                    Serve clean, enriched, and consistent data to LLMs for accurate results with
                    less hallucinations.</p>
                  <p className="hero__subtitle">
                    Combine data across sources for customized tooling that is tailored to agents.
                  </p>
                </div>
              </div>
              <div className="row margin-bottom--xl margin-top--lg">
                <div className="col col--6">
                  <CodeBlock language="sql">
                    {`-- Time window aggregation to compute weekly spending profile
SpendingTimeWindow := SELECT account_id, type, window_start AS week,
        SUM(amount) AS total_spending, COUNT(*) AS transaction_count
   FROM TABLE(CUMULATE(TABLE SpendingTransactions, DESCRIPTOR(tx_time),
                 INTERVAL '1' HOUR, INTERVAL '7' DAY))
   GROUP BY debit_account_id, type, window_start, window_end;
-- REST endpoint to retrieve spending profile as context
/*+query_by_all(account_id) */
SpendingByWeek := DISTINCT SpendingTimeWindow ON account_id, type, week ORDER BY window_time;`}
                  </CodeBlock>
                </div>
                <div className="col col--5 text--left">
                  <h2>Build Context Retrieval in One SQL Script</h2>
                  <p className="hero__subtitle">
                    Fill pre-processed data into your prompt template with a single API call.</p>
                  <p className="hero__subtitle">
                    Define the processing in SQL and DataSQRL generates the API.
                  </p>
                </div>
              </div>
              <div className="row margin-bottom--xl margin-top--lg">
                <div className="col col--6">
                  <CodeBlock language="sql">
                    {`-- Create a relationship between holder and accounts
AccountHolders.accounts(status STRING) := SELECT * FROM Accounts a 
    WHERE a.holder_id = this.holder_id AND a.status = :status 
    ORDER BY a.account_type ASC;

-- Link accounts with spending transactions
Accounts.spendingTransactions(fromTime TIMESTAMP NOT NULL, toTime TIMESTAMP NOT NULL) :=
    SELECT * FROM SpendingTransactions t
    WHERE t.debit_account_id = this.account_id
          AND :fromTime <= tx_time AND :toTime > tx_time 
    ORDER BY tx_time DESC;`}
                  </CodeBlock>
                </div>
                <div className="col col--5 text--left">
                  <h2>Build GraphQL APIs in One SQL Script</h2>
                  <p className="hero__subtitle">
                    Define relationships and data structures in SQL, DataSQRL generates a
                    fast GraphQL API for you.</p>
                  <p className="hero__subtitle">
                    Build flexible data APIs quickly.
                  </p>
                  <Link className="button button--primary button--lg" to="https://github.com/DataSQRL/datasqrl-examples">
                    See more Examples
                  </Link>
                </div>
              </div>
              <div className="row margin-bottom--xl margin-top--lg">
                <div className="col col--6 text--center">
                  <img src={useBaseUrl("/img/diagrams/architecture_components.png")}
                       alt="DataSQRL compiles consistent data pipelines"/>
                </div>
                <div className="col col--5 text--left">
                  <h2>Data Consistency and Integrity</h2>
                  <p className="hero__subtitle">
                    DataSQRL compiles SQL to a consistent data pipeline with at-least-once or exactly-once
                    processing guarantees and data integrity validation.
                  </p>
                  <p className="hero__subtitle">
                    DataSQRL provides complete transparency into the data pipeline artifacts and
                    compile-time verification for results you can trust.
                  </p>
                </div>
              </div>
              <div className="row margin-bottom--xl margin-top--lg">
                <div className="col col--6">
                  <CodeBlock language="sql">
                    {`/*+test */
EnrichedTransactionsTest := SELECT debit_holder_name, 
                                   COUNT(*) AS debit_tx_count, 
                                   SUM(amount) AS total_debit_amount
          FROM EnrichedTransactions
          GROUP BY debit_holder_name ORDER BY debit_holder_name ASC;`}
                  </CodeBlock>
                </div>
                <div className="col col--5 text--left">
                  <h2>Ensure Correctness with Tests</h2>
                  <p className="hero__subtitle">
                    Write snapshot tests and assertions to ensure correctness and
                  spot regressions in CI/CD.</p>
                  <p className="hero__subtitle">
                    DataSQRL automates tests with 100% reproducibility.
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
                    That's a lot of glue code you don't have to write.
                  </p>
                </div>
              </div>
              <div className="row margin-bottom--xl margin-top--lg">
                <div className="col col--6">
                  <CodeBlock language="sql">
                    {`IMPORT stdlib.openai.*;

ContentEmbedding := SELECT *, 
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
                    introspection, debugging - DataSQRL brings software engineering best
                    practices to data pipelines.
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
                <div className="col col--6">
                  <CodeBlock language="sql">
                    {`-- Deduplicate an update stream to a stateful table
Accounts := DISTINCT AccountsCDC ON account_id ORDER BY update_time DESC;

-- Join transactions with accounts at the time of the transaction consistently
SpendingTransactions := SELECT t.*, h.name AS creditor_name, h.type AS creditor_type
    FROM Transactions t JOIN Accounts FOR SYSTEM_TIME AS OF t.tx_time a ON t.credit_account_id=a.account_id;

-- Aggregate over tumbling time windows
SpendingByWeek := SELECT account_id, type, window_start AS week,
        SUM(amount) AS total_spending, COUNT(*) AS transaction_count
   FROM TABLE(TUMBLE(TABLE SpendingTransactions, DESCRIPTOR(tx_time),
                 INTERVAL '1' DAY))
   GROUP BY debit_account_id, type, window_start, window_end;`}
                  </CodeBlock>
                </div>
                <div className="col col--5 text--left">
                  <h2>Focus on what Matters</h2>
                  <p className="hero__subtitle">
                    Express time-window aggregations, deduplication, time-consistent joins, and other complex transformations
                  succinctly in SQL. Express what you need and let the compiler handle low-level optimizations.</p>
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
EnrichedTransactions := SELECT t.*, hc.name AS credit_holder_name, 
                                hc.type AS credit_holder_type,
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

                </div>
                <div className="col col--5 text--left">
                  <h2>Developer Workflow</h2>
                  <p className="hero__subtitle">
                    DataSQRL integrates into your developer workflow and enables
                    quick iterations. <br/>
                    {/*Watch the video to see for yourself.*/}
                  </p>
                  <Link className="button button--primary button--lg"
                        to="/docs/getting-started">Get Started</Link>
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

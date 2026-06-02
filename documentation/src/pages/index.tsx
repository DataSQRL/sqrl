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
  title: 'DataSQRL - Data Engineering Harness',
  tagLine: 'Agentic Data Engineering Harness',
  text: (
      <>
        The open-source harness that gives coding agents the guardrails, validation, and feedback
        they need to build production-grade data pipelines.
      </>
  ),
  buttonLink: 'docs/intro/getting-started',
  buttonText: 'Start Building',
  image: "/img/diagrams/agentic/harness_overview.svg"
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
                  <h2>SQL: The Logical Layer for Agent Output</h2>
                  <p className="hero__subtitle">
                    SQL is ideal for AI-generated data pipelines: its declarative nature enables
                    deep introspection by the compiler, most LLMs are well-trained on SQL syntax,
                    and humans can easily verify agent output.
                  </p>
                  <p className="hero__subtitle">
                    The relational algebra foundation provides mathematical rigor for deterministic
                    validation and optimization that agents can rely on.
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
                  <img src={useBaseUrl("/img/diagrams/agentic/complete_framework.png")}
                       alt="DataSQRL physical layer optimization"/>
                </div>
                <div className="col col--5 text--left">
                  <h2>Physical Layer Optimization</h2>
                  <p className="hero__subtitle">
                    The harness handles what agents struggle with: mapping logical operations to
                    physical engines. A cost-based optimizer assigns computations to Flink, Kafka,
                    Postgres, or Iceberg while respecting capability constraints.
                  </p>
                  <p className="hero__subtitle">
                    Schema alignment, data type mapping, and connector configuration are generated
                    deterministically—eliminating subtle bugs that probabilistic generation introduces.
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
                  <h2>Simulation for Real-World Feedback</h2>
                  <p className="hero__subtitle">
                    Agents need feedback beyond static validation. The simulator executes pipelines
                    locally with timestamp-accurate event replay, providing real-world results that
                    drive iterative refinement.
                  </p>
                  <p className="hero__subtitle">
                    100% reproducibility means agents can test edge cases—late data, race conditions,
                    schema changes—that only occur rarely in production.
                  </p>
                </div>
              </div>
              <div className="row margin-bottom--xl margin-top--lg">
                <div className="col col--6 text--center">
                  <img src={useBaseUrl("/img/screenshots/banking_dag.png")}
                       alt="DataSQRL builds the processing DAG"/>
                </div>
                <div className="col col--5 text--left">
                  <h2>Deterministic Artifact Generation</h2>
                  <p className="hero__subtitle">
                    The transpiler generates deployment artifacts from the optimized DAG: Flink plans,
                    Kafka topics, Postgres schemas, GraphQL models. Deterministic generation means
                    consistent results regardless of how many iterations the agent runs.
                  </p>
                  <p className="hero__subtitle">
                    Agents focus on business logic while the harness handles the complex plumbing
                    that would otherwise introduce data inconsistencies.
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
                  <h2>AI-Native Data Processing</h2>
                  <p className="hero__subtitle">
                    Built-in functions for vector embeddings, LLM invocation, and ML model inference.
                    Agents can generate pipelines that incorporate AI capabilities without needing
                    to understand the underlying integration complexity.
                  </p>
                </div>
              </div>
              <div className="row margin-bottom--xl margin-top--lg">
                <div className="col col--6 text--center">
                  <img src={useBaseUrl("/img/diagrams/architecture_proven_oss.png")}
                       alt="DataSQRL compiles to proven open-source technologies"/>
                </div>
                <div className="col col--5 text--left">
                  <h2>Non-Functional Requirements Built In</h2>
                  <p className="hero__subtitle">
                    The harness encodes requirements that agents struggle with: scalability through
                    proper partitioning, reliability through proven technologies like Flink and Kafka,
                    and consistency through exactly-once semantics.
                  </p>
                  <p className="hero__subtitle">
                    Agent-generated code benefits from production-grade infrastructure without
                    needing to reason about distributed systems complexity.
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
                  <h2>Serving Layer for Data APIs</h2>
                  <p className="hero__subtitle">
                    Table functions and relationships extend the logical layer to support data serving.
                    The harness maps these to GraphQL, REST, and MCP endpoints automatically.
                  </p>
                  <p className="hero__subtitle">
                    Agents define data access patterns in SQL; the harness generates the API schema
                    and query mappings with proper validation.
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
                  <h2>Security Guardrails</h2>
                  <p className="hero__subtitle">
                    The harness enforces security patterns that agents might overlook: JWT authentication,
                    parameterized queries that prevent injection attacks, and fine-grained authorization.
                  </p>
                  <p className="hero__subtitle">
                    Agent-generated code inherits these protections through the transpilation process,
                    not through prompting the agent to remember security best practices.
                  </p>
                </div>
              </div>
              <div className="row margin-bottom--xl margin-top--lg">
                <div className="col col--6 text--center">
                  <img src={useBaseUrl("/img/screenshots/banking_dag_expanded.png")}
                       alt="DataSQRL DAG with full lineage"/>
                </div>
                <div className="col col--5 text--left">
                  <h2>Validation Through Introspection</h2>
                  <p className="hero__subtitle">
                    The compiler produces detailed representations of the computational DAG: table types,
                    execution stages, inferred keys, timestamps, and complete schemas. This output
                    feeds back to agents for iterative refinement.
                  </p>
                  <p className="hero__subtitle">
                    Data lineage tracking provides governance compliance without agent intervention.
                  </p>
                </div>
              </div>
              <div className="row margin-bottom--xl">
                <div className="col col--6">
                  <CodeBlock language="sh">
                    {`# Agent compiles and gets validation feedback
docker run --rm -v $PWD:/build \\
             datasqrl/cmd compile pipeline.sqrl;
# Agent runs tests and gets real-world feedback
docker run --rm -v $PWD:/build \\
             datasqrl/cmd test pipeline.sqrl;
# Agent iterates until tests pass, then deploys
docker run --rm -v $PWD:/build \\
             datasqrl/cmd compile pipeline.sqrl;
# Deployment artifacts ready for K8s or cloud
(cd build/deploy/plan; ls)`}
                  </CodeBlock>
                </div>
                <div className="col col--5 text--left">
                  <h2>Agentic Workflow Integration</h2>
                  <p className="hero__subtitle">
                    Simple CLI commands that agents invoke in iterative loops: compile for validation
                    feedback, test for simulation results, compile again after refinement.
                  </p>
                  <p className="hero__subtitle">
                    Each command produces structured output that agents consume to improve the pipeline
                    toward production requirements.
                  </p>
                </div>
              </div>
              <div className="row margin-bottom--xl margin-top--lg">
                <div className="col col--6 text--center">
                  <img src={useBaseUrl("/img/screenshots/deployment_options.png")}
                       alt="DataSQRL deployment options"
                       style={{ height: '300px' }} />
                </div>
                <div className="col col--5 text--left">
                  <h2>Flexible Deployment</h2>
                  <p className="hero__subtitle">
                    The same artifacts that run in local simulation deploy to Kubernetes or
                    cloud-managed services. Agents don't need to reason about deployment targets—the
                    harness abstracts that complexity.
                  </p>
                  <p className="hero__subtitle">
                    Production telemetry hooks correlate runtime behavior back to source code for
                    autonomous troubleshooting.
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
                  <h2>Connector Configuration</h2>
                  <p className="hero__subtitle">
                    Agents generate connector configurations for Kafka, databases, data lakes, and APIs.
                    The harness validates configuration parameters and handles the integration complexity.
                  </p>
                  <p className="hero__subtitle">
                    Schema discovery and alignment happen automatically during compilation.
                  </p>
                </div>
              </div>
              <div className="row margin-bottom--xl margin-top--lg">
                <div className="col col--6 text--center">
                  <img src={useBaseUrl("/img/screenshots/open_source_technologies.png")}
                       alt="DataSQRL compiles to open-source technologies"
                       style={{ height: '250px' }} />
                </div>
                <div className="col col--5 text--left">
                  <h2>Extensible Open-Source Framework</h2>
                  <p className="hero__subtitle">
                    DataSQRL compiles to proven open-source technologies: Flink, Kafka, Postgres, Iceberg.
                    The framework is extensible—add custom functions, connectors, or execution engines
                    to build a harness tailored to your organization.
                  </p>
                  <p className="hero__subtitle">
                    Encode domain-specific knowledge into the harness so agents benefit from it automatically.
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
                  <h2>Complex Transformations, Simple Syntax</h2>
                  <p className="hero__subtitle">
                    CDC deduplication, temporal joins, windowed aggregations—expressed concisely in SQL.
                    Agents reason about business logic using familiar syntax while the harness handles
                    the complex stream processing semantics.
                  </p>
                  <p className="hero__subtitle">
                    Custom UDFs extend the vocabulary when SQL alone isn't enough.
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
                  <h2>Agent Hints as Constraints</h2>
                  <p className="hero__subtitle">
                    Agents provide hints to guide the optimizer: force specific engine assignments,
                    set partition keys, or configure execution parameters. The optimizer respects
                    these constraints while ensuring consistency.
                  </p>
                  <p className="hero__subtitle">
                    Humans can inject domain knowledge through hints that agents then propagate.
                  </p>
                </div>
              </div>
              <div className="row margin-bottom--xl margin-top--lg">
                <div className="col col--6 text--center">
                  <div style={{ position: 'relative', paddingBottom: '56.25%', height: 0, overflow: 'hidden' }}>
                    <iframe
                        style={{ position: 'absolute', top: 0, left: 0, width: '100%', height: '100%' }}
                        src="https://www.youtube.com/embed/RfMzdrtrEqQ"
                        title="DataSQRL Agentic Workflow"
                        frameBorder="0"
                        allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture"
                        allowFullScreen
                    ></iframe>
                  </div>
                </div>
                <div className="col col--5 text--left">
                  <h2>See the Harness in Action</h2>
                  <p className="hero__subtitle">
                    Watch a coding agent use DataSQRL to build a complete data pipeline—from initial
                    SQL through iterative refinement to production deployment.
                  </p>
                  <p className="hero__subtitle">
                    The harness guides the agent at every step, providing the feedback needed to
                    produce pipelines that actually work in production.
                  </p>
                  <Link className="button button--primary button--lg margin-right--sm"
                        to="/docs/intro/getting-started">Get Started</Link>
                  <Link className="button button--primary button--lg"
                        to="/blog/agentic-data-engineering-harness">Learn More</Link>
                </div>
              </div>
            </div>
          </section>

        </main>
      </Layout>
  );
}

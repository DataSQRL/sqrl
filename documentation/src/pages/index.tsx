import React from 'react';
import Layout from '@theme/Layout';
import Link from '@docusaurus/Link';
import styles from './index.module.css';
import useBaseUrl from "@docusaurus/useBaseUrl";

import HomepageHeader, {HomepageHeaderProps} from '../components/HomepageHeader';
import CodeBlock from "@theme/CodeBlock";
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

// Matches the rounded corners of the SVG diagrams
const roundedImage: React.CSSProperties = {borderRadius: '16px'};


const header: HomepageHeaderProps = {
  title: 'DataSQRL - Agentic Data Engineering Harness',
  tagLine: 'Data Engineering Harness',
  text: (
      <>
        DataSQRL is an open-source toolkit for building data engineering agents
        designed around human control, correctness, and safety.
      </>
  ),
  buttonLink: 'docs/intro/getting-started',
  buttonText: 'Start Building',
  image: "/img/diagrams/agentic/harness_toolkit.svg"
};

interface Pillar {
  title: string;
  claim: string;
  text: string;
  link: string;
  linkText: string;
}

const pillars: Pillar[] = [
  {
    title: 'Human Control',
    claim: 'One SQL file your team can read.',
    text: 'Not thousands of lines of Python, dbt, YAML, and glue. The whole pipeline, ' +
        'from ingest to API, is expressed in SQL your engineers can understand, run, and approve.',
    link: '/blog/p4-human-understanding-one-sql-file',
    linkText: 'Why readability is the bottleneck',
  },
  {
    title: 'Correctness',
    claim: 'A compiler instead of guesswork.',
    text: 'Deterministic generation removes errors at the seams between systems. Relational ' +
        'validation and event-time replay tests catch the bugs that code review misses.',
    link: '/blog/p1-broken-at-seams',
    linkText: 'Where coding agents break pipelines',
  },
  {
    title: 'Safety',
    claim: 'Guardrails you can inspect and extend.',
    text: 'Problems surface at compile time. Every compile writes out lineage, schemas, and the ' +
        'full data flow, so you can enforce your organization\'s policies automatically.',
    link: '/blog/p2-validator-relational-introspection',
    linkText: 'What deep introspection catches',
  },
];

interface UseCase {
  title: string;
  text: string;
}

const useCases: UseCase[] = [
  {title: 'Streaming & Batch Pipelines', text: 'CDC, temporal joins, windowed aggregations, and deduplication on Flink, Kafka, and Iceberg.'},
  {title: 'Data APIs', text: 'GraphQL, REST, and MCP endpoints generated from SQL, with authentication and authorization built in.'},
  {title: 'Data Products', text: 'Curated, documented, and tested datasets for analytics and downstream teams.'},
  {title: 'Operational Data', text: 'Low-latency data for applications and AI agents, including embeddings and LLM enrichment.'},
];

function PillarCard({title, claim, text, link, linkText}: Pillar) {
  return (
      <div className="col col--4 margin-bottom--lg">
        <div className="card shadow--md" style={{height: '100%'}}>
          <div className="card__header">
            <h3 style={{marginBottom: '0.25rem'}}>{title}</h3>
            <strong>{claim}</strong>
          </div>
          <div className="card__body">
            <p>{text}</p>
          </div>
          <div className="card__footer">
            <Link to={link}>{linkText} →</Link>
          </div>
        </div>
      </div>
  );
}

function UseCaseCard({title, text}: UseCase) {
  return (
      <div className="col col--3 margin-bottom--lg">
        <div className="card" style={{height: '100%'}}>
          <div className="card__body">
            <h4>{title}</h4>
            <p className="margin-bottom--none">{text}</p>
          </div>
        </div>
      </div>
  );
}

export default function Home() {
  return (
      <Layout title={header.title} description={header.tagLine}>
        <HomepageHeader {...header} />
        <main>
          <section className={styles.content}>
            <div className="container">

              {/* ---------- The problem ---------- */}
              <div className="row margin-top--lg margin-bottom--xl">
                <div className="col col--10 col--offset-1 text--center">
                  <h2>Agents write data code faster than humans can verify it</h2>
                  <p className="hero__subtitle">
                    A general-purpose coding agent handles a data engineering task by producing
                    dozens of files in several languages. The result looks plausible and the demo
                    works. The expensive bugs sit at the seams between systems and in time
                    semantics, and they pass code review. Writing code is no longer the bottleneck
                    for data teams. Trusting it is.
                  </p>
                  <img src={useBaseUrl("/img/diagrams/agentic/control_comparison.svg")}
                       alt="A coding agent on its own produces dozens of files; with the DataSQRL harness it produces one readable SQL file that the compiler turns into every deployment asset"
                       style={{maxWidth: '900px', width: '100%'}}/>
                </div>
              </div>

              {/* ---------- Three pillars ---------- */}
              <div className="row margin-bottom--md">
                <div className="col text--center">
                  <h2>Automate data engineering without giving up control</h2>
                </div>
              </div>
              <div className="row margin-bottom--xl">
                {pillars.map((p) => <PillarCard key={p.title} {...p} />)}
              </div>

              {/* ---------- 1. Human control ---------- */}
              <div className="row margin-bottom--xl margin-top--lg">
                <div className="col col--6">
                  <CodeBlock language="sql" title="banking.sqrl">
                    {`IMPORT banking_data.*;

-- Latest version of each account from the CDC stream
Accounts := DISTINCT AccountsCDC ON account_id ORDER BY update_time DESC;

-- Enrich transactions with the account as of transaction time
SpendingTransactions :=
    SELECT t.*, h.name AS creditor_name, h.type AS creditor_type
    FROM Transactions t
      JOIN Accounts FOR SYSTEM_TIME AS OF t.tx_time a
        ON t.credit_account_id = a.account_id
      JOIN AccountHolders FOR SYSTEM_TIME AS OF t.tx_time h
        ON a.holder_id = h.holder_id;

/** Spending transactions for the authenticated account
    within [from_time, to_time). Exposed via GraphQL, REST, and MCP. */
SpendingTransactionsByTime(
  account_id STRING NOT NULL METADATA FROM 'auth.accountId',
  from_time TIMESTAMP NOT NULL,
  to_time TIMESTAMP NOT NULL
) :=
    SELECT * FROM SpendingTransactions
    WHERE debit_account_id = :account_id
      AND :from_time <= tx_time AND :to_time > tx_time
    ORDER BY tx_time DESC;`}
                  </CodeBlock>
                </div>
                <div className="col col--5 col--offset-1 text--left">
                  <h2>Human Control: Review the Logic, Not the Plumbing</h2>
                  <p className="hero__subtitle">
                    The agent's output is one declarative SQL file covering ingestion,
                    transformation, storage, and a secure MCP/REST/GraphQL endpoint. You
                    can understand it in one sitting.
                  </p>
                  <p className="hero__subtitle">
                    Grain, units, filters, and joins are on one screen, so reviewers check what the
                    pipeline means. Changes arrive as small, readable diffs. One command runs the
                    whole thing locally so you can look at real results before you approve.
                  </p>
                </div>
              </div>

              {/* ---------- 2. Correctness: compiler ---------- */}
              <div className="row margin-bottom--xl margin-top--lg">
                <div className="col col--6 text--center">
                  <img src={useBaseUrl("/img/diagrams/agentic/complete_framework_clean.svg")}
                       alt="DataSQRL compiles one logical model into assets for every engine"
                       style={roundedImage}/>
                </div>
                <div className="col col--5 col--offset-1 text--left">
                  <h2>Correctness: Deterministic Where It Matters</h2>
                  <p className="hero__subtitle">
                    Language models are probabilistic. Mapping types, keys, schemas, and connectors
                    across Flink, Kafka, Postgres, Iceberg, and the API layer requires strict
                    rule-following that's better handled by a cmopiler.
                  </p>
                  <p className="hero__subtitle">
                    Every boundary asset is generated from one logical model, so the systems cannot
                    disagree. A relational validator catches wrong keys, time-dependent joins,
                    stalled watermarks, and unbounded state. Each error comes with a fix the agent
                    can apply.
                  </p>
                </div>
              </div>

              {/* ---------- 2b. Correctness: event-time testing ---------- */}
              <div className="row margin-bottom--xl margin-top--lg">
                <div className="col col--6">
                  <CodeBlock language="sql" title="tests">
                    {`-- Snapshot test, replayed at original event timestamps
/*+ test */
SpendingByHolderTest :=
    SELECT creditor_name, COUNT(*) AS tx_count, SUM(amount) AS total
    FROM SpendingTransactions
    GROUP BY creditor_name ORDER BY creditor_name;

-- Assert that every transaction was enriched with its creditor
/*+ test(no_rows) */
NoUnenrichedTransactions :=
    SELECT * FROM SpendingTransactions WHERE creditor_name IS NULL;`}
                  </CodeBlock>
                </div>
                <div className="col col--5 col--offset-1 text--left">
                  <h2>Correctness: Test the Pipeline in Motion</h2>
                  <p className="hero__subtitle">
                    Static test fixtures miss the bugs that only appear over time. The simulator runs
                    the real deployment artifacts and replays events at their original timestamps.
                  </p>
                  <p className="hero__subtitle">
                    Late and out-of-order data, races between streams, idle sources, updates, and
                    deletes become deterministic tests. The agent keeps iterating until they pass.
                  </p>
                  <Link to="/blog/p3-testing-framework">
                    Why standard integration tests fall short →
                  </Link>
                </div>
              </div>

              {/* ---------- 3. Safety ---------- */}
              <div className="row margin-bottom--xl margin-top--lg">
                <div className="col col--6">
                  <CodeBlock language="text" title="build/pipeline_explain.txt">
                    {`=== CustomerTransaction
Type:   stream
Stage:  flink
Inputs: _CardAssignment, _Merchant, sources.Transaction
Annotations:
 - stream-root: Transaction
Primary Key: transactionId, time
Timestamp  : time
Schema:
 - transactionId: BIGINT NOT NULL
 - cardNo: VARCHAR NOT NULL
 - time: TIMESTAMP_LTZ(3) *ROWTIME* NOT NULL
 - amount: DOUBLE NOT NULL
 - merchantName: VARCHAR NOT NULL
 - customerId: BIGINT NOT NULL`}
                  </CodeBlock>
                </div>
                <div className="col col--5 col--offset-1 text--left">
                  <h2>Safety: Every Pipeline Is Fully Inspectable</h2>
                  <p className="hero__subtitle">
                    Each compile
                    writes out the complete data flow: table types, inferred keys, timestamps,
                    schemas, engine assignments, and every deployment asset.
                  </p>
                  <p className="hero__subtitle">
                    Use these artifacts for lineage, impact analysis, and audit. Add custom rules
                    that enforce your policies on PII, naming, retention, or residency for every
                    pipeline the agent builds.
                  </p>
                </div>
              </div>

              <div className="row margin-bottom--xl margin-top--lg">
                <div className="col col--6 text--center">
                  <img src={useBaseUrl("/img/screenshots/banking_dag_expanded.png")}
                       alt="DataSQRL data flow DAG with full lineage"
                       style={roundedImage}/>
                </div>
                <div className="col col--5 col--offset-1 text--left">
                  <h2>Safety: Guardrails by Construction</h2>
                  <p className="hero__subtitle">
                    The compiler rejects invalid engine assignments, capability mismatches, and
                    inconsistent data flows before anything is deployed.
                  </p>
                  <p className="hero__subtitle">
                    API endpoints use parameterized queries, and authorization is bound to JWT
                    claims in the SQL itself. Agent-built endpoints get these protections from the
                    compiler, not from a prompt.
                  </p>
                </div>
              </div>

              {/* ---------- Your harness ---------- */}
              <div className="row margin-bottom--lg margin-top--xl">
                <div className="col col--10 col--offset-1 text--center">
                  <h2>Your Harness, Your Organization</h2>
                  <p className="hero__subtitle">
                    A generic coding agent doesn't know your sources, conventions, domain, or
                    compliance rules. DataSQRL is a toolkit for building a harness that does and
                    packages it one Docker container: a data engineering
                    agent your teams, CI pipelines, and platforms can call.
                  </p>
                </div>
              </div>
              <div className="row margin-bottom--xl">
                <div className="col col--6">
                  <table className={styles.table}>
                    <tbody>
                    <tr><td><strong>Skills</strong></td><td>How your team gathers requirements, plans, implements, tests, and deploys, plus domain and catalog knowledge</td></tr>
                    <tr><td><strong>Validators &amp; policies</strong></td><td>Custom compiler rules for governance, security, and data quality</td></tr>
                    <tr><td><strong>Functions &amp; connectors</strong></td><td>Your UDFs, sources, sinks, and formats</td></tr>
                    <tr><td><strong>Engines &amp; deployment</strong></td><td>Flink, Kafka, Postgres, Iceberg on Docker, Kubernetes, or cloud</td></tr>
                    <tr><td><strong>Coding agent</strong></td><td>Claude Code, Codex, OpenCode, Pi: your choice</td></tr>
                    </tbody>
                  </table>
                </div>
                <div className="col col--5 col--offset-1 text--left">
                  <CodeBlock language="sh">
                    {`# The agent's inner loop, driven by the harness
# 1. Validate and explain the data flow
docker run --rm -v $PWD:/workspace \\
    datasqrl/cmd compile package.json
# 2. Event-time replay tests
docker run --rm -v $PWD:/workspace \\
    datasqrl/cmd test test-package.json
# 3. Deployment assets for K8s or cloud
ls build/deploy/plan`}
                  </CodeBlock>
                </div>
              </div>

              {/* ---------- Use cases ---------- */}
              <div className="row margin-bottom--md margin-top--lg">
                <div className="col text--center">
                  <h2>What a DataSQRL Data Engineering Agent Builds</h2>
                </div>
              </div>
              <div className="row margin-bottom--xl">
                {useCases.map((u) => <UseCaseCard key={u.title} {...u} />)}
              </div>

              <div className="row margin-bottom--xl margin-top--lg">
                <div className="col col--6 text--center">
                  <img src={useBaseUrl("/img/screenshots/open_source_technologies.png")}
                       alt="DataSQRL compiles to open-source technologies"
                       style={{...roundedImage, height: '250px'}}/>
                </div>
                <div className="col col--5 col--offset-1 text--left">
                  <h2>Runs on Infrastructure You Own</h2>
                  <p className="hero__subtitle">
                    DataSQRL compiles to Flink, Kafka, Postgres, and Iceberg and deploys to Docker,
                    Kubernetes, or managed cloud services. There is no proprietary runtime, and the
                    harness is open source.
                  </p>
                </div>
              </div>

              {/* ---------- Getting started ---------- */}
              <div className="row margin-bottom--xl margin-top--lg">
                <div className="col col--6">
                  <p>
                    All you need is <Link to="https://www.docker.com/products/docker-desktop/">Docker</Link>{' '}
                    and an API key from your LLM provider. Run the DataSQRL agent in your project folder:
                  </p>
                  <Tabs groupId="os">
                    <TabItem value="macOS" label="macOS" default>
                      <CodeBlock language="bash">
                        {`docker run -e ANTHROPIC_API_KEY -it --rm \\
  --detach-keys="ctrl-],ctrl-]" -e TERM -e COLORTERM \\
  -v "$PWD":/workspace -w /workspace datasqrl/datasqrl-pi`}
                      </CodeBlock>
                    </TabItem>
                    <TabItem value="windows" label="Windows">
                      <CodeBlock language="powershell">
                        {`docker run -e ANTHROPIC_API_KEY -it --rm \`
  --detach-keys="ctrl-],ctrl-]" -e TERM -e COLORTERM \`
  -v "\${PWD}:/workspace" -w /workspace datasqrl/datasqrl-pi`}
                      </CodeBlock>
                    </TabItem>
                    <TabItem value="linux" label="Linux">
                      <CodeBlock language="bash">
                        {`docker run -e ANTHROPIC_API_KEY -it --rm \\
  --detach-keys="ctrl-],ctrl-]" -e TERM -e COLORTERM \\
  -v "$PWD":/workspace -w /workspace datasqrl/datasqrl-pi`}
                      </CodeBlock>
                    </TabItem>
                  </Tabs>
                  <p>
                    Using OpenAI, Bedrock, Azure, or Vertex AI? Swap in that provider&apos;s
                    environment variables, as shown in
                    the <Link to="/docs/intro/getting-started">getting started guide</Link>.
                  </p>
                </div>
                <div className="col col--5 col--offset-1 text--left">
                  <h2>Getting Started</h2>
                  <p className="hero__subtitle">
                    The agent wraps the Pi coding agent with the DataSQRL framework and skills. Once
                    it is running, tell it what you need in plain English:
                  </p>
                  <blockquote>
                    Build a pipeline that ingests our order data from Kafka in real time and serves
                    hourly revenue per product through an API.
                  </blockquote>
                  <p>
                    You get SQL scripts with tests that you can read, run, and verify.
                  </p>
                  <p>
                    Want planning, iterative refinement, and deployment workflows? <Link to="https://github.com/DataSQRL/datasqrl-plugin">Install the
                    advanced DataSQRL agent</Link> as a plugin for Claude Code, Codex, Cursor, or GitHub
                    Copilot.
                  </p>
                  <Link className="button button--primary button--lg margin-right--sm margin-bottom--sm"
                        to="/docs/intro/getting-started">Getting Started Guide</Link>
                  <Link className="button button--secondary button--lg margin-bottom--sm"
                        to="https://github.com/DataSQRL/datasqrl-plugin">Plugin Docs</Link>
                </div>
              </div>

            </div>
          </section>
        </main>
      </Layout>
  );
}

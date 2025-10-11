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
  title: 'DataSQRL - AI Data Platform',
  tagLine: 'Accurate Data for AI',
  text: (
      <>
        DataSQRL makes it fast and easy to build accurate data interfaces
        for GenAI and ML applications from all your data sources.
      </>
  ),
  buttonLink: 'docs/intro/getting-started',
  buttonText: 'Build Flink Apps in 10 min',
  image: "/img/diagrams/ai_infra_summary.png"
};

const WhyDataSQRLList = [
  {
    title: 'Automate Data Plumbing',
    image: '/img/undraw/code.svg',
    description: (
        <>
          DataSQRL allows you to focus on your data by automating the busywork:
          data mapping, connector management, schema alignment, data serving,
          SQL dialect translation, API generation, and configuration management.
        </>
    ),
  },
  {
    title: 'Easy to Use',
    image: '/img/undraw/programming.svg',
    description: (
        <>
          Implement your data enrichment with the SQL you already know.
          DataSQRL allows you to focus on the "what" and worry less about the "how".
          Develop locally, iterate quickly, and deploy with confidence.
        </>
    ),
  },
  {
    title: 'Production Grade',
    image: '/img/undraw/secure.svg',
    description: (
        <>
          DataSQRL compiles efficient data pipelines that run on proven open-source
          technologies. Out of the box data consistency, high availability, scalability,
          and observability.
        </>
    ),
  },
];

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
                    {`CREATE TABLE UserTokens (
  userid INT NOT NULL,
  tokens BIGINT NOT NULL,
  request_time TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'
);

IMPORT local-data.userinfo AS _UserInfo;
_CurrentUser := DISTINCT _UserInfo ON userid ORDER BY last_updated DESC;

_EnrichedTokens := SELECT t.*, u.orgid FROM UserTokens t
    JOIN _CurrentUser FOR SYSTEM_TIME AS OF t.request_time u 
    ON u.userid = t.userid;

/**
  Retrieve the total token consumption and total request 
  by organization and - optionally - user.
 */
/*+query_by_any(orgid, userid) */
TokenAnalysis := SELECT orgid, userid, sum(tokens) as total_tokens,
                       count(tokens) as total_requests
                 FROM _EnrichedTokens GROUP BY orgid, userid;`}
                  </CodeBlock>
                </div>
                <div className="col col--5 text--left">
                  <h2>Powerful SQL</h2>
                  <p className="hero__subtitle">
                    Ingest, integrate, enrich, semantically annotate, and serve your data with a
                    single SQL script.
                  </p>
                  <p className="hero__subtitle">
                    Get LLM tooling, realtime feature stores, and curated training data for your
                    AI applications.
                  </p>
                </div>
              </div>
              <div className="row margin-bottom--xl margin-top--lg">
                <div className="col col--6 text--center">
                  <img src={useBaseUrl("/img/diagrams/ai_infra_diagram.png")}
                       alt="DataSQRL unlocks the value of your data"/>
                </div>
                <div className="col col--5 text--left">
                  <h2>DataSQRL Compiler</h2>
                  <p className="hero__subtitle">
                    DataSQRL compiles SQL to an integrated data pipeline that combines proven OSS
                    technologies for seamless data enrichment that interfaces directly with your
                    GenAI or ML app.
                  </p>
                  <p className="hero__subtitle">
                    Deploy with Docker, Kubernetes, or cloud-managed services.
                  </p>
                </div>
              </div>
              <div className="row margin-bottom--xl">
                <div className="col col--6 text--center">
                  <img src={useBaseUrl("/img/screenshots/dag_example.png")}
                       alt="DataSQRL compiled data pipeline"/>
                </div>
                <div className="col col--5 text--left">
                  <h2>Developer Tooling</h2>
                  <p className="hero__subtitle">
                    Local development, automated tests, CI/CD support, data lineage, pipeline optimization,
                    introspection, debugging - DataSQRL brings developer convenience and automation.
                  </p>
                  <Link className="button button--primary button--lg"
                      to="/docs/intro/getting-started">Get Started</Link>
                  <Link className="button button--primary button--lg"
                        to="/docs/intro">Learn More</Link>
                </div>
              </div>
            </div>
          </section>
          <HomepageFeatures FeatureList={WhyDataSQRLList}/>

        </main>
      </Layout>
  );
}

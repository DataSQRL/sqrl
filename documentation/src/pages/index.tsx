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
  title: 'DataSQRL - Data Platform Framework',
  tagLine: 'Data Platform Framework',
  text: (
      <>
        Build consistent and reliable data APIs, MCP servers,
        and lakehouse views with SQL.
      </>
  ),
  buttonLink: 'docs/getting-started',
  buttonText: 'Build Robust Data Apps in 10 min',
  image: "/img/diagrams/streaming_summary.png"
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
          Implement your data applications with the SQL you already know.
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
          DataSQRL compiles efficient data architectures that run on proven open-source
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

/*+query_by_all(userid) */
TotalUserTokens := SELECT userid, sum(tokens) as tokens,
  count(tokens) as requests FROM UserTokens GROUP BY userid;

UsageAlert := SUBSCRIBE SELECT * FROM UserTokens 
                                 WHERE tokens > 100000;

/** Returns all requests for the given user since fromTime 
  (inclusive) and until toTime (exclusive) */
PotentialRewards(userid BIGINT, fromTime TIMESTAMP, toTime TIMESTAMP) :=
   SELECT * FROM UserTokens WHERE userid = :userid
    AND :fromTime <= request_time AND :toTime > request_time;`}
                  </CodeBlock>
                </div>
                <div className="col col--5 text--left">
                  <h2>Complete Pipeline in 1 SQL Script</h2>
                  <p className="hero__subtitle">
                    Implement the entire data pipeline in SQL to ingest, process, analyze,
                    store, and serve your data.
                  </p>
                  <p className="hero__subtitle">
                    Eliminate the glue code with guaranteed consistency and high reliability.
                  </p>
                </div>
              </div>
              <div className="row margin-bottom--xl margin-top--lg">
                <div className="col col--6 text--center">
                  <img src={useBaseUrl("/img/diagrams/streaming_architecture.png")}
                       alt="DataSQRL unlocks the value of your data"/>
                </div>
                <div className="col col--5 text--left">
                  <h2>Compiler Validation and Consistency</h2>
                  <p className="hero__subtitle">
                    DataSQRL compiles SQL to an integrated data architecture that runs on mature
                    open-source technologies.
                  </p>
                  <p className="hero__subtitle">
                    Deploy with Docker, Kubernetes, or cloud-managed services.
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
                    practices to data engineers.
                  </p>
                  <Link className="button button--primary button--lg"
                      to="/docs/getting-started">Get Started</Link>
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

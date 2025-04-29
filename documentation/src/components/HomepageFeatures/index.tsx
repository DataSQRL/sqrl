import React from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import styles from './styles.module.css';
import useBaseUrl from "@docusaurus/core/lib/client/exports/useBaseUrl";


interface FeatureProps {
  image?: string;
  title: string;
  link?: string;
  linkText?: string;
  description?: React.JSX.Element;
}

interface HomepageFeaturesProps {
  FeatureList: FeatureProps[];
  headline?: string;
}

function Feature({ image, title, link, linkText, description }: FeatureProps): JSX.Element {
  const imageHTML = image ? (
      <div className="text--center">
        <img className={styles.featureSvg} loading="lazy" src={useBaseUrl(image)} alt={title} />
      </div>
  ) : null;

  const linkContent = link ? (
      <div className="text--center">
        <Link className="button button--secondary button--md" to={link}>
          {linkText}
        </Link>
      </div>
  ) : null;

  const mainContent = description ? (
      <p className="text--left margin-bottom--sm">{description}</p>
  ) : null;

  return (
      <div className={clsx('col col--4', styles.feature)}>
        {imageHTML}
        <div className="padding-horiz--md">
          <h3 className="text--center">{title}</h3>
          {mainContent}
          {linkContent}
        </div>
      </div>
  );
}

export default function HomepageFeatures({ FeatureList, headline }: HomepageFeaturesProps): JSX.Element {
  const header = headline ? (
      <h2 className="margin-bottom--md text--center">{headline}</h2>
  ) : null;

  return (
      <section className={styles.features}>
        <div className="container">
          {header}
          <div className="row">
            {FeatureList.map((props, idx) => (
                <Feature key={idx} {...props} />
            ))}
          </div>
        </div>
      </section>
  );
}

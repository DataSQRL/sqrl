import React, {Context, useContext} from 'react';
import clsx from 'clsx';
import Layout from '@theme/Layout';
import Link from '@docusaurus/Link';
import styles from './styles.module.css';
import useBaseUrl from "@docusaurus/useBaseUrl";


export interface HomepageHeaderProps {
  title: string;
  tagLine: string;
  text: React.ReactNode;
  buttonLink?: string;
  buttonText?: string;
  image?: string;
  youtubeURL?: string;
}

export default function HomepageHeader({
                                         title,
                                         tagLine,
                                         text,
                                         buttonLink,
                                         buttonText,
                                         image,
                                         youtubeURL,
                                       }: HomepageHeaderProps): JSX.Element {
  const renderButton = () => {
    if (!buttonLink || !buttonText) return null;
    return (
        <div className={clsx('margin-bottom--lg', styles.buttons)}>
          <Link className="button button--primary button--lg" to={buttonLink}>
            {buttonText}
          </Link>
        </div>
    );
  };

  const renderVisualization = () => {
    if (youtubeURL) {
      return (
          <iframe
              width="100%"
              height="100%"
              src={youtubeURL}
              title="DataSQRL Introduction"
              frameBorder="0"
              allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture"
              allowFullScreen
          ></iframe>
      );
    } else if (image) {
      return <img src={useBaseUrl(image)} height="350"
                  alt="DataSQRL unlocks the value of your data"/>
    } else {
      return null;
    }
  };

  return (
      <header className={clsx('hero hero--secondary', styles.heroBanner)}>
        <div className="container">
          <div className="row">
            <div className={clsx('col col--6', styles.col)}>
              <h1 className="hero__title">{tagLine}</h1>
              <p className="hero__subtitle">{text}</p>
              {renderButton()}
            </div>
            <div className={clsx('col col--6', styles.pictureCol)}>
              {renderVisualization()}
            </div>
          </div>
        </div>
      </header>
  );
}

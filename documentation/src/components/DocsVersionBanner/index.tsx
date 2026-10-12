import React, {useEffect, useRef} from 'react';
import Link from '@docusaurus/Link';
import {
  useActivePlugin,
  useDocsPreferredVersion,
  useDocsVersion,
  useDocVersionSuggestions,
} from '@docusaurus/plugin-content-docs/client';
import styles from './styles.module.css';

/**
 * Full width banner pinned below the navbar for the docs versions other than the latest release
 * (unreleased `main` and older releases). It replaces the theme's DocVersionBanner, which is
 * rendered within the doc content.
 */
export default function DocsVersionBanner(): React.JSX.Element | null {
  const versionMetadata = useDocsVersion();
  return versionMetadata.banner ? <Banner /> : null;
}

function Banner(): React.JSX.Element {
  const versionMetadata = useDocsVersion();
  const pluginId = useActivePlugin()?.pluginId;
  const {savePreferredVersionName} = useDocsPreferredVersion(pluginId);
  const {latestDocSuggestion, latestVersionSuggestion: latest} = useDocVersionSuggestions(pluginId);
  // Link to the same page in the latest version if it exists, otherwise to its main page
  const latestDoc = latestDocSuggestion ?? latest.docs.find((doc) => doc.id === latest.mainDocId);

  // Expose the banner height, so the sticky elements of the docs layout are placed below it
  const ref = useRef<HTMLDivElement>(null);
  useEffect(() => {
    const root = document.documentElement;
    const observer = new ResizeObserver(([entry]) =>
      root.style.setProperty(
        '--docs-version-banner-height',
        `${entry.target.getBoundingClientRect().height}px`,
      ),
    );
    observer.observe(ref.current!);
    return () => {
      observer.disconnect();
      root.style.removeProperty('--docs-version-banner-height');
    };
  }, []);

  const label = <b>{versionMetadata.label}</b>;
  return (
    <div ref={ref} className={styles.banner} role="alert">
      {versionMetadata.banner === 'unreleased' ? (
        <>
          This is the documentation of the unreleased {label} branch, which may describe features
          that are not released yet.
        </>
      ) : (
        <>This is the documentation of DataSQRL {label}, which is not the latest release.</>
      )}{' '}
      For the documentation of the latest release, see{' '}
      <Link
        to={latestDoc?.path ?? latest.path}
        onClick={() => savePreferredVersionName(latest.name)}>
        the latest version
      </Link>{' '}
      ({latest.label}).
    </div>
  );
}

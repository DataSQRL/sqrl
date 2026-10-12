import React from 'react';
import {useHistory} from '@docusaurus/router';
import {
  type GlobalVersion,
  useActiveDocContext,
  useActivePlugin,
  useDocsPreferredVersion,
  useVersions,
} from '@docusaurus/plugin-content-docs/client';
import styles from './styles.module.css';

/**
 * Version picker for the docs sidebar. Switching versions keeps the current page when it exists in
 * the selected version, otherwise it opens the main page of that version.
 */
export default function DocsVersionPicker(): React.JSX.Element | null {
  const pluginId = useActivePlugin()?.pluginId;
  const versions = useVersions(pluginId);
  const {activeVersion, alternateDocVersions} = useActiveDocContext(pluginId);
  const {savePreferredVersionName} = useDocsPreferredVersion(pluginId);
  const history = useHistory();

  if (versions.length <= 1 || !activeVersion) {
    return null;
  }

  const targetPath = (version: GlobalVersion) =>
    (alternateDocVersions[version.name] ?? version.docs.find((doc) => doc.id === version.mainDocId))
      ?.path ?? version.path;

  const onChange = (event: React.ChangeEvent<HTMLSelectElement>) => {
    const version = versions.find((v) => v.name === event.target.value);
    if (version) {
      savePreferredVersionName(version.name);
      history.push(targetPath(version));
    }
  };

  return (
    <li className={styles.versionPicker}>
      <label className={styles.label} htmlFor="docs-version-picker">
        Version
      </label>
      <select
        id="docs-version-picker"
        className={styles.select}
        value={activeVersion.name}
        onChange={onChange}>
        {versions.map((version) => (
          <option key={version.name} value={version.name}>
            {version.isLast ? `${version.label} (latest)` : version.label}
          </option>
        ))}
      </select>
    </li>
  );
}

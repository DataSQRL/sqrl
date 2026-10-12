import React from 'react';
import DocSidebarItems from '@theme-original/DocSidebarItems';
import type DocSidebarItemsType from '@theme/DocSidebarItems';
import type {WrapperProps} from '@docusaurus/types';
import DocsVersionPicker from '@site/src/components/DocsVersionPicker';

type Props = WrapperProps<typeof DocSidebarItemsType>;

// Adds the version picker on top of the docs sidebar, both on desktop and in the mobile menu
export default function DocSidebarItemsWrapper(props: Props): React.JSX.Element {
  return (
    <>
      {props.level === 1 && <DocsVersionPicker />}
      <DocSidebarItems {...props} />
    </>
  );
}

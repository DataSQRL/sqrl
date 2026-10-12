import React, {useEffect} from 'react';
import Layout from '@theme-original/DocRoot/Layout';
import type LayoutType from '@theme/DocRoot/Layout';
import type {WrapperProps} from '@docusaurus/types';
import DocsVersionBanner from '@site/src/components/DocsVersionBanner';
import './styles.module.css';

type Props = WrapperProps<typeof LayoutType>;

// Exposes how far the footer reaches into the viewport, so the docs sidebar (fixed to the viewport,
// see styles.module.css) shrinks above the footer instead of being pushed up by it
function useFooterOverlap(): void {
  useEffect(() => {
    const root = document.documentElement;
    const footer = document.querySelector('footer');
    const update = () => {
      const overlap = footer ? Math.max(0, window.innerHeight - footer.getBoundingClientRect().top) : 0;
      root.style.setProperty('--docs-footer-overlap', `${overlap}px`);
    };
    update();
    window.addEventListener('scroll', update, {passive: true});
    window.addEventListener('resize', update);
    return () => {
      window.removeEventListener('scroll', update);
      window.removeEventListener('resize', update);
      root.style.removeProperty('--docs-footer-overlap');
    };
  }, []);
}

// Adds the full width docs version banner on top of the docs layout (sidebar, content and TOC)
export default function LayoutWrapper(props: Props): React.JSX.Element {
  useFooterOverlap();
  return (
    <>
      <DocsVersionBanner />
      <Layout {...props} />
    </>
  );
}

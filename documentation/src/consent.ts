///
/// Copyright © 2021 DataSQRL (contact@datasqrl.com)
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///     http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
///

/** Cookie consent (banner, footer "Cookie settings") and the Google tag it gates: Analytics and Ads. */
// Same contract as www.datasqrl.com (DataSQRL/datasqrl-sales website/src/scripts/consent.ts): cookie, value format, VERSION.
import ExecutionEnvironment from '@docusaurus/ExecutionEnvironment';

const GA_ID = 'G-Y4XLW4QZYX';
const ADS_ID = 'AW-17038638522';
const COOKIE = 'dsq_consent';
// bump with the website when the cookie notice adds a purpose or a provider, so every visitor is asked again
const VERSION = '1';
const MAX_AGE_S = 182 * 24 * 60 * 60;
const NOTICE = 'https://www.datasqrl.com/cookie-notice';

interface Consent { analytics: boolean; marketing: boolean }

declare global {
  interface Window { dataLayer: unknown[]; [gaDisable: `ga-disable-${string}`]: boolean }
}

const BANNER = `
  <p id="dsq-consent-title" class="dsq-consent__title">Cookies</p>
  <p class="dsq-consent__text">
    We’d like to use cookies to see how our sites are used and to measure our ads. Your choice applies to
    datasqrl.com and our docs. See the <a href="${NOTICE}">Cookie Notice</a> for details.
  </p>
  <p class="dsq-consent__text" data-consent-status role="status"></p>
  <fieldset class="dsq-consent__choices" data-consent-choices hidden>
    <legend class="dsq-consent__title">Choose cookies</legend>
    <label><input type="checkbox" name="analytics" /><span><strong>Analytics</strong> Google Analytics, and the campaign that brought you here</span></label>
    <label><input type="checkbox" name="marketing" /><span><strong>Marketing</strong> Google Ads: which ads lead to contact requests</span></label>
  </fieldset>
  <div class="dsq-consent__actions">
    <button type="button" class="button button--primary button--sm" data-consent-all="granted">Accept all</button>
    <button type="button" class="button button--outline button--primary button--sm" data-consent-save hidden>Save choices</button>
    <div class="dsq-consent__links">
      <button type="button" class="dsq-consent__link" data-consent-all="denied">Reject all</button>
      <button type="button" class="dsq-consent__link" data-consent-customize>Choose cookies</button>
    </div>
  </div>`;

function init() {
  // previews and local builds show the banner but never load the Google tag
  const onDatasqrl = /(^|\.)datasqrl\.com$/.test(location.hostname);

  const read = (): Consent | undefined => {
    const raw = document.cookie.split('; ').find((c) => c.startsWith(`${COOKIE}=`))?.slice(COOKIE.length + 1);
    const p = new URLSearchParams(raw ?? '');
    if (p.get('v') !== VERSION) return undefined;
    return { analytics: p.get('analytics') === '1', marketing: p.get('marketing') === '1' };
  };

  const write = (c: Consent) => {
    const value = `v=${VERSION}&analytics=${Number(c.analytics)}&marketing=${Number(c.marketing)}`;
    const domain = onDatasqrl ? '; Domain=datasqrl.com' : '';
    const secure = location.protocol === 'https:' ? '; Secure' : '';
    document.cookie = `${COOKIE}=${value}; Max-Age=${MAX_AGE_S}; Path=/; SameSite=Lax${domain}${secure}`;
  };

  const deleteCookies = (name: RegExp) => {
    for (const n of document.cookie.split('; ').map((c) => c.split('=')[0]).filter((n) => name.test(n))) {
      document.cookie = `${n}=; Max-Age=0; Path=/`;
      if (onDatasqrl) document.cookie = `${n}=; Max-Age=0; Path=/; Domain=datasqrl.com`;
    }
  };

  let choice = read();

  window.dataLayer ??= [];
  // gtag.js reads Arguments objects from dataLayer, not arrays
  function gtag(..._: unknown[]) { window.dataLayer.push(arguments); }
  gtag('consent', 'default', { analytics_storage: 'denied', ad_storage: 'denied', ad_user_data: 'denied', ad_personalization: 'denied' });

  let loaded = false;
  let adsConfigured = false;
  // route changes are counted by the GA stream's enhanced measurement (browser history events), not here
  const apply = (c: Consent) => {
    if (!c.analytics) deleteCookies(/^_ga(_|$)/);
    if (!c.marketing) deleteCookies(/^_gcl_/);
    if (!onDatasqrl) return;
    const mode = (on: boolean) => (on ? 'granted' : 'denied');
    // denied consent alone still lets GA send cookieless pings
    window[`ga-disable-${GA_ID}`] = !c.analytics;
    gtag('consent', 'update', { analytics_storage: mode(c.analytics), ad_storage: mode(c.marketing), ad_user_data: mode(c.marketing), ad_personalization: mode(c.marketing) });
    if (!c.analytics && !c.marketing) return;
    if (!loaded) {
      loaded = true;
      gtag('js', new Date());
      // 13 months: the longest analytics cookie lifetime EU regulators accept
      gtag('config', GA_ID, { allow_google_signals: false, cookie_expires: 395 * 24 * 60 * 60 });
      const s = document.createElement('script');
      s.async = true;
      s.src = `https://www.googletagmanager.com/gtag/js?id=${GA_ID}`;
      document.head.appendChild(s);
    }
    // also linked to GA_ID's Google tag today; configuring it again sends no duplicate hits
    if (c.marketing && !adsConfigured) {
      adsConfigured = true;
      gtag('config', ADS_ID);
    }
  };

  const banner = document.createElement('section');
  banner.className = 'dsq-consent';
  banner.tabIndex = -1;
  banner.hidden = true;
  banner.setAttribute('aria-labelledby', 'dsq-consent-title');
  banner.innerHTML = BANNER;
  document.body.appendChild(banner);
  const $ = <T extends HTMLElement>(s: string) => banner.querySelector<T>(s)!;
  const box = (name: keyof Consent) => $<HTMLInputElement>(`input[name=${name}]`);

  const showChoices = (show: boolean) => {
    $('[data-consent-choices]').hidden = !show;
    $('[data-consent-save]').hidden = !show;
    $('[data-consent-customize]').hidden = show;
  };

  const choose = (c: Consent) => {
    const revoked = (choice?.analytics && !c.analytics) || (choice?.marketing && !c.marketing);
    choice = c;
    write(c);
    banner.hidden = true;
    apply(c);
    // a loaded Google tag keeps its destinations until the page is reloaded
    if (revoked && loaded) location.reload();
  };

  const open = (fromSettings: boolean) => {
    const on = (v?: boolean) => (v ? 'on' : 'off');
    $('[data-consent-status]').textContent = choice ? `Currently: analytics ${on(choice.analytics)}, marketing ${on(choice.marketing)}.` : '';
    box('analytics').checked = !!choice?.analytics;
    box('marketing').checked = !!choice?.marketing;
    showChoices(fromSettings && !!choice);
    banner.hidden = false;
    if (fromSettings) banner.focus();
  };

  banner.querySelectorAll<HTMLButtonElement>('[data-consent-all]').forEach((b) => b.addEventListener('click', () => {
    const all = b.dataset.consentAll === 'granted';
    choose({ analytics: all, marketing: all });
  }));
  $('[data-consent-customize]').addEventListener('click', () => showChoices(true));
  $('[data-consent-save]').addEventListener('click', () => choose({ analytics: box('analytics').checked, marketing: box('marketing').checked }));
  // the footer re-renders on navigation, so its button is handled by delegation
  document.addEventListener('click', (e) => {
    if ((e.target as Element | null)?.closest?.('[data-consent-open]')) open(true);
  });

  if (choice) apply(choice);
  else open(false);
}

if (ExecutionEnvironment.canUseDOM) init();

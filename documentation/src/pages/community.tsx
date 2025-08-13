import React, { useState, FormEvent, ChangeEvent } from 'react';
import clsx from 'clsx';
import Layout from '@theme/Layout';
import Link from '@docusaurus/Link';
import styles from './index.module.css';
import HomepageFeatures from '../components/HomepageFeatures';
import HomepageHeader, {HomepageHeaderProps} from '../components/HomepageHeader';


const header: HomepageHeaderProps = {
  title: 'DataSQRL Community',
  tagLine: "Let's build with data together",
  text: (
      <>
        DataSQRL is a friendly, supportive, and inclusive community of data developers.
        We would love for you to nerd out with us and join our open-source community.
      </>
  ),
  buttonLink: 'https://github.com/DataSQRL/sqrl',
  buttonText: 'Join us on GitHub',
  image: '/img/undraw/community.svg',
};

const Support = [
  {
    title: 'GitHub',
    image: '/img/logos/github.svg',
    link: 'https://github.com/DataSQRL/sqrl',
    linkText: 'Contribute to DataSQRL',
    description: (
        <>
          <Link to="https://github.com/DataSQRL/sqrl">GitHub</Link> is where all open-source development on DataSQRL takes place.&nbsp;
          <Link to="https://github.com/DataSQRL/sqrl/issues">File a bug</Link>, star DataSQRL,&nbsp;
          <Link to="https://github.com/DataSQRL/sqrl/discussions/">ask questions</Link>,
          or contribute to the codebase. That's the beauty of open-source: when
          everybody contributes a little, something great can happen.
        </>
    ),
  },
  {
    title: 'Blog',
    image: '/img/undraw/blog.svg',
    link: '/blog',
    linkText: 'Read the Blog',
    description: (
        <>
          The <Link to="/blog">DataSQRL blog</Link> regularly publishes articles on the development of
          DataSQRL, how to implement data products, and lessons we learned along the way. Great morning reading.
        </>
    ),
  },
  {
    title: 'Youtube',
    image: '/img/logos/youtube_sqrl.svg',
    link: 'https://www.youtube.com/@datasqrl',
    linkText: 'Subscribe',
    description: (
        <>
          We publish tutorial videos, DataSQRL examples, and how-tos on the <Link to="https://www.youtube.com/@datasqrl">DataSQRL Youtube channel</Link>.
          If you like to learn by watching, this is the place to go.
          And yes, we are too uncool for TikTok.
        </>
    ),
  },
  // {
  //   title: 'Slack',
  //   image: '/img/logos/slack_sqrl.svg',
  //   link: 'https://join.slack.com/t/datasqrlcommunity/shared_invite/zt-2l3rl1g6o-im6YXYCqU7t55CNaHqz_Kg',
  //   linkText: 'Share Your Thoughts',
  //   description: (
  //       <>
  //         If you want to talk to the community, ask questions, brainstorm on your problem or tune into the development process behind DataSQRL,
  //         join us <Link to="https://join.slack.com/t/datasqrlcommunity/shared_invite/zt-2l3rl1g6o-im6YXYCqU7t55CNaHqz_Kg">on Slack</Link>. Get help and share your thoughts while watching how the sausage gets made.
  //       </>
  //   ),
  // },
];

const Updates = [
  // {
  //   title: 'LinkedIn',
  //   image: '/img/community/linkedin_sqrl.svg',
  //   link: 'https://www.linkedin.com/company/89940086',
  //   linkText: 'Follow DataSQRL',
  //   description: (
  //       <>
  //         Follow <Link to="https://www.linkedin.com/company/89940086">DataSQRL on LinkedIn</Link> to keep up to date on the latest and greatest.
  //         We'll share development progress, milestones, and other relevant community news - without
  //         spamming your feed.
  //       </>
  //   ),
  // },

];


export default function Community() {
  return (
      <Layout title={header.title} description={header.tagLine}>
        <HomepageHeader {...header} />
        <main>
          <HomepageFeatures FeatureList={Support} />
          <HomepageFeatures FeatureList={Updates} />
        </main>
      </Layout>
  );
}
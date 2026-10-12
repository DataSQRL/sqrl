# Website

This website is built using [Docusaurus](https://docusaurus.io/), a modern static website generator.
It is deployed to https://docs.datasqrl.com.

## Prerequisites

- **Node.js 22.18 or newer** (see the `engines` field in `package.json`) and npm.
  If you use [nvm](https://github.com/nvm-sh/nvm), run `nvm install 22 && nvm use 22`.
- The `docs/stdlib-docs` git submodule, which provides the function definitions the docs are
  generated from. Initialize it once from the repository root:

  ```bash
  git submodule update --init --recursive
  ```

All commands below are run from the `documentation/` directory.

## Installation

```bash
npm install
```

## Local Development

```bash
npm start
```

This starts a local development server on http://localhost:3000 and opens a browser window.
Most changes are reflected live without having to restart the server.

To use a different port:

```bash
npm start -- --port 3001
```

Note that `npm start` first runs `npm run generate-docs`, which regenerates
`docs/functions-system-generated.md` and `docs/functions-library-generated.md` from the YAML files
in the `docs/stdlib-docs` submodule. Those generated files should not be edited by hand.

## Build

```bash
npm run build
```

This generates static content into the `build` directory, which can be served by any static
content hosting service. To preview the production build locally:

```bash
npm run serve
```

After the site build, `npm run build` runs `npm run generate-llms-txt`, which generates
agent-friendly docs ([llms.txt](https://llmstxt.org)) into `build/` from the docs sources, in
sidebar order, for every docs version:

- `llms.txt`: index of the docs pages with a short description of each, starting with the curated
  introduction in `scripts/llms-intro.md`. The root `/llms.txt` is the one of the default version.
- `llms-full.txt`: all docs pages in a single Markdown file. The agent image bundles the one of
  its own docs (see `agent/Dockerfile`).
- `<page>.md`: each docs page as plain Markdown, next to its HTML page.

The generated files are not committed, so they always match the docs. Pages must be listed in
`sidebars.ts` to be included.

Other useful commands:

```bash
npm run typecheck   # TypeScript type checking
npm run clear       # clear the Docusaurus cache when the dev server misbehaves
```

## Deployment

The site is deployed automatically through CI/CD.
Create a PR against either the `docsUpdate` branch or `main`.

## Versioning

Only the docs (`/docs`) are versioned, by major version. The docs of each released major (`vX`) are
taken from its latest `release-X.Y` branch. Before 1.0, each minor release (`v0.Y`) is a version of
its own, taken from its `release-0.Y` branch, starting with 0.11. Everything else (homepage, blog,
community) always comes from `main`:

| Docs version   | Path                          | Banner                 |
|----------------|-------------------------------|------------------------|
| Latest release | `/docs/`                      | none                   |
| `main`         | `/docs/main/`                 | unreleased version     |
| Older releases | `/docs/vX/` (or `/docs/v0-Y/`) | not the latest release |

The version picker is at the top of the docs sidebar. During deployment,
`scripts/prepare-versioned-docs.sh` snapshots the docs of the release branches into
`versioned_docs/`, `versioned_sidebars/` and `versions.json`; these are generated and not
committed. Without them, as with `npm start`, the docs are unversioned.

Documentation fixes for a released major go to its latest release branch; a push there redeploys
the site. To build the versioned site locally (with the `release-*` branches fetched):

```bash
REMOTE=upstream scripts/prepare-versioned-docs.sh
npm run build
npm run serve
```

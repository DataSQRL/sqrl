# Website

This website is built using [Docusaurus](https://docusaurus.io/), a modern static website generator.
It is deployed to https://docs.datasqrl.com.

## Prerequisites

- **Node.js 22 or newer** (see the `engines` field in `package.json`) and npm.
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

Other useful commands:

```bash
npm run typecheck   # TypeScript type checking
npm run clear       # clear the Docusaurus cache when the dev server misbehaves
```

## Deployment

The site is deployed automatically through CI/CD.
Create a PR against either the `docsUpdate` branch or `main`.

## Versioning

Only the docs (`/docs`) are versioned, by major version. The docs of each released major are taken
from its latest `release-X.Y` branch, everything else (homepage, blog, community) always comes from
`main`:

| Docs version         | Path          | Banner                 |
|----------------------|---------------|------------------------|
| Latest major release | `/docs/`      | none                   |
| `main`               | `/docs/main/` | unreleased version     |
| Older major releases | `/docs/vX/`   | not the latest release |

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

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

The site is versioned by major version, and each release version is built from the latest
`release-X.Y` branch of its major:

| Version              | Path      | Banner                       |
|----------------------|-----------|------------------------------|
| Latest major release | `/`       | none                         |
| `main`               | `/main/`  | unreleased documentation     |
| Older major releases | `/vX/`    | not the latest release       |

The blog is always taken from `main` and published at `/blog`. Without any release branch, `main`
is served at `/` without a version dropdown. Release branches must contain the versioning support
(`scripts/build-versioned-site.sh` and the environment variables read by `docusaurus.config.ts`),
which is the case from `release-0.11` on.

Documentation fixes for a released major go to its latest release branch; a push there redeploys
the site. To build the versioned site locally (with the `release-*` branches fetched):

```bash
REMOTE=upstream scripts/build-versioned-site.sh
```

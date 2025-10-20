# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is the documentation website for DataSQRL, built using Docusaurus 3.7.0. It contains comprehensive documentation, tutorials, blog posts, and community resources for the DataSQRL data streaming framework. The site is deployed to https://docs.datasqrl.com.

## Essential Commands

### Development Commands
```bash
# Install dependencies
npm install
# or
yarn

# Start local development server (hot reload)
npm start
# or
yarn start
```

### Build Commands
```bash
# Build production site
npm run build
# or
yarn build

# Serve built site locally for testing
npm run serve
# or
yarn serve

# Type checking
npm run typecheck
# or
yarn typecheck
```

### Deployment Commands
```bash
# Deploy using SSH
USE_SSH=true npm run deploy
# or
USE_SSH=true yarn deploy

# Deploy without SSH (requires GIT_USER)
GIT_USER=<username> npm run deploy
# or  
GIT_USER=<username> yarn deploy
```

### Content Management
```bash
# Generate translation files
npm run write-translations

# Generate heading IDs
npm run write-heading-ids

# Clear cache
npm run clear

# Swizzle theme components (for customization)
npm run swizzle
```

## Architecture and Structure

### Core Configuration
- **docusaurus.config.ts**: Main configuration file defining site metadata, plugins, theme settings, and navigation
- **sidebars.ts**: Defines documentation sidebar structure and navigation hierarchy
- **package.json**: Dependencies and build scripts
- **tsconfig.json**: TypeScript configuration extending Docusaurus defaults

### Content Structure
- **docs/**: Main documentation content in Markdown format
  - **intro/**: Introductory documentation (intro.md, getting-started.md, concepts.md, tutorials.md)
  - Core documentation files include sqrl-language.md, interface.md, configuration.md, etc.
  - **stdlib-docs/**: Embedded function library documentation
- **blog/**: Release notes, updates, and technical blog posts
- **src/**: React components and custom pages
  - **components/**: Reusable components like HomepageFeatures and HomepageHeader
  - **pages/**: Custom pages (community.tsx, ai.tsx, flink.tsx, index.tsx)
  - **css/**: Custom styling

### Static Assets
- **static/**: Static files served directly
  - **img/**: Images, diagrams, logos, screenshots
  - **diagrams/**: Architecture and concept diagrams
  - **screenshots/**: Application screenshots for documentation

### Theme and Styling
The site uses Docusaurus's classic theme with:
- Dark mode as default with toggle support
- Custom CSS in `src/css/custom.css`
- Google Analytics integration (G-Y4XLW4QZYX)
- Custom logo and favicon

## Development Workflow

### Local Development
1. Run `npm start` to start the development server at http://localhost:3000
2. Changes to Markdown files and React components auto-reload
3. Use browser dev tools to inspect and debug

### Content Updates
- Documentation: Edit files in `docs/` directory
- Blog posts: Add new `.md` or `.mdx` files to `blog/` directory
- Navigation: Update `sidebars.ts` for documentation structure
- Homepage: Modify `src/pages/index.tsx` and related components

### Common Issues
The build process may show warnings for:
- Broken links and anchors (configure `onBrokenLinks` in config to ignore)
- Undefined blog tags (define in `blog/tags.yml`)
- Missing dependencies or outdated packages

### Component Customization
- Custom React components in `src/components/`
- Theme component swizzling available via `npm run swizzle`
- TypeScript support with proper type definitions

## Documentation Standards

### Content Guidelines
- Use Markdown with MDX support for interactive elements
- Include code examples with proper syntax highlighting
- Provide clear headings and navigation structure
- Add alt text for images and accessibility

### File Organization
- Group related documentation in logical directories
- Use descriptive filenames that match content topics
- Maintain consistent naming conventions
- Include README files for complex directories when needed

## Deployment Process

The site deploys to GitHub Pages at https://docs.datasqrl.com:
- Production builds generate static files in the `build/` directory
- Deployment pushes to `gh-pages` branch
- SSL certificate and custom domain configured via GitHub Pages settings
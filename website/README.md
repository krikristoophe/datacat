# Datacat documentation site

Bilingual (English + French) documentation for **Datacat**, built with
[Astro Starlight](https://starlight.astro.build/) and deployed to GitHub Pages.

## Prerequisites

- Node.js 20+ (CI uses Node 20)
- npm

## Commands

All commands are run from this `website/` directory:

```bash
npm install        # install dependencies
npm run dev        # start the local dev server at http://localhost:4321
npm run build      # build the production site to ./dist (runs Pagefind indexing)
npm run preview    # preview the built site locally
```

## Authoring content

The site is **bilingual**. English is the default (root) locale and French lives
under `/fr/`. Pages are Markdown / MDX files with frontmatter (`title`, `description`).

- **English** pages: `src/content/docs/*.md`
- **French** pages: `src/content/docs/fr/*.md`

Every English page should have a French counterpart with the **same file name** so the
built-in language picker can switch between them. Example: the architecture page lives at
`src/content/docs/architecture.md` (EN) and `src/content/docs/fr/architecture.md` (FR).

### Sidebar

The sidebar groups and ordering are configured in [`astro.config.mjs`](./astro.config.mjs)
via the `sidebar` option. Group labels are translated through the `translations` field on
each group. Add a new page to the relevant group's `items` array (by `slug`) after creating
its `.md` files in both locales.

## Configuration

Site-wide settings (title, tagline, i18n locales, social links, GitHub Pages `site`/`base`)
live in [`astro.config.mjs`](./astro.config.mjs). Before deploying, replace the `OWNER`
placeholder in `site` and the GitHub `social`/hero links with the real org/user. The `base`
is set to `/datacat` to match the repository name for a GitHub Pages **project page**.

## Search

Full-text search is provided by [Pagefind](https://pagefind.app/), which Starlight enables
automatically. The index is generated during `npm run build`, so search only works on the
built site (not in `npm run dev`).

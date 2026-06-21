// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';

// https://astro.build/config
export default defineConfig({
  // GitHub Pages project page. Replace the org/user in `site` with the real one.
  // `base` must match the repository name so assets resolve under the project page.
  site: 'https://krikristoophe.github.io',
  base: '/datacat',

  integrations: [
    starlight({
      title: 'Datacat',
      tagline: 'Self-hosted, idempotent event ingestion on PostgreSQL.',
      description:
        'Documentation for Datacat — a lightweight, self-hosted analytics & observability event-ingestion platform written in Rust.',

      // Dark-first, clean/minimal aesthetic.
      // Pagefind search is enabled by default in Starlight; the dark theme is
      // the default color scheme. We keep the light scheme available too.
      social: [
        {
          icon: 'github',
          label: 'GitHub',
          href: 'https://github.com/krikristoophe/datacat',
        },
      ],

      // Bilingual: English is the root (default) locale, French under /fr/.
      defaultLocale: 'root',
      locales: {
        root: {
          label: 'English',
          lang: 'en',
        },
        fr: {
          label: 'Français',
          lang: 'fr',
        },
      },

      // Sidebar groups. `label` is the English label; `translations` provides
      // the French label so the built-in language picker shows localized groups.
      sidebar: [
        {
          label: 'Guides',
          translations: { fr: 'Guides' },
          items: [
            { slug: 'quickstart' },
            { slug: 'installation' },
            { slug: 'sdks' },
            { slug: 'docker-telemetry' },
            { slug: 'companion' },
          ],
        },
        {
          label: 'Getting Started',
          translations: { fr: 'Premiers pas' },
          items: [
            { slug: 'overview' },
            { slug: 'architecture' },
            { slug: 'configuration' },
          ],
        },
        {
          label: 'Ingestion',
          translations: { fr: 'Ingestion' },
          items: [
            { slug: 'contract' },
            { slug: 'token' },
            { slug: 'otel-logs' },
            { slug: 'otel-metrics' },
            { slug: 'traces' },
          ],
        },
        {
          label: 'Reading',
          translations: { fr: 'Lecture' },
          items: [
            { slug: 'read-hot' },
            { slug: 'read-cold' },
            { slug: 'mcp' },
          ],
        },
        {
          label: 'Operations',
          translations: { fr: 'Exploitation' },
          items: [
            { slug: 'alerting' },
            { slug: 'cold-storage' },
            { slug: 'deployment' },
            { slug: 'security' },
          ],
        },
      ],
    }),
  ],
});

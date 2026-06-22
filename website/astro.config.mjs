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

      // Brand mark — used as the browser favicon and shown in the header next to the title.
      favicon: '/favicon.svg',
      logo: { src: './src/assets/logo.svg', alt: 'Datacat' },

      // Site-wide brand theme (palette, typography, polish). Landing-page styling is scoped
      // inside src/components/Landing.astro.
      customCss: ['./src/styles/theme.css'],

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
      // Usage-first: how to integrate and use Datacat comes first; the technical project docs
      // (wire format, protocol, internals) are grouped under "Reference" at the bottom.
      sidebar: [
        { slug: 'start', label: 'Start here', translations: { fr: 'Commencer ici' } },
        {
          label: 'Get started',
          translations: { fr: 'Premiers pas' },
          items: [{ slug: 'overview' }, { slug: 'quickstart' }, { slug: 'installation' }],
        },
        {
          label: 'Integrate',
          translations: { fr: 'Intégrer' },
          items: [
            { slug: 'integrate/web-app' },
            { slug: 'integrate/backend' },
            { slug: 'integrate/flutter' },
            { slug: 'integrate/opentelemetry' },
          ],
        },
        {
          label: 'Tutorials',
          translations: { fr: 'Tutoriels' },
          items: [
            { slug: 'tutorials/first-event' },
            { slug: 'tutorials/instrument-a-service' },
            { slug: 'tutorials/alert-to-slack' },
          ],
        },
        {
          label: 'Use Datacat',
          translations: { fr: 'Utiliser' },
          items: [
            { slug: 'sdks' },
            { slug: 'alerting' },
            { slug: 'read-hot' },
            { slug: 'read-cold' },
            { slug: 'mcp' },
            { slug: 'companion' },
            { slug: 'docker-telemetry' },
          ],
        },
        {
          label: 'Deploy & operate',
          translations: { fr: 'Déployer & exploiter' },
          items: [{ slug: 'configuration' }, { slug: 'deployment' }, { slug: 'cold-storage' }],
        },
        {
          label: 'Reference',
          translations: { fr: 'Référence' },
          items: [
            { slug: 'architecture' },
            { slug: 'contract' },
            { slug: 'token' },
            { slug: 'otel-logs' },
            { slug: 'otel-metrics' },
            { slug: 'traces' },
            { slug: 'security' },
          ],
        },
      ],
    }),
  ],
});

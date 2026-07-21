import { defineConfig } from "blume";

/**
 * Blume docs site for libsql-durable / PVM.
 *
 * Content: `docs/` (Markdown + MDX)
 * Search: Orama (local, zero hosted service)
 * Deploy: static `dist/` — any CDN / GitHub Pages / Vercel
 *
 * Commands:
 *   npm run docs:dev
 *   npm run docs:build
 *   npm run docs:preview
 */
export default defineConfig({
  title: "libsql-durable",
  description:
    "Process Virtual Machine kernel for Duroxide: durable worlds on libSQL — one file, disposable host, journal as truth.",
  logo: {
    text: "libsql-durable",
  },

  content: {
    root: "docs",
  },

  github: {
    owner: "DSamuelHodge",
    repo: "libSQL-durable",
    branch: "main",
  },

  lastModified: true,

  theme: {
    accent: "teal",
    radius: "md",
    mode: "system",
  },

  // Local search index (built into dist/) — no external service required.
  search: {
    provider: "orama",
  },

  markdown: {
    imageZoom: true,
    code: {
      icons: true,
      wrap: false,
    },
  },

  ai: {
    llmsTxt: true,
  },

  seo: {
    og: { enabled: true },
    sitemap: true,
    robots: true,
    structuredData: true,
  },

  // GitHub Pages project site:
  //   https://dsamuelhodge.github.io/libSQL-durable/
  // `base` is required for asset/link rewriting under the repo subpath.
  deployment: {
    output: "static",
    site: "https://dsamuelhodge.github.io/libSQL-durable",
    base: "/libSQL-durable",
  },
});


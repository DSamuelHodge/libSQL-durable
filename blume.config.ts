import { defineConfig } from "blume";

/**
 * PVM / World kernel docs (Blume).
 *
 * Product story first. Implementation crate name is secondary.
 */
export default defineConfig({
  title: "PVM",
  description:
    "Process Virtual Machine — the World substrate for durable processes. Departure from agent frameworks and harness stacks. Mesh (multi-verse) next.",
  logo: {
    text: "PVM",
  },

  banner: {
    content:
      "Team / pre-public — product language first. Public brand launch not ready. Features live in Features & Get started.",
    dismissible: true,
    id: "pre-public-pvm",
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

  deployment: {
    output: "static",
    site: "https://dsamuelhodge.github.io/libSQL-durable",
    base: "/libSQL-durable",
  },
});

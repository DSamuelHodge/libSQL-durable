import { defineMeta } from "blume";

/**
 * Product-first navigation.
 * Reference/kernel docs sit under "Reference" — not the landing story.
 */
export default defineMeta({
  pages: [
    // Product surface
    "index",
    "vision",
    "features",
    "get-started",
    "---",
    // Core capabilities (benefit-shaped entry → deep docs)
    "WORLD_PACKAGE",
    "RUNTIME",
    "DEFINITIONS",
    "FORK",
    "MESH",
    "---",
    // Operator / day-2
    "INTROSPECTION",
    "HEALING",
    "POLICY",
    "SYSCALLS",
    "---",
    // Team recipes + deep architecture
    "cookbook",
    "PVM",
    "COLLAPSE_PR_PLAN",
    "changelog",
  ],
});

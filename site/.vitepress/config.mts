// The swath.varve.io website: the home page in site/ and the user documentation in docs/,
// built by VitePress. The Markdown in docs/ is the source of truth and stays readable on
// GitHub: no front matter, relative `.md` links, GitHub alerts and GitHub heading anchors.
// Everything that adapts it to the site happens here, at build time:
//
// - srcDir is the repository root, so docs/x.md is served at /docs/x.html and the page's
//   edit link names its real path. Every other top-level entry, and the contributor pages
//   under docs/, are excluded, so they are neither published nor searchable.
// - Heading anchors use GitHub's slug rules, so `page.md#some-heading` links resolve on
//   the site exactly as they do on GitHub.
// - A link from a published page to anything outside the published set (internals, code,
//   the repository README) is rewritten to its GitHub URL. A link to a directory's README.md
//   becomes a link to the directory, which the README is served as.
import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { defineConfig } from 'vitepress'

const SITE_DIR = path.resolve(fileURLToPath(new URL('.', import.meta.url)), '..')
const REPO_ROOT = path.resolve(SITE_DIR, '..')
const GITHUB = 'https://github.com/varveio/swath'
const ORIGIN = 'https://swath.varve.io'

// Pages under docs/ that are not user documentation: published on GitHub only.
// A trailing slash names a directory.
const UNPUBLISHED_DOCS = ['docs/internals/', 'docs/ops/', 'docs/style.md', 'docs/upgrade.md']

// Only docs/ and the site's own pages are published: every other top-level entry is
// excluded by name, so a new directory of Markdown is never published by accident.
const srcExclude = [
  ...fs.readdirSync(REPO_ROOT, { withFileTypes: true })
    .filter((e) => e.name !== 'docs' && e.name !== 'site')
    .map((e) => (e.isDirectory() ? `${e.name}/**` : e.name)),
  'site/README.md',
  ...UNPUBLISHED_DOCS.map((p) => (p.endsWith('/') ? `${p}**` : p)),
]

function isPublished(rel: string): boolean {
  return (
    rel.startsWith('docs/') &&
    rel.endsWith('.md') &&
    !UNPUBLISHED_DOCS.some((p) => (p.endsWith('/') ? rel.startsWith(p) : rel === p)) &&
    fs.existsSync(path.join(REPO_ROOT, rel))
  )
}

// GitHub's heading slug: lowercase; drop everything but letters, marks, digits, `_`, `-`
// and spaces; spaces become hyphens. Repeats get `-1`, `-2` from markdown-it-anchor, as on
// GitHub. scripts/ci/check-site.py's gh_slug is the same rule for links into the repository.
function githubSlug(text: string): string {
  return text
    .trim()
    .toLowerCase()
    .replace(/[^\p{L}\p{M}\p{N}\p{Pc}\- ]/gu, '')
    .replace(/ /g, '-')
}

// Rewritten links per source page, reset on each render so a page rendered twice by the
// client and server builds is counted once. Printed at the end of the build.
const rewrittenLinks = new Map<string, string[]>()

function rewriteOutsideLinks(md: any) {
  md.core.ruler.push('swath_outside_links', (state: any) => {
    const file = state.env?.realPath || state.env?.path
    if (!file) return // an inline render of a title or a heading, not a page
    const source = path.relative(REPO_ROOT, file).split(path.sep).join('/')
    const rewritten: string[] = []
    for (const block of state.tokens) {
      for (const token of block.children || []) {
        if (token.type !== 'link_open') continue
        const href: string = token.attrGet('href') || ''
        if (!href || href.startsWith('#') || href.startsWith('/') || /^[a-z][a-z0-9+.-]*:/i.test(href)) continue
        const [target, fragment] = href.split(/#(.*)/s, 2)
        const rel = path.posix.normalize(path.posix.join(path.posix.dirname(source), decodeURI(target)))
        if (rel.startsWith('../')) continue
        const hash = fragment ? `#${fragment}` : ''
        if (isPublished(rel)) {
          // A README is served as its directory. A page in site/ is served from another
          // directory than its source, so its links are made absolute.
          if (path.posix.basename(rel) === 'README.md') {
            token.attrSet('href', `/${path.posix.dirname(rel)}/${hash}`)
          } else if (!source.startsWith('docs/')) {
            token.attrSet('href', `/${rel}${hash}`)
          }
          continue
        }
        const abs = path.join(REPO_ROOT, rel)
        if (!fs.existsSync(abs)) continue // left for the dead-link check to report
        const kind = fs.statSync(abs).isDirectory() ? 'tree' : 'blob'
        const url = `${GITHUB}/${kind}/main/${rel.replace(/\/$/, '')}${hash}`
        token.attrSet('href', url)
        rewritten.push(url)
      }
    }
    rewrittenLinks.set(source, rewritten)
  })
}

// The pages in docs/ have no node_modules above them, so the bare imports VitePress compiles
// into every page (`vue`, `vue/server-renderer`) are resolved as if the page were in site/.
const resolveFromSite = {
  name: 'swath:resolve-from-site',
  enforce: 'pre' as const,
  async resolveId(this: any, id: string, importer: string | undefined, options: any) {
    if (!importer || id.startsWith('.') || id.startsWith('/') || id.startsWith('\0') || importer.startsWith(SITE_DIR + path.sep)) {
      return null
    }
    if (!/^(?:@[\w.-]+\/)?[\w.-]+(?:\/|$)/.test(id)) return null
    return this.resolve(id, path.join(SITE_DIR, 'index.md'), { ...options, skipSelf: true })
  },
}

export default defineConfig({
  vite: { plugins: [resolveFromSite] },
  srcDir: '..',
  srcExclude,
  rewrites: {
    'site/index.md': 'index.md',
    'docs/README.md': 'docs/index.md',
  },
  outDir: '.vitepress/dist',
  cacheDir: '.vitepress/cache',

  lang: 'en-US',
  title: 'swath',
  titleTemplate: ':title · swath',
  description:
    'Parallel listing and inventory for large object stores and filesystems: S3, GCS, Azure Blob and local trees.',
  head: [
    ['link', { rel: 'icon', type: 'image/svg+xml', href: '/favicon.svg' }],
    ['meta', { name: 'theme-color', content: '#2B7A88' }],
    ['meta', { property: 'og:type', content: 'website' }],
    ['meta', { property: 'og:site_name', content: 'swath' }],
    ['meta', { property: 'og:image', content: `${ORIGIN}/card.png` }],
    ['meta', { name: 'twitter:card', content: 'summary_large_image' }],
    ['meta', { name: 'twitter:image', content: `${ORIGIN}/card.png` }],
  ],
  sitemap: { hostname: ORIGIN },

  markdown: {
    anchor: { slugify: githubSlug },
    config: rewriteOutsideLinks,
  },

  themeConfig: {
    siteTitle: 'swath',
    // Varve's chevron mark, in its light and dark strokes; the name beside it is the text.
    logo: { light: '/logo-light.svg', dark: '/logo-dark.svg', alt: '' },
    nav: [
      { text: 'Docs', link: '/docs/start-here', activeMatch: '^/docs/(?!commands/)' },
      { text: 'Commands', link: '/docs/commands/swath', activeMatch: '^/docs/commands/' },
      // The Go field guide and run explainer get a slot here once they exist.
      { text: 'Releases', link: `${GITHUB}/releases` },
    ],
    socialLinks: [{ icon: 'github', link: GITHUB }],
    search: { provider: 'local' },
    // An unknown address is rendered in the browser by the theme's not-found page.
    notFound: {
      title: 'Page not found',
      quote: 'There is no page at this address. Start from the home page, or search for a command, an option or an error message.',
      linkLabel: 'Go to the home page',
      linkText: 'Go to the home page',
    },
    editLink: {
      pattern: `${GITHUB}/edit/main/:path`,
      text: 'Edit this page on GitHub',
    },
    outline: { level: [2, 3] },
    footer: {
      message: 'Released under the Apache-2.0 license · by <a href="https://varve.io">Varve</a>',
    },
    sidebar: {
      '/docs/': [
        {
          text: 'Start',
          items: [
            { text: 'Start here', link: '/docs/start-here' },
            { text: 'Install', link: '/docs/install' },
            { text: 'Your first inventory', link: '/docs/first-inventory' },
            { text: 'Understand your results', link: '/docs/results' },
          ],
        },
        {
          text: 'Tasks',
          items: [
            { text: 'Run large listings', link: '/docs/running' },
            { text: 'Run on a schedule', link: '/docs/production' },
            { text: 'Filter rows', link: '/docs/filters' },
          ],
        },
        {
          text: 'Sources',
          items: [
            { text: 'Overview', link: '/docs/sources' },
            { text: 'Amazon S3', link: '/docs/sources#amazon-s3' },
            { text: 'S3-compatible stores', link: '/docs/sources#s3-compatible-stores' },
            { text: 'Google Cloud Storage', link: '/docs/sources#google-cloud-storage' },
            { text: 'Azure Blob Storage', link: '/docs/sources#azure-blob-storage' },
            { text: 'Local and mounted trees', link: '/docs/sources#local-and-mounted-trees' },
          ],
        },
        {
          text: 'Reference',
          items: [
            {
              text: 'Commands',
              collapsed: false,
              items: [
                { text: 'swath', link: '/docs/commands/swath' },
                { text: 'swath list', link: '/docs/commands/swath-list' },
                { text: 'swath du', link: '/docs/commands/swath-du' },
                { text: 'swath show', link: '/docs/commands/swath-show' },
                { text: 'swath resume', link: '/docs/commands/swath-resume' },
                { text: 'swath completion', link: '/docs/commands/swath-completion' },
              ],
            },
            { text: 'Dataset', link: '/docs/dataset' },
            { text: 'Text formats', link: '/docs/output' },
            { text: 'Exit codes', link: '/docs/reference/exit-codes' },
            { text: 'Run report', link: '/docs/reference/run-report' },
            { text: 'Log events', link: '/docs/reference/log-events' },
          ],
        },
        {
          text: 'Help',
          items: [
            { text: 'Troubleshooting', link: '/docs/troubleshooting' },
            { text: 'All documentation', link: '/docs/' },
          ],
        },
      ],
    },
  },

  buildEnd() {
    const all = [...rewrittenLinks.values()].flat()
    const pages = [...rewrittenLinks.entries()].filter(([, links]) => links.length).length
    console.log(
      `outside links rewritten to GitHub: ${all.length} occurrences, ` +
        `${new Set(all.map((u) => u.split('#')[0])).size} distinct targets, on ${pages} pages`,
    )
  },
})

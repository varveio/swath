// The swath.varve.io website: the home page in site/ and the user documentation in docs/,
// built by VitePress. The Markdown in docs/ is the source of truth and stays readable on
// GitHub: no front matter, relative `.md` links, GitHub alerts and GitHub heading anchors.
// Everything that adapts it to the site happens here, at build time:
//
// - srcDir is the repository root, so docs/x.md is served at /docs/x (clean URLs; the
//   /docs/x.html file behind it answers too) and the page's edit link names its real path. Every other top-level entry, and the contributor pages
//   under docs/, are excluded, so they are neither published nor searchable.
// - Heading anchors use GitHub's slug rules, so `page.md#some-heading` links resolve on
//   the site exactly as they do on GitHub.
// - A link from a published page to anything outside the published set (internals, code,
//   the repository README) is rewritten to its GitHub URL. A link to a directory's README.md
//   becomes a link to the directory, which the README is served as.
// - Each page's description (meta, og:description) is its first sentence of prose, and its
//   canonical URL the clean one; a few titles are made distinct from the site's name.
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

// Each table cell carries its column's header as data-label, so a narrow screen can show a row
// as a stack of labelled cells (custom.css) instead of squeezing every column.
function labelTableCells(md: any) {
  md.core.ruler.push('swath_table_labels', (state: any) => {
    let headers: string[] = []
    let column = 0
    for (let i = 0; i < state.tokens.length; i++) {
      const token = state.tokens[i]
      if (token.type === 'thead_open') headers = []
      else if (token.type === 'th_open') {
        headers.push((state.tokens[i + 1]?.content ?? '').replace(/[`*_]/g, '').trim())
      } else if (token.type === 'tr_open') column = 0
      else if (token.type === 'td_open') {
        const label = headers[column++]
        if (label) token.attrSet('data-label', label)
      }
    }
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

// While the site documents main ahead of a release, a notice above the navigation says so and
// links to what differs in the newest download. Set it to false when a release is documented
// (scripts/ci/check-site.py requires it while data/channel.json names the main channel).
const DEV_DOCS_NOTICE: false | { text: string; linkText: string; link: string } = {
  text: 'These docs describe the upcoming 0.4.0; the newest download is 0.4.0-rc.2.',
  linkText: 'What differs',
  link: '/docs/release-candidate',
}

// Titles that would repeat the site's name or another page's.
const TITLES: Record<string, string> = {
  'docs/commands/swath.md': 'The swath command',
}

// The first sentence of a page's prose: the paragraph after its title, without Markdown.
function firstSentence(file: string): string | undefined {
  let text: string
  try {
    text = fs.readFileSync(path.join(REPO_ROOT, file), 'utf8')
  } catch {
    return undefined
  }
  text = text.replace(/^---\n[\s\S]*?\n---\n/, '').replace(/<!--[\s\S]*?-->/g, '')
  let fenced = false
  const paragraph: string[] = []
  for (const line of text.split('\n')) {
    if (/^\s*(```|~~~)/.test(line)) {
      fenced = !fenced
      if (paragraph.length) break
      continue
    }
    if (fenced) continue
    const plain = line.trim()
    if (!plain) {
      if (paragraph.length) break
      continue
    }
    if (/^(#|\||>|[-*] |\d+\. |<)/.test(plain)) {
      if (paragraph.length) break
      continue
    }
    paragraph.push(plain)
  }
  if (!paragraph.length) return undefined
  const prose = paragraph
    .join(' ')
    .replace(/!?\[([^\]]*)\]\([^)]*\)/g, '$1')
    .replace(/[`*_]/g, '')
    .replace(/\s+/g, ' ')
    .trim()
  const sentence = prose.match(/^.*?\.(?=\s|$)/)?.[0] ?? prose
  return sentence.length > 200 ? sentence.slice(0, 197).replace(/\s+\S*$/, '') + '…' : sentence
}

function cleanPath(relativePath: string): string {
  return '/' + relativePath.replace(/(^|\/)index\.md$/, '$1').replace(/\.md$/, '')
}

export default defineConfig({
  vite: { plugins: [resolveFromSite] },
  srcDir: '..',
  srcExclude,
  rewrites: {
    'site/index.md': 'index.md',
    'docs/README.md': 'docs/index.md',
  },
  cleanUrls: true,
  outDir: '.vitepress/dist',
  cacheDir: '.vitepress/cache',

  lang: 'en-US',
  title: 'swath',
  titleTemplate: ':title · swath',
  description:
    'Parallel listing and inventory for object stores and filesystems: totals by directory or prefix, and sorted Parquet inventories you can query.',
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

  transformPageData(pageData) {
    const title = TITLES[pageData.filePath]
    if (title) pageData.title = title
    if (!pageData.frontmatter.description && !pageData.isNotFound) {
      const description = firstSentence(pageData.filePath)
      if (description) pageData.description = description
    }
  },

  transformHead({ pageData, siteData }) {
    if (pageData.isNotFound) return []
    const url = ORIGIN + cleanPath(pageData.relativePath)
    const home = pageData.relativePath === 'index.md'
    const title = home ? siteData.title : `${pageData.title} · ${siteData.title}`
    const description = pageData.description || siteData.description
    return [
      ['link', { rel: 'canonical', href: url }],
      ['meta', { property: 'og:url', content: url }],
      ['meta', { property: 'og:title', content: title }],
      ['meta', { property: 'og:description', content: description }],
    ]
  },

  markdown: {
    anchor: { slugify: githubSlug },
    config: (md) => {
      rewriteOutsideLinks(md)
      labelTableCells(md)
    },
  },

  themeConfig: {
    siteTitle: 'swath',
    // Varve's chevron mark, in its light and dark strokes; the name beside it is the text.
    logo: { light: '/logo-light.svg', dark: '/logo-dark.svg', alt: 'swath logo' },
    devDocsNotice: DEV_DOCS_NOTICE,
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
            { text: 'Differences in 0.4.0-rc.2', link: '/docs/release-candidate' },
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
            { text: 'Local and mounted filesystems', link: '/docs/sources#local-and-mounted-trees' },
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
            { text: 'Output', link: '/docs/output' },
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

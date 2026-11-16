// The default VitePress theme with Varve's fonts and colors. Three layout slots carry site content:
// the development-docs notice above the navigation (themeConfig.devDocsNotice; false turns it off),
// the home page's lead terminal example under the hero text, and the inventory example with the
// replay under that.
// The fonts are the ones Varve's UI uses, self-hosted from npm: Inter for text and Geist Mono
// for code, in place of the theme's bundled Inter.
import { defineComponent, h } from 'vue'
import { useData, withBase } from 'vitepress'
import DefaultTheme from 'vitepress/theme-without-fonts'
import '@fontsource-variable/inter/opsz.css'
import '@fontsource-variable/geist-mono/wght.css'
import './custom.css'

const REPO = 'https://github.com/varveio/swath/blob/main/'
const MEASURED = REPO + 'docs/internals/measurements.md#a-public-bucket-for-the-home-page-noaa-gestofs-pds-2026-11-15'
// The run report rendered from the inventory run's trace by tools/explainer: a static page in site/static/, outside
// the VitePress build, so its link loads the page itself (target _self) instead of going through the router.
const RUN_REPORT = '/runs/noaa-gestofs-pds/'

type Caption = (string | { text: string; href: string })[]
type Session = [command: string, output: string][]

// The lead example: real output of `swath du` on a public bucket, run once on a 100x40 terminal with TZ=UTC and
// main at 1085eb58 (docs/internals/measurements.md, "A public bucket for the home page"). Escape codes are
// stripped (Terminal draws swath's colors again) and the hint lines at the end are left out where the output
// reads "…". Nothing else is changed.
const HERO = {
  caption: [
    'The largest prefixes at any depth in a 1.38 PiB public bucket, listed in 54 seconds on a 4-vCPU cloud VM. ',
    { text: 'How this was measured', href: MEASURED },
  ] as Caption,
  session: [
    ['swath du s3://noaa-gestofs-pds --anonymous --s3-region us-east-1',
      `    size  objects  share                        newest          path
1.38 PiB    37.1M   100%  ████████████████████  2026-11-16 UTC  s3://noaa-gestofs-pds
 217 TiB    2.14M  15.4%  ███                   2026-08-28        _para4/
 146 TiB    1.42M  10.4%  ██                    2026-09-26        _para/
 103 TiB    27.5k   7.3%  █▍                    2026-07-30        _para2/
59.1 TiB     250k   4.2%  ▊                     2026-06-03        _hindcast_archive/
40.1 TiB    31.9k   2.8%  ▌                     2026-06-03          STOFS3DAlt/
19.0 TiB     218k   1.3%  ▎                     2026-05-04          AK_Runs/
 884 TiB    33.2M  62.7%  ████████████▌         2026-11-16        (2,174 more)
  1.38 PiB in 37,081,362 objects
  complete: 37,081,362 objects, 1.38 PiB in 53.7s · 690,207 keys/s
…`],
  ] as Session,
}

// The second example: real output of a live run on 2026-11-15 with main at d8fbe439, a sorted inventory of the same
// bucket, then a DuckDB query over it (the same measurements section). The run was `swath diag list` with --trace
// and --report added, for the replay beside it; swath's cost estimate line is left out where the output reads
// "…", and DuckDB's trailing spaces are trimmed. Nothing else is changed. The video is a release asset that the
// deploy fetches and verifies (site/static/data/media.json, scripts/ci/fetch-site-media.sh); the poster is in
// site/static/media/.
const INVENTORY = {
  heading: 'Save an inventory, query it with SQL',
  caption: [
    'The same bucket the day before: a sorted Parquet inventory in 61 seconds, then a DuckDB query over it in ' +
      '3 seconds.',
  ] as Caption,
  session: [
    [`swath list s3://noaa-gestofs-pds/ -o noaa \\
    --anonymous --s3-region us-east-1`,
      `  37,064,981 objects in 1m00s · 609,802 keys/s
  39,016 API calls · in flight avg 52.27 · peak 64
  …
  11 parts · 825 MiB written · peak RSS 1.51 GiB`],
    [`duckdb -column -c "SELECT
    regexp_extract(key, '\\.(\\w+)$', 1) AS ext,
    count(*) AS objects,
    format_bytes(sum(size)::BIGINT) AS bytes
  FROM 'noaa/data/*.parquet'
  GROUP BY ext ORDER BY sum(size) DESC
  LIMIT 5"`,
      `ext    objects   bytes
-----  --------  --------
nc     382202    1.3 PiB
grib2  13178305  39.8 TiB
tgz    1051      5.3 TiB
tar    18140     4.5 TiB
png    22913029  1.3 TiB`],
  ] as Session,
  video: '/media/noaa-gestofs-pds-replay.mp4',
  poster: '/media/noaa-gestofs-pds-replay-poster.jpg',
  label: 'A 36-second replay of the swath listing of s3://noaa-gestofs-pds in this section, rendered from its run trace',
  text: [
    'The video replays that listing from its trace. swath hands the busiest part of the bucket to workers as ' +
      'they become free, so the listing keeps moving without being told how the keys are laid out: one of its ' +
      '257 starting ranges held 64.5% of the objects and was split 1,834 times.',
  ],
  links: [['How this was measured', MEASURED], ['The run, range by range', RUN_REPORT], ['How it works', '/field-guide/']],
}

interface DevDocsNotice {
  text: string
  linkText: string
  link: string
}

const DevDocsBanner = defineComponent({
  setup() {
    const { theme } = useData()
    return () => {
      const notice = theme.value.devDocsNotice as DevDocsNotice | false | undefined
      if (!notice) return null
      return h('div', { class: 'swath-dev-banner', role: 'note' }, [
        h('span', notice.text + ' '),
        h('a', { href: withBase(notice.link) }, notice.linkText),
      ])
    }
  },
})

// swath's colors on a terminal: bars and the closing line in cyan, the view's heading and its (N more) lines dim.
const BAR = /([█▉▊▋▌▍▎▏]+)/
const lineClass = (line: string) =>
  /^ +size +objects /.test(line) || / \(\d[\d,]* more\)$/.test(line) ? 'swath-terminal-dim'
    : /^ +complete: /.test(line) ? 'swath-terminal-cyan' : undefined
const paint = (line: string) =>
  line.split(BAR).map((part, j) => j % 2 ? h('span', { class: 'swath-terminal-cyan' }, part) : part)

const Terminal = (caption: Caption, session: Session) =>
  h('figure', { class: 'swath-terminal' }, [
    h('figcaption', { class: 'swath-terminal-caption' }, caption.map((c) =>
      typeof c === 'string' ? c : h('a', { href: c.href }, c.text),
    )),
    h('pre', { tabindex: 0 }, [
      h('code', session.flatMap(([command, output], i) => [
        i ? '\n' : '',
        h('span', { class: 'swath-terminal-prompt', 'aria-hidden': 'true' }, '$ '),
        command, '\n',
        ...output.split('\n').flatMap((line, j) => {
          const cls = lineClass(line)
          return [j ? '\n' : '', ...(cls ? [h('span', { class: cls }, paint(line))] : paint(line))]
        }),
      ])),
    ]),
  ])

const HomeInventory = () =>
  h('section', { class: 'swath-replay', 'aria-labelledby': 'save-an-inventory' }, [
    h('div', { class: 'swath-replay-container' }, [
      h('video', {
        class: 'swath-replay-video',
        controls: true, muted: true, playsinline: true, preload: 'metadata',
        poster: withBase(INVENTORY.poster), src: withBase(INVENTORY.video), 'aria-label': INVENTORY.label,
      }, 'The replay is an MP4 video your browser does not play.'),
      h('div', { class: 'swath-replay-text' }, [
        h('h2', { id: 'save-an-inventory' }, INVENTORY.heading),
        Terminal(INVENTORY.caption, INVENTORY.session),
        ...INVENTORY.text.map((t) => h('p', t)),
        h('p', { class: 'swath-replay-links' }, INVENTORY.links.flatMap(([text, href], i) => [
          i ? ' · ' : '', h('a', { href, target: href === RUN_REPORT ? '_self' : undefined }, text),
        ])),
      ]),
    ]),
  ])

export default {
  extends: DefaultTheme,
  Layout() {
    return h(DefaultTheme.Layout, null, {
      'layout-top': () => h(DevDocsBanner),
      'home-hero-image': () => Terminal(HERO.caption, HERO.session),
      'home-features-before': () => HomeInventory(),
    })
  },
}

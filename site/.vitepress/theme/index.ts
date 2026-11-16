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
const MEASURED_LEAD = REPO + 'docs/internals/measurements.md#the-home-pages-lead-view-1000genomes-2026-11-16'
// The run report rendered from the inventory run's trace by tools/explainer: a static page in site/static/, outside
// the VitePress build, so its link loads the page itself (target _self) instead of going through the router.
const RUN_REPORT = '/runs/noaa-gestofs-pds/'

type Caption = (string | { text: string; href: string })[]
type Session = [command: string, output: string][]

// The lead example: real output of `swath du` on a public bucket, run once on a 100x30 terminal with TZ=UTC and
// main at a07cba64 (docs/internals/measurements.md, "The home page's lead view"). Escape codes are stripped
// (Terminal draws swath's colors again); nothing else is changed.
const HERO = {
  caption: [
    'Where 697 TiB sits in a public genomics bucket: 477,714 objects totalled in 3.5 seconds on a 4-vCPU cloud VM. ',
    { text: 'How this was measured', href: MEASURED_LEAD },
  ] as Caption,
  session: [
    ['swath du s3://1000genomes --anonymous --s3-region us-east-1',
      `    size  objects  share                        newest          path
 697 TiB     478k   100%  ████████████████████  2022-01-25 UTC  s3://1000genomes
 242 TiB     119k  34.7%  ██████▉               2016-04-29        technical/
 156 TiB    20.0k  22.4%  ████▍                 2016-04-29          working/
79.8 TiB    5.15k  11.4%  ██▎                   2014-09-03            20130718_phase…_cortex_graphs/
44.4 TiB    1.56k   6.4%  █▎                    2014-09-03            20121015_cortex_phase2/
32.2 TiB    13.3k   4.6%  ▉                     2016-04-29            (294 more)
37.2 TiB    11.7k   5.3%  █                     2014-09-03          ncbi_varpipe_data/
37.2 TiB    11.7k   5.3%  █                     2014-09-03            alignment/
1.38 MiB        3   0.0%                        2014-09-02            (3 more)
28.0 TiB    4.69k   4.0%  ▊                     2014-09-04          other_exome_alignments/
20.6 TiB    82.2k   3.0%  ▌                     2015-07-18          (14 more)
 236 TiB     280k  33.8%  ██████▊               2016-03-23        phase3/
 235 TiB     279k  33.7%  ██████▋               2016-03-23          data/
 588 GiB    1.18k   0.1%                        2016-03-22          (1 more)
89.1 TiB    21.7k  12.8%  ██▌                   2016-03-22        phase1/
51.6 TiB    13.3k   7.4%  █▍                    2014-09-09          data/
36.5 TiB    7.90k   5.2%  █                     2016-03-22          technical/
17.2 TiB    3.85k   2.5%  ▍                     2014-09-03            ncbi_varpipe_data/
19.3 TiB    4.05k   2.8%  ▌                     2016-03-22            (3 more)
1.00 TiB      538   0.1%                        2014-12-02          (10 more)
63.3 TiB    8.08k   9.1%  █▊                    2015-09-08        data/
49.3 TiB    6.66k   7.1%  █▍                    2022-01-25        1000G_2504_high_coverage/
37.6 TiB    5.01k   5.4%  █                     2019-04-25          data/
11.7 TiB    1.65k   1.7%  ▎                     2022-01-25          (3 more)
17.5 TiB    42.4k   2.5%  ▌                     2016-12-15        (26 more)
  697 TiB in 477,714 objects
  complete: 477,714 objects, 697 TiB in 3.5s · 135,869 keys/s`],
  ] as Session,
}

// The second example: real output of a live run on 2026-11-15 with main at d8fbe439, a sorted inventory of
// s3://noaa-gestofs-pds, then a DuckDB query over it (docs/internals/measurements.md, "A public bucket for the home
// page"). The run was `swath diag list` with --trace
// and --report added, for the replay beside it; swath's cost estimate line is left out where the output reads
// "…", and DuckDB's trailing spaces are trimmed. Nothing else is changed. The video is a release asset that the
// deploy fetches and verifies (site/static/data/media.json, scripts/ci/fetch-site-media.sh); the poster is in
// site/static/media/.
const INVENTORY = {
  heading: 'Save an inventory, query it with SQL',
  caption: [
    'A bigger public bucket, s3://noaa-gestofs-pds: a sorted Parquet inventory of 37 million objects in 61 seconds, ' +
      'then a DuckDB query over it in 3 seconds.',
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

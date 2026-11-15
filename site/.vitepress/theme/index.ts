// The default VitePress theme with Varve's fonts and colors. Two layout slots carry site content:
// the development-docs notice above the navigation (themeConfig.devDocsNotice; false turns it off)
// and the terminal example beside the home page's hero.
// The fonts are the ones Varve's UI uses, self-hosted from npm: Inter for text and Geist Mono
// for code, in place of the theme's bundled Inter.
import { defineComponent, h } from 'vue'
import { useData, withBase } from 'vitepress'
import DefaultTheme from 'vitepress/theme-without-fonts'
import '@fontsource-variable/inter/opsz.css'
import '@fontsource-variable/geist-mono/wght.css'
import './custom.css'

// Real output of a live `swath du` of a public bucket on 2026-11-15, with main at d8fbe439 (the run in
// docs/internals/measurements.md, "A public bucket for the home page"). Prefix rows and the cost line are left
// out where the lines read "…"; nothing else is changed.
const HERO_CAPTION = 'A live run on a public NOAA bucket, 2026-11-15: totals per prefix, largest first. … marks lines left out.'
const HERO_COMMAND = 'swath du s3://noaa-gestofs-pds/ -d 1 --sort size \\\n    --anonymous --s3-region us-east-1'
const HERO_OUTPUT = `    size   objects  last_modified (local)  prefix
 217 TiB   2139038  2026-08-28 19:43:10    _para4/
 146 TiB   1416203  2026-09-26 10:30:08    _para/
 103 TiB     27538  2026-07-30 12:15:24    _para2/
59.1 TiB    249640  2026-06-03 06:59:57    _hindcast_archive/
8.38 TiB      2422  2024-09-20 13:37:44    _archive/
5.50 TiB    156590  2025-12-11 18:37:31    _not_active/
2.67 TiB  21315345  2026-11-15 11:10:09    _post_processing/
       …  (2,162 more prefixes)
1.38 PiB  37064981  2026-11-15 11:10:09    (total)
  37,064,981 objects in 53.0s · 699,313 keys/s
  39,191 API calls · in flight avg 60.71 · peak 64
  …
  peak RSS 109 MiB`

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

const HeroTerminal = () =>
  h('figure', { class: 'swath-terminal' }, [
    h('figcaption', { class: 'swath-terminal-caption' }, HERO_CAPTION),
    h('pre', { tabindex: 0 }, [
      h('code', [h('span', { class: 'swath-terminal-prompt', 'aria-hidden': 'true' }, '$ '), HERO_COMMAND, '\n', HERO_OUTPUT]),
    ]),
  ])

export default {
  extends: DefaultTheme,
  Layout() {
    return h(DefaultTheme.Layout, null, {
      'layout-top': () => h(DevDocsBanner),
      'home-hero-image': () => HeroTerminal(),
    })
  },
}

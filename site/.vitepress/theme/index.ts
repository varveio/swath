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

// Real output of `swath du -d 1 ~/Downloads` on a sample tree, verified with 0.4.0-rc.2 and main.
const HERO_COMMAND = 'swath du -d 1 ~/Downloads'
const HERO_OUTPUT = `    size  files  dirs  last_modified (local)  path
3.00 GiB      2     0  2026-11-10 18:42:00    datasets/
1.72 GiB      2     0  2026-11-10 18:42:00    installers/
 960 MiB    240     1  2026-11-10 18:42:00    photos/
5.66 GiB    245     4  2026-11-10 18:42:00    (total)`

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
    h('figcaption', { class: 'swath-terminal-caption' }, 'Example: totals per directory, one level down'),
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

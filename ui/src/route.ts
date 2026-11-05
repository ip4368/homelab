/**
 * Where the UI is, kept in the URL hash so a link (or the back button) opens
 * the same page, list and detail pane: `#/workloads/pods?open=host:ms01-01&tab=pods`.
 */

export const PAGES = ['overview', 'hosts', 'workloads', 'topology', 'networks', 'lab'] as const
export type Page = (typeof PAGES)[number]

export const PAGE_LABELS: Record<Page, string> = {
  overview: 'Overview',
  hosts: 'Hosts',
  workloads: 'Workloads',
  topology: 'Topology',
  networks: 'Networks',
  lab: 'Lab',
}

/** What the detail pane shows: a host, a workload (`<namespace>/<kind>/<name>`) or a switch. */
export type OpenKind = 'host' | 'workload' | 'switch'
export interface Open {
  kind: OpenKind
  id: string
}

export interface Route {
  page: Page
  /** The page's own sub-view, e.g. the Workloads page's `pods` list or the Networks page's `templates`. */
  list: string | null
  open: Open | null
  /** The detail pane's tab. */
  tab: string | null
}

export const HOME: Route = { page: 'overview', list: null, open: null, tab: null }

/** The hashes of the old tabbed UI, so their links still open the right page. */
const LEGACY: Record<string, Page> = { fleet: 'hosts', topology: 'topology', networks: 'networks', workloads: 'workloads' }

function openOf(text: string | null): Open | null {
  if (!text) return null
  const colon = text.indexOf(':')
  const kind = text.slice(0, colon)
  const id = text.slice(colon + 1)
  if (colon < 0 || id === '' || (kind !== 'host' && kind !== 'workload' && kind !== 'switch')) return null
  return { kind, id }
}

/** The route a hash names; anything unknown is the overview. */
export function parseRoute(hash: string): Route {
  const raw = hash.replace(/^#/, '')
  if (raw in LEGACY) return { ...HOME, page: LEGACY[raw] }
  if (!raw.startsWith('/')) return HOME
  const q = raw.indexOf('?')
  const path = (q < 0 ? raw : raw.slice(0, q)).split('/').filter(Boolean)
  const params = new URLSearchParams(q < 0 ? '' : raw.slice(q + 1))
  const page = PAGES.find((p) => p === path[0])
  if (!page) return HOME
  const open = openOf(params.get('open'))
  return { page, list: path[1] ? decodeURIComponent(path[1]) : null, open, tab: open ? params.get('tab') : null }
}

export function formatRoute(r: Route): string {
  const params = new URLSearchParams()
  if (r.open) {
    params.set('open', `${r.open.kind}:${r.open.id}`)
    if (r.tab) params.set('tab', r.tab)
  }
  const query = params.toString()
  return `#/${r.page}${r.list ? `/${encodeURIComponent(r.list)}` : ''}${query ? `?${query}` : ''}`
}

export function sameOpen(a: Open | null, b: Open | null): boolean {
  return a === b || (a !== null && b !== null && a.kind === b.kind && a.id === b.id)
}

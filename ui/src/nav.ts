/**
 * The current route, followed from the URL hash, and the moves the UI makes
 * through it. Opening something keeps the page under it, so a host opened
 * from a workload's pods shows beside the workloads list.
 */
import { useSyncExternalStore } from 'react'
import { formatRoute, parseRoute, type OpenKind, type Page, type Route } from './route'

function subscribe(l: () => void) {
  window.addEventListener('hashchange', l)
  return () => window.removeEventListener('hashchange', l)
}

let cache = { hash: '', route: parseRoute('') }
function current(): Route {
  if (cache.hash !== location.hash) cache = { hash: location.hash, route: parseRoute(location.hash) }
  return cache.route
}

export function useRoute(): Route {
  return useSyncExternalStore(subscribe, current)
}

/** Goes to `r`; `replace` swaps the history entry, for moves not worth a Back (a tab in the pane). */
export function navigate(r: Route, replace = false) {
  const hash = formatRoute(r)
  if (hash === location.hash) return
  if (replace) {
    history.replaceState(null, '', `${location.pathname}${location.search}${hash}`)
    window.dispatchEvent(new HashChangeEvent('hashchange'))
  } else location.hash = hash
}

export const nav = {
  /** A page (and its list), keeping what the pane shows. */
  page: (page: Page, list: string | null = null) => {
    const r = current()
    navigate({ ...r, page, list })
  },
  list: (list: string | null) => navigate({ ...current(), list }, true),
  open: (kind: OpenKind, id: string, tab: string | null = null) => navigate({ ...current(), open: { kind, id }, tab }),
  /** Shows the pane's tab without a history entry. */
  tab: (tab: string) => navigate({ ...current(), tab }, true),
  close: () => navigate({ ...current(), open: null, tab: null }),
  host: (id: string, tab: string | null = null) => nav.open('host', id, tab),
  workload: (key: string, tab: string | null = null) => nav.open('workload', key, tab),
  switch: (id: string) => nav.open('switch', id),
}

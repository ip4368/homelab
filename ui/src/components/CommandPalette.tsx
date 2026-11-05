import { useEffect, useMemo, useRef, useState } from 'react'
import { dock, logsSession } from '../dock'
import { HEALTH_LABELS, workloadKey } from '../kube'
import { nav } from '../nav'
import { rank } from '../palette'
import { PAGE_LABELS, PAGES } from '../route'
import { store } from '../store'

interface Item {
  key: string
  group: string
  label: string
  hint?: string
  run(): void
}

/** Everything the palette can go to: pages, hosts, workloads, switches, and pods' logs. */
function items(): Item[] {
  const { view, kube } = store.getState()
  return [
    ...PAGES.map((p) => ({ key: `page:${p}`, group: 'Pages', label: PAGE_LABELS[p], run: () => nav.page(p) })),
    { key: 'page:pods', group: 'Pages', label: 'Pods', hint: 'Workloads', run: () => nav.page('workloads', 'pods') },
    { key: 'page:volumes', group: 'Pages', label: 'Volumes', hint: 'Workloads Longhorn', run: () => nav.page('workloads', 'volumes') },
    ...Object.values(view.hosts).map((h) => ({ key: `host:${h.id}`, group: 'Hosts', label: h.id, hint: `${h.lifecycle} ${h.profile.id} ${h.location}`, run: () => nav.host(h.id) })),
    ...kube.workloads.map((w) => ({
      key: `workload:${workloadKey(w)}`,
      group: 'Workloads',
      label: w.name,
      hint: `${w.namespace} ${w.kind} ${HEALTH_LABELS[w.health.state]}`,
      run: () => nav.workload(workloadKey(w)),
    })),
    ...Object.values(view.switches).map((s) => ({ key: `switch:${s.id}`, group: 'Switches', label: s.id, hint: s.model, run: () => nav.switch(s.id) })),
    ...kube.pods.map((p) => ({ key: `pod:${p.namespace}/${p.name}`, group: 'Pod logs', label: p.name, hint: `${p.namespace} ${p.status} logs`, run: () => dock.open(logsSession(p)) })),
  ]
}

/** Ctrl+K (or /): type a few letters of anything, Enter to open it. */
export function CommandPalette({ onClose }: { onClose(): void }) {
  const [query, setQuery] = useState('')
  const [at, setAt] = useState(0)
  const all = useMemo(items, [])
  const found = useMemo(() => rank(all, query, 40), [all, query])
  const list = useRef<HTMLUListElement>(null)
  useEffect(() => setAt(0), [query])
  useEffect(() => {
    list.current?.querySelector<HTMLElement>('[aria-selected="true"]')?.scrollIntoView({ block: 'nearest' })
  }, [at])

  const choose = (i: Item | undefined) => {
    if (!i) return
    onClose()
    i.run()
  }
  const onKey = (e: React.KeyboardEvent) => {
    if (e.key === 'ArrowDown') setAt((n) => Math.min(found.length - 1, n + 1))
    else if (e.key === 'ArrowUp') setAt((n) => Math.max(0, n - 1))
    else if (e.key === 'Enter') choose(found[at])
    else if (e.key === 'Escape') onClose()
    else return
    e.preventDefault()
  }

  let group = ''
  return (
    <div className="modal-backdrop palette-backdrop" onClick={onClose}>
      <div className="palette" role="dialog" aria-label="search" onClick={(e) => e.stopPropagation()}>
        <input
          autoFocus
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          onKeyDown={onKey}
          placeholder="Go to a host, workload, switch, page or pod's logs…"
          aria-label="search"
          role="combobox"
          aria-expanded
          aria-controls="palette-results"
        />
        <ul id="palette-results" ref={list} role="listbox">
          {found.map((i, n) => {
            const head = i.group !== group
            group = i.group
            return (
              <li key={i.key} role="presentation">
                {head && query === '' && <div className="palette-group">{i.group}</div>}
                <button role="option" aria-selected={n === at} className={n === at ? 'active' : undefined} onMouseMove={() => setAt(n)} onClick={() => choose(i)}>
                  <span className="palette-label">{i.label}</span>
                  <span className="palette-hint">{query === '' ? i.hint : `${i.group} · ${i.hint ?? ''}`}</span>
                </button>
              </li>
            )
          })}
          {found.length === 0 && <li className="muted palette-empty">Nothing matches.</li>}
        </ul>
        <div className="palette-foot muted">
          <kbd>↑</kbd> <kbd>↓</kbd> to move, <kbd>Enter</kbd> to open, <kbd>Esc</kbd> to close
        </div>
      </div>
    </div>
  )
}

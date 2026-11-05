import { memo, useMemo, useState } from 'react'
import type { HostObj } from '../gen/HostObj'
import type { MetricSample } from '../gen/MetricSample'
import { GROUP_BYS, groupHosts, type GroupBy } from '../grouping'
import { SYSTEM_NAMESPACES } from '../kube'
import { nav } from '../nav'
import { usePref } from '../prefs'
import { useStore } from '../store'
import { truthDiff } from '../truth'
import { FleetCanvas } from './FleetCanvas'
import { lifecycleTone } from './HostDetail'
import { Icon } from './Icons'

/** Above this many hosts the tiles are drawn on a canvas instead of a DOM tile each. */
export const CANVAS_ABOVE = 120

/** A percentage as a bar and its number; `bare` leaves the label to a column header. */
function Bar({ label, pct, bare = false }: { label: string; pct: number | undefined; bare?: boolean }) {
  const value = pct === undefined ? 0 : Math.max(0, Math.min(100, pct))
  return (
    <div className={`meter${bare ? ' bare' : ''}`} title={pct === undefined ? `${label}: no sample yet` : `${label} ${value.toFixed(0)}%`}>
      {!bare && <span className="meter-label">{label}</span>}
      <div className="bar">
        <div className={`bar-fill ${value > 90 ? 'bad' : value > 75 ? 'warn' : ''}`} style={{ width: `${value}%` }} />
      </div>
      <span className="meter-value">{pct === undefined ? '–' : `${value.toFixed(0)}%`}</span>
    </div>
  )
}

function k3sLabel(h: HostObj): string {
  const k = h.kube
  if (!k || k.phase === 'Absent' || !k.role) return 'no K3s yet'
  if (typeof k.phase === 'object') return `K3s ${k.role.toLowerCase()} failed`
  if (k.phase === 'Installing') return `K3s ${k.role.toLowerCase()} installing`
  return `K3s ${k.role.toLowerCase()}${k.node_ready ? '' : ', node not Ready'}`
}

interface TileProps {
  host: HostObj
  metric: MetricSample | undefined
  pods: number
  leader: boolean
  selected: boolean
  /** Set when the ground truth contradicts the view of this host: what the truth says. */
  mismatch: string | undefined
}

const Tile = memo(function Tile({ host, metric, pods, leader, selected, mismatch }: TileProps) {
  return (
    <button
      className={`tile lc-${host.lifecycle}${selected ? ' selected' : ''}${mismatch ? ' truth-diff' : ''}`}
      onClick={() => nav.host(host.id)}
      title={mismatch ? `${host.id} — ${host.lifecycle} in the view, but ${mismatch}` : `${host.id} — ${host.lifecycle}`}
    >
      <span className="tile-head">
        <span className="tile-id">{host.id}</span>
        {leader && (
          <span className="leader-mark" title="Leader: holds the VIP">
            VIP
          </span>
        )}
        <span className={`pill ${lifecycleTone(host.lifecycle)}`}>{host.lifecycle}</span>
      </span>
      <span className="tile-sub">
        {host.profile.id} · {host.location}
      </span>
      <span className="tile-roles">
        {host.roles.map((r) => (
          <span key={r} className="badge">
            {r}
          </span>
        ))}
      </span>
      <Bar label="CPU" pct={metric?.cpu_pct} />
      <Bar label="RAM" pct={metric?.ram_pct} />
      <span className="tile-foot">
        {k3sLabel(host)} · {pods} pod{pods === 1 ? '' : 's'}
      </span>
    </button>
  )
})

type SortKey = 'id' | 'lifecycle' | 'cpu' | 'ram' | 'pods' | 'location'

/**
 * Every host, as tiles grouped by rack, switch, role or profile, or as a
 * sortable table. Clicking one opens it in the detail pane.
 */
export function HostsPage({ selected, showTruth }: { selected: string | null; showTruth: boolean }) {
  const hosts = useStore((s) => s.view.hosts)
  // The truth changes with every push; only follow it while the overlay shows it.
  const truth = useStore((s) => (showTruth ? s.truth : null))
  const metrics = useStore((s) => s.view.metrics)
  const kubePods = useStore((s) => s.kube.pods)
  const leader = useStore((s) => s.view.leader)
  const [by, setBy] = usePref<GroupBy>('hostsGroupBy', 'rack')
  const [mode, setMode] = usePref<'tiles' | 'table'>('hostsView', 'tiles')
  const [text, setText] = useState('')
  const [problems, setProblems] = useState(false)
  const [sort, setSort] = useState<{ key: SortKey; desc: boolean }>({ key: 'id', desc: false })

  const mismatches = useMemo(
    () => (truth ? new Map(truthDiff({ hosts }, truth).map((d) => [d.hostId, d.actual])) : new Map<string, string>()),
    [hosts, truth],
  )
  const podCounts = useMemo(() => {
    const m = new Map<string, number>()
    for (const p of kubePods) if (p.node && !SYSTEM_NAMESPACES.includes(p.namespace)) m.set(p.node, (m.get(p.node) ?? 0) + 1)
    return m
  }, [kubePods])
  const shown = useMemo(() => {
    const t = text.trim().toLowerCase()
    return Object.values(hosts).filter(
      (h) =>
        (!t || h.id.toLowerCase().includes(t) || h.location.toLowerCase().includes(t) || h.roles.some((r) => r.toLowerCase().includes(t)) || h.profile.id.toLowerCase().includes(t)) &&
        (!problems || h.lifecycle !== 'Ready' || mismatches.has(h.id)),
    )
  }, [hosts, text, problems, mismatches])
  const groups = useMemo(() => groupHosts(shown, by), [shown, by])
  const rows = useMemo(() => {
    const val = (h: HostObj): string | number => {
      switch (sort.key) {
        case 'id':
          return h.id
        case 'lifecycle':
          return h.lifecycle
        case 'location':
          return h.location
        case 'cpu':
          return metrics[h.id]?.cpu_pct ?? -1
        case 'ram':
          return metrics[h.id]?.ram_pct ?? -1
        case 'pods':
          return podCounts.get(h.id) ?? 0
      }
    }
    const sorted = [...shown].sort((a, b) => {
      const x = val(a)
      const y = val(b)
      return typeof x === 'number' && typeof y === 'number' ? x - y : String(x).localeCompare(String(y), 'en', { numeric: true })
    })
    return sort.desc ? sorted.reverse() : sorted
  }, [shown, sort, metrics, podCounts])
  const canvas = mode === 'tiles' && shown.length > CANVAS_ABOVE
  const counts = useMemo(() => {
    const all = Object.values(hosts)
    return { ready: all.filter((h) => h.lifecycle === 'Ready').length, other: all.filter((h) => h.lifecycle !== 'Ready').length }
  }, [hosts])

  const th = (key: SortKey, label: string, num = false, cls = '') => (
    <th className={`${num ? 'num' : ''} ${cls}`.trim() || undefined} aria-sort={sort.key === key ? (sort.desc ? 'descending' : 'ascending') : undefined}>
      <button className="sort" onClick={() => setSort({ key, desc: sort.key === key ? !sort.desc : num })}>
        {label}
        {sort.key === key && <span aria-hidden>{sort.desc ? ' ↓' : ' ↑'}</span>}
      </button>
    </th>
  )

  return (
    <section className="hosts-page">
      <div className="toolbar">
        <div className="seg" role="group" aria-label="view">
          <button className={mode === 'tiles' ? 'active' : ''} aria-pressed={mode === 'tiles'} onClick={() => setMode('tiles')}>
            <Icon name="grid" size={14} /> Tiles
          </button>
          <button className={mode === 'table' ? 'active' : ''} aria-pressed={mode === 'table'} onClick={() => setMode('table')}>
            <Icon name="list" size={14} /> Table
          </button>
        </div>
        {mode === 'tiles' && (
          <label className="inline">
            Group by{' '}
            <select value={by} onChange={(e) => setBy(e.target.value as GroupBy)}>
              {GROUP_BYS.map((g) => (
                <option key={g}>{g}</option>
              ))}
            </select>
          </label>
        )}
        <input type="search" placeholder="Filter by name, rack, role…" value={text} onChange={(e) => setText(e.target.value)} aria-label="filter hosts" />
        <label className={`chip-toggle${problems ? ' on' : ''}`}>
          <input type="checkbox" checked={problems} onChange={(e) => setProblems(e.target.checked)} /> Not Ready only
        </label>
        <span className="muted">
          {counts.ready} Ready{counts.other > 0 && `, ${counts.other} other`}
          {shown.length !== Object.keys(hosts).length && ` · ${shown.length} shown`}
        </span>
        {showTruth && (
          <span className="truth-legend">
            <span className="truth-swatch" /> hatched: the view and the ground truth disagree ({mismatches.size})
          </span>
        )}
      </div>
      {Object.keys(hosts).length === 0 && <p className="empty">No hosts yet. Create a join token in the activity panel (Joins) and run the join command on a machine.</p>}
      {canvas && <FleetCanvas groups={groups} metrics={metrics} selected={selected} mismatches={mismatches} onSelect={(id) => nav.host(id)} />}
      {mode === 'tiles' &&
        !canvas &&
        groups.map((g) => (
          <div key={g.key} className="group">
            <h3>
              {g.key} <span className="muted">{g.hosts.length}</span>
            </h3>
            <div className="tiles">
              {g.hosts.map((h) => (
                <Tile key={h.id} host={h} metric={metrics[h.id]} pods={podCounts.get(h.id) ?? 0} leader={leader === h.id} selected={h.id === selected} mismatch={mismatches.get(h.id)} />
              ))}
            </div>
          </div>
        ))}
      {mode === 'table' && (
        <div className="table-wrap card flush">
          <table className="grid-table">
            <thead>
              <tr>
                {th('id', 'Host')}
                {th('lifecycle', 'State')}
                <th className="hide-narrow">Roles</th>
                {th('location', 'Location', false, 'hide-compact')}
                {th('cpu', 'CPU', true)}
                {th('ram', 'RAM', true)}
                <th className="hide-narrow">Kubernetes</th>
                {th('pods', 'Pods', true, 'hide-compact')}
                <th className="hide-narrow">Network</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((h) => {
                const m = metrics[h.id]
                const mismatch = mismatches.get(h.id)
                return (
                  <tr key={h.id} className={`clickable${h.id === selected ? ' selected' : ''}${mismatch ? ' truth-diff' : ''}`} onClick={() => nav.host(h.id)} title={mismatch ? `the ground truth: ${mismatch}` : undefined}>
                    <td>
                      <span className="cell-title">{h.id}</span>
                      {leader === h.id && <span className="leader-mark">VIP</span>}
                      <div className="cell-sub">{h.profile.id}</div>
                    </td>
                    <td>
                      <span className={`pill ${lifecycleTone(h.lifecycle)}`}>{h.lifecycle}</span>
                    </td>
                    <td className="hide-narrow">
                      {h.roles.map((r) => (
                        <span key={r} className="badge">
                          {r}
                        </span>
                      ))}
                    </td>
                    <td className="hide-compact">{h.location}</td>
                    <td className="num">
                      <Bar label="CPU" pct={m?.cpu_pct} bare />
                    </td>
                    <td className="num">
                      <Bar label="RAM" pct={m?.ram_pct} bare />
                    </td>
                    <td className="hide-narrow muted">{k3sLabel(h)}</td>
                    <td className="num hide-compact">{podCounts.get(h.id) ?? 0}</td>
                    <td className={`hide-narrow ${h.net.state === 'Applied' ? 'muted' : h.net.state === 'Failed' ? 'bad' : 'warn'}`}>
                      {h.net.state}
                      {h.net.issues.length > 0 && <span className="bad"> · {h.net.issues.length} issue{h.net.issues.length === 1 ? '' : 's'}</span>}
                    </td>
                  </tr>
                )
              })}
              {rows.length === 0 && Object.keys(hosts).length > 0 && (
                <tr>
                  <td colSpan={9} className="muted">
                    Nothing matches the filter.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      )}
    </section>
  )
}


import { memo, useMemo, useState } from 'react'
import type { HostObj } from '../gen/HostObj'
import type { MetricSample } from '../gen/MetricSample'
import { GROUP_BYS, groupHosts, type GroupBy } from '../grouping'
import { useStore } from '../store'
import { truthDiff } from '../truth'
import { FleetCanvas } from './FleetCanvas'

/** Above this many hosts the grid is drawn on a canvas instead of a DOM tile each. */
export const CANVAS_ABOVE = 120

function Bar({ label, pct }: { label: string; pct: number | undefined }) {
  const value = pct === undefined ? 0 : Math.max(0, Math.min(100, pct))
  return (
    <div className="bar" title={pct === undefined ? `${label}: no sample yet` : `${label} ${value.toFixed(0)}%`}>
      <div className={`bar-fill ${value > 90 ? 'bad' : value > 75 ? 'warn' : ''}`} style={{ width: `${value}%` }} />
    </div>
  )
}

interface TileProps {
  host: HostObj
  metric: MetricSample | undefined
  selected: boolean
  /** Set when the ground truth contradicts the view of this host: what the truth says. */
  mismatch: string | undefined
  onSelect(id: string): void
}

const Tile = memo(function Tile({ host, metric, selected, mismatch, onSelect }: TileProps) {
  return (
    <button
      className={`tile lc-${host.lifecycle}${selected ? ' selected' : ''}${mismatch ? ' truth-diff' : ''}`}
      onClick={() => onSelect(host.id)}
      title={mismatch ? `${host.id} — ${host.lifecycle} in the view, but ${mismatch}` : `${host.id} — ${host.lifecycle}`}
    >
      <span className="tile-id">{host.id}</span>
      <span className="tile-roles">
        {host.roles.map((r) => (
          <span key={r} className="badge">
            {r}
          </span>
        ))}
      </span>
      <span className="tile-metrics">
        <span>CPU {metric ? `${metric.cpu_pct.toFixed(0)}%` : '—'}</span>
        <span>RAM {metric ? `${metric.ram_pct.toFixed(0)}%` : '—'}</span>
      </span>
      <Bar label="CPU" pct={metric?.cpu_pct} />
      <Bar label="RAM" pct={metric?.ram_pct} />
    </button>
  )
})

export function FleetGrid({ selected, showTruth, onSelect }: { selected: string | null; showTruth: boolean; onSelect(id: string): void }) {
  const hosts = useStore((s) => s.view.hosts)
  // The truth changes with every push; only follow it while the overlay shows it.
  const truth = useStore((s) => (showTruth ? s.truth : null))
  const metrics = useStore((s) => s.view.metrics)
  const [by, setBy] = useState<GroupBy>('rack')
  const mismatches = useMemo(
    () => (truth ? new Map(truthDiff({ hosts }, truth).map((d) => [d.hostId, d.actual])) : new Map<string, string>()),
    [hosts, truth],
  )
  const groups = useMemo(() => groupHosts(Object.values(hosts), by), [hosts, by])
  const canvas = Object.keys(hosts).length > CANVAS_ABOVE
  const counts = useMemo(() => {
    const values = Object.values(hosts)
    return {
      ready: values.filter((h) => h.lifecycle === 'Ready').length,
      notReady: values.filter((h) => h.lifecycle === 'NotReady').length,
      pending: values.filter((h) => h.lifecycle === 'Pending').length,
      busy: values.filter((h) => (metrics[h.id]?.cpu_pct ?? 0) >= 75 || (metrics[h.id]?.ram_pct ?? 0) >= 75).length,
    }
  }, [hosts, metrics])

  return (
    <section className="fleet workspace-page">
      <div className="page-heading compact-heading">
        <div>
          <span className="eyebrow">Infrastructure</span>
          <h2>Fleet</h2>
          <p className="muted">Select a machine to inspect it beside the fleet, without losing your place.</p>
        </div>
        <div className="summary-pills" aria-label="Fleet summary">
          <span><b className="ok">{counts.ready}</b> ready</span>
          <span><b className={counts.notReady ? 'bad' : ''}>{counts.notReady}</b> not ready</span>
          <span><b className={counts.pending ? 'warn' : ''}>{counts.pending}</b> pending</span>
          <span><b className={counts.busy ? 'warn' : ''}>{counts.busy}</b> busy</span>
        </div>
      </div>
      <div className="fleet-bar toolbar-card">
        <label>
          Group by{' '}
          <select value={by} onChange={(e) => setBy(e.target.value as GroupBy)}>
            {GROUP_BYS.map((g) => (
              <option key={g}>{g}</option>
            ))}
          </select>
        </label>
        <span className="muted">{Object.keys(hosts).length} hosts · {groups.length} groups</span>
        {showTruth && (
          <span className="truth-legend">
            <span className="truth-swatch" /> hatched: the view and the ground truth disagree ({mismatches.size})
          </span>
        )}
      </div>
      {groups.length === 0 && <p className="muted">No hosts yet.</p>}
      {canvas && <FleetCanvas groups={groups} metrics={metrics} selected={selected} mismatches={mismatches} onSelect={onSelect} />}
      {!canvas && (
        <div className="fleet-groups">
          {groups.map((g) => (
            <div key={g.key} className="group">
              <h3>
                {g.key} <span className="muted">({g.hosts.length})</span>
              </h3>
              <div className="tiles">
                {g.hosts.map((h) => (
                  <Tile key={h.id} host={h} metric={metrics[h.id]} selected={h.id === selected} mismatch={mismatches.get(h.id)} onSelect={onSelect} />
                ))}
              </div>
            </div>
          ))}
        </div>
      )}
    </section>
  )
}

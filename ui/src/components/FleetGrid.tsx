import { memo, useMemo, useState } from 'react'
import type { HostObj } from '../gen/HostObj'
import type { MetricSample } from '../gen/MetricSample'
import { GROUP_BYS, groupHosts, type GroupBy } from '../grouping'
import { useStore } from '../store'
import { truthDiff } from '../workloads'

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
      <Bar label="CPU" pct={metric?.cpu_pct} />
      <Bar label="RAM" pct={metric?.ram_pct} />
    </button>
  )
})

export function FleetGrid({ selected, showTruth, onSelect }: { selected: string | null; showTruth: boolean; onSelect(id: string): void }) {
  const hosts = useStore((s) => s.view.hosts)
  const truth = useStore((s) => s.truth)
  const metrics = useStore((s) => s.view.metrics)
  const [by, setBy] = useState<GroupBy>('rack')
  const mismatches = useMemo(
    () => (showTruth ? new Map(truthDiff({ hosts }, truth).map((d) => [d.hostId, d.actual])) : new Map<string, string>()),
    [showTruth, hosts, truth],
  )
  const groups = useMemo(() => groupHosts(Object.values(hosts), by), [hosts, by])

  return (
    <section className="fleet">
      <div className="fleet-bar">
        <label>
          Group by{' '}
          <select value={by} onChange={(e) => setBy(e.target.value as GroupBy)}>
            {GROUP_BYS.map((g) => (
              <option key={g}>{g}</option>
            ))}
          </select>
        </label>
        <span className="muted">{Object.keys(hosts).length} hosts</span>
        {showTruth && (
          <span className="truth-legend">
            <span className="truth-swatch" /> hatched: the view and the ground truth disagree ({mismatches.size})
          </span>
        )}
      </div>
      {groups.length === 0 && <p className="muted">No hosts yet.</p>}
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
    </section>
  )
}

import { useMemo, useState } from 'react'
import { DEFAULT_COLLAPSE_OVER, NODE_H, NODE_W, layoutTopology, utilisationBand, type TopoEdge, type TopoNode } from '../topology'
import { useStore } from '../store'

const SIZE: Record<TopoNode['kind'], { w: number; h: number }> = {
  core: { w: 120, h: NODE_H + 6 },
  access: { w: 110, h: NODE_H + 4 },
  host: { w: NODE_W, h: NODE_H },
  group: { w: 140, h: 44 },
}

/** Paint order: healthy links underneath, problems on top. */
const ORDER = { up: 0, down: 1, mismatch: 2 }

function edgeTitle(e: TopoEdge): string {
  const ports = e.hostPort ? `${e.a} ${e.hostPort} - ${e.b} ${e.switchPort}` : `${e.a} - ${e.b}${e.switchPort ? ` (${e.switchPort})` : ''}`
  const state = e.state === 'up' ? `${Math.round(e.utilisation * 100)}% used` : e.state === 'down' ? 'link down' : 'config mismatch'
  return `${ports}: ${state}`
}

function Legend() {
  return (
    <ul className="topo-legend">
      <li>
        <svg width="28" height="8" aria-hidden>
          <line x1="0" x2="28" y1="4" y2="4" className="edge low" />
        </svg>
        0-50%
      </li>
      <li>
        <svg width="28" height="8" aria-hidden>
          <line x1="0" x2="28" y1="4" y2="4" className="edge mid" />
        </svg>
        50-80%
      </li>
      <li>
        <svg width="28" height="8" aria-hidden>
          <line x1="0" x2="28" y1="4" y2="4" className="edge high" />
        </svg>
        over 80%
      </li>
      <li>
        <svg width="28" height="8" aria-hidden>
          <line x1="0" x2="28" y1="4" y2="4" className="edge mismatch" />
        </svg>
        mismatch
      </li>
      <li>
        <svg width="28" height="8" aria-hidden>
          <line x1="0" x2="28" y1="4" y2="4" className="edge down" />
        </svg>
        link down
      </li>
    </ul>
  )
}

interface Props {
  selectedSwitch: string | null
  selectedHost: string | null
  onSelectSwitch(id: string): void
  onSelectHost(id: string): void
}

export function Topology({ selectedSwitch, selectedHost, onSelectSwitch, onSelectHost }: Props) {
  const switches = useStore((s) => s.view.switches)
  const hosts = useStore((s) => s.view.hosts)
  const ports = useStore((s) => s.truth.ports)
  const [expand, setExpand] = useState(false)
  const layout = useMemo(
    () =>
      layoutTopology(Object.values(switches), Object.values(hosts), {
        collapseOver: expand ? Infinity : DEFAULT_COLLAPSE_OVER,
        ports,
      }),
    [switches, hosts, ports, expand],
  )
  const at = new Map(layout.nodes.map((n) => [n.id, n]))
  const width = Math.max(600, ...layout.nodes.map((n) => n.x + SIZE[n.kind].w / 2 + 40))
  const height = Math.max(300, ...layout.nodes.map((n) => n.y + SIZE[n.kind].h + 40))
  const edges = [...layout.edges].sort((p, q) => ORDER[p.state] - ORDER[q.state])

  const busiest = layout.edges.reduce((m, e) => Math.max(m, e.utilisation), 0)
  const mismatches = layout.edges.filter((e) => e.state === 'mismatch').length

  return (
    <section className="topology">
      <div className="fleet-bar">
        <Legend />
        <label>
          <input type="checkbox" checked={expand} onChange={(e) => setExpand(e.target.checked)} /> Expand large switches
        </label>
        <span className="muted">
          {Object.keys(switches).length} switches, busiest link {Math.round(busiest * 100)}%
          {mismatches > 0 && <span className="bad"> , {mismatches} mismatched</span>}
        </span>
      </div>
      {layout.nodes.length === 0 && <p className="muted">No switches observed yet.</p>}
      <div className="topo-scroll">
        <svg width={width} height={height} role="img" aria-label="network topology">
          {edges.map((e) => {
            const a = at.get(e.a)!
            const b = at.get(e.b)!
            const cls = e.state === 'up' ? utilisationBand(e.utilisation) : e.state
            return (
              <g key={`${e.a}|${e.b}|${e.hostPort ?? ''}`}>
                <line className={`edge ${cls}`} x1={a.x} y1={a.y} x2={b.x} y2={b.y}>
                  <title>{edgeTitle(e)}</title>
                </line>
                {e.state === 'mismatch' && (
                  <g className="edge-mark" transform={`translate(${(a.x + b.x) / 2} ${(a.y + b.y) / 2})`}>
                    <circle r="8" />
                    <text textAnchor="middle" dominantBaseline="central">
                      !
                    </text>
                  </g>
                )}
              </g>
            )
          })}
          {layout.nodes.map((n) => {
            const { w, h } = SIZE[n.kind]
            const isSwitch = n.kind === 'core' || n.kind === 'access'
            const host = n.kind === 'host' ? hosts[n.id] : undefined
            const selected = n.id === selectedSwitch || n.id === selectedHost
            const click = () => (isSwitch ? onSelectSwitch(n.id) : n.kind === 'host' ? onSelectHost(n.id) : onSelectSwitch(n.id.replace(/^group:/, '')))
            const problems = n.kind === 'group' ? [n.down ? `${n.down} down` : '', n.mismatched ? `${n.mismatched} mismatch` : ''].filter(Boolean).join(', ') : ''
            return (
              <g key={n.id} className={`topo-node ${n.kind}${host ? ` lc-${host.lifecycle}` : ''}${selected ? ' selected' : ''}`} onClick={click}>
                <title>
                  {n.kind === 'group' ? `${n.label} on ${n.id.replace(/^group:/, '')}; tick "Expand large switches" to see them` : n.label}
                </title>
                <rect x={n.x - w / 2} y={n.y - h / 2} width={w} height={h} rx={4} />
                <text x={n.x} y={n.kind === 'group' ? n.y - 5 : n.y} textAnchor="middle" dominantBaseline="central">
                  {n.label}
                </text>
                {n.kind === 'group' && (
                  <text className={problems ? 'bad' : 'muted'} x={n.x} y={n.y + 11} textAnchor="middle" dominantBaseline="central">
                    {problems || 'all links ok'}
                  </text>
                )}
              </g>
            )
          })}
        </svg>
      </div>
      <p className="muted">Link colour is the busier direction over link capacity. Click a switch for its ports.</p>
    </section>
  )
}


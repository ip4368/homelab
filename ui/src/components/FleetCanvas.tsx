import { useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react'
import { hitIndex, tileLayout, type TileLayout } from '../canvasLayout'
import type { HostObj } from '../gen/HostObj'
import type { Lifecycle } from '../gen/Lifecycle'
import type { MetricSample } from '../gen/MetricSample'
import type { Role } from '../gen/Role'
import type { HostGroup } from '../grouping'

/** The lifecycle strip down a tile's left edge, as in the DOM grid's stylesheet. */
const LIFECYCLE: Record<Lifecycle, string> = {
  Ready: '#17803d',
  NotReady: '#c0261c',
  Pending: '#b45309',
  Approved: '#b45309',
  Provisioning: '#b45309',
  Cordoned: '#7c5cc4',
  Removed: '#667085',
}
/**
 * Role pips along a tile's top, each role always in its own slot, coloured
 * from the dataviz reference palette's first five categorical slots in order
 * (validated: adjacent CVD ΔE ≥ 9.1). Slot position and the tooltip carry the
 * role too, so colour is never the only cue.
 */
const ROLES: { role: Role; label: string; color: string }[] = [
  { role: 'ControlPlane', label: 'CP', color: '#2a78d6' },
  { role: 'Storage', label: 'Storage', color: '#eb6834' },
  { role: 'Vm', label: 'VM', color: '#1baf7a' },
  { role: 'Compute', label: 'Compute', color: '#eda100' },
  { role: 'Transcode', label: 'Transcode', color: '#e87ba4' },
]
const C = { panel: '#ffffff', notReady: '#fdecea', border: '#d9dde5', track: '#e8ecf5', accent: '#2456c9', warn: '#b45309', bad: '#c0261c', text: '#1c2330', muted: '#667085' }

const barColor = (pct: number) => (pct > 90 ? C.bad : pct > 75 ? C.warn : C.accent)

const hatches = new WeakMap<CanvasRenderingContext2D, CanvasPattern | null>()

/** Diagonal red hatching, like the DOM grid's `.truth-diff`; made once per canvas. */
function hatch(ctx: CanvasRenderingContext2D): CanvasPattern | null {
  if (!hatches.has(ctx)) hatches.set(ctx, makeHatch(ctx))
  return hatches.get(ctx) ?? null
}

function makeHatch(ctx: CanvasRenderingContext2D): CanvasPattern | null {
  const tile = document.createElement('canvas')
  tile.width = tile.height = 6
  const t = tile.getContext('2d')
  if (!t) return null
  t.strokeStyle = 'rgba(192, 38, 28, 0.55)'
  t.lineWidth = 2
  t.beginPath()
  t.moveTo(-1, 7)
  t.lineTo(7, -1)
  t.stroke()
  return ctx.createPattern(tile, 'repeat')
}

interface Paint {
  layout: TileLayout
  hosts: Map<string, HostObj>
  metrics: Record<string, MetricSample>
  selected: string | null
  /** The index of the hovered tile in `layout.rects`. */
  hovered: number | null
  mismatches: Map<string, string>
}

function draw(ctx: CanvasRenderingContext2D, width: number, p: Paint) {
  ctx.clearRect(0, 0, width, p.layout.height)
  ctx.textBaseline = 'alphabetic'
  for (const h of p.layout.headers) {
    ctx.font = '600 12px system-ui, sans-serif'
    ctx.fillStyle = C.text
    const label = h.key.toUpperCase()
    ctx.fillText(label, h.x, h.y + 14)
    ctx.font = '12px system-ui, sans-serif'
    ctx.fillStyle = C.muted
    ctx.fillText(` (${h.count})`, h.x + ctx.measureText(label).width + 2, h.y + 14)
  }
  const pattern = p.mismatches.size > 0 ? hatch(ctx) : null
  p.layout.rects.forEach((r, i) => {
    const host = p.hosts.get(r.hostId)
    if (!host) return
    const m = p.metrics[r.hostId]
    ctx.globalAlpha = host.lifecycle === 'Removed' ? 0.6 : 1
    ctx.fillStyle = host.lifecycle === 'NotReady' ? C.notReady : C.panel
    ctx.fillRect(r.x, r.y, r.w, r.h)
    ctx.strokeStyle = C.border
    ctx.lineWidth = 1
    ctx.strokeRect(r.x + 0.5, r.y + 0.5, r.w - 1, r.h - 1)
    ctx.fillStyle = LIFECYCLE[host.lifecycle]
    ctx.fillRect(r.x, r.y, 3, r.h)
    ROLES.forEach(({ role, color }, i) => {
      if (!host.roles.includes(role)) return
      ctx.fillStyle = color
      ctx.fillRect(r.x + 4 + i * 3, r.y + 3, 2, 3)
    })
    const bar = (pct: number | undefined, y: number) => {
      const w = r.w - 5
      ctx.fillStyle = C.track
      ctx.fillRect(r.x + 4, y, w, 3)
      if (pct === undefined) return
      const v = Math.max(0, Math.min(100, pct))
      ctx.fillStyle = barColor(v)
      ctx.fillRect(r.x + 4, y, (w * v) / 100, 3)
    }
    bar(m?.cpu_pct, r.y + r.h - 8)
    bar(m?.ram_pct, r.y + r.h - 4)
    ctx.globalAlpha = 1
    if (pattern && p.mismatches.has(r.hostId)) {
      ctx.fillStyle = pattern
      ctx.fillRect(r.x, r.y, r.w, r.h)
      ctx.setLineDash([3, 2])
      ctx.strokeStyle = C.bad
      ctx.lineWidth = 2
      ctx.strokeRect(r.x + 1, r.y + 1, r.w - 2, r.h - 2)
      ctx.setLineDash([])
    }
    if (r.hostId === p.selected) {
      ctx.strokeStyle = C.accent
      ctx.lineWidth = 2
      ctx.strokeRect(r.x - 1, r.y - 1, r.w + 2, r.h + 2)
    } else if (i === p.hovered) {
      ctx.strokeStyle = C.text
      ctx.lineWidth = 1
      ctx.strokeRect(r.x - 0.5, r.y - 0.5, r.w + 1, r.h + 1)
    }
  })
}

function Tooltip({ host, metric, mismatch, x, y }: { host: HostObj; metric: MetricSample | undefined; mismatch: string | undefined; x: number; y: number }) {
  const pct = (v: number | undefined) => (v === undefined ? 'no sample yet' : `${v.toFixed(0)}%`)
  return (
    <div className="canvas-tip" style={{ left: x, top: y }} role="tooltip">
      <strong>{host.id}</strong> — {host.lifecycle}
      <div>{host.roles.length > 0 ? host.roles.join(', ') : 'no roles'}</div>
      <div>
        CPU {pct(metric?.cpu_pct)} · RAM {pct(metric?.ram_pct)}
      </div>
      {mismatch && <div className="bad">ground truth: {mismatch}</div>}
    </div>
  )
}

export interface FleetCanvasProps {
  groups: HostGroup[]
  metrics: Record<string, MetricSample>
  selected: string | null
  /** Hosts whose view the ground truth contradicts: what the truth says. */
  mismatches: Map<string, string>
  onSelect(id: string): void
}

/** The fleet grid drawn on one canvas, for fleets too big for a DOM tile each. */
export function FleetCanvas({ groups, metrics, selected, mismatches, onSelect }: FleetCanvasProps) {
  const wrap = useRef<HTMLDivElement>(null)
  const canvas = useRef<HTMLCanvasElement>(null)
  const [width, setWidth] = useState(0)
  // The hovered tile, by index: a host grouped by role has a tile in each of its role groups.
  const [hovered, setHovered] = useState<number | null>(null)
  useLayoutEffect(() => {
    const el = wrap.current
    if (!el) return
    setWidth(el.clientWidth)
    const resize = new ResizeObserver(() => setWidth(el.clientWidth))
    resize.observe(el)
    return () => resize.disconnect()
  }, [])
  const layout = useMemo(() => tileLayout(groups, width), [groups, width])
  // A new layout moves the tiles: the pointer is over a different one, if any.
  useEffect(() => setHovered(null), [layout])
  const hosts = useMemo(() => new Map(groups.flatMap((g) => g.hosts.map((h) => [h.id, h] as const))), [groups])

  useEffect(() => {
    const el = canvas.current
    const ctx = el?.getContext('2d')
    if (!el || !ctx || width === 0) return
    const dpr = window.devicePixelRatio || 1
    const [w, h] = [Math.round(width * dpr), Math.round(layout.height * dpr)]
    if (el.width !== w || el.height !== h) {
      el.width = w
      el.height = h
    }
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0)
    draw(ctx, width, { layout, hosts, metrics, selected, hovered, mismatches })
  }, [layout, width, hosts, metrics, selected, hovered, mismatches])

  const at = (e: React.MouseEvent<HTMLCanvasElement>) => {
    const box = e.currentTarget.getBoundingClientRect()
    return hitIndex(layout.rects, e.clientX - box.left, e.clientY - box.top)
  }
  const tip = hovered === null ? undefined : layout.rects[hovered]
  const tipHost = tip && hosts.get(tip.hostId)

  return (
    <div className="fleet-canvas" ref={wrap}>
      <div className="role-legend" aria-label="role pips">
        {ROLES.map(({ role, label, color }) => (
          <span key={role}>
            <span className="role-pip" style={{ background: color }} /> {label}
          </span>
        ))}
      </div>
      <div className="fleet-canvas-area">
        <canvas
          ref={canvas}
          role="img"
          aria-label={`${hosts.size} hosts; hover a tile for details, click to open it`}
          style={{ width, height: layout.height, cursor: hovered === null ? 'default' : 'pointer' }}
          onMouseMove={(e) => {
            const i = at(e)
            if (i !== hovered) setHovered(i)
          }}
          onMouseLeave={() => setHovered(null)}
          onClick={(e) => {
            const i = at(e)
            if (i !== null) onSelect(layout.rects[i].hostId)
          }}
        />
        {tip && tipHost && (
          <Tooltip host={tipHost} metric={metrics[tipHost.id]} mismatch={mismatches.get(tipHost.id)} x={Math.min(tip.x + tip.w + 6, Math.max(0, width - 220))} y={tip.y + tip.h + 4} />
        )}
      </div>
    </div>
  )
}

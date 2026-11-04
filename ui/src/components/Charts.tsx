import { useEffect, useMemo, useRef, useState } from 'react'
import uPlot from 'uplot'
import 'uplot/dist/uPlot.min.css'
import { alignSeries, type Aligned, fetchKubeMetrics, fetchMetrics, foldOutcome, formatBps, metricsSince, startPolling, type MetricsOutcome, type MetricsView } from '../metrics'
import { byteRate, cpu as cpuFmt, mem as memFmt } from '../kube'
import { formatClock } from '../clock'

/** How often a visible chart asks for fresh series. */
export const POLL_MS = 5000
const HEIGHT = 140
/** Categorical slots 1 and 2 of the dataviz reference palette (validated: CVD ΔE 9.1+, normal ΔE 19.6+). */
const SERIES = ['#2a78d6', '#eb6834']
const GRID = '#eceef2'
const AXIS_TEXT = '#667085'
/** Time-axis tick steps, in ms. */
const TIME_STEPS = [10, 30, 60, 120, 300, 600, 900, 1800, 3600].map((s) => s * 1000)
/** Rows in a chart's table view. */
const TABLE_ROWS = 6

type Unit = 'pct' | 'bps' | 'milli' | 'bytes' | 'bytesRate'

interface ChartSpec {
  title: string
  unit: Unit
  series: { metric: string; label: string }[]
}

/** One host, or a rollup: average CPU and RAM, total port throughput. */
const CHARTS: ChartSpec[] = [
  { title: 'CPU', unit: 'pct', series: [{ metric: 'cpu_pct', label: 'CPU' }] },
  { title: 'RAM', unit: 'pct', series: [{ metric: 'ram_pct', label: 'RAM' }] },
  {
    title: 'Port throughput',
    unit: 'bps',
    series: [
      { metric: 'port_rx_bps', label: 'rx' },
      { metric: 'port_tx_bps', label: 'tx' },
    ],
  },
]
const METRIC_NAMES = CHARTS.flatMap((c) => c.series.map((s) => s.metric))

/** A pod's or workload's usage: CPU, memory, and network from the kubelet's counters. */
const KUBE_CHARTS: ChartSpec[] = [
  { title: 'CPU', unit: 'milli', series: [{ metric: 'cpu_milli', label: 'CPU' }] },
  { title: 'Memory', unit: 'bytes', series: [{ metric: 'memory_bytes', label: 'memory' }] },
  {
    title: 'Network',
    unit: 'bytesRate',
    series: [
      { metric: 'net_rx_bps', label: 'rx' },
      { metric: 'net_tx_bps', label: 'tx' },
    ],
  },
]
const KUBE_METRIC_NAMES = KUBE_CHARTS.flatMap((c) => c.series.map((s) => s.metric))

const format = (unit: Unit, v: number | null | undefined) =>
  v === null || v === undefined ? '–' : unit === 'pct' ? `${v.toFixed(0)}%` : unit === 'bps' ? formatBps(v) : unit === 'milli' ? cpuFmt(v) : unit === 'bytes' ? memFmt(v) : byteRate(v)

/** Polls series every [`POLL_MS`] while mounted, keeping the last answer through pauses and errors. `key` restarts the poll; `load` is read fresh each tick. */
function useSeries(key: string, load: () => Promise<MetricsOutcome>): MetricsView {
  const [view, setView] = useState<MetricsView>({ reply: null, note: null })
  const latest = useRef(load)
  latest.current = load
  useEffect(() => {
    setView({ reply: null, note: null })
    return startPolling(
      () => latest.current(),
      (outcome) => setView((prev) => foldOutcome(prev, outcome)),
      POLL_MS,
    )
  }, [key])
  return view
}

function options(spec: ChartSpec, width: number): uPlot.Options {
  const axis = { stroke: AXIS_TEXT, font: '11px system-ui', grid: { stroke: GRID, width: 1 }, ticks: { stroke: GRID, width: 1 } }
  return {
    width,
    height: HEIGHT,
    cursor: { drag: { x: false, y: false } },
    legend: { live: true },
    scales: {
      x: { time: false },
      // Rates start at zero and end on a round tick.
      y: spec.unit === 'pct' ? { range: [0, 100] } : { range: (_u, _min, max) => uPlot.rangeNum(0, Math.max(max, spec.unit === 'bps' ? 1000 : 1), 0.1, true) },
    },
    axes: [
      { ...axis, incrs: TIME_STEPS, values: (_u, ticks) => ticks.map(formatClock) },
      { ...axis, size: 64, values: (_u, ticks) => ticks.map((t) => format(spec.unit, t)) },
    ],
    series: [
      { label: 'time', value: (_u, v) => (v === null ? '–' : formatClock(v)) },
      ...spec.series.map((s, i) => ({
        label: s.label,
        stroke: SERIES[i],
        width: 2,
        // A wash under a lone line; overlapping washes would muddy each other.
        fill: spec.series.length === 1 ? `${SERIES[i]}1a` : undefined,
        points: { show: false },
        value: (_u: uPlot, v: number | null) => format(spec.unit, v),
      })),
    ],
  }
}

/** A uPlot line chart that follows its container's width. */
function LineChart({ spec, data }: { spec: ChartSpec; data: Aligned }) {
  const box = useRef<HTMLDivElement>(null)
  const plot = useRef<uPlot | null>(null)
  const latest = useRef(data)
  latest.current = data
  useEffect(() => {
    const el = box.current
    if (!el) return
    const u = new uPlot(options(spec, el.clientWidth || 300), latest.current as uPlot.AlignedData, el)
    plot.current = u
    const resize = new ResizeObserver(() => u.setSize({ width: el.clientWidth, height: HEIGHT }))
    resize.observe(el)
    return () => {
      resize.disconnect()
      u.destroy()
      plot.current = null
    }
  }, [spec])
  useEffect(() => plot.current?.setData(data as uPlot.AlignedData), [data])
  return <div className="chart-plot" ref={box} />
}

/** The newest points as a table, for reading exact values. */
function ChartTable({ spec, data }: { spec: ChartSpec; data: Aligned }) {
  const xs = data[0]
  const rows = xs.map((_, i) => i).slice(-TABLE_ROWS).reverse()
  return (
    <details className="chart-table">
      <summary>Table</summary>
      <table>
        <thead>
          <tr>
            <th>time</th>
            {spec.series.map((s) => (
              <th key={s.metric}>{s.label}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((i) => (
            <tr key={xs[i]}>
              <td>{formatClock(xs[i])}</td>
              {spec.series.map((s, n) => (
                <td key={s.metric}>{format(spec.unit, data[n + 1][i])}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </details>
  )
}

/** The charts `specs` name, on series `load` fetches, refreshed every [`POLL_MS`] while shown. */
function SeriesCharts({ title, specs, loadKey, load, row = false, sinceLabel }: { title: string; specs: ChartSpec[]; loadKey: string; load(): Promise<MetricsOutcome>; row?: boolean; sinceLabel(since: number): string }) {
  const view = useSeries(loadKey, load)
  const data = useMemo(() => specs.map((spec) => alignSeries(view.reply, spec.series.map((s) => s.metric))), [view.reply, specs])
  return (
    <div className="charts">
      <div className="charts-head">
        <h4>{title}</h4>
        {view.reply && <span className="muted">{sinceLabel(view.reply.since)}</span>}
        {view.note && (
          <span className="warn" role="status">
            {view.note}
          </span>
        )}
      </div>
      <div className={row ? 'charts-row' : 'charts-col'}>
        {specs.map((spec, i) => (
          <figure key={spec.title} className="chart">
            <figcaption>{spec.title}</figcaption>
            <LineChart spec={spec} data={data[i]} />
            <ChartTable spec={spec} data={data[i]} />
          </figure>
        ))}
      </div>
    </div>
  )
}

/**
 * CPU, RAM and port throughput of `entity` (`host:<id>` or `fleet`), from the
 * leader's metrics history. While the control plane does not answer, the
 * last series stay on screen with a note saying so.
 */
export function Charts({ entity, title, row = false }: { entity: string; title: string; row?: boolean }) {
  return <SeriesCharts title={title} specs={CHARTS} loadKey={entity} load={() => fetchMetrics(entity, METRIC_NAMES)} row={row} sinceLabel={metricsSince} />
}

/**
 * A pod's or workload's CPU, memory and network, from the server's hour of
 * Kubernetes samples (`/api/kube/metrics`). Until a series exists the note
 * says why (the pod or workload is new, or its node gave no summary).
 */
export function KubeCharts({ entity, title, row = false }: { entity: { pod: string } | { workload: string }; title: string; row?: boolean }) {
  const key = 'pod' in entity ? `pod:${entity.pod}` : `workload:${entity.workload}`
  return <SeriesCharts title={title} specs={KUBE_CHARTS} loadKey={key} load={() => fetchKubeMetrics(entity, KUBE_METRIC_NAMES)} row={row} sinceLabel={metricsSince} />
}

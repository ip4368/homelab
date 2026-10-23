/**
 * Metric series from `GET /api/metrics`, which reads the leader's in-memory
 * history: it answers 409 while the simulation is paused, and a new leader
 * starts empty (`since` says from when it has data). The UI keeps showing
 * the last series it got through both.
 */
import { formatClock } from './clock'

export interface MetricsReply {
  /** Sim ms from which the leader has metrics. */
  since: number
  /** Per metric name: [sim ms, value] points, oldest first. */
  series: Record<string, [number, number][]>
}

export type MetricsOutcome = { ok: MetricsReply } | { paused: true } | { error: string }

/** What a chart shows: the last series fetched, and why it may be stale. */
export interface MetricsView {
  reply: MetricsReply | null
  /** 'paused', an error, or null when `reply` is fresh. */
  note: string | null
}

export async function fetchMetrics(entity: string, metrics: string[]): Promise<MetricsOutcome> {
  const query = new URLSearchParams({ entity, m: metrics.join(',') })
  try {
    const res = await fetch(`/api/metrics?${query}`)
    const json: unknown = await res.json().catch(() => null)
    if (res.ok) return { ok: json as MetricsReply }
    if (res.status === 409) return { paused: true }
    const error = (json as { error?: unknown } | null)?.error
    return { error: typeof error === 'string' ? error : `HTTP ${res.status}` }
  } catch (e) {
    return { error: String(e) }
  }
}

/**
 * Runs `fetchOnce` now and every `everyMs`, handing each outcome to `apply`.
 * A tick is skipped while the previous fetch is still out, so outcomes are
 * applied in order; none is applied after the returned stop function runs.
 */
export function startPolling(fetchOnce: () => Promise<MetricsOutcome>, apply: (o: MetricsOutcome) => void, everyMs: number): () => void {
  let live = true
  let inFlight = false
  const poll = () => {
    if (inFlight) return
    inFlight = true
    fetchOnce().then((outcome) => {
      inFlight = false
      if (live) apply(outcome)
    })
  }
  poll()
  const timer = setInterval(poll, everyMs)
  return () => {
    live = false
    clearInterval(timer)
  }
}

/** Folds a fetch's outcome in: a reply replaces the series, anything else keeps them and says why. */
export function foldOutcome(prev: MetricsView, outcome: MetricsOutcome): MetricsView {
  if ('ok' in outcome) return { reply: outcome.ok, note: null }
  return { reply: prev.reply, note: 'paused' in outcome ? 'paused' : outcome.error }
}

/** uPlot's data shape: timestamps, then each series on them. */
export type Aligned = [number[], ...(number | null)[][]]

/** The union of the metrics' timestamps, then each metric on it (null where it has no point). */
export function alignSeries(reply: MetricsReply | null, metrics: string[]): Aligned {
  const series = metrics.map((m) => new Map(reply?.series[m] ?? []))
  const xs = [...new Set(series.flatMap((s) => [...s.keys()]))].sort((a, b) => a - b)
  return [xs, ...series.map((s) => xs.map((x) => s.get(x) ?? null))]
}

export function metricsSince(since: number): string {
  return `metrics since ${formatClock(since)}`
}

/** Bits per second, compactly: 950 b/s, 12.3 Mb/s, 2.5 Gb/s. */
export function formatBps(bps: number | null): string {
  if (bps === null) return '–'
  const units: [number, string][] = [
    [1e9, 'Gb/s'],
    [1e6, 'Mb/s'],
    [1e3, 'kb/s'],
  ]
  for (const [scale, unit] of units) {
    if (Math.abs(bps) >= scale) return `${parseFloat((bps / scale).toFixed(1))} ${unit}`
  }
  return `${Math.round(bps)} b/s`
}

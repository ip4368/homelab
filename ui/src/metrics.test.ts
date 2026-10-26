import { afterEach, describe, expect, it, vi } from 'vitest'
import { alignSeries, fetchMetrics, formatBps, foldOutcome, metricsSince, startPolling, type MetricsOutcome, type MetricsReply, type MetricsView } from './metrics'

afterEach(() => vi.unstubAllGlobals())

const reply = (since: number, cpu: [number, number][]): MetricsReply => ({ since, series: { cpu_pct: cpu } })

describe('fetchMetrics', () => {
  it('asks for the entity and metrics and resolves with the reply', async () => {
    const body = reply(10_000, [[10_000, 5]])
    const f = vi.fn(async (_url: string) => new Response(JSON.stringify(body), { status: 200 }))
    vi.stubGlobal('fetch', f)
    await expect(fetchMetrics('host:ms01-01', ['cpu_pct', 'ram_pct'])).resolves.toEqual({ ok: body })
    expect(f).toHaveBeenCalledWith('/api/metrics?entity=host%3Ams01-01&m=cpu_pct%2Cram_pct')
  })

  it('reports a paused simulation (409) apart from other failures', async () => {
    vi.stubGlobal('fetch', vi.fn(async () => new Response(JSON.stringify({ error: 'simulation paused' }), { status: 409 })))
    await expect(fetchMetrics('fleet', ['cpu_pct'])).resolves.toEqual({ paused: true })
    vi.stubGlobal('fetch', vi.fn(async () => new Response(JSON.stringify({ error: 'control plane unavailable' }), { status: 503 })))
    await expect(fetchMetrics('fleet', ['cpu_pct'])).resolves.toEqual({ error: 'control plane unavailable' })
    vi.stubGlobal('fetch', vi.fn(async () => Promise.reject(new TypeError('network down'))))
    await expect(fetchMetrics('fleet', ['cpu_pct'])).resolves.toEqual({ error: 'TypeError: network down' })
  })
})

describe('startPolling', () => {
  it('skips ticks while a fetch is out, so a slow answer never lands after a newer one', async () => {
    vi.useFakeTimers()
    const answers: ((o: MetricsOutcome) => void)[] = []
    const fetchOnce = vi.fn(() => new Promise<MetricsOutcome>((resolve) => answers.push(resolve)))
    const applied: MetricsOutcome[] = []
    const stop = startPolling(fetchOnce, (o) => applied.push(o), 5000)
    expect(fetchOnce).toHaveBeenCalledTimes(1)
    await vi.advanceTimersByTimeAsync(15_000)
    expect(fetchOnce, 'the first fetch is still out').toHaveBeenCalledTimes(1)
    answers[0]({ paused: true })
    await vi.advanceTimersByTimeAsync(5000)
    expect(fetchOnce).toHaveBeenCalledTimes(2)
    expect(applied).toEqual([{ paused: true }])
    stop()
    answers[1]({ error: 'late' })
    await vi.advanceTimersByTimeAsync(10_000)
    expect(applied, 'nothing is applied once stopped').toEqual([{ paused: true }])
    expect(fetchOnce).toHaveBeenCalledTimes(2)
    vi.useRealTimers()
  })
})

describe('foldOutcome', () => {
  it('keeps the last series while paused or failing, and says why', () => {
    const first = reply(1000, [[1000, 3]])
    let v: MetricsView = { reply: null, note: null }
    v = foldOutcome(v, { paused: true })
    expect(v).toEqual({ reply: null, note: 'paused' })
    v = foldOutcome(v, { ok: first })
    expect(v).toEqual({ reply: first, note: null })
    v = foldOutcome(v, { paused: true })
    expect(v).toEqual({ reply: first, note: 'paused' })
    v = foldOutcome(v, { error: 'control plane unavailable' })
    expect(v).toEqual({ reply: first, note: 'control plane unavailable' })
  })
})

describe('alignSeries', () => {
  it('puts every metric on the union of timestamps, with gaps as null', () => {
    const r: MetricsReply = {
      since: 0,
      series: { port_rx_bps: [[0, 1], [10_000, 2]], port_tx_bps: [[10_000, 5], [20_000, 6]] },
    }
    expect(alignSeries(r, ['port_rx_bps', 'port_tx_bps'])).toEqual([
      [0, 10_000, 20_000],
      [1, 2, null],
      [null, 5, 6],
    ])
    expect(alignSeries(r, ['cpu_pct']), 'a metric the reply lacks').toEqual([[], []])
    expect(alignSeries(null, ['cpu_pct'])).toEqual([[], []])
  })
})

describe('labels', () => {
  it('says from when the leader has metrics, in local time', () => {
    expect(metricsSince(new Date(2026, 9, 26, 1, 2, 5).getTime())).toBe('metrics since 01:02:05')
  })

  it('formats bit rates compactly', () => {
    expect(formatBps(0)).toBe('0 b/s')
    expect(formatBps(950)).toBe('950 b/s')
    expect(formatBps(12_300_000)).toBe('12.3 Mb/s')
    expect(formatBps(2_500_000_000)).toBe('2.5 Gb/s')
    expect(formatBps(null)).toBe('–')
  })
})

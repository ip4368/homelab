/**
 * A container's log as the viewer holds it: lines cut from the stream's
 * chunks (a line may span two), the newest kept up to a limit, each with
 * the timestamp Kubernetes puts in front (the server always asks for it).
 */

/** The most lines a viewer keeps. */
export const MAX_LINES = 5000

export interface LogLine {
  /** Kubernetes' RFC 3339 timestamp, or null if the line has none. */
  at: string | null
  text: string
}

/** Kubernetes' timestamp at the start of a line: `2026-11-04T10:00:05.123456789Z `. */
const STAMP = /^(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d+)?(?:Z|[+-]\d\d:\d\d)) /

/** ANSI escape sequences (colours, cursor moves), which a plain viewer cannot show. */
const ANSI = /\x1b\[[0-?]*[ -/]*[@-~]|\x1b\][^\x07]*(?:\x07|\x1b\\)/g

export function parseLine(raw: string): LogLine {
  const text = raw.replace(/\r$/, '').replace(ANSI, '')
  const m = STAMP.exec(text)
  return m ? { at: m[1], text: text.slice(m[0].length) } : { at: null, text }
}

/** Cuts a stream into lines: `push` a chunk, get the lines it completes; `flush` at the end for a last unterminated one. */
export class LineSplitter {
  private rest = ''

  push(chunk: string): string[] {
    const parts = (this.rest + chunk).split('\n')
    this.rest = parts.pop() ?? ''
    return parts
  }

  flush(): string[] {
    const last = this.rest
    this.rest = ''
    return last === '' ? [] : [last]
  }
}

/** `lines` with `more` added, keeping the newest `max`. */
export function appendLines(lines: LogLine[], more: LogLine[], max = MAX_LINES): LogLine[] {
  if (more.length === 0) return lines
  const all = lines.concat(more)
  return all.length > max ? all.slice(all.length - max) : all
}

/** The lines containing `filter`, ignoring case; all of them for an empty filter. */
export function filterLines(lines: LogLine[], filter: string): LogLine[] {
  const f = filter.trim().toLowerCase()
  return f === '' ? lines : lines.filter((l) => l.text.toLowerCase().includes(f))
}

/** A timestamp as the viewer shows it: the local time to the millisecond, `10:00:05.123`. */
export function shortStamp(at: string): string {
  const d = new Date(at)
  if (Number.isNaN(d.getTime())) return at
  const pad = (n: number, w = 2) => String(n).padStart(w, '0')
  return `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}.${pad(d.getMilliseconds(), 3)}`
}

/** What to ask `GET /api/kube/logs` for. */
export interface LogTarget {
  namespace: string
  pod: string
  container: string | null
  previous: boolean
}

export function logsUrl(t: LogTarget, opts: { follow: boolean; tail?: number }): string {
  const q = new URLSearchParams({ namespace: t.namespace, pod: t.pod })
  if (t.container) q.set('container', t.container)
  if (t.previous) q.set('previous', 'true')
  if (opts.tail !== undefined) q.set('tail', String(opts.tail))
  q.set('follow', String(opts.follow))
  return `/api/kube/logs?${q}`
}

/**
 * Streams the log at `url`, handing each batch of complete lines to `onLines`.
 * Resolves when the stream ends; rejects with Kubernetes' (or the server's)
 * words if it cannot start. Aborting `signal` stops it.
 */
export async function streamLog(url: string, onLines: (lines: LogLine[]) => void, signal: AbortSignal): Promise<void> {
  const res = await fetch(url, { signal })
  if (!res.ok) {
    const json: unknown = await res.json().catch(() => null)
    const error = (json as { error?: unknown } | null)?.error
    throw new Error(typeof error === 'string' ? error : `HTTP ${res.status}`)
  }
  if (!res.body) return
  const reader = res.body.getReader()
  const decoder = new TextDecoder()
  const split = new LineSplitter()
  for (;;) {
    const { done, value } = await reader.read()
    if (done) break
    const lines = split.push(decoder.decode(value, { stream: true }))
    if (lines.length > 0) onLines(lines.map(parseLine))
  }
  const last = split.push(decoder.decode()).concat(split.flush())
  if (last.length > 0) onLines(last.map(parseLine))
}

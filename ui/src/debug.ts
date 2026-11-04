/**
 * The debug surface's pure parts: the log, terminal and journal requests
 * the UI makes, and what it does with what comes back. The server side is
 * `crates/server/src/kube_log.rs`, `kube_exec.rs` and `lab_debug.rs`;
 * the shell and unit lists must agree with theirs.
 */

/** Shells the container terminal offers (the server refuses any other). */
export const SHELLS = ['/bin/sh', '/bin/bash', '/bin/ash'] as const

/** Units whose journal a lab VM's drawer may read. */
export const JOURNAL_UNITS = ['homelabd', 'k3s', 'k3s-agent'] as const

/** The most log text the panel holds while following. */
export const LOG_CAP = 200_000

export interface LogRequest {
  namespace: string
  pod: string
  container?: string
  tail?: number
  previous?: boolean
  sinceSeconds?: number
  timestamps?: boolean
}

/** The query string both log endpoints take (`LogQuery` on the server). */
export function logParams(q: LogRequest): string {
  const p = new URLSearchParams({ namespace: q.namespace, pod: q.pod })
  if (q.container) p.set('container', q.container)
  if (q.tail !== undefined) p.set('tail', String(q.tail))
  if (q.previous) p.set('previous', 'true')
  if (q.sinceSeconds !== undefined) p.set('since_seconds', String(q.sinceSeconds))
  if (q.timestamps) p.set('timestamps', 'true')
  return p.toString()
}

/** `ws://` URL for `path` on the page's own host, as `ws.ts` builds one. */
export function socketUrl(path: string, loc: Pick<Location, 'protocol' | 'host'> = location): string {
  return `${loc.protocol === 'https:' ? 'wss' : 'ws'}://${loc.host}${path}`
}

export function logFollowUrl(q: LogRequest, loc?: Pick<Location, 'protocol' | 'host'>): string {
  return socketUrl(`/api/kube/logs/ws?${logParams(q)}`, loc)
}

export function execUrl(q: { namespace: string; pod: string; container?: string; shell?: string }, loc?: Pick<Location, 'protocol' | 'host'>): string {
  const p = new URLSearchParams({ namespace: q.namespace, pod: q.pod })
  if (q.container) p.set('container', q.container)
  if (q.shell) p.set('shell', q.shell)
  return socketUrl(`/api/kube/exec/ws?${p}`, loc)
}

export function labTerminalUrl(vm: string, loc?: Pick<Location, 'protocol' | 'host'>): string {
  return socketUrl(`/api/lab/terminal/ws?${new URLSearchParams({ vm })}`, loc)
}

export function journalUrl(vm: string, unit: string, lines?: number): string {
  const p = new URLSearchParams({ vm, unit })
  if (lines !== undefined) p.set('lines', String(lines))
  return `/api/lab/journal?${p}`
}

export function consoleUrl(vm: string): string {
  return `/api/lab/console?${new URLSearchParams({ vm })}`
}

/** A terminal text frame in words: an error to show, or how the session ended; null for anything else. */
export function terminalNotice(text: string): { kind: 'error' | 'status'; message: string } | null {
  try {
    const v: unknown = JSON.parse(text)
    if (typeof v !== 'object' || v === null) return null
    const o = v as { error?: unknown; status?: unknown }
    if (typeof o.error === 'string') return { kind: 'error', message: o.error }
    if (typeof o.status === 'string') return { kind: 'status', message: o.status }
    return null
  } catch {
    return null
  }
}

/** The resize text frame a terminal sends (the server's bridges read this shape). */
export function resizeFrame(width: number, height: number): string {
  return JSON.stringify({ resize: { width, height } })
}

/** Appends a followed chunk, keeping the newest `cap` characters on a line boundary. */
export function appendLog(prev: string, chunk: string, cap = LOG_CAP): string {
  const joined = prev + chunk
  if (joined.length <= cap) return joined
  const cut = joined.slice(joined.length - cap)
  const nl = cut.indexOf('\n')
  return nl === -1 ? cut : cut.slice(nl + 1)
}

/** The lines of `text` containing `filter` (case-insensitive); all of them without one. */
export function filterLog(text: string, filter: string): string {
  const f = filter.trim().toLowerCase()
  if (f === '') return text
  return text.split('\n').filter((l) => l.toLowerCase().includes(f)).join('\n')
}

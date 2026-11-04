import { useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react'
import { dock, logsSession } from '../dock'
import { appendLines, filterLines, logsUrl, shortStamp, streamLog, type LogLine, type LogTarget } from '../logs'
import { useStore } from '../store'

type Status = { state: 'connecting' } | { state: 'live' } | { state: 'ended' } | { state: 'failed'; error: string }

/**
 * A container's log, followed as it grows: the last 500 lines first, then
 * new ones as they come. Scrolled to the end unless you scroll up. A filter,
 * wrapping and timestamps; another container, or the run before the last
 * restart, opens in its own tab.
 */
export function LogViewer({ target }: { target: LogTarget }) {
  const pod = useStore((s) => s.kube.pods.find((p) => p.namespace === target.namespace && p.name === target.pod))
  const [lines, setLines] = useState<LogLine[]>([])
  const [status, setStatus] = useState<Status>({ state: 'connecting' })
  const [attempt, setAttempt] = useState(0)
  const [filter, setFilter] = useState('')
  const [wrap, setWrap] = useState(true)
  const [stamps, setStamps] = useState(false)
  const [atEnd, setAtEnd] = useState(true)
  const box = useRef<HTMLDivElement>(null)

  useEffect(() => {
    const abort = new AbortController()
    setLines([])
    setStatus({ state: 'connecting' })
    streamLog(
      logsUrl(target, { follow: !target.previous }),
      (more) => {
        setStatus({ state: 'live' })
        setLines((prev) => appendLines(prev, more))
      },
      abort.signal,
    ).then(
      () => !abort.signal.aborted && setStatus({ state: 'ended' }),
      (e: unknown) => !abort.signal.aborted && setStatus({ state: 'failed', error: e instanceof Error ? e.message : String(e) }),
    )
    return () => abort.abort()
  }, [target, attempt])

  const shown = useMemo(() => filterLines(lines, filter), [lines, filter])
  useLayoutEffect(() => {
    if (atEnd && box.current) box.current.scrollTop = box.current.scrollHeight
  }, [shown, atEnd])
  const onScroll = () => {
    const el = box.current
    if (el) setAtEnd(el.scrollHeight - el.scrollTop - el.clientHeight < 24)
  }

  const containers = pod?.container_states ?? []
  const restarted = containers.find((c) => c.name === target.container)?.restarts ?? 0
  return (
    <div className="log-viewer">
      <div className="log-bar">
        {containers.length > 1 && (
          <select
            aria-label="container"
            value={target.container ?? ''}
            onChange={(e) => pod && dock.open(logsSession(pod, e.target.value, target.previous))}
          >
            {containers.map((c) => (
              <option key={c.name} value={c.name}>
                {c.name}
                {c.role !== 'App' ? ` (${c.role.toLowerCase()})` : ''}
              </option>
            ))}
          </select>
        )}
        {!target.previous && restarted > 0 && pod && (
          <button onClick={() => dock.open(logsSession(pod, target.container, true))} title="the log of the run that ended at the last restart">
            Previous run
          </button>
        )}
        <input placeholder="filter" value={filter} onChange={(e) => setFilter(e.target.value)} aria-label="filter log" size={18} />
        <label className="inline">
          <input type="checkbox" checked={wrap} onChange={(e) => setWrap(e.target.checked)} /> wrap
        </label>
        <label className="inline">
          <input type="checkbox" checked={stamps} onChange={(e) => setStamps(e.target.checked)} /> timestamps
        </label>
        <a href={logsUrl(target, { follow: false, tail: 0 })} download>
          Download
        </a>
        <span className="spacer" />
        <LogStatus status={status} lines={lines.length} shown={shown.length} onRetry={() => setAttempt((n) => n + 1)} />
      </div>
      <div className={`log-lines${wrap ? ' wrap' : ''}`} ref={box} onScroll={onScroll} role="log" aria-label={`log of ${target.pod}`}>
        {shown.map((l, n) => (
          <div key={n}>
            {stamps && l.at && <span className="muted">{shortStamp(l.at)} </span>}
            {l.text}
          </div>
        ))}
        {status.state === 'live' && lines.length === 0 && <div className="muted">Nothing logged yet.</div>}
      </div>
      {!atEnd && (
        <button className="log-end" onClick={() => setAtEnd(true)}>
          Jump to end ↓
        </button>
      )}
    </div>
  )
}

function LogStatus({ status, lines, shown, onRetry }: { status: Status; lines: number; shown: number; onRetry(): void }) {
  const count = shown === lines ? `${lines} lines` : `${shown} of ${lines} lines`
  if (status.state === 'connecting') return <span className="muted">connecting…</span>
  if (status.state === 'live') return <span className="muted">{count} · following</span>
  return (
    <span>
      <span className={status.state === 'failed' ? 'bad' : 'muted'}>{status.state === 'failed' ? status.error : `${count} · stream ended`}</span>{' '}
      <button onClick={onRetry}>Reconnect</button>
    </span>
  )
}

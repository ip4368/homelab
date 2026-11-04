import { useCallback, useEffect, useRef, useState } from 'react'
import { kubeLogs } from '../api'
import { appendLog, filterLog, logFollowUrl, terminalNotice } from '../debug'

const TAILS = [100, 200, 500, 2000]

/**
 * One pod's logs: pick a container, read a tail (or the previous, crashed
 * instance's), follow it live, and filter or download what is on screen.
 */
export function LogPanel({ namespace, pod, containers, restarts }: { namespace: string; pod: string; containers: string[]; restarts: number }) {
  const [container, setContainer] = useState(containers[0] ?? '')
  const [tail, setTail] = useState(200)
  const [previous, setPrevious] = useState(false)
  const [following, setFollowing] = useState(false)
  const [filter, setFilter] = useState('')
  const [text, setText] = useState('')
  const [error, setError] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)
  const pre = useRef<HTMLPreElement>(null)
  const socket = useRef<WebSocket | null>(null)

  // A different pod starts over: its first container, nothing followed.
  useEffect(() => {
    setContainer(containers[0] ?? '')
    setText('')
    setError(null)
    setFollowing(false)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [namespace, pod])

  const stopFollow = useCallback(() => {
    socket.current?.close()
    socket.current = null
    setFollowing(false)
  }, [])

  useEffect(() => stopFollow, [stopFollow])

  const load = useCallback(async () => {
    stopFollow()
    setBusy(true)
    setError(null)
    try {
      setText(await kubeLogs({ namespace, pod, container: container || undefined, tail, previous, timestamps: true }))
    } catch (e) {
      setError(String(e))
      setText('')
    } finally {
      setBusy(false)
    }
  }, [namespace, pod, container, tail, previous, stopFollow])

  const follow = useCallback(() => {
    stopFollow()
    setError(null)
    setText('')
    const ws = new WebSocket(logFollowUrl({ namespace, pod, container: container || undefined, tail, previous: false, timestamps: true }))
    socket.current = ws
    setFollowing(true)
    ws.onmessage = (e) => {
      if (typeof e.data !== 'string') return
      const n = terminalNotice(e.data)
      if (n?.kind === 'error') {
        setError(n.message)
        setFollowing(false)
        return
      }
      setText((prev) => {
        const next = appendLog(prev, e.data as string)
        requestAnimationFrame(() => {
          const el = pre.current
          if (el) el.scrollTop = el.scrollHeight
        })
        return next
      })
    }
    ws.onclose = () => setFollowing(false)
    ws.onerror = () => setFollowing(false)
  }, [namespace, pod, container, tail, stopFollow])

  const shown = filterLog(text, filter)
  const download = () => {
    const a = document.createElement('a')
    a.href = URL.createObjectURL(new Blob([shown], { type: 'text/plain' }))
    a.download = `${pod}-${container || 'pod'}.log`
    a.click()
    URL.revokeObjectURL(a.href)
  }

  return (
    <div className="log-panel">
      <div className="fleet-bar">
        <label className="inline">
          Container{' '}
          <select value={container} onChange={(e) => setContainer(e.target.value)} aria-label="log container">
            {containers.map((c) => (
              <option key={c}>{c}</option>
            ))}
          </select>
        </label>
        <label className="inline">
          Last{' '}
          <select value={tail} onChange={(e) => setTail(Number(e.target.value))} aria-label="log lines">
            {TAILS.map((t) => (
              <option key={t} value={t}>
                {t} lines
              </option>
            ))}
          </select>
        </label>
        <label className="inline" title={restarts === 0 ? 'this pod has not restarted' : undefined}>
          <input type="checkbox" checked={previous} disabled={restarts === 0 || following} onChange={(e) => setPrevious(e.target.checked)} /> previous
          instance
        </label>
        <button disabled={busy || following} onClick={() => void load()}>
          Load
        </button>
        {following ? (
          <button onClick={stopFollow}>Stop following</button>
        ) : (
          <button disabled={busy} onClick={follow}>
            Follow
          </button>
        )}
        <input placeholder="filter lines" value={filter} onChange={(e) => setFilter(e.target.value)} aria-label="filter log lines" size={14} />
        <button disabled={shown === ''} onClick={download}>
          Download
        </button>
      </div>
      {error && (
        <div className="bad refusal" role="alert">
          {error}
        </div>
      )}
      <pre className="log-view" ref={pre} aria-label={`logs of ${pod}`}>
        {shown || (busy ? 'loading…' : 'No logs loaded yet. Load reads the last lines; Follow streams new ones.')}
      </pre>
    </div>
  )
}

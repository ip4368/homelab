import { useEffect, useRef, useState } from 'react'
import { endingLine, parseEnding, resizeMessage, type Ending } from '../terminal'

type Status = 'loading' | 'connecting' | 'open' | 'ended'

/**
 * A terminal on a WebSocket in the server's terminal protocol: a shell in a
 * container (`url` from `execUrl`). xterm.js is loaded the first time one
 * opens, so the rest of the UI does not carry it. The terminal follows the
 * dock's size and tells the server; once the session ends it stays readable,
 * and can be opened again.
 */
export function Terminal({ url, label }: { url: string; label: string }) {
  const box = useRef<HTMLDivElement>(null)
  const [status, setStatus] = useState<Status>('loading')
  const [ending, setEnding] = useState<string | null>(null)
  const [attempt, setAttempt] = useState(0)

  useEffect(() => {
    const el = box.current
    if (!el) return
    let live = true
    let cleanup = () => {}
    ;(async () => {
      const [{ Terminal: XTerm }, { FitAddon }] = await Promise.all([import('@xterm/xterm'), import('@xterm/addon-fit'), import('@xterm/xterm/css/xterm.css')])
      if (!live) return
      const term = new XTerm({ cursorBlink: true, fontSize: 13, fontFamily: 'ui-monospace, SFMono-Regular, Menlo, monospace', theme: { background: '#0f1419' } })
      const fit = new FitAddon()
      term.loadAddon(fit)
      term.open(el)
      fit.fit()
      term.focus()
      setStatus('connecting')
      setEnding(null)
      const ws = new WebSocket(url)
      ws.binaryType = 'arraybuffer'
      let ended: Ending | null = null
      const encoder = new TextEncoder()
      const sendSize = () => ws.readyState === WebSocket.OPEN && ws.send(resizeMessage(term.cols, term.rows))
      ws.onopen = () => {
        setStatus('open')
        sendSize()
      }
      ws.onmessage = (m) => {
        if (m.data instanceof ArrayBuffer) term.write(new Uint8Array(m.data))
        else ended = parseEnding(String(m.data)) ?? ended
      }
      ws.onclose = () => {
        if (!live) return
        const line = endingLine(ended)
        term.write(`\r\n\x1b[90m[${line}]\x1b[0m\r\n`)
        setEnding(line)
        setStatus('ended')
      }
      const typed = term.onData((d) => ws.readyState === WebSocket.OPEN && ws.send(encoder.encode(d)))
      const resize = new ResizeObserver(() => {
        if (el.clientWidth === 0) return
        fit.fit()
        sendSize()
      })
      resize.observe(el)
      cleanup = () => {
        resize.disconnect()
        typed.dispose()
        ws.close()
        term.dispose()
      }
    })()
    return () => {
      live = false
      cleanup()
    }
  }, [url, attempt])

  return (
    <div className="terminal">
      <div className="log-bar">
        <span className="muted">{label}</span>
        <span className="spacer" />
        {status === 'loading' && <span className="muted">loading…</span>}
        {status === 'connecting' && <span className="muted">connecting…</span>}
        {status === 'open' && <span className="ok">connected</span>}
        {status === 'ended' && (
          <span>
            <span className="muted">{ending}</span> <button onClick={() => setAttempt((n) => n + 1)}>Open again</button>
          </span>
        )}
      </div>
      <div className="terminal-screen" ref={box} />
    </div>
  )
}

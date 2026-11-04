import { useEffect, useRef, useState } from 'react'
import { Terminal as XTerm } from '@xterm/xterm'
import { FitAddon } from '@xterm/addon-fit'
import '@xterm/xterm/css/xterm.css'
import { resizeFrame, terminalNotice } from '../debug'

/**
 * A terminal on the WebSocket at `url`: the server's bridges (container
 * exec, lab SSH) speak binary bytes both ways, with text frames for
 * resizes out and session errors or endings in. Closing unmounts, which
 * closes the socket and so the session.
 */
export function Terminal({ url, title, onClose }: { url: string; title: string; onClose(): void }) {
  const box = useRef<HTMLDivElement>(null)
  const [notice, setNotice] = useState<string | null>(null)

  useEffect(() => {
    const el = box.current
    if (!el) return
    const term = new XTerm({ fontSize: 13, convertEol: false, cursorBlink: true })
    const fit = new FitAddon()
    term.loadAddon(fit)
    term.open(el)
    try {
      fit.fit()
    } catch {
      // A hidden container has no size yet; the first resize event fits it.
    }
    const socket = new WebSocket(url)
    socket.binaryType = 'arraybuffer'
    const sendResize = () => {
      if (socket.readyState === WebSocket.OPEN) socket.send(resizeFrame(term.cols, term.rows))
    }
    socket.onopen = sendResize
    socket.onmessage = (e) => {
      if (typeof e.data === 'string') {
        const n = terminalNotice(e.data)
        if (n) {
          setNotice(n.message)
          term.write(`\r\n[${n.message}]\r\n`)
        } else {
          term.write(e.data)
        }
      } else if (e.data instanceof ArrayBuffer) {
        term.write(new Uint8Array(e.data))
      }
    }
    socket.onclose = () => setNotice((prev) => prev ?? 'session closed')
    socket.onerror = () => setNotice((prev) => prev ?? 'the connection failed')
    const dataSub = term.onData((data) => {
      if (socket.readyState === WebSocket.OPEN) socket.send(new TextEncoder().encode(data))
    })
    const resizeSub = term.onResize(sendResize)
    const observer = new ResizeObserver(() => {
      try {
        fit.fit()
      } catch {
        // Still no size; ignore.
      }
    })
    observer.observe(el)
    term.focus()
    return () => {
      observer.disconnect()
      dataSub.dispose()
      resizeSub.dispose()
      socket.close()
      term.dispose()
    }
  }, [url])

  return (
    <div className="terminal-wrap">
      <div className="terminal-head">
        <strong>{title}</strong>
        {notice && <span className="warn" role="status">{notice}</span>}
        <div className="spacer" />
        <button onClick={onClose} aria-label="close terminal">
          Close
        </button>
      </div>
      <div className="terminal" ref={box} />
    </div>
  )
}

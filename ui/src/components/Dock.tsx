import { useRef, useState } from 'react'
import { dock, useDock } from '../dock'
import { execUrl } from '../terminal'
import { LogViewer } from './LogViewer'
import { Terminal } from './Terminal'

const MIN_HEIGHT = 120

/**
 * The dock at the bottom of the page: a tab per log or terminal session,
 * with a draggable height. Hidden sessions keep running; closing a tab ends
 * its session. Minimised, it is just its tabs.
 */
export function Dock() {
  const { sessions, active } = useDock()
  const [height, setHeight] = useState(320)
  const drag = useRef<{ y: number; h: number } | null>(null)
  if (sessions.length === 0) return null

  const onDown = (e: React.PointerEvent) => {
    drag.current = { y: e.clientY, h: height }
    ;(e.target as HTMLElement).setPointerCapture(e.pointerId)
  }
  const onMove = (e: React.PointerEvent) => {
    if (drag.current) setHeight(Math.max(MIN_HEIGHT, Math.min(window.innerHeight - 120, drag.current.h + drag.current.y - e.clientY)))
  }
  return (
    <section className="dock" aria-label="logs and terminals">
      {active && <div className="dock-resize" onPointerDown={onDown} onPointerMove={onMove} onPointerUp={() => (drag.current = null)} title="drag to resize" />}
      <div className="dock-tabs" role="tablist">
        {sessions.map((s) => (
          <span key={s.id} className={`dock-tab${s.id === active ? ' active' : ''}`}>
            <button role="tab" aria-selected={s.id === active} onClick={() => dock.show(s.id === active ? null : s.id)} title={s.id}>
              {s.kind === 'logs' ? '≡' : '>_'} {s.title}
            </button>
            <button className="link" aria-label={`close ${s.title}`} onClick={() => dock.close(s.id)}>
              ×
            </button>
          </span>
        ))}
        <span className="spacer" />
        {active && (
          <button className="link" onClick={() => dock.show(null)} aria-label="minimise dock">
            ▾
          </button>
        )}
      </div>
      {/* Every session stays mounted, so a hidden log keeps following. */}
      {sessions.map((s) => (
        <div key={s.id} className="dock-body" style={{ height, display: s.id === active ? undefined : 'none' }}>
          {s.kind === 'logs' ? (
            <LogViewer target={s.target} />
          ) : (
            <Terminal url={execUrl(s.target, location)} label={`shell in ${s.target.namespace}/${s.target.pod}${s.target.container ? ` (${s.target.container})` : ''}`} />
          )}
        </div>
      ))}
    </section>
  )
}
